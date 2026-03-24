"""DB Blueprint v2 — FastAPI Application"""
import json, asyncio, hashlib
from pathlib import Path
from typing import Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from connectors.base import ConnectorRegistry
import dedup as dd
import prompts as pr
from profile_manager import ProfileManager

BASE_DIR  = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
OLLAMA    = "http://localhost:11434"
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)
static_dir = BASE_DIR / "static"; static_dir.mkdir(exist_ok=True)

# Auto-load all connector plugins
ConnectorRegistry.load_all()

app = FastAPI(title="DB Blueprint v2", version="2.0.0")
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── Pages ────────────────────────────────────────────────────

@app.get("/",           response_class=HTMLResponse)
async def pg_dashboard(request: Request):
    return TEMPLATES.TemplateResponse("index.html", {"request": request})

@app.get("/blueprint",  response_class=HTMLResponse)
async def pg_blueprint(request: Request):
    return TEMPLATES.TemplateResponse("blueprint.html", {"request": request})

@app.get("/profiles",   response_class=HTMLResponse)
async def pg_profiles(request: Request):
    return TEMPLATES.TemplateResponse("profiles.html", {"request": request})

@app.get("/explorer",   response_class=HTMLResponse)
async def pg_explorer(request: Request):
    return TEMPLATES.TemplateResponse("explorer.html", {"request": request})

@app.get("/diagram",    response_class=HTMLResponse)
async def pg_diagram(request: Request):
    return TEMPLATES.TemplateResponse("diagram.html", {"request": request})


# ── Health + connectors ──────────────────────────────────────

@app.get("/api/health")
async def health():
    ollama_ok, models = False, []
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{OLLAMA}/api/tags")
            if r.status_code == 200:
                ollama_ok = True
                models = [m["name"] for m in r.json().get("models", [])]
    except Exception: pass
    return {"status": "ok", "ollama": {"running": ollama_ok, "models": models},
            "connectors": len(ConnectorRegistry.all())}

@app.get("/api/connectors")
async def list_connectors():
    return {"connectors": ConnectorRegistry.all_meta()}

@app.get("/api/models")
async def list_models():
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{OLLAMA}/api/tags")
            if r.status_code != 200: return {"ok": False, "models": []}
            return {"ok": True, "models": [
                {"name": m["name"], "size_gb": round(m.get("size",0)/1e9, 1),
                 "family": m.get("details",{}).get("family",""),
                 "params": m.get("details",{}).get("parameter_size","")}
                for m in r.json().get("models", [])
            ]}
    except Exception as e: return {"ok": False, "models": [], "error": str(e)}


# ── Profiles CRUD ────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name: str; db_type: str; params: dict
    color: str = "#6c63ff"; group_name: str = ""

class ProfileUpdate(BaseModel):
    name: Optional[str]=None; color: Optional[str]=None
    group_name: Optional[str]=None; params: Optional[dict]=None
    is_favourite: Optional[bool]=None

@app.get("/api/profiles")
async def list_profiles(): return {"profiles": ProfileManager.list_profiles()}

@app.post("/api/profiles")
async def create_profile(body: ProfileCreate):
    return ProfileManager.create_profile(body.name, body.db_type, body.params, body.color, body.group_name)

@app.get("/api/profiles/{pid}")
async def get_profile(pid: str): 
    p = ProfileManager.get_profile(pid)
    if not p: raise HTTPException(404, "Profile not found")
    return p

@app.put("/api/profiles/{pid}")
async def update_profile(pid: str, body: ProfileUpdate):
    return ProfileManager.update_profile(pid, **{k:v for k,v in body.dict().items() if v is not None})

@app.delete("/api/profiles/{pid}")
async def delete_profile(pid: str):
    if not ProfileManager.delete_profile(pid): raise HTTPException(404)
    return {"ok": True}

@app.post("/api/profiles/{pid}/duplicate")
async def dup_profile(pid: str): return ProfileManager.duplicate_profile(pid)

@app.post("/api/profiles/{pid}/favourite")
async def fav_profile(pid: str, body: dict):
    return ProfileManager.update_profile(pid, is_favourite=body.get("is_favourite", True))


# ── Connection test ──────────────────────────────────────────

class ConnReq(BaseModel):
    db_type: str; params: dict

class ProfileConnReq(BaseModel):
    profile_id: str

@app.post("/api/test-conn")
async def test_conn(req: ConnReq):
    connector = ConnectorRegistry.get(req.db_type)()
    result = await asyncio.to_thread(connector.test_connection, **req.params)
    return result

@app.post("/api/test-conn/profile")
async def test_conn_profile(req: ProfileConnReq):
    params = ProfileManager.get_profile_params(req.profile_id)
    if not params: raise HTTPException(404, "Profile not found")
    p = ProfileManager.get_profile(req.profile_id)
    connector = ConnectorRegistry.get(p["db_type"])()
    result = await asyncio.to_thread(connector.test_connection, **params)
    return result


# ── Schema extraction ────────────────────────────────────────

class ExtractReq(BaseModel):
    db_type: str; params: dict; use_cache: bool = True

class ExtractProfileReq(BaseModel):
    profile_id: str; use_cache: bool = True

def _cache_key(db_type: str, params: dict) -> str:
    raw = json.dumps({"t": db_type, "p": {k:v for k,v in params.items() if k!="password"}}, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"

async def _do_extract(db_type: str, params: dict, use_cache: bool) -> dict:
    key   = _cache_key(db_type, params)
    cpath = _cache_path(key)
    if use_cache and cpath.exists():
        schema = json.loads(cpath.read_text())
        schema["_from_cache"] = True
        return schema
    connector = ConnectorRegistry.get(db_type)()
    schema = await asyncio.to_thread(connector.extract_schema, **params)
    cpath.write_text(json.dumps(schema))
    return schema

@app.post("/api/extract")
async def extract(req: ExtractReq):
    try:
        schema = await _do_extract(req.db_type, req.params, req.use_cache)
        return {"ok": True, "schema": schema}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/extract/profile")
async def extract_profile(req: ExtractProfileReq):
    params = ProfileManager.get_profile_params(req.profile_id)
    if not params: raise HTTPException(404, "Profile not found")
    p = ProfileManager.get_profile(req.profile_id)
    ProfileManager.mark_used(req.profile_id)
    try:
        schema = await _do_extract(p["db_type"], params, req.use_cache)
        return {"ok": True, "schema": schema}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Explorer ─────────────────────────────────────────────────

class ExplorerReq(BaseModel):
    profile_id: Optional[str]=None; db_type: Optional[str]=None; params: Optional[dict]=None

@app.post("/api/explorer/tables")
async def explorer_tables(req: ExplorerReq):
    if req.profile_id:
        p = ProfileManager.get_profile_params(req.profile_id)
        pr2= ProfileManager.get_profile(req.profile_id)
        db_type, params = pr2["db_type"], p
    else:
        db_type, params = req.db_type, req.params
    connector = ConnectorRegistry.get(db_type)()
    tables = await asyncio.to_thread(connector.get_table_list, **params)
    return {"tables": tables}

@app.post("/api/explorer/table-detail")
async def explorer_table_detail(body: dict):
    pid = body.get("profile_id")
    table = body.get("table")
    if pid:
        p = ProfileManager.get_profile_params(pid)
        pr2= ProfileManager.get_profile(pid)
        db_type, params = pr2["db_type"], p
    else:
        db_type, params = body.get("db_type"), body.get("params", {})
    connector = ConnectorRegistry.get(db_type)()
    detail = await asyncio.to_thread(connector.get_table_detail, table=table, **params)
    return detail

@app.post("/api/explorer/sample")
async def explorer_sample(body: dict):
    pid = body.get("profile_id"); table = body.get("table"); limit = body.get("limit", 20)
    if pid:
        p = ProfileManager.get_profile_params(pid)
        pr2= ProfileManager.get_profile(pid)
        db_type, params = pr2["db_type"], p
    else:
        db_type, params = body.get("db_type"), body.get("params", {})
    connector = ConnectorRegistry.get(db_type)()
    rows = await asyncio.to_thread(connector.get_sample_rows, table=table, limit=limit, **params)
    return {"rows": rows}


# ── Dedup / similarity ───────────────────────────────────────

@app.post("/api/dedup")
async def run_dedup(body: dict):
    schema = body.get("schema")
    if not schema: raise HTTPException(400, "schema required")
    results = await asyncio.to_thread(dd.run_all, schema)
    return results


# ── Analyze (SSE streaming) ──────────────────────────────────

class AnalyzeReq(BaseModel):
    schema: dict; model: str
    industry: str="other"; subdomain: str=""; region: str=""; hints: list[str]=[]
    use_dedup: bool=True

async def _stream_ollama(prompt: str, model: str) -> AsyncGenerator[str, None]:
    payload = {"model": model, "messages": [{"role":"user","content":prompt}],
               "stream": True, "options": {"temperature": 0.1, "num_predict": 8192}}
    async with httpx.AsyncClient(timeout=httpx.Timeout(180)) as c:
        async with c.stream("POST", f"{OLLAMA}/api/chat", json=payload) as r:
            async for line in r.aiter_lines():
                if not line.strip(): continue
                try:
                    d = json.loads(line)
                    t = d.get("message",{}).get("content","")
                    if t: yield t
                    if d.get("done"): break
                except: continue

@app.post("/api/analyze/stream")
async def analyze_stream(req: AnalyzeReq):
    dedup_results = None
    if req.use_dedup:
        dedup_results = await asyncio.to_thread(dd.run_all, req.schema)
    prompt = pr.build_analysis_prompt(req.schema, req.industry, req.subdomain,
                                       req.region, req.hints, dedup_results)
    async def gen():
        buf = ""
        try:
            async for tok in _stream_ollama(prompt, req.model):
                buf += tok
                yield f"data: {json.dumps({'token':tok})}\n\n"
            yield f"data: {json.dumps({'done':True,'full':buf})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error':str(e)})}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
