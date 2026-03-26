"""DB Blueprint v2 — FastAPI Application"""
import asyncio
import hashlib
import json
from pathlib import Path
from typing import AsyncGenerator, Optional

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

import source_analyzer as sa
import fusion_engine as fu
import stack_cartographer as sc
import impact_engine as ie
import analysis_store as astore

from connectors.base import ConnectorRegistry
import curation as cur   # FIX BUG-1: was missing, caused NameError on all /api/dedup + /api/dictionaries routes
import dedup as dd
from graph import graph_store as gs
from graph.routes import router as graph_router
from profile_manager import ProfileManager
import prompts as pr
from settings import CACHE_DIR, OLLAMA_URL, STATIC_DIR, TEMPLATES_DIR

TEMPLATES = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Auto-load all connector plugins
ConnectorRegistry.load_all()

app = FastAPI(title="DB Blueprint v2", version="2.4.0")
app.include_router(graph_router)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _resolve_connector(db_type: str):
    try:
        return ConnectorRegistry.get(db_type)
    except KeyError as exc:
        raise HTTPException(400, f"Unknown connector type: {db_type}") from exc


def _require_profile(profile_id: str) -> tuple[dict, dict]:
    params = ProfileManager.get_profile_params(profile_id)
    profile = ProfileManager.get_profile(profile_id)
    if not params or not profile:
        raise HTTPException(404, "Profile not found")
    return profile, params


# ── Pages ────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def pg_dashboard(request: Request):
    return TEMPLATES.TemplateResponse("index.html", {"request": request})


@app.get("/blueprint", response_class=HTMLResponse)
async def pg_blueprint(request: Request):
    return TEMPLATES.TemplateResponse("blueprint.html", {"request": request})


@app.get("/profiles", response_class=HTMLResponse)
async def pg_profiles(request: Request):
    return TEMPLATES.TemplateResponse("profiles.html", {"request": request})


@app.get("/explorer", response_class=HTMLResponse)
async def pg_explorer(request: Request):
    return TEMPLATES.TemplateResponse("explorer.html", {"request": request})


@app.get("/graph", response_class=HTMLResponse)
async def pg_graph(request: Request):
    return TEMPLATES.TemplateResponse("graph.html", {"request": request})


@app.get("/diagram", response_class=HTMLResponse)
async def pg_diagram(request: Request):
    return TEMPLATES.TemplateResponse("diagram.html", {"request": request})


@app.get("/dedup", response_class=HTMLResponse)
async def pg_dedup(request: Request):
    return TEMPLATES.TemplateResponse("dedup.html", {"request": request})


@app.get("/source", response_class=HTMLResponse)
async def pg_source(request: Request):
    return TEMPLATES.TemplateResponse("source.html", {"request": request})


# ── Health + connectors ──────────────────────────────────────

@app.get("/api/health")
async def health():
    ollama_ok, models = False, []
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{OLLAMA_URL}/api/tags")
            if r.status_code == 200:
                ollama_ok = True
                models = [m["name"] for m in r.json().get("models", [])]
    except Exception:
        pass
    graph_st = await asyncio.to_thread(gs.status)
    return {
        "status": "ok",
        "ollama": {"running": ollama_ok, "models": models, "url": OLLAMA_URL},
        "graph": graph_st,
        "connectors": len(ConnectorRegistry.all()),
    }


@app.get("/api/connectors")
async def list_connectors():
    return {"connectors": ConnectorRegistry.all_meta()}


@app.get("/api/models")
async def list_models():
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{OLLAMA_URL}/api/tags")
            r.raise_for_status()
            models = []
            for m in r.json().get("models", []):
                size = m.get("size") or 0
                details = m.get("details") or {}
                models.append({
                    "name": m.get("name", "unknown"),
                    "family": details.get("family") or details.get("format") or "LLM",
                    "params": details.get("parameter_size") or details.get("quantization_level") or "",
                    "size_gb": round(size / (1024**3), 2) if size else 0,
                })
            return {"ok": True, "models": models}
    except Exception as exc:
        return {"ok": False, "models": [], "error": str(exc)}


# ── Profiles ─────────────────────────────────────────────────

class ProfileCreate(BaseModel):
    name: str
    db_type: str
    params: dict
    color: str = "#6c63ff"
    group_name: str = ""


class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    group_name: Optional[str] = None
    params: Optional[dict] = None
    is_favourite: Optional[bool] = None


@app.get("/api/profiles")
async def list_profiles():
    return {"profiles": ProfileManager.list_profiles()}


@app.post("/api/profiles")
async def create_profile(body: ProfileCreate):
    _resolve_connector(body.db_type)
    return ProfileManager.create_profile(body.name, body.db_type, body.params, body.color, body.group_name)


@app.get("/api/profiles/{pid}")
async def get_profile(pid: str):
    p = ProfileManager.get_profile(pid)
    if not p:
        raise HTTPException(404, "Profile not found")
    return p


@app.put("/api/profiles/{pid}")
async def update_profile(pid: str, body: ProfileUpdate):
    updated = ProfileManager.update_profile(pid, **body.model_dump(exclude_none=True))
    if not updated:
        raise HTTPException(404, "Profile not found")
    return updated


@app.delete("/api/profiles/{pid}")
async def delete_profile(pid: str):
    if not ProfileManager.delete_profile(pid):
        raise HTTPException(404, "Profile not found")
    return {"ok": True}


@app.post("/api/profiles/{pid}/duplicate")
async def dup_profile(pid: str):
    duplicated = ProfileManager.duplicate_profile(pid)
    if not duplicated:
        raise HTTPException(404, "Profile not found")
    return duplicated


@app.post("/api/profiles/{pid}/favourite")
async def fav_profile(pid: str, body: dict):
    updated = ProfileManager.update_profile(pid, is_favourite=body.get("is_favourite", True))
    if not updated:
        raise HTTPException(404, "Profile not found")
    return updated


# ── Connection test ──────────────────────────────────────────

class ConnReq(BaseModel):
    db_type: str
    params: dict


class ProfileConnReq(BaseModel):
    profile_id: str


@app.post("/api/test-conn")
async def test_conn(req: ConnReq):
    connector = _resolve_connector(req.db_type)()
    return await asyncio.to_thread(connector.test_connection, **req.params)


@app.post("/api/test-conn/profile")
async def test_conn_profile(req: ProfileConnReq):
    profile, params = _require_profile(req.profile_id)
    connector = _resolve_connector(profile["db_type"])()
    return await asyncio.to_thread(connector.test_connection, **params)


# ── Schema extraction ────────────────────────────────────────

class ExtractReq(BaseModel):
    db_type: str
    params: dict
    use_cache: bool = True


class ExtractProfileReq(BaseModel):
    profile_id: str
    use_cache: bool = True
    auto_ingest_graph: bool = False


class ExplorerReq(BaseModel):
    profile_id: Optional[str] = None
    db_type: Optional[str] = None
    params: Optional[dict] = None


def _cache_key(db_type: str, params: dict) -> str:
    raw = json.dumps({"t": db_type, "p": {k: v for k, v in params.items() if k != "password"}}, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


async def _do_extract(db_type: str, params: dict, use_cache: bool) -> dict:
    key = _cache_key(db_type, params)
    cpath = _cache_path(key)
    if use_cache and cpath.exists():
        schema = json.loads(cpath.read_text())
        schema["_from_cache"] = True
        return schema
    connector = _resolve_connector(db_type)()
    schema = await asyncio.to_thread(connector.extract_schema, **params)
    cpath.write_text(json.dumps(schema, default=str))
    return schema


@app.post("/api/extract")
async def extract(req: ExtractReq):
    try:
        schema = await _do_extract(req.db_type, req.params, req.use_cache)
        return {"ok": True, "schema": schema}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/extract/profile")
async def extract_profile(req: ExtractProfileReq):
    profile, params = _require_profile(req.profile_id)
    ProfileManager.mark_used(req.profile_id)
    try:
        schema = await _do_extract(profile["db_type"], params, req.use_cache)
        payload = {"ok": True, "schema": schema}
        if req.auto_ingest_graph and gs.is_available():
            from graph import fuzzy_engine as fe
            fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, schema, 0.40)
            name_results = await asyncio.to_thread(dd.run_all, schema, {"industry": "other", "subdomain": "", "region": "", "hints": []})
            ok = await asyncio.to_thread(gs.ingest_schema, schema, req.profile_id, profile["name"], fuzzy_results, name_results)
            payload["graph_ingest"] = {
                "ok": ok,
                "fuzzy_relations": len(fuzzy_results.get("relations", [])),
                "name_relations": len(name_results.get("inferred_fks", [])),
                "table_duplicates": len(fuzzy_results.get("table_duplicates", [])),
            }
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


# ── Explorer ─────────────────────────────────────────────────

def _resolve_explorer_request(req: ExplorerReq | dict) -> tuple[str, dict]:
    if isinstance(req, dict):
        profile_id = req.get("profile_id")
        db_type = req.get("db_type")
        params = req.get("params", {})
    else:
        profile_id = req.profile_id
        db_type = req.db_type
        params = req.params or {}
    if profile_id:
        profile, params = _require_profile(profile_id)
        return profile["db_type"], params
    if not db_type:
        raise HTTPException(400, "db_type or profile_id is required")
    return db_type, params


@app.post("/api/explorer/tables")
async def explorer_tables(req: ExplorerReq):
    db_type, params = _resolve_explorer_request(req)
    connector = _resolve_connector(db_type)()
    tables = await asyncio.to_thread(connector.get_table_list, **params)
    return {"tables": tables}


@app.post("/api/explorer/table-detail")
async def explorer_table_detail(body: dict):
    table = body.get("table")
    if not table:
        raise HTTPException(400, "table is required")
    db_type, params = _resolve_explorer_request(body)
    connector = _resolve_connector(db_type)()
    detail = await asyncio.to_thread(connector.get_table_detail, table=table, **params)
    return detail


@app.post("/api/explorer/sample")
async def explorer_sample(body: dict):
    table = body.get("table")
    if not table:
        raise HTTPException(400, "table is required")
    limit = max(1, min(int(body.get("limit", 20)), 200))
    db_type, params = _resolve_explorer_request(body)
    connector = _resolve_connector(db_type)()
    rows = await asyncio.to_thread(connector.get_sample_rows, table=table, limit=limit, **params)
    return {"rows": rows}


# ── Dedup / similarity ───────────────────────────────────────

@app.post("/api/dedup")
async def run_dedup(body: dict):
    schema = body.get("schema")
    if not schema:
        raise HTTPException(400, "schema required")
    resolved = cur.resolve_dictionary(body.get("industry", "other"), body.get("subdomain", ""), body.get("region", ""))
    merged_dictionary = {**(resolved.get("dictionary") or {}), **(body.get("dictionary") or {})}
    context = {
        "industry": body.get("industry", "other"),
        "subdomain": body.get("subdomain", ""),
        "region": body.get("region", ""),
        "hints": body.get("hints", []),
        "dictionary": merged_dictionary,
    }
    results = await asyncio.to_thread(dd.run_all, schema, context)
    return {**results, "dictionary_library": resolved, "dictionary_effective": merged_dictionary}


@app.post("/api/dedup/profile")
async def run_dedup_profile(body: dict):
    profile_id = body.get("profile_id")
    if not profile_id:
        raise HTTPException(400, "profile_id required")
    profile, params = _require_profile(profile_id)
    schema = await _do_extract(profile["db_type"], params, body.get("use_cache", True))
    resolved = cur.resolve_dictionary(body.get("industry", "other"), body.get("subdomain", ""), body.get("region", ""))
    merged_dictionary = {**(resolved.get("dictionary") or {}), **(body.get("dictionary") or {})}
    context = {
        "industry": body.get("industry", "other"),
        "subdomain": body.get("subdomain", ""),
        "region": body.get("region", ""),
        "hints": body.get("hints", []),
        "dictionary": merged_dictionary,
    }
    results = await asyncio.to_thread(dd.run_all, schema, context)
    return {"ok": True, "schema": schema, "profile": profile, **results, "dictionary_library": resolved, "dictionary_effective": merged_dictionary}


@app.get("/api/dictionaries")
async def api_list_dictionaries(scope_type: str | None = None):
    return {"items": cur.list_dictionaries(scope_type)}


@app.get("/api/dictionaries/resolve")
async def api_resolve_dictionary(industry: str = 'other', subdomain: str = '', region: str = ''):
    return cur.resolve_dictionary(industry, subdomain, region)


@app.get("/api/dictionaries/{scope_type}/{scope_key}")
async def api_get_dictionary(scope_type: str, scope_key: str):
    item = cur.get_dictionary(scope_type, scope_key)
    if not item:
        raise HTTPException(404, "Dictionary not found")
    return item


@app.post("/api/dictionaries/{scope_type}/{scope_key}")
async def api_save_dictionary(scope_type: str, scope_key: str, body: dict):
    return cur.save_dictionary(
        scope_type=scope_type,
        scope_key=scope_key,
        dictionary=body.get('dictionary') or body.get('dictionary_json') or {},
        industry=body.get('industry', 'other'),
        subdomain=body.get('subdomain', ''),
        region=body.get('region', ''),
        notes=body.get('notes', ''),
    )


@app.delete("/api/dictionaries/{scope_type}/{scope_key}")
async def api_delete_dictionary(scope_type: str, scope_key: str):
    if not cur.delete_dictionary(scope_type, scope_key):
        raise HTTPException(404, "Dictionary not found")
    return {"ok": True}




# ── Source code DB hunter ──────────────────────────────

class SourceAnalyzeReq(BaseModel):
    upload_id: str


class SourceDiffReq(BaseModel):
    upload_id: str
    profile_id: str
    use_cache: bool = True


@app.post("/api/source/upload")
async def source_upload(file: UploadFile = File(...)):
    name = (file.filename or '').lower()
    if not name.endswith('.zip'):
        raise HTTPException(400, 'Please upload a .zip file')
    data = await file.read()
    if not data:
        raise HTTPException(400, 'Uploaded file is empty')
    saved = sa.save_upload(file.filename or 'source.zip', data)
    return {"ok": True, **saved}


@app.post("/api/source/analyze")
async def source_analyze(req: SourceAnalyzeReq):
    try:
        result = await asyncio.to_thread(sa.analyze_zip, sa.get_upload_path(req.upload_id))
        await asyncio.to_thread(astore.save_run, 'source_analyze', f'source:{req.upload_id[:8]}', result, result.get('summary', {}), req.upload_id, '')
        return result
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/source/diff")
async def source_diff(req: SourceDiffReq):
    profile, params = _require_profile(req.profile_id)
    try:
        source_result = await asyncio.to_thread(sa.analyze_zip, sa.get_upload_path(req.upload_id))
        schema = await _do_extract(profile["db_type"], params, req.use_cache)
        diff = await asyncio.to_thread(sa.diff_against_schema, source_result, schema)
        payload = {"ok": True, "profile": profile, "schema": schema, "source": source_result, "diff": diff}
        await asyncio.to_thread(astore.save_run, 'source_diff', f'diff:{profile["name"]}', payload, diff.get('summary', {}), req.upload_id, req.profile_id)
        return payload
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/source/boost")
async def source_boost(body: dict):
    approved = []
    for rel in body.get('relations') or []:
        approved.append(cur.approve_relation(body.get('profile_id',''), {
            'from_table': rel.get('from_table',''),
            'from_col': rel.get('from_col',''),
            'to_table': rel.get('to_table',''),
            'to_col': rel.get('to_col','id'),
            'rel_type': 'source_model',
            'score': 1.0,
            'signal': 'orm_model',
            'source': 'orm_model',
            'evidence': rel.get('evidence','source confirmed relation'),
        }))
    return {"ok": True, "approved": len([a for a in approved if a])}




class HistoryListReq(BaseModel):
    run_type: str = ""
    limit: int = 25


@app.get("/api/history/runs")
async def history_runs(run_type: str = '', limit: int = 25):
    return {"ok": True, "runs": await asyncio.to_thread(astore.list_runs, run_type, limit)}


@app.get("/api/history/run/{run_id}")
async def history_run(run_id: str):
    item = await asyncio.to_thread(astore.get_run, run_id)
    if not item:
        raise HTTPException(404, 'Run not found')
    return {"ok": True, "run": item}


class CartographerImpactReq(BaseModel):
    upload_id: str
    profile_id: Optional[str] = None
    ingest_to_graph: bool = False


@app.post("/api/cartographer/impact-score")
async def cartographer_impact_score(req: CartographerImpactReq):
    path = sa.get_upload_path(req.upload_id)
    cart = await asyncio.to_thread(sc.analyze_backend_api_db, path)
    graph_info = {"ok": False, "ingested": False, "reason": "profile_id not provided"}
    if req.profile_id and req.ingest_to_graph:
        profile, _params = _require_profile(req.profile_id)
        graph_info = await asyncio.to_thread(sc.ingest_stack_map, req.profile_id, profile['name'], cart)
    impact = await asyncio.to_thread(ie.score_cartography, cart)
    payload = {"ok": True, "cartography": cart, "impact": impact, "graph": graph_info}
    await asyncio.to_thread(astore.save_run, 'impact_score', f'impact:{req.upload_id[:8]}', payload, impact.get('summary', {}), req.upload_id, req.profile_id or '')
    return payload


class FusionProfileReq(BaseModel):
    profile_id: str
    upload_id: str = ""
    use_cache: bool = True
    industry: str = "other"
    subdomain: str = ""
    region: str = ""
    hints: list[str] = []
    dictionary: dict = {}




class CartographerReq(BaseModel):
    upload_id: str
    profile_id: Optional[str] = None
    ingest_to_graph: bool = True


@app.post("/api/cartographer/analyze")
async def cartographer_analyze(req: CartographerReq):
    path = sa.get_upload_path(req.upload_id)
    cart = await asyncio.to_thread(sc.analyze_backend_api_db, path)
    graph_info = {"ok": False, "ingested": False, "reason": "profile_id not provided"}
    if req.profile_id and req.ingest_to_graph:
        profile, _params = _require_profile(req.profile_id)
        graph_info = await asyncio.to_thread(sc.ingest_stack_map, req.profile_id, profile['name'], cart)
    payload = {"ok": True, "cartography": cart, "graph": graph_info}
    await asyncio.to_thread(astore.save_run, 'cartography', f'cartography:{req.upload_id[:8]}', payload, cart.get('summary', {}), req.upload_id, req.profile_id or '')
    return payload


@app.post("/api/fusion/profile")
async def fusion_profile(req: FusionProfileReq):
    profile, params = _require_profile(req.profile_id)
    schema = await _do_extract(profile["db_type"], params, req.use_cache)
    resolved = cur.resolve_dictionary(req.industry, req.subdomain, req.region)
    merged_dictionary = {**(resolved.get("dictionary") or {}), **(req.dictionary or {})}
    context = {
        "industry": req.industry,
        "subdomain": req.subdomain,
        "region": req.region,
        "hints": req.hints,
        "dictionary": merged_dictionary,
    }
    dedup_results = await asyncio.to_thread(dd.run_all, schema, context)
    from graph import fuzzy_engine as fe
    fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, schema, 0.40)
    source_result = None
    source_diff = None
    if req.upload_id:
        source_result = await asyncio.to_thread(sa.analyze_zip, sa.get_upload_path(req.upload_id))
        source_diff = await asyncio.to_thread(sa.diff_against_schema, source_result, schema)
    approved = await asyncio.to_thread(cur.list_approved_relations, req.profile_id)
    fused = await asyncio.to_thread(fu.fuse_relations, schema, dedup_results, fuzzy_results, source_diff or {}, approved)
    payload = {
        "ok": True,
        "profile": profile,
        "schema": schema,
        "context": context,
        "dictionary_library": resolved,
        "dictionary_effective": merged_dictionary,
        "dedup": dedup_results.get("summary", {}),
        "fuzzy": fuzzy_results.get("summary", {}),
        "source": source_result.get("summary", {}) if source_result else None,
        "source_diff": source_diff,
        "fusion": fused,
    }
    await asyncio.to_thread(astore.save_run, 'fusion', f'fusion:{profile["name"]}', payload, fused.get('summary', {}), req.upload_id, req.profile_id)
    return payload


@app.post("/api/fusion/promote")
async def fusion_promote(body: dict):
    profile_id = body.get("profile_id", "")
    if not profile_id:
        raise HTTPException(400, "profile_id required")
    approved = []
    for rel in body.get("relations") or []:
        score = float(rel.get("fusion_score") or rel.get("score") or 0)
        if score < float(body.get("min_score", 0.72)):
            continue
        approved.append(cur.approve_relation(profile_id, {
            'from_table': rel.get('from_table',''),
            'from_col': rel.get('from_col',''),
            'to_table': rel.get('to_table',''),
            'to_col': rel.get('to_col','id'),
            'rel_type': 'fused_relation',
            'score': score,
            'signal': ','.join(rel.get('provenance') or []) or 'fusion_engine',
            'source': 'fusion_engine',
            'evidence': rel.get('evidence','fused relation'),
            'notes': body.get('notes',''),
        }, notes=body.get('notes','')))
    return {"ok": True, "approved": len([x for x in approved if x])}


# ── Analyze (SSE streaming) ──────────────────────────────────

class AnalyzeReq(BaseModel):
    schema: dict
    model: str
    industry: str = "other"
    subdomain: str = ""
    region: str = ""
    hints: list[str] = []
    dictionary: dict = {}
    use_dedup: bool = True
    profile_id: str = ""


async def _stream_ollama(prompt: str, model: str) -> AsyncGenerator[str, None]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "options": {"temperature": 0.1, "num_predict": 8192},
    }
    timeout = httpx.Timeout(connect=10, read=180, write=30, pool=30)
    async with httpx.AsyncClient(timeout=timeout) as c:
        async with c.stream("POST", f"{OLLAMA_URL}/api/chat", json=payload) as r:
            if r.status_code != 200:
                body = await r.aread()
                detail = body.decode(errors="ignore")[:500] or r.reason_phrase
                raise RuntimeError(f"Ollama request failed ({r.status_code}): {detail}")
            async for line in r.aiter_lines():
                if not line.strip():
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if d.get("error"):
                    raise RuntimeError(d["error"])
                token = d.get("message", {}).get("content", "")
                if token:
                    yield token
                if d.get("done"):
                    break


@app.post("/api/analyze/stream")
async def analyze_stream(req: AnalyzeReq):
    dedup_results = None
    resolved = cur.resolve_dictionary(req.industry, req.subdomain, req.region)
    merged_dictionary = {**(resolved.get("dictionary") or {}), **(req.dictionary or {})}
    if req.use_dedup:
        dedup_results = await asyncio.to_thread(dd.run_all, req.schema, {"industry": req.industry, "subdomain": req.subdomain, "region": req.region, "hints": req.hints, "dictionary": merged_dictionary})

    graph_context = None
    if gs.is_available() and req.profile_id:
        focus = list(req.schema.get("tables", {}).keys())[:15]
        sg = await asyncio.to_thread(gs.query_subgraph_for_llm, req.profile_id, focus, 2)
        if sg.get("tables"):
            lines = ["Tables and relations from knowledge graph:"]
            for table in sg["tables"]:
                cols = ", ".join(c["name"] for c in table.get("columns", [])[:6])
                lines.append(f"  {table['table_name']} — {cols}")
            for rel in sg.get("relations", [])[:20]:
                lines.append(
                    f"  {rel['from_table']}.{rel.get('from_col', '?')} -> "
                    f"{rel['to_table']}.{rel.get('to_col', '?')} [{rel.get('confidence', '?')}]"
                )
            graph_context = "\n".join(lines)

    prompt = pr.build_analysis_prompt(
        req.schema,
        req.industry,
        req.subdomain,
        req.region,
        req.hints,
        dedup_results,
        graph_context=graph_context,
        dictionary=merged_dictionary,
        dictionary_scopes=resolved.get("matched_scopes") or [],
    )

    async def gen():
        buf = ""
        try:
            async for tok in _stream_ollama(prompt, req.model):
                buf += tok
                yield f"data: {json.dumps({'token': tok})}\n\n"
            yield f"data: {json.dumps({'done': True, 'full': buf})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
