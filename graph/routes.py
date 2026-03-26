"""DB Blueprint v2 — Graph & Fuzzy API Routes"""

import asyncio
import hashlib
import json
from typing import Optional

import dedup as dd  # FIX WARN-4: was imported lazily inside route handlers
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from graph import fuzzy_engine as fe
from graph import graph_store as gs
from profile_manager import ProfileManager
from settings import CACHE_DIR
from curation import approve_relation, delete_approved_relation, list_approved_relations

router = APIRouter(prefix="/api/graph", tags=["graph"])


def _load_schema_for_profile(profile_id: str, use_cache: bool = True) -> tuple[dict, dict]:
    params = ProfileManager.get_profile_params(profile_id)
    profile = ProfileManager.get_profile(profile_id)
    if not params or not profile:
        raise HTTPException(404, "Profile not found")

    raw = json.dumps({"t": profile["db_type"], "p": {k: v for k, v in params.items() if k != "password"}}, sort_keys=True)
    cache_key = hashlib.md5(raw.encode()).hexdigest()[:12]
    cache_path = CACHE_DIR / f"{cache_key}.json"

    if use_cache and cache_path.exists():
        schema = json.loads(cache_path.read_text())
    else:
        from connectors.base import ConnectorRegistry
        try:
            connector_cls = ConnectorRegistry.get(profile["db_type"])
        except KeyError as exc:
            raise HTTPException(400, f"Unknown connector type: {profile['db_type']}") from exc
        connector = connector_cls()
        schema = connector.extract_schema(**params)
        cache_path.write_text(json.dumps(schema, default=str))

    return profile, schema


def _schema_relations_payload(schema: dict, fuzzy_results: dict, name_results: dict, approved: list[dict] | None = None) -> dict:
    relations = []
    for fk in schema.get("explicit_fks", []):
        relations.append({
            "from_table": fk["from_table"],
            "from_col": fk.get("from_col"),
            "to_table": fk["to_table"],
            "to_col": fk.get("to_col"),
            "rel_type": "explicit",
            "source": "explicit_fk",
            "confidence": "explicit",
            "score": 1.0,
            "signal": "foreign_key",
            "evidence": f"Explicit FK {fk['from_table']}.{fk.get('from_col', '?')} -> {fk['to_table']}.{fk.get('to_col', '?')}"
        })
    for rel in name_results.get("inferred_fks", []):
        relations.append({
            "from_table": rel["from_table"],
            "from_col": rel.get("from_col"),
            "to_table": rel["to_table"],
            "to_col": rel.get("to_col"),
            "rel_type": "inferred",
            "source": "name_or_value_inference",
            "confidence": rel.get("confidence", "medium"),
            "score": rel.get("score", 0),
            "signal": rel.get("reason", "inferred_fk"),
            "evidence": rel.get("reason", "Inferred relationship")
        })
    for rel in fuzzy_results.get("relations", []):
        relations.append({
            "from_table": rel["from_table"],
            "from_col": rel.get("from_col"),
            "to_table": rel["to_table"],
            "to_col": rel.get("to_col"),
            "rel_type": "fuzzy",
            "source": "fuzzy_fingerprint",
            "confidence": rel.get("confidence", "medium"),
            "score": rel.get("composite_score", 0),
            "signal": rel.get("dominant_signal", "fuzzy"),
            "evidence": rel.get("evidence", "Fuzzy similarity")
        })
    approved = approved or []
    for rel in approved:
        src = rel.get("source", "user_approved")
        rel_type = "approved"
        if src in {"orm_model", "fusion_engine"}:
            rel_type = "source_confirmed" if src == "orm_model" else "fused"
        relations.append({
            "id": rel.get("id"),
            "from_table": rel["from_table"], "from_col": rel.get("from_col"),
            "to_table": rel["to_table"], "to_col": rel.get("to_col"),
            "rel_type": rel_type, "source": src,
            "confidence": "approved", "score": rel.get("score", 1.0),
            "signal": rel.get("signal", "human_review"), "evidence": rel.get("notes") or rel.get("evidence") or "Approved relation",
            "source_confirmed": src in {"orm_model", "fusion_engine"},
        })
    duplicates = []
    for dup in fuzzy_results.get("table_duplicates", []):
        duplicates.append({
            "table_a": dup["table_a"],
            "table_b": dup["table_b"],
            "rel_type": "clone",
            "score": dup.get("score", 0),
            "evidence": dup.get("reason", "Possible duplicate tables")
        })
    summary = {
        "tables": len(schema.get("tables", {})),
        "explicit_relations": len(schema.get("explicit_fks", [])),
        "inferred_relations": len(name_results.get("inferred_fks", [])),
        "fuzzy_relations": len(fuzzy_results.get("relations", [])),
        "table_duplicates": len(fuzzy_results.get("table_duplicates", [])),
        "approved_relations": len(approved),
    }
    return {"relations": relations, "duplicates": duplicates, "summary": summary}


class FuzzyReq(BaseModel):
    schema: dict
    min_score: float = 0.40


class IngestReq(BaseModel):
    schema: dict
    profile_id: str
    profile_name: str = ""
    run_fuzzy: bool = True
    run_name_dedup: bool = True


class NeighborReq(BaseModel):
    profile_id: str
    table_name: str
    hops: int = 2
    min_confidence: str = "medium"


class ImpactReq(BaseModel):
    profile_id: str
    table_name: str
    col_name: Optional[str] = None


class PathReq(BaseModel):
    profile_id: str
    table_a: str
    table_b: str


class NLQueryReq(BaseModel):
    profile_id: str
    question: str


class IngestProfileReq(BaseModel):
    profile_id: str
    run_fuzzy: bool = True
    use_cache: bool = True


class ApproveReq(BaseModel):
    profile_id: str
    relation: dict
    notes: str = ""


@router.get("/status")
async def graph_status():
    return await asyncio.to_thread(gs.status)


@router.post("/fuzzy")
async def run_fuzzy(req: FuzzyReq):
    return await asyncio.to_thread(fe.run_fuzzy_analysis, req.schema, req.min_score)


@router.post("/fuzzy/column")
async def fingerprint_column(body: dict):
    col_name = body.get("col_name", "unknown")
    col_type = body.get("col_type", "")
    values = body.get("values", [])
    fp = fe.fingerprint_column(col_name, col_type, values)
    fp["value_set"] = list(fp["value_set"])[:20]
    return fp


@router.post("/fuzzy/compare")
async def compare_columns(body: dict):
    fp_a = body.get("fp_a")
    fp_b = body.get("fp_b")
    if not fp_a or not fp_b:
        raise HTTPException(400, "fp_a and fp_b required")
    fp_a["value_set"] = set(fp_a.get("value_set", []))
    fp_b["value_set"] = set(fp_b.get("value_set", []))
    return fe.score_column_pair(fp_a, fp_b)


@router.post("/ingest")
async def ingest_schema(req: IngestReq):
    fuzzy_results = None
    name_results = None
    if req.run_fuzzy:
        fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, req.schema, 0.40)
    if req.run_name_dedup:
        name_results = await asyncio.to_thread(dd.run_all, req.schema, None)
    ok = await asyncio.to_thread(gs.ingest_schema, req.schema, req.profile_id, req.profile_name, fuzzy_results, name_results)
    return {
        "ok": ok,
        "fuzzy_relations": len(fuzzy_results["relations"]) if fuzzy_results else 0,
        "name_relations": len(name_results.get("inferred_fks", [])) if name_results else 0,
    }


@router.post("/ingest/profile")
async def ingest_from_profile(req: IngestProfileReq):
    profile, schema = await asyncio.to_thread(_load_schema_for_profile, req.profile_id, req.use_cache)
    ProfileManager.mark_used(req.profile_id)
    fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, schema, 0.40)
    name_results = await asyncio.to_thread(dd.run_all, schema, None)
    approved = await asyncio.to_thread(list_approved_relations, req.profile_id)

    ok = await asyncio.to_thread(gs.ingest_schema, schema, req.profile_id, profile["name"], fuzzy_results, name_results)
    return {
        "ok": ok,
        "tables": len(schema.get("tables", {})),
        "fuzzy_relations": len(fuzzy_results["relations"]),
        "name_relations": len(name_results.get("inferred_fks", [])),
        "table_duplicates": len(fuzzy_results.get("table_duplicates", [])),
        "approved_relations": len(approved),
        "fuzzy_summary": fuzzy_results["summary"],
    }


@router.post("/neighbors")
async def get_neighbors(req: NeighborReq):
    return await asyncio.to_thread(gs.query_neighbors, req.profile_id, req.table_name, req.hops, req.min_confidence)


@router.post("/impact")
async def get_impact(req: ImpactReq):
    return await asyncio.to_thread(gs.query_impact, req.profile_id, req.table_name, req.col_name)


@router.post("/path")
async def get_path(req: PathReq):
    return await asyncio.to_thread(gs.query_path_between, req.profile_id, req.table_a, req.table_b)


@router.post("/fuzzy-relations")
async def get_fuzzy_relations(body: dict):
    pid = body.get("profile_id", "")
    min_score = body.get("min_score", 0.5)
    limit = body.get("limit", 100)
    result = await asyncio.to_thread(gs.query_fuzzy_relations, pid, min_score, limit)
    return {"relations": result}


@router.post("/nl-query")
async def nl_query(req: NLQueryReq):
    context = await asyncio.to_thread(gs.natural_language_to_cypher_context, req.profile_id, req.question)
    return {"context": context, "neo4j_available": gs.is_available()}


@router.post("/subgraph")
async def get_subgraph(body: dict):
    pid = body.get("profile_id", "")
    tables = body.get("tables", [])
    hops = body.get("hops", 2)
    return await asyncio.to_thread(gs.query_subgraph_for_llm, pid, tables, hops)


@router.post("/relations/profile")
async def relations_from_profile(body: dict):
    profile_id = body.get("profile_id", "")
    if not profile_id:
        raise HTTPException(400, "profile_id is required")
    min_score = float(body.get("min_score", 0.40))
    profile, schema = await asyncio.to_thread(_load_schema_for_profile, profile_id, body.get("use_cache", True))
    fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, schema, min_score)
    context = {
        "industry": body.get("industry", "other"),
        "subdomain": body.get("subdomain", ""),
        "region": body.get("region", ""),
        "hints": body.get("hints", []),
    }
    name_results = await asyncio.to_thread(dd.run_all, schema, context)
    approved = await asyncio.to_thread(list_approved_relations, profile_id)
    payload = _schema_relations_payload(schema, fuzzy_results, name_results, approved)
    payload["profile"] = {"id": profile["id"], "name": profile["name"], "db_type": profile["db_type"]}
    payload["context"] = context
    return payload




@router.get("/approvals/{profile_id}")
async def approvals_for_profile(profile_id: str):
    return {"relations": await asyncio.to_thread(list_approved_relations, profile_id)}


@router.post("/approve")
async def approve(req: ApproveReq):
    rec = await asyncio.to_thread(approve_relation, req.profile_id, req.relation, req.notes)
    return {"ok": True, "relation": rec}


@router.delete("/approve/{relation_id}")
async def delete_approval(relation_id: str):
    ok = await asyncio.to_thread(delete_approved_relation, relation_id)
    if not ok:
        raise HTTPException(404, "approved relation not found")
    return {"ok": True}


@router.post("/export/dbml")
async def export_dbml(body: dict):
    profile_id = body.get("profile_id", "")
    if not profile_id:
        raise HTTPException(400, "profile_id is required")
    profile, schema = await asyncio.to_thread(_load_schema_for_profile, profile_id, body.get("use_cache", True))
    payload = await relations_from_profile(body)
    rels = payload["relations"]
    lines = [f"Project {profile['name'].replace(' ', '_')} {{", "  database_type: 'generic'", "}", ""]
    for table_name, table_data in schema.get("tables", {}).items():
        lines.append(f"Table {table_name} {{")
        for col in table_data.get("columns", [])[:80]:
            cname = col.get("column_name", "col")
            dtype = str(col.get("data_type", "text") or "text").replace(' ', '_')
            attrs = []
            if col.get("is_pk"):
                attrs.append('pk')
            if str(col.get("is_nullable", "YES")).upper() in {'NO', 'FALSE', '0'}:
                attrs.append('not null')
            suffix = f" [{' ,'.join(attrs)}]" if attrs else ''
            lines.append(f"  {cname} {dtype}{suffix}")
        lines.append("}")
        lines.append("")
    for rel in rels:
        if rel.get('rel_type') == 'clone':
            continue
        op = '>'
        lines.append(f"Ref: {rel['from_table']}.{rel.get('from_col') or 'id'} {op} {rel['to_table']}.{rel.get('to_col') or 'id'}")
    return {"profile": {"id": profile["id"], "name": profile["name"]}, "dbml": "\n".join(lines) + "\n"}

@router.post("/export/mermaid")
async def export_mermaid(body: dict):
    profile_id = body.get("profile_id", "")
    if not profile_id:
        raise HTTPException(400, "profile_id is required")
    profile, schema = await asyncio.to_thread(_load_schema_for_profile, profile_id, body.get("use_cache", True))
    fuzzy_results = await asyncio.to_thread(fe.run_fuzzy_analysis, schema, float(body.get("min_score", 0.50)))
    name_results = await asyncio.to_thread(dd.run_all, schema, {"industry": body.get("industry", "other"), "subdomain": body.get("subdomain", ""), "region": body.get("region", ""), "hints": body.get("hints", [])})
    rels = _schema_relations_payload(schema, fuzzy_results, name_results)["relations"]
    lines = ["erDiagram"]
    for table_name, table_data in schema.get("tables", {}).items():
        safe_name = table_name.replace(" ", "_").replace("-", "_")
        lines.append(f"    {safe_name} {{")
        for col in table_data.get("columns", [])[:50]:
            dtype = str(col.get("data_type", "string") or "string").replace(' ', '_')
            cname = str(col.get("column_name", "col")).replace(' ', '_').replace('-', '_')
            lines.append(f"        {dtype} {cname}")
        lines.append("    }")
    seen = set()
    for rel in rels:
        a = rel["from_table"].replace(" ", "_").replace("-", "_")
        b = rel["to_table"].replace(" ", "_").replace("-", "_")
        label = f"{rel.get('source', rel.get('rel_type', 'rel'))}:{rel.get('from_col', '?')}->{rel.get('to_col', '?')}"
        key = (a,b,label)
        if key in seen:
            continue
        seen.add(key)
        connector = "}o--||" if rel.get("rel_type") in {"explicit", "inferred", "fuzzy"} else "}o..o{"
        lines.append(f"    {a} {connector} {b} : \"{label}\"")
    mermaid = "\n".join(lines) + "\n"
    return {"profile": {"id": profile["id"], "name": profile["name"]}, "mermaid": mermaid}
