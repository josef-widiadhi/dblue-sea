"""
DB Blueprint v2 — Neo4j Knowledge Graph Store
Optional module. Detected at startup — if Neo4j is not running,
all graph operations fall back gracefully to no-op.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone

from settings import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

log = logging.getLogger("graph_store")

try:
    from neo4j import GraphDatabase, basic_auth
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    log.warning("neo4j package not installed — graph features disabled. pip install neo4j to enable.")


def _node_id(parts: list[str]) -> str:
    raw = ":".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


_driver = None


def _get_driver():
    global _driver
    if _driver is None and NEO4J_AVAILABLE:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


def _ensure_constraints(session):
    statements = [
        "CREATE CONSTRAINT schema_id IF NOT EXISTS FOR (n:Schema) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT table_id IF NOT EXISTS FOR (n:Table) REQUIRE n.id IS UNIQUE",
        "CREATE CONSTRAINT column_id IF NOT EXISTS FOR (n:Column) REQUIRE n.id IS UNIQUE",
    ]
    for stmt in statements:
        session.run(stmt)


def _apoc_available(session) -> bool:
    try:
        session.run("RETURN apoc.version() AS version").single()
        return True
    except Exception:
        return False


def is_available() -> bool:
    if not NEO4J_AVAILABLE:
        return False
    try:
        driver = _get_driver()
        driver.verify_connectivity()
        return True
    except Exception:
        return False


def status() -> dict:
    if not NEO4J_AVAILABLE:
        return {"available": False, "reason": "neo4j package not installed"}
    try:
        driver = _get_driver()
        driver.verify_connectivity()
        with driver.session() as session:
            _ensure_constraints(session)
            counts = session.run(
                """
                MATCH (s:Schema)
                WITH count(s) AS schema_count
                MATCH (t:Table)
                WITH schema_count, count(t) AS table_count
                MATCH (c:Column)
                RETURN schema_count, table_count, count(c) AS column_count
                """
            ).single()
            apoc_ok = _apoc_available(session)
        return {
            "available": True,
            "uri": NEO4J_URI,
            "user": NEO4J_USER,
            "schema_count": counts["schema_count"],
            "table_count": counts["table_count"],
            "column_count": counts["column_count"],
            "apoc_available": apoc_ok,
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc), "uri": NEO4J_URI, "user": NEO4J_USER}


def ingest_schema(schema: dict, profile_id: str, profile_name: str, fuzzy_results: dict | None = None, name_results: dict | None = None) -> bool:
    if not is_available():
        return False

    db_type = schema.get("db_type", "unknown")
    schema_name = schema.get("schema_name", "default")
    schema_id = _node_id([profile_id, schema_name])
    analyzed_at = datetime.now(timezone.utc).isoformat()

    try:
        with _get_driver().session() as session:
            _ensure_constraints(session)
            session.run(
                """
                MERGE (sc:Schema {id: $id})
                SET sc.name = $name,
                    sc.db_type = $db_type,
                    sc.profile_id = $profile_id,
                    sc.profile_name = $profile_name,
                    sc.analyzed_at = $analyzed_at
                """,
                id=schema_id,
                name=schema_name,
                db_type=db_type,
                profile_id=profile_id,
                profile_name=profile_name,
                analyzed_at=analyzed_at,
            )

            for table_name, table_data in schema.get("tables", {}).items():
                table_id = _node_id([schema_id, table_name])
                session.run(
                    """
                    MERGE (t:Table {id: $id})
                    SET t.name = $name,
                        t.schema_id = $schema_id,
                        t.schema_name = $schema_name,
                        t.profile_id = $profile_id,
                        t.profile_name = $profile_name,
                        t.db_type = $db_type,
                        t.row_count = $row_count
                    WITH t
                    MATCH (sc:Schema {id: $schema_id})
                    MERGE (sc)-[:HAS_TABLE]->(t)
                    """,
                    id=table_id,
                    name=table_name,
                    schema_id=schema_id,
                    schema_name=schema_name,
                    profile_id=profile_id,
                    profile_name=profile_name,
                    db_type=db_type,
                    row_count=table_data.get("row_count", 0),
                )

                for col in table_data.get("columns", []):
                    col_name = col["column_name"]
                    col_id = _node_id([table_id, col_name])
                    session.run(
                        """
                        MERGE (c:Column {id: $id})
                        SET c.name = $name,
                            c.table_id = $table_id,
                            c.table_name = $table_name,
                            c.schema_id = $schema_id,
                            c.schema_name = $schema_name,
                            c.profile_id = $profile_id,
                            c.profile_name = $profile_name,
                            c.data_type = $data_type,
                            c.is_pk = $is_pk,
                            c.is_nullable = $is_nullable
                        WITH c
                        MATCH (t:Table {id: $table_id})
                        MERGE (t)-[:HAS_COLUMN]->(c)
                        """,
                        id=col_id,
                        name=col_name,
                        table_id=table_id,
                        table_name=table_name,
                        schema_id=schema_id,
                        schema_name=schema_name,
                        profile_id=profile_id,
                        profile_name=profile_name,
                        data_type=col.get("data_type", ""),
                        is_pk=col.get("is_pk", False),
                        is_nullable=col.get("is_nullable", "YES"),
                    )

            for fk in schema.get("explicit_fks", []):
                ft_id = _node_id([schema_id, fk["from_table"]])
                tt_id = _node_id([schema_id, fk["to_table"]])
                session.run(
                    """
                    MATCH (a:Table {id: $ft}), (b:Table {id: $tt})
                    MERGE (a)-[r:RELATES_TO {from_col: $fc, to_col: $tc, source: 'explicit_fk'}]->(b)
                    SET r.confidence = 'explicit',
                        r.type = 'many-to-one'
                    """,
                    ft=ft_id,
                    tt=tt_id,
                    fc=fk["from_col"],
                    tc=fk["to_col"],
                )

            if name_results:
                for rel in name_results.get("inferred_fks", []):
                    ft_id = _node_id([schema_id, rel["from_table"]])
                    tt_id = _node_id([schema_id, rel["to_table"]])
                    session.run(
                        """
                        MATCH (a:Table {id: $ft}), (b:Table {id: $tt})
                        MERGE (a)-[r:RELATES_TO {from_col: $fc, to_col: $tc, source: 'name_similarity'}]->(b)
                        SET r.confidence = $conf,
                            r.score = $score,
                            r.reason = $reason
                        """,
                        ft=ft_id,
                        tt=tt_id,
                        fc=rel["from_col"],
                        tc=rel["to_col"],
                        conf=rel["confidence"],
                        score=rel.get("score", 0),
                        reason=rel.get("reason", ""),
                    )

            if fuzzy_results:
                for rel in fuzzy_results.get("relations", []):
                    from_table_id = _node_id([schema_id, rel["from_table"]])
                    to_table_id = _node_id([schema_id, rel["to_table"]])
                    from_col_id = _node_id([from_table_id, rel["from_col"]])
                    to_col_id = _node_id([to_table_id, rel["to_col"]])

                    session.run(
                        """
                        MATCH (a:Column {id: $fc}), (b:Column {id: $tc})
                        MERGE (a)-[r:FUZZY_LINKS]->(b)
                        SET r.composite_score = $score,
                            r.confidence = $conf,
                            r.dominant_signal = $dom,
                            r.evidence = $evidence,
                            r.signals_json = $signals_json
                        """,
                        fc=from_col_id,
                        tc=to_col_id,
                        score=rel["composite_score"],
                        conf=rel["confidence"],
                        dom=rel["dominant_signal"],
                        evidence=rel["evidence"],
                        signals_json=json.dumps(rel["signals"]),
                    )

                    if rel["composite_score"] >= 0.55:
                        session.run(
                            """
                            MATCH (a:Table {id: $ft}), (b:Table {id: $tt})
                            MERGE (a)-[r:FUZZY_MATCH {from_col: $fc, to_col: $tc}]->(b)
                            SET r.score = $score,
                                r.confidence = $conf,
                                r.dominant_signal = $dom,
                                r.evidence = $evidence,
                                r.source = 'fuzzy_fingerprint'
                            """,
                            ft=from_table_id,
                            tt=to_table_id,
                            fc=rel["from_col"],
                            tc=rel["to_col"],
                            score=rel["composite_score"],
                            conf=rel["confidence"],
                            dom=rel["dominant_signal"],
                            evidence=rel["evidence"],
                        )

                for dup in fuzzy_results.get("table_duplicates", []):
                    ta_id = _node_id([schema_id, dup["table_a"]])
                    tb_id = _node_id([schema_id, dup["table_b"]])
                    session.run(
                        """
                        MATCH (a:Table {id: $ta}), (b:Table {id: $tb})
                        MERGE (a)-[r:LIKELY_CLONE]->(b)
                        SET r.score = $score,
                            r.reason = $reason
                        """,
                        ta=ta_id,
                        tb=tb_id,
                        score=dup["score"],
                        reason=dup["reason"],
                    )
        return True
    except Exception as exc:
        log.error("Neo4j ingestion failed: %s", exc)
        return False


def _profile_table_filters() -> str:
    return "profile_id: $profile_id"


def query_neighbors(profile_id: str, table_name: str, hops: int = 2, min_confidence: str = "medium") -> dict:
    if not is_available():
        return {"error": "Neo4j not available", "nodes": [], "edges": []}

    conf_order = {"explicit": 4, "high": 3, "medium": 2, "low": 1}
    min_val = conf_order.get(min_confidence, 2)

    with _get_driver().session() as session:
        if _apoc_available(session):
            try:
                record = session.run(
                    """
                    MATCH (start:Table {name: $tname, profile_id: $profile_id})
                    CALL apoc.path.subgraphAll(start, {
                        relationshipFilter: 'RELATES_TO>|FUZZY_MATCH>|<RELATES_TO|<FUZZY_MATCH',
                        maxLevel: $hops
                    })
                    YIELD nodes, relationships
                    RETURN nodes, relationships
                    """,
                    tname=table_name,
                    profile_id=profile_id,
                    hops=hops,
                ).single()
                if record:
                    nodes = [dict(n) for n in record["nodes"] if dict(n).get("profile_id") == profile_id]
                    edges = []
                    for rel in record["relationships"]:
                        start_node = dict(rel.start_node)
                        end_node = dict(rel.end_node)
                        confidence = dict(rel).get("confidence", "medium")
                        if start_node.get("profile_id") != profile_id or end_node.get("profile_id") != profile_id:
                            continue
                        if conf_order.get(confidence, 2) < min_val:
                            continue
                        edges.append({"from": start_node.get("name"), "to": end_node.get("name"), "type": rel.type, **dict(rel)})
                    return {"nodes": nodes, "edges": edges}
            except Exception:
                pass
        return _simple_neighbor_query(profile_id, table_name, hops, min_val)


def _simple_neighbor_query(profile_id: str, table_name: str, hops: int, min_val: int = 2) -> dict:
    if not is_available():
        return {"nodes": [], "edges": []}
    with _get_driver().session() as session:
        result = session.run(
            """
            MATCH (start:Table {name: $tname, profile_id: $profile_id})
            MATCH path = (start)-[:RELATES_TO|FUZZY_MATCH*1..6]-(neighbor:Table {profile_id: $profile_id})
            WHERE length(path) <= $hops
            RETURN DISTINCT neighbor.name AS name, length(path) AS distance
            ORDER BY distance, name
            """,
            tname=table_name,
            profile_id=profile_id,
            hops=hops,
        )
        nodes = [{"name": r["name"], "distance": r["distance"]} for r in result]

        edge_result = session.run(
            """
            MATCH (a:Table {profile_id: $profile_id})-[r:RELATES_TO|FUZZY_MATCH]-(b:Table {profile_id: $profile_id})
            WHERE (a.name = $tname OR b.name = $tname)
            RETURN a.name AS from_name, b.name AS to_name, type(r) AS rel_type, r.confidence AS confidence, r.from_col AS from_col, r.to_col AS to_col, r.score AS score
            """,
            profile_id=profile_id,
            tname=table_name,
        )
        conf_order = {"explicit": 4, "high": 3, "medium": 2, "low": 1}
        edges = [
            {"from": r["from_name"], "to": r["to_name"], "type": r["rel_type"], "confidence": r["confidence"], "from_col": r["from_col"], "to_col": r["to_col"], "score": r["score"]}
            for r in edge_result
            if conf_order.get(r["confidence"] or "medium", 2) >= min_val
        ]
        return {"nodes": nodes, "edges": edges}


def query_impact(profile_id: str, table_name: str, col_name: str | None = None) -> dict:
    if not is_available():
        return {"error": "Neo4j not available", "affected": []}

    with _get_driver().session() as session:
        if col_name:
            result = session.run(
                """
                MATCH (src:Column {name: $cname, profile_id: $profile_id})<-[:HAS_COLUMN]-(t:Table {name: $tname, profile_id: $profile_id})
                OPTIONAL MATCH (src)-[:FUZZY_LINKS*1..3]-(affected:Column {profile_id: $profile_id})
                RETURN DISTINCT 'Column' AS type, affected.name AS name, affected.table_name AS table_name
                ORDER BY table_name, name
                """,
                tname=table_name,
                cname=col_name,
                profile_id=profile_id,
            )
        else:
            result = session.run(
                """
                MATCH (src:Table {name: $tname, profile_id: $profile_id})
                OPTIONAL MATCH (src)-[:RELATES_TO|FUZZY_MATCH*1..3]-(affected:Table {profile_id: $profile_id})
                RETURN DISTINCT 'Table' AS type, affected.name AS name, affected.schema_id AS schema_id
                ORDER BY name
                """,
                tname=table_name,
                profile_id=profile_id,
            )
        affected = [dict(r) for r in result if dict(r).get("name")]
        return {"table": table_name, "column": col_name, "affected": affected}


def query_path_between(profile_id: str, table_a: str, table_b: str) -> dict:
    if not is_available():
        return {"error": "Neo4j not available", "path": []}
    with _get_driver().session() as session:
        rec = session.run(
            """
            MATCH (a:Table {name: $ta, profile_id: $profile_id}), (b:Table {name: $tb, profile_id: $profile_id})
            MATCH path = shortestPath((a)-[:RELATES_TO|FUZZY_MATCH*..6]-(b))
            RETURN [n IN nodes(path) | n.name] AS node_names, length(path) AS hops
            """,
            ta=table_a,
            tb=table_b,
            profile_id=profile_id,
        ).single()
        if not rec:
            return {"path": [], "hops": -1, "message": "No path found"}
        return {"path": rec["node_names"], "hops": rec["hops"]}


def query_fuzzy_relations(profile_id: str, min_score: float = 0.5, limit: int = 100) -> list[dict]:
    if not is_available():
        return []
    with _get_driver().session() as session:
        result = session.run(
            """
            MATCH (a:Column {profile_id: $profile_id})-[r:FUZZY_LINKS]->(b:Column {profile_id: $profile_id})
            WHERE r.composite_score >= $min_score
            MATCH (ta:Table {profile_id: $profile_id})-[:HAS_COLUMN]->(a)
            MATCH (tb:Table {profile_id: $profile_id})-[:HAS_COLUMN]->(b)
            RETURN ta.name AS from_table, a.name AS from_col,
                   tb.name AS to_table, b.name AS to_col,
                   r.composite_score AS score,
                   r.confidence AS confidence,
                   r.dominant_signal AS signal,
                   r.evidence AS evidence
            ORDER BY r.composite_score DESC
            LIMIT $limit
            """,
            profile_id=profile_id,
            min_score=min_score,
            limit=limit,
        )
        return [dict(r) for r in result]


def query_subgraph_for_llm(profile_id: str, focus_tables: list[str], hops: int = 2) -> dict:
    if not is_available():
        return {"tables": [], "relations": []}

    with _get_driver().session() as session:
        table_result = session.run(
            """
            UNWIND $names AS tname
            MATCH (t:Table {name: tname, profile_id: $profile_id})
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {profile_id: $profile_id})
            RETURN t.name AS table_name, t.row_count AS row_count,
                   collect({name: c.name, type: c.data_type, is_pk: c.is_pk}) AS columns
            """,
            names=focus_tables,
            profile_id=profile_id,
        )
        tables = [dict(r) for r in table_result]

        relation_result = session.run(
            """
            UNWIND $names AS tname
            MATCH (a:Table {name: tname, profile_id: $profile_id})-[r:RELATES_TO|FUZZY_MATCH]-(b:Table {profile_id: $profile_id})
            RETURN a.name AS from_table, b.name AS to_table,
                   type(r) AS rel_type,
                   r.confidence AS confidence,
                   r.from_col AS from_col, r.to_col AS to_col,
                   r.score AS score, r.evidence AS evidence
            ORDER BY r.confidence DESC, r.score DESC
            """,
            names=focus_tables,
            profile_id=profile_id,
        )
        relations = [dict(r) for r in relation_result]
    return {"tables": tables, "relations": relations}


def natural_language_to_cypher_context(profile_id: str, nl_question: str) -> str:
    if not is_available():
        return ""

    with _get_driver().session() as session:
        all_tables = session.run("MATCH (t:Table {profile_id: $profile_id}) RETURN t.name AS name ORDER BY t.name", profile_id=profile_id)
        table_names = [r["name"] for r in all_tables]

    question_lower = nl_question.lower()
    mentioned = [t for t in table_names if t.lower() in question_lower or any(part in question_lower for part in t.lower().split("_"))]
    if not mentioned:
        mentioned = table_names[:10]

    subgraph = query_subgraph_for_llm(profile_id, mentioned, hops=2)
    lines = ["=== RELEVANT SCHEMA SUBGRAPH (from Neo4j) ==="]
    for table in subgraph["tables"]:
        cols = ", ".join(f"{c['name']}{'(PK)' if c.get('is_pk') else ''}" for c in table.get("columns", [])[:8])
        lines.append(f"  Table: {table['table_name']} ({table.get('row_count', 0):,} rows) — {cols}")

    if subgraph["relations"]:
        lines.append("  Relations:")
        for rel in subgraph["relations"][:20]:
            conf = rel.get("confidence", "?")
            evidence = rel.get("evidence", "")
            lines.append(
                f"    {rel['from_table']}.{rel.get('from_col', '?')} → {rel['to_table']}.{rel.get('to_col', '?')} [{conf}] {evidence}"
            )
    return "\n".join(lines)


def close():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
