from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from settings import DATA_DIR

DB_PATH = DATA_DIR / "curation.db"


def _norm_nullable(value: Optional[str]) -> str:
    return (value or "").strip()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS approved_relations (
            id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            from_table TEXT NOT NULL,
            from_col TEXT,
            from_col_norm TEXT NOT NULL DEFAULT '',
            to_table TEXT NOT NULL,
            to_col TEXT,
            to_col_norm TEXT NOT NULL DEFAULT '',
            rel_type TEXT DEFAULT 'approved',
            source TEXT DEFAULT 'user_approved',
            confidence TEXT DEFAULT 'approved',
            score REAL DEFAULT 1.0,
            evidence TEXT DEFAULT '',
            signal TEXT DEFAULT 'human_review',
            notes TEXT DEFAULT '',
            raw_payload TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_approved_relations ON approved_relations(profile_id, from_table, from_col_norm, to_table, to_col_norm, rel_type)"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS domain_dictionaries (
            id TEXT PRIMARY KEY,
            scope_type TEXT NOT NULL DEFAULT 'industry',
            scope_key TEXT NOT NULL,
            industry TEXT DEFAULT 'other',
            subdomain TEXT DEFAULT '',
            region TEXT DEFAULT '',
            dictionary_json TEXT NOT NULL DEFAULT '{}',
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_domain_dictionaries ON domain_dictionaries(scope_type, scope_key)"
    )

    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(approved_relations)").fetchall()
    }
    if cols and "from_col_norm" not in cols:
        conn.execute(
            "ALTER TABLE approved_relations ADD COLUMN from_col_norm TEXT DEFAULT ''"
        )
        conn.execute(
            "UPDATE approved_relations SET from_col_norm=COALESCE(from_col,'')"
        )
    cols = {
        r[1] for r in conn.execute("PRAGMA table_info(approved_relations)").fetchall()
    }
    if cols and "to_col_norm" not in cols:
        conn.execute(
            "ALTER TABLE approved_relations ADD COLUMN to_col_norm TEXT DEFAULT ''"
        )
        conn.execute("UPDATE approved_relations SET to_col_norm=COALESCE(to_col,'')")
    conn.commit()


def _conn():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _row_to_dict(row) -> dict:
    d = dict(row)
    for k in ("raw_payload", "dictionary_json"):
        if k in d:
            try:
                d[k] = json.loads(d.get(k) or "{}")
            except Exception:
                d[k] = {}
    d.pop("from_col_norm", None)
    d.pop("to_col_norm", None)
    return d


def list_approved_relations(profile_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM approved_relations WHERE profile_id=? ORDER BY created_at DESC",
            (profile_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def approve_relation(profile_id: str, relation: dict, notes: str = "") -> dict:
    payload = {
        "id": str(uuid.uuid4()),
        "profile_id": profile_id,
        "from_table": relation.get("from_table") or "",
        "from_col": relation.get("from_col"),
        "from_col_norm": _norm_nullable(relation.get("from_col")),
        "to_table": relation.get("to_table") or "",
        "to_col": relation.get("to_col"),
        "to_col_norm": _norm_nullable(relation.get("to_col")),
        "rel_type": relation.get("rel_type") or "approved",
        "source": relation.get("source") or "user_approved",
        "confidence": "approved",
        "score": float(relation.get("score") or 1.0),
        "evidence": relation.get("evidence") or "Approved by reviewer",
        "signal": relation.get("signal") or relation.get("source") or "human_review",
        "notes": notes or relation.get("notes") or "",
        "raw_payload": json.dumps(relation),
        "created_at": datetime.utcnow().isoformat(),
    }
    if not payload["from_table"] or not payload["to_table"]:
        raise ValueError("from_table and to_table are required")
    with _conn() as conn:
        existing = conn.execute(
            """SELECT id FROM approved_relations WHERE profile_id=? AND from_table=? AND from_col_norm=?
               AND to_table=? AND to_col_norm=? AND rel_type=?""",
            (
                profile_id,
                payload["from_table"],
                payload["from_col_norm"],
                payload["to_table"],
                payload["to_col_norm"],
                payload["rel_type"],
            ),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE approved_relations SET from_col=?, to_col=?, confidence='approved', score=?, evidence=?, signal=?, notes=?, raw_payload=?, created_at=? WHERE id=?""",
                (
                    payload["from_col"],
                    payload["to_col"],
                    payload["score"],
                    payload["evidence"],
                    payload["signal"],
                    payload["notes"],
                    payload["raw_payload"],
                    payload["created_at"],
                    existing["id"],
                ),
            )
            payload["id"] = existing["id"]
        else:
            conn.execute(
                """INSERT INTO approved_relations (id, profile_id, from_table, from_col, from_col_norm, to_table, to_col, to_col_norm, rel_type, source, confidence, score, evidence, signal, notes, raw_payload, created_at)
                   VALUES (:id, :profile_id, :from_table, :from_col, :from_col_norm, :to_table, :to_col, :to_col_norm, :rel_type, :source, :confidence, :score, :evidence, :signal, :notes, :raw_payload, :created_at)""",
                payload,
            )
        conn.commit()
    payload["raw_payload"] = relation
    payload.pop("from_col_norm", None)
    payload.pop("to_col_norm", None)
    return payload


def delete_approved_relation(relation_id: str) -> bool:
    with _conn() as conn:
        cur = conn.execute("DELETE FROM approved_relations WHERE id=?", (relation_id,))
        conn.commit()
        return cur.rowcount > 0


def list_dictionaries(scope_type: Optional[str] = None) -> list[dict]:
    query = "SELECT * FROM domain_dictionaries"
    args: list[str] = []
    if scope_type:
        query += " WHERE scope_type=?"
        args.append(scope_type)
    query += " ORDER BY updated_at DESC, scope_key ASC"
    with _conn() as conn:
        rows = conn.execute(query, args).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_dictionary(scope_type: str, scope_key: str) -> Optional[dict]:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM domain_dictionaries WHERE scope_type=? AND scope_key=?",
            (scope_type, scope_key),
        ).fetchone()
    return _row_to_dict(row) if row else None


def save_dictionary(
    scope_type: str,
    scope_key: str,
    dictionary: dict,
    industry: str = "other",
    subdomain: str = "",
    region: str = "",
    notes: str = "",
) -> dict:
    now = datetime.utcnow().isoformat()
    payload = {
        "id": str(uuid.uuid4()),
        "scope_type": scope_type,
        "scope_key": scope_key,
        "industry": industry or "other",
        "subdomain": subdomain or "",
        "region": region or "",
        "dictionary_json": json.dumps(dictionary or {}, sort_keys=True),
        "notes": notes or "",
        "created_at": now,
        "updated_at": now,
    }
    with _conn() as conn:
        existing = conn.execute(
            "SELECT id, created_at FROM domain_dictionaries WHERE scope_type=? AND scope_key=?",
            (scope_type, scope_key),
        ).fetchone()
        if existing:
            payload["id"] = existing["id"]
            payload["created_at"] = existing["created_at"]
            conn.execute(
                "UPDATE domain_dictionaries SET industry=?, subdomain=?, region=?, dictionary_json=?, notes=?, updated_at=? WHERE id=?",
                (
                    payload["industry"],
                    payload["subdomain"],
                    payload["region"],
                    payload["dictionary_json"],
                    payload["notes"],
                    payload["updated_at"],
                    payload["id"],
                ),
            )
        else:
            conn.execute(
                """INSERT INTO domain_dictionaries (id, scope_type, scope_key, industry, subdomain, region, dictionary_json, notes, created_at, updated_at)
                   VALUES (:id, :scope_type, :scope_key, :industry, :subdomain, :region, :dictionary_json, :notes, :created_at, :updated_at)""",
                payload,
            )
        conn.commit()
    payload["dictionary_json"] = dictionary or {}
    return payload


def delete_dictionary(scope_type: str, scope_key: str) -> bool:
    with _conn() as conn:
        cur = conn.execute(
            "DELETE FROM domain_dictionaries WHERE scope_type=? AND scope_key=?",
            (scope_type, scope_key),
        )
        conn.commit()
        return cur.rowcount > 0


def resolve_dictionary(
    industry: str = "other", subdomain: str = "", region: str = ""
) -> dict:
    industry = (industry or "other").strip().lower()
    subdomain = (subdomain or "").strip().lower()
    region = (region or "").strip().lower()
    candidates = [("shared", "global")]
    if industry:
        candidates.append(("industry", industry))
    if industry and (subdomain or region):
        candidates.append(("context", f"{industry}|{subdomain or '-'}|{region or '-'}"))
    merged: dict = {}
    matched = []
    for scope_type, scope_key in candidates:
        item = get_dictionary(scope_type, scope_key)
        if item:
            merged.update(item.get("dictionary_json") or {})
            matched.append(
                {
                    "scope_type": scope_type,
                    "scope_key": scope_key,
                    "term_count": len(item.get("dictionary_json") or {}),
                    "notes": item.get("notes", ""),
                }
            )
    return {
        "dictionary": merged,
        "matched_scopes": matched,
        "resolution_order": candidates,
    }
