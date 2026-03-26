from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from settings import DATA_DIR

DB_PATH = DATA_DIR / "analysis_history.db"


def _conn():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE IF NOT EXISTS analysis_runs (
            id TEXT PRIMARY KEY,
            run_type TEXT NOT NULL,
            label TEXT NOT NULL,
            upload_id TEXT DEFAULT '',
            profile_id TEXT DEFAULT '',
            summary_json TEXT NOT NULL DEFAULT '{}',
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )"""
    )
    conn.execute('CREATE INDEX IF NOT EXISTS ix_analysis_runs_type_created ON analysis_runs(run_type, created_at DESC)')
    conn.execute('CREATE INDEX IF NOT EXISTS ix_analysis_runs_upload_profile ON analysis_runs(upload_id, profile_id)')
    conn.commit()
    return conn


def save_run(run_type: str, label: str, payload: dict, summary: Optional[dict] = None, upload_id: str = '', profile_id: str = '') -> dict:
    row = {
        'id': str(uuid.uuid4()),
        'run_type': run_type,
        'label': label or run_type,
        'upload_id': upload_id or '',
        'profile_id': profile_id or '',
        'summary_json': json.dumps(summary or {}, ensure_ascii=False),
        'payload_json': json.dumps(payload or {}, ensure_ascii=False),
        'created_at': datetime.utcnow().isoformat(),
    }
    with _conn() as conn:
        conn.execute(
            '''INSERT INTO analysis_runs (id, run_type, label, upload_id, profile_id, summary_json, payload_json, created_at)
               VALUES (:id, :run_type, :label, :upload_id, :profile_id, :summary_json, :payload_json, :created_at)''',
            row,
        )
        conn.commit()
    return get_run(row['id'])


def _row_to_dict(row) -> dict:
    if not row:
        return {}
    d = dict(row)
    for k in ('summary_json', 'payload_json'):
        try:
            d[k] = json.loads(d.get(k) or '{}')
        except Exception:
            d[k] = {}
    return d


def get_run(run_id: str) -> Optional[dict]:
    with _conn() as conn:
        row = conn.execute('SELECT * FROM analysis_runs WHERE id=?', (run_id,)).fetchone()
    return _row_to_dict(row) if row else None


def list_runs(run_type: str = '', limit: int = 25) -> list[dict]:
    q = 'SELECT * FROM analysis_runs'
    args = []
    if run_type:
        q += ' WHERE run_type=?'
        args.append(run_type)
    q += ' ORDER BY created_at DESC LIMIT ?'
    args.append(max(1, min(int(limit or 25), 200)))
    with _conn() as conn:
        rows = conn.execute(q, args).fetchall()
    return [_row_to_dict(r) for r in rows]
