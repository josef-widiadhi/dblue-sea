from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class SQLiteConnector(BaseConnector):
    name = "sqlite"
    display_name = "SQLite"
    icon = "🪨"
    default_port = 0
    category = "relational"
    description = "SQLite — embedded file-based database"

    @classmethod
    def fields(cls):
        return [ConnectorField("dbname", "Database file path", placeholder="/path/to/database.db", width="full")]

    def _conn(self, dbname, **kw):
        import sqlite3
        return sqlite3.connect(dbname)

    def _ensure_table_exists(self, cur, table: str) -> None:
        self._validate_identifier(table, "table")
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            raise ValueError(f"Unknown table: {table}")

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def test_connection(self, dbname, **kw):
        try:
            conn = self._conn(dbname)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            n = cur.fetchone()[0]
            conn.close()
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_table_list(self, dbname, **kw):
        conn = self._conn(dbname)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        result = [x[0] for x in cur.fetchall()]
        conn.close()
        return result

    def get_table_detail(self, table, dbname, **kw):
        conn = self._conn(dbname)
        cur = conn.cursor()
        self._ensure_table_exists(cur, table)
        safe_table = self._quote_identifier(table)
        cur.execute(f"PRAGMA table_info({safe_table})")
        cols = [{"name": r[1], "type": r[2], "nullable": "NO" if r[3] else "YES", "default": str(r[4]) if r[4] else "", "is_pk": bool(r[5])} for r in cur.fetchall()]
        cur.execute(f"PRAGMA index_list({safe_table})")
        idxs = [{"name": r[1], "def": ""} for r in cur.fetchall()]
        try:
            cur.execute(f"SELECT COUNT(*) FROM {safe_table}")
            count = cur.fetchone()[0]
        except Exception:
            count = 0
        conn.close()
        return {"table": table, "columns": cols, "indexes": idxs, "row_count": count}

    def get_sample_rows(self, table, limit=10, dbname="", **kw):
        conn = self._conn(dbname)
        cur = conn.cursor()
        self._ensure_table_exists(cur, table)
        safe_table = self._quote_identifier(table)
        cur.execute(f"SELECT * FROM {safe_table} LIMIT ?", (self._bounded_limit(limit),))
        cols = [d[0] for d in cur.description]
        rows = self._sanitize_rows([dict(zip(cols, r)) for r in cur.fetchall()])
        conn.close()
        return rows

    def extract_schema(self, dbname, schema=None, **kw):
        conn = self._conn(dbname)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [r[0] for r in cur.fetchall()]
        tables = {}
        fks = []
        for table_name in table_names:
            safe_table = self._quote_identifier(table_name)
            cur.execute(f"PRAGMA table_info({safe_table})")
            cols = [{"column_name": r[1], "data_type": r[2], "is_nullable": "NO" if r[3] else "YES", "column_default": str(r[4]) if r[4] else "", "is_pk": bool(r[5])} for r in cur.fetchall()]
            cur.execute(f"PRAGMA foreign_key_list({safe_table})")
            for r in cur.fetchall():
                fks.append({"from_table": table_name, "from_col": r[3], "to_table": r[2], "to_col": r[4]})
            try:
                cur.execute(f"SELECT COUNT(*) FROM {safe_table}")
                count = cur.fetchone()[0]
            except Exception:
                count = 0
            try:
                cur.execute(f"SELECT * FROM {safe_table} LIMIT 8")
                dcols = [d[0] for d in cur.description]
                samples = self._sanitize_rows([dict(zip(dcols, r)) for r in cur.fetchall()])
            except Exception:
                samples = []
            tables[table_name] = {"columns": cols, "row_count": count, "samples": samples}
        conn.close()
        return self.canonical_schema("sqlite", dbname, tables, fks, [])
