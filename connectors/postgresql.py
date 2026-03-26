from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class PostgreSQLConnector(BaseConnector):
    name = "postgresql"
    display_name = "PostgreSQL"
    icon = "🐘"
    default_port = 5432
    category = "relational"
    description = "PostgreSQL — open-source relational DB"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host", "Host", placeholder="localhost", default="localhost", width="half"),
            ConnectorField("port", "Port", type="number", default="5432", width="third"),
            ConnectorField("schema", "Schema", placeholder="public", default="public", width="third"),
            ConnectorField("dbname", "Database", placeholder="mydb", width="full"),
            ConnectorField("user", "Username", placeholder="postgres", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    def _conn(self, host, port, dbname, user, password, **kw):
        import psycopg2
        return psycopg2.connect(host=host, port=int(port), dbname=dbname, user=user, password=password, connect_timeout=10)

    def _ensure_table_exists(self, cur, schema: str, table: str) -> None:
        self._validate_identifier(schema, "schema")
        self._validate_identifier(table, "table")
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_type='BASE TABLE' AND table_name=%s
            """,
            (schema, table),
        )
        if not cur.fetchone():
            raise ValueError(f"Unknown table: {schema}.{table}")

    def test_connection(self, host, port, dbname, user, password, schema="public", **kw):
        try:
            conn = self._conn(host, port, dbname, user, password)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s", (schema,))
            n = cur.fetchone()[0]
            conn.close()
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_table_list(self, host, port, dbname, user, password, schema="public", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE' ORDER BY table_name",
            (schema,),
        )
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return tables

    def get_table_detail(self, table, host, port, dbname, user, password, schema="public", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        self._ensure_table_exists(cur, schema, table)
        cur.execute(
            """
            SELECT c.column_name, c.data_type, c.is_nullable, c.column_default,
                   COALESCE(bool_or(tc.constraint_type='PRIMARY KEY'), false) as is_pk
            FROM information_schema.columns c
            LEFT JOIN information_schema.key_column_usage kcu
                ON c.table_name=kcu.table_name AND c.column_name=kcu.column_name AND c.table_schema=kcu.table_schema
            LEFT JOIN information_schema.table_constraints tc
                ON kcu.constraint_name=tc.constraint_name AND tc.constraint_type='PRIMARY KEY'
            WHERE c.table_schema=%s AND c.table_name=%s
            GROUP BY c.column_name,c.data_type,c.is_nullable,c.column_default,c.ordinal_position
            ORDER BY c.ordinal_position
            """,
            (schema, table),
        )
        cols = [{"name": r[0], "type": r[1], "nullable": r[2], "default": self._safe(r[3]), "is_pk": bool(r[4])} for r in cur.fetchall()]
        cur.execute("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=%s AND tablename=%s", (schema, table))
        idxs = [{"name": r[0], "def": r[1]} for r in cur.fetchall()]
        cur.execute("SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname=%s AND relname=%s", (schema, table))
        row = cur.fetchone()
        count = row[0] if row else 0
        conn.close()
        return {"table": table, "columns": cols, "indexes": idxs, "row_count": count}

    def get_sample_rows(self, table, limit=10, host="", port=5432, dbname="", user="", password="", schema="public", **kw):
        import psycopg2.extras
        from psycopg2 import sql

        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        self._ensure_table_exists(cur, schema, table)
        dcur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(sql.Identifier(schema), sql.Identifier(table))
        dcur.execute(query, (self._bounded_limit(limit),))
        rows = self._sanitize_rows([dict(r) for r in dcur.fetchall()])
        conn.close()
        return rows

    def extract_schema(self, host, port, dbname, user, password, schema="public", **kw):
        import psycopg2.extras
        from psycopg2 import sql

        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.table_name,c.column_name,c.data_type,c.udt_name,c.is_nullable,
                   c.column_default,c.character_maximum_length,c.ordinal_position,
                   COALESCE(bool_or(tc.constraint_type='PRIMARY KEY'),false) as is_pk,
                   COALESCE(bool_or(tc.constraint_type='UNIQUE'),false) as is_unique
            FROM information_schema.columns c
            LEFT JOIN information_schema.key_column_usage kcu
                ON c.table_name=kcu.table_name AND c.column_name=kcu.column_name AND c.table_schema=kcu.table_schema
            LEFT JOIN information_schema.table_constraints tc
                ON kcu.constraint_name=tc.constraint_name AND tc.constraint_type IN('PRIMARY KEY','UNIQUE')
            WHERE c.table_schema=%s
              AND c.table_name IN(SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE')
            GROUP BY c.table_name,c.column_name,c.data_type,c.udt_name,c.is_nullable,
                     c.column_default,c.character_maximum_length,c.ordinal_position
            ORDER BY c.table_name,c.ordinal_position
            """,
            (schema, schema),
        )
        raw = cur.fetchall()
        cur.execute(
            """
            SELECT tc.table_name,kcu.column_name,ccu.table_name,ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name=tc.constraint_name
            WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema=%s
            """,
            (schema,),
        )
        fks = [{"from_table": r[0], "from_col": r[1], "to_table": r[2], "to_col": r[3]} for r in cur.fetchall()]
        cur.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname=%s", (schema,))
        idxs = [{"table": r[0], "name": r[1], "definition": r[2]} for r in cur.fetchall()]
        cur.execute("SELECT relname,n_live_tup FROM pg_stat_user_tables WHERE schemaname=%s", (schema,))
        counts = {r[0]: r[1] for r in cur.fetchall()}
        tables = {}
        for r in raw:
            table_name = r[0]
            if table_name not in tables:
                tables[table_name] = {"columns": [], "row_count": counts.get(table_name, 0), "samples": []}
            tables[table_name]["columns"].append(
                {
                    "column_name": r[1],
                    "data_type": r[2],
                    "udt_name": r[3],
                    "is_nullable": r[4],
                    "column_default": self._safe(r[5]),
                    "max_length": r[6],
                    "is_pk": bool(r[8]),
                    "is_unique": bool(r[9]),
                }
            )
        dc = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        for table_name in list(tables)[:40]:
            try:
                query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(sql.Identifier(schema), sql.Identifier(table_name))
                dc.execute(query, (8,))
                tables[table_name]["samples"] = self._sanitize_rows([dict(r) for r in dc.fetchall()])
            except Exception:
                pass
        conn.close()
        return self.canonical_schema("postgresql", schema, tables, fks, idxs)
