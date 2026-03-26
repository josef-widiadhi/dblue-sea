from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class MSSQLConnector(BaseConnector):
    name = "mssql"
    display_name = "MS SQL Server"
    icon = "🪟"
    default_port = 1433
    category = "relational"
    description = "Microsoft SQL Server / Azure SQL"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host", "Host", placeholder="localhost", default="localhost", width="half"),
            ConnectorField("port", "Port", type="number", default="1433", width="third"),
            ConnectorField("schema", "Schema", placeholder="dbo", default="dbo", width="third"),
            ConnectorField("dbname", "Database", placeholder="master", width="full"),
            ConnectorField("user", "Username", placeholder="sa", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    def _conn(self, host, port, dbname, user, password, **kw):
        import pymssql
        return pymssql.connect(server=host, port=str(port), database=dbname, user=user, password=password, timeout=10)

    def _ensure_table_exists(self, cur, schema: str, table: str) -> None:
        self._validate_identifier(schema, "schema")
        self._validate_identifier(table, "table")
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' AND TABLE_NAME=%s",
            (schema, table),
        )
        if not cur.fetchone():
            raise ValueError(f"Unknown table: {schema}.{table}")

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return "[" + value.replace("]", "]]" ) + "]"

    def test_connection(self, host, port, dbname, user, password, schema="dbo", **kw):
        try:
            conn = self._conn(host, port, dbname, user, password)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s", (schema,))
            n = cur.fetchone()[0]
            conn.close()
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_table_list(self, host, port, dbname, user, password, schema="dbo", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (schema,))
        result = [x[0] for x in cur.fetchall()]
        conn.close()
        return result

    def get_table_detail(self, table, host, port, dbname, user, password, schema="dbo", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor(as_dict=True)
        self._ensure_table_exists(cur, schema, table)
        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        cols = [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"], "nullable": r["IS_NULLABLE"], "default": self._safe(r["COLUMN_DEFAULT"]), "is_pk": False} for r in cur.fetchall()]
        conn.close()
        return {"table": table, "columns": cols, "indexes": [], "row_count": 0}

    def get_sample_rows(self, table, limit=10, host="", port=1433, dbname="", user="", password="", schema="dbo", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor(as_dict=True)
        self._ensure_table_exists(cur, schema, table)
        safe_schema = self._quote_identifier(schema)
        safe_table = self._quote_identifier(table)
        cur.execute(f"SELECT TOP {self._bounded_limit(limit)} * FROM {safe_schema}.{safe_table}")
        rows = self._sanitize_rows(cur.fetchall())
        conn.close()
        return rows

    def extract_schema(self, host, port, dbname, user, password, schema="dbo", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT c.TABLE_NAME,c.COLUMN_NAME,c.DATA_TYPE,c.IS_NULLABLE,c.COLUMN_DEFAULT,
                   CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_pk
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_NAME,ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku ON tc.CONSTRAINT_NAME=ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY' AND tc.TABLE_SCHEMA=%s
            ) pk ON c.TABLE_NAME=pk.TABLE_NAME AND c.COLUMN_NAME=pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA=%s
            ORDER BY c.TABLE_NAME,c.ORDINAL_POSITION
            """,
            (schema, schema),
        )
        raw = cur.fetchall()
        cur.execute(
            """
            SELECT fk_t.name ft,fk_c.name fc,pk_t.name pt,pk_c.name pc
            FROM sys.foreign_key_columns fkc
            JOIN sys.tables fk_t ON fkc.parent_object_id=fk_t.object_id
            JOIN sys.columns fk_c ON fkc.parent_object_id=fk_c.object_id AND fkc.parent_column_id=fk_c.column_id
            JOIN sys.tables pk_t ON fkc.referenced_object_id=pk_t.object_id
            JOIN sys.columns pk_c ON fkc.referenced_object_id=pk_c.object_id AND fkc.referenced_column_id=pk_c.column_id
            """
        )
        fks = [{"from_table": r["ft"], "from_col": r["fc"], "to_table": r["pt"], "to_col": r["pc"]} for r in cur.fetchall()]
        tables = {}
        for r in raw:
            table_name = r["TABLE_NAME"]
            if table_name not in tables:
                tables[table_name] = {"columns": [], "row_count": 0, "samples": []}
            tables[table_name]["columns"].append(
                {
                    "column_name": r["COLUMN_NAME"],
                    "data_type": r["DATA_TYPE"],
                    "is_nullable": r["IS_NULLABLE"],
                    "column_default": self._safe(r["COLUMN_DEFAULT"]),
                    "is_pk": bool(r["is_pk"]),
                }
            )
        safe_schema = self._quote_identifier(schema)
        for table_name in list(tables)[:40]:
            try:
                safe_table = self._quote_identifier(table_name)
                cur.execute(f"SELECT TOP 8 * FROM {safe_schema}.{safe_table}")
                tables[table_name]["samples"] = self._sanitize_rows(cur.fetchall())
            except Exception:
                pass
        conn.close()
        return self.canonical_schema("mssql", schema, tables, fks, [])
