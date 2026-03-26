from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class MySQLConnector(BaseConnector):
    name = "mysql"
    display_name = "MySQL / MariaDB"
    icon = "🐬"
    default_port = 3306
    category = "relational"
    description = "MySQL and MariaDB — open-source relational DB"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host", "Host", placeholder="localhost", default="localhost", width="half"),
            ConnectorField("port", "Port", type="number", default="3306", width="third"),
            ConnectorField("dbname", "Database", placeholder="mydb", width="full"),
            ConnectorField("user", "Username", placeholder="root", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    def _conn(self, host, port, dbname, user, password, **kw):
        import pymysql
        return pymysql.connect(host=host, port=int(port), db=dbname, user=user, password=password, connect_timeout=10, cursorclass=pymysql.cursors.DictCursor)

    def _ensure_table_exists(self, cur, dbname: str, table: str) -> None:
        self._validate_identifier(table, "table")
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' AND TABLE_NAME=%s",
            (dbname, table),
        )
        if not cur.fetchone():
            raise ValueError(f"Unknown table: {table}")

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return "`" + value.replace("`", "``") + "`"

    def test_connection(self, host, port, dbname, user, password, **kw):
        try:
            conn = self._conn(host, port, dbname, user, password)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) as n FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (dbname,))
            n = cur.fetchone()["n"]
            conn.close()
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def get_table_list(self, host, port, dbname, user, password, **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (dbname,))
        result = [x["TABLE_NAME"] for x in cur.fetchall()]
        conn.close()
        return result

    def get_table_detail(self, table, host, port, dbname, user, password, **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        self._ensure_table_exists(cur, dbname, table)
        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (dbname, table),
        )
        cols = [{"name": r["COLUMN_NAME"], "type": r["DATA_TYPE"], "nullable": r["IS_NULLABLE"], "default": self._safe(r["COLUMN_DEFAULT"]), "is_pk": r["COLUMN_KEY"] == "PRI"} for r in cur.fetchall()]
        conn.close()
        return {"table": table, "columns": cols, "indexes": [], "row_count": 0}

    def get_sample_rows(self, table, limit=10, host="", port=3306, dbname="", user="", password="", **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        self._ensure_table_exists(cur, dbname, table)
        cur.execute(f"SELECT * FROM {self._quote_identifier(table)} LIMIT %s", (self._bounded_limit(limit),))
        rows = self._sanitize_rows(cur.fetchall())
        conn.close()
        return rows

    def extract_schema(self, host, port, dbname, user, password, **kw):
        conn = self._conn(host, port, dbname, user, password)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,
                   CASE WHEN COLUMN_KEY='PRI' THEN 1 ELSE 0 END as is_pk
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s
            ORDER BY TABLE_NAME,ORDINAL_POSITION
            """,
            (dbname,),
        )
        raw = cur.fetchall()
        cur.execute(
            """
            SELECT TABLE_NAME,COLUMN_NAME,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME IS NOT NULL
            """,
            (dbname,),
        )
        fks = [{"from_table": r["TABLE_NAME"], "from_col": r["COLUMN_NAME"], "to_table": r["REFERENCED_TABLE_NAME"], "to_col": r["REFERENCED_COLUMN_NAME"]} for r in cur.fetchall()]
        cur.execute("SELECT TABLE_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE'", (dbname,))
        counts = {r["TABLE_NAME"]: (r["TABLE_ROWS"] or 0) for r in cur.fetchall()}
        tables = {}
        for r in raw:
            table_name = r["TABLE_NAME"]
            if table_name not in tables:
                tables[table_name] = {"columns": [], "row_count": counts.get(table_name, 0), "samples": []}
            tables[table_name]["columns"].append(
                {
                    "column_name": r["COLUMN_NAME"],
                    "data_type": r["DATA_TYPE"],
                    "is_nullable": r["IS_NULLABLE"],
                    "column_default": self._safe(r["COLUMN_DEFAULT"]),
                    "is_pk": bool(r["is_pk"]),
                }
            )
        for table_name in list(tables)[:40]:
            try:
                cur.execute(f"SELECT * FROM {self._quote_identifier(table_name)} LIMIT %s", (8,))
                tables[table_name]["samples"] = self._sanitize_rows(cur.fetchall())
            except Exception:
                pass
        conn.close()
        return self.canonical_schema("mysql", dbname, tables, fks, [])
