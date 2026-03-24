from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class SQLiteConnector(BaseConnector):
    name="sqlite"; display_name="SQLite"; icon="🪨"; default_port=0; category="relational"
    description="SQLite — embedded file-based database"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("dbname","Database file path",placeholder="/path/to/database.db",width="full"),
        ]

    def _conn(self,dbname,**kw):
        import sqlite3
        return sqlite3.connect(dbname)

    def test_connection(self,dbname,**kw):
        try:
            conn=self._conn(dbname); cur=conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            n=cur.fetchone()[0]; conn.close(); return {"ok":True,"table_count":n}
        except Exception as e: return {"ok":False,"error":str(e)}

    def get_table_list(self,dbname,**kw):
        conn=self._conn(dbname); cur=conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        r=[x[0] for x in cur.fetchall()]; conn.close(); return r

    def get_table_detail(self,table,dbname,**kw):
        conn=self._conn(dbname); cur=conn.cursor()
        cur.execute(f"PRAGMA table_info(`{table}`)")
        cols=[{"name":r[1],"type":r[2],"nullable":"NO" if r[3] else "YES","default":str(r[4]) if r[4] else "","is_pk":bool(r[5])} for r in cur.fetchall()]
        cur.execute(f"PRAGMA index_list(`{table}`)")
        idxs=[{"name":r[1],"def":""} for r in cur.fetchall()]
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{table}`"); count=cur.fetchone()[0]
        except: count=0
        conn.close(); return {"table":table,"columns":cols,"indexes":idxs,"row_count":count}

    def get_sample_rows(self,table,limit=10,dbname="",**kw):
        conn=self._conn(dbname); cur=conn.cursor()
        cur.execute(f"SELECT * FROM `{table}` LIMIT {int(limit)}")
        cols=[d[0] for d in cur.description]
        rows=self._sanitize_rows([dict(zip(cols,r)) for r in cur.fetchall()])
        conn.close(); return rows

    def extract_schema(self,dbname,schema=None,**kw):
        conn=self._conn(dbname); cur=conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names=[r[0] for r in cur.fetchall()]
        tables={}; fks=[]
        for t in table_names:
            cur.execute(f"PRAGMA table_info(`{t}`)")
            cols=[{"column_name":r[1],"data_type":r[2],"is_nullable":"NO" if r[3] else "YES","column_default":str(r[4]) if r[4] else "","is_pk":bool(r[5])} for r in cur.fetchall()]
            cur.execute(f"PRAGMA foreign_key_list(`{t}`)")
            for r in cur.fetchall(): fks.append({"from_table":t,"from_col":r[3],"to_table":r[2],"to_col":r[4]})
            try: cur.execute(f"SELECT COUNT(*) FROM `{t}`"); count=cur.fetchone()[0]
            except: count=0
            try: cur.execute(f"SELECT * FROM `{t}` LIMIT 8"); dcols=[d[0] for d in cur.description]; samples=self._sanitize_rows([dict(zip(dcols,r)) for r in cur.fetchall()])
            except: samples=[]
            tables[t]={"columns":cols,"row_count":count,"samples":samples}
        conn.close()
        return self.canonical_schema("sqlite",dbname,tables,fks,[])
