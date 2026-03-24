from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry


@ConnectorRegistry.register
class MongoDBConnector(BaseConnector):
    name="mongodb"; display_name="MongoDB"; icon="🍃"; default_port=27017; category="nosql"
    description="MongoDB — document store"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host","Host",placeholder="localhost",default="localhost",width="half"),
            ConnectorField("port","Port",type="number",default="27017",width="third"),
            ConnectorField("dbname","Database",placeholder="mydb",width="full"),
            ConnectorField("user","Username",placeholder="(optional)",required=False,width="half"),
            ConnectorField("password","Password",type="password",required=False,width="half"),
            ConnectorField("uri","URI Override",placeholder="mongodb://user:pass@host:27017/db (optional)",required=False,width="full"),
        ]

    def _client(self,host,port,user,password,uri=None,**kw):
        from pymongo import MongoClient
        if uri: return MongoClient(uri,serverSelectionTimeoutMS=8000)
        if user and password:
            return MongoClient(host=host,port=int(port),username=user,password=password,authSource="admin",serverSelectionTimeoutMS=8000)
        return MongoClient(host=host,port=int(port),serverSelectionTimeoutMS=8000)

    def test_connection(self,host,port,dbname,user="",password="",uri=None,**kw):
        try:
            cl=self._client(host,port,user,password,uri); db=cl[dbname]
            n=len(db.list_collection_names()); cl.close(); return {"ok":True,"table_count":n}
        except Exception as e: return {"ok":False,"error":str(e)}

    def get_table_list(self,host,port,dbname,user="",password="",uri=None,**kw):
        cl=self._client(host,port,user,password,uri); r=cl[dbname].list_collection_names(); cl.close(); return sorted(r)

    def get_table_detail(self,table,host,port,dbname,user="",password="",uri=None,**kw):
        cl=self._client(host,port,user,password,uri); coll=cl[dbname][table]
        docs=list(coll.aggregate([{"$sample":{"size":20}}]))
        fields={}
        for doc in docs:
            for k,v in doc.items():
                if k not in fields: fields[k]={"name":k,"type":type(v).__name__,"nullable":"YES","default":"","is_pk":k=="_id"}
        count=coll.estimated_document_count(); cl.close()
        return {"table":table,"columns":list(fields.values()),"indexes":[],"row_count":count}

    def get_sample_rows(self,table,limit=10,host="",port=27017,dbname="",user="",password="",uri=None,**kw):
        cl=self._client(host,port,user,password,uri)
        docs=list(cl[dbname][table].aggregate([{"$sample":{"size":limit}}]))
        rows=self._sanitize_rows([{k:v for k,v in d.items() if k!="_id"} for d in docs])
        cl.close(); return rows

    def extract_schema(self,host,port,dbname,user="",password="",uri=None,schema=None,**kw):
        cl=self._client(host,port,user,password,uri); db=cl[dbname]; tables={}
        for cname in db.list_collection_names():
            docs=list(db[cname].aggregate([{"$sample":{"size":15}}]))
            count=db[cname].estimated_document_count()
            fields={}
            for doc in docs:
                for k,v in doc.items():
                    if k not in fields: fields[k]={"column_name":k,"data_type":type(v).__name__,"is_nullable":"YES","column_default":"","is_pk":k=="_id"}
            tables[cname]={"columns":list(fields.values()),"row_count":count,
                "samples":self._sanitize_rows([{k:v for k,v in d.items() if k!="_id"} for d in docs[:5]])}
        cl.close()
        return self.canonical_schema("mongodb",dbname,tables,[],[])
