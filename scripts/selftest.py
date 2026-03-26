from __future__ import annotations

import io
import tempfile
import zipfile
from pathlib import Path

import curation as cur
import dedup as dd
import source_analyzer as sa
import stack_cartographer as sc
import impact_engine as ie
import analysis_store as astore
from graph import fuzzy_engine as fe


def synthetic_schema():
    return {
        "db_type": "sqlite",
        "schema_name": "banking",
        "tables": {
            "acct": {
                "columns": [
                    {"column_name": "acct_no", "data_type": "text", "is_pk": True},
                    {"column_name": "cust_id", "data_type": "integer", "is_pk": False},
                ],
                "samples": [
                    {"acct_no": "AC001", "cust_id": 1},
                    {"acct_no": "AC002", "cust_id": 2},
                    {"acct_no": "AC003", "cust_id": 3},
                    {"acct_no": "AC004", "cust_id": 1},
                ],
                "row_count": 4,
            },
            "txn": {
                "columns": [
                    {"column_name": "id", "data_type": "integer", "is_pk": True},
                    {"column_name": "src_acct", "data_type": "text", "is_pk": False},
                    {"column_name": "dst_acct", "data_type": "text", "is_pk": False},
                    {"column_name": "amt", "data_type": "numeric", "is_pk": False},
                ],
                "samples": [
                    {"id": 100, "src_acct": "AC001", "dst_acct": "AC002", "amt": 10.5},
                    {"id": 101, "src_acct": "AC002", "dst_acct": "AC003", "amt": 15.0},
                    {"id": 102, "src_acct": "AC003", "dst_acct": "AC001", "amt": 8.0},
                    {"id": 103, "src_acct": "AC004", "dst_acct": "AC001", "amt": 25.0},
                ],
                "row_count": 4,
            },
            "customer": {
                "columns": [
                    {"column_name": "id", "data_type": "integer", "is_pk": True},
                    {"column_name": "name", "data_type": "text", "is_pk": False},
                ],
                "samples": [
                    {"id": 1, "name": "Ada"},
                    {"id": 2, "name": "Bima"},
                    {"id": 3, "name": "Cici"},
                ],
                "row_count": 3,
            },
        },
        "explicit_fks": [],
        "indexes": [],
    }


def source_zip() -> Path:
    tmp = Path(tempfile.mkdtemp()) / 'source.zip'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr(
            'models.py',
            'from sqlalchemy import Column, Integer, ForeignKey\n'
            'from sqlalchemy.orm import relationship\n'
            'class Account(Base):\n'
            '    __tablename__ = "acct"\n'
            '    acct_no = Column(Integer, primary_key=True)\n'
            'class Txn(Base):\n'
            '    __tablename__ = "txn"\n'
            '    id = Column(Integer, primary_key=True)\n'
            '    src_acct = Column(Integer, ForeignKey("acct.acct_no"))\n'
            '    acct = relationship("Account")\n'
        )
        zf.writestr(
            'api.py',
            'from fastapi import FastAPI\n'
            'app = FastAPI()\n'
            'from models import Txn, Account\n'
            '@app.get("/api/txns")\n'
            'async def list_txns():\n'
            '    return {"table": Txn.__tablename__, "acct": Account.__tablename__}\n'
            '@app.get("/api/accounts/{acct_no}")\n'
            'async def get_account(acct_no: str):\n'
            '    return {"acct": Account.__tablename__, "acct_no": acct_no}\n'
        )
        zf.writestr(
            'templates/dashboard.html',
            '<html><body><script>fetch(\"/api/txns\");</script>{% include \"partials/account_card.html\" %}</body></html>'
        )
        zf.writestr(
            'templates/partials/account_card.html',
            '<div hx-get=\"/api/accounts/AC001\">Account</div>'
        )
        zf.writestr(
            'frontend/pages/TxnPage.tsx',
            'import AccountCard from "../components/AccountCard"; export default function TxnPage(){ fetch("/api/txns"); return <AccountCard /> }'
        )
        zf.writestr(
            'frontend/components/AccountCard.tsx',
            'export default function AccountCard(){ return <button onClick={()=>fetch("/api/accounts/AC001")}>Open</button> }'
        )
        zf.writestr(
            'dj_models.py',
            'from django.db import models\n'
            'class Customer(models.Model):\n'
            '    code = models.CharField(max_length=20, db_column="cust_code")\n'
        )
        zf.writestr(
            'entity.ts',
            "@Entity('users')\n"
            'export class User {\n'
            '  @PrimaryGeneratedColumn()\n'
            '  id: number\n'
            '  @ManyToOne(() => Company)\n'
            '  company: Company\n'
            '}\n'
        )
        zf.writestr(
            'schema.prisma',
            'model Account {\n'
            ' id Int @id\n'
            ' customer Customer?\n'
            '}\n'
            'model Customer {\n'
            ' id Int @id\n'
            ' accounts Account[]\n'
            '}\n'
        )
        zf.writestr(
            'Order.java',
            '@Entity\n'
            '@Table(name="orders")\n'
            'public class Order {\n'
            ' @Id\n'
            ' private Long id;\n'
            ' @ManyToOne\n'
            ' @JoinColumn(name="customer_id")\n'
            ' private Customer customer;\n'
            '}\n'
        )
    tmp.write_bytes(buf.getvalue())
    return tmp


def main():
    tmp = Path(tempfile.mkdtemp())
    cur.DB_PATH = tmp / 'curation.db'
    astore.DB_PATH = tmp / 'analysis_history.db'
    d = cur.save_dictionary('industry', 'banking', {'txn': 'transaction', 'acct': 'account'}, industry='banking')
    assert d['dictionary_json']['txn'] == 'transaction'
    r = cur.resolve_dictionary('banking')
    assert r['dictionary']['txn'] == 'transaction'
    rel = cur.approve_relation('p1', {'from_table': 'txn', 'from_col': 'src_acct', 'to_table': 'acct', 'to_col': 'acct_no', 'score': 0.97})
    assert rel['id']
    assert cur.list_approved_relations('p1')

    schema = synthetic_schema()
    ddres = dd.run_all(schema, {'industry': 'banking'})
    assert any(x['token'] == 'txn' for x in ddres['dictionary_found'])
    fuzzy = fe.run_fuzzy_analysis(schema)
    good = [
        r for r in fuzzy['relations']
        if {r['from_table'], r['to_table']} == {'txn', 'acct'} and {r['from_col'], r['to_col']} == {'src_acct', 'acct_no'}
    ]
    assert good, fuzzy['relations'][:10]

    z = source_zip()
    sres = sa.analyze_zip(z)
    assert sres['parser_modes'].get('ast', 0) >= 2, sres['parser_modes']
    assert any(m['framework'] == 'typeorm' for m in sres['models']), sres['summary']
    assert any(m['framework'] == 'prisma' for m in sres['models']), sres['summary']
    assert any(m['framework'] == 'hibernate/jpa' for m in sres['models']), sres['summary']
    cart = sc.analyze_backend_api_db(z)
    assert cart['summary']['api_routes'] >= 1, cart['summary']
    assert any(r['path'] == '/api/txns' for r in cart['routes']), cart['routes']
    assert any('txn' in r['tables'] for r in cart['routes']), cart['routes']
    assert cart['summary']['frontend_pages'] >= 1, cart['summary']
    assert cart['summary']['frontend_api_links'] >= 2, cart['summary']
    assert any('txn' in (p['tables'] or []) for p in cart['page_usage']), cart['page_usage']
    assert cart.get('visual', {}).get('summary', {}).get('node_count', 0) >= 6, cart.get('visual', {})
    assert any(n['kind'] == 'Table' for n in cart.get('visual', {}).get('nodes', [])), cart.get('visual', {})

    impact = ie.score_cartography(cart)
    assert impact['summary']['node_count'] >= 6, impact
    assert impact['hotspots'], impact
    saved = astore.save_run('cartography', 'selftest', {'cartography': cart, 'impact': impact}, cart['summary'], 'u1', 'p1')
    assert saved['id']
    runs = astore.list_runs('cartography', 5)
    assert runs and runs[0]['run_type'] == 'cartography'
    print('selftest: ok', {
        'dedup_fks': len(ddres['inferred_fks']),
        'fuzzy_relations': len(fuzzy['relations']),
        'source_models': sres['model_count'],
        'parser_modes': sres['parser_modes'],
        'cartography_routes': cart['summary']['api_routes'],
        'impact_hotspots': len(impact['hotspots']),
        'frontend_pages': cart['summary']['frontend_pages'],
        'frontend_api_links': cart['summary']['frontend_api_links'],
    })


if __name__ == '__main__':
    main()
