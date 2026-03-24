# DB Blueprint v2

AI-powered database reverse engineering — plugin connector system, encrypted profiles, schema explorer, ER diagram viewer, and similarity/dedup engine. Runs fully locally via FastAPI + Ollama.

## Quick start

```bash
# Linux / macOS
chmod +x run.sh && ./run.sh

# Windows
run.bat
```

Then open **http://localhost:8000**

## Pages

| URL | Description |
|---|---|
| `/` | Dashboard — health, models, recent profiles |
| `/blueprint` | 5-step analysis wizard |
| `/profiles` | Create / edit / test connection profiles |
| `/explorer` | 3-pane schema tree + column inspector + data preview |
| `/diagram` | Interactive ER diagram with filter controls |

## Connector plugin system

Each database is a self-contained plugin in `connectors/`. The registry auto-discovers any `.py` file dropped into that folder — no changes to `main.py` needed.

### Built-in connectors

| Plugin | DB | Driver |
|---|---|---|
| `postgresql.py` | PostgreSQL | psycopg2-binary |
| `mssql.py` | MS SQL Server | pymssql |
| `mysql.py` | MySQL / MariaDB | pymysql |
| `mongodb.py` | MongoDB | pymongo |
| `sqlite.py` | SQLite | built-in |

### Adding a new connector

```python
# connectors/my_db.py
from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry

@ConnectorRegistry.register
class MyDBConnector(BaseConnector):
    name         = "my_db"
    display_name = "My Database"
    icon         = "🔌"
    default_port = 1234
    category     = "relational"

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host", "Host", default="localhost", width="half"),
            ConnectorField("port", "Port", type="number", default="1234", width="third"),
            ConnectorField("dbname", "Database", width="full"),
            ConnectorField("user", "Username", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    def test_connection(self, host, port, dbname, user, password, **kw):
        try:
            # ... connect and count tables
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def extract_schema(self, host, port, dbname, user, password, **kw):
        # ... build tables dict
        return self.canonical_schema("my_db", dbname, tables, fks, indexes)

    def get_table_list(self, **kw): ...
    def get_table_detail(self, table, **kw): ...
    def get_sample_rows(self, table, limit=10, **kw): ...
```

Restart the server — the new connector appears in the UI automatically.

## Credentials security

- Stored in `data/profiles.db` (SQLite, local only)
- Encrypted with **Fernet** (AES-128-CBC + HMAC-SHA256)
- Key derived from `BLUEPRINT_SECRET` env var + machine hostname
- Set a custom secret: `export BLUEPRINT_SECRET=your-secret-here`

## Similarity & dedup engine

`dedup.py` runs three passes before AI analysis:

| Pass | Method | What it finds |
|---|---|---|
| Name similarity | RapidFuzz + Jaccard | Duplicate/alias tables, inferred FK columns |
| Value overlap | Sample intersection | FK relations via actual data matching |
| Structural fingerprint | Column type distribution | Partition tables, clones |

Results are injected into the AI prompt as high-confidence hints, significantly improving relation inference accuracy on schemas without explicit FKs.

## API reference

```
GET  /api/health                  — server + Ollama status
GET  /api/connectors              — list registered connector plugins
GET  /api/models                  — list Ollama models

GET  /api/profiles                — list all profiles
POST /api/profiles                — create profile
GET  /api/profiles/{id}           — get profile (credentials masked)
PUT  /api/profiles/{id}           — update profile
DELETE /api/profiles/{id}         — delete profile
POST /api/profiles/{id}/duplicate — clone profile
POST /api/profiles/{id}/favourite — toggle favourite

POST /api/test-conn               — test {db_type, params}
POST /api/test-conn/profile       — test by profile_id

POST /api/extract                 — extract schema {db_type, params}
POST /api/extract/profile         — extract by profile_id (cached)

POST /api/explorer/tables         — list tables (fast)
POST /api/explorer/table-detail   — columns + indexes for one table
POST /api/explorer/sample         — sample rows for one table

POST /api/dedup                   — run similarity analysis on schema
POST /api/analyze/stream          — SSE streaming AI analysis
```

## Recommended Ollama models

```bash
ollama pull qwen2.5-coder:7b    # best structured JSON output
ollama pull llama3.2:3b         # fast, lightweight
ollama pull mistral:7b          # solid general purpose
ollama pull deepseek-coder:6.7b # great for technical tasks
```

## Project structure

```
dbblueprint_v2/
├── main.py               # FastAPI app + all API routes
├── dedup.py              # Similarity & deduplication engine
├── prompts.py            # LLM prompt builder (domain-aware)
├── profile_manager.py    # Encrypted profile CRUD (SQLAlchemy)
├── requirements.txt
├── run.sh / run.bat
├── connectors/
│   ├── base.py           # BaseConnector + ConnectorRegistry
│   ├── postgresql.py
│   ├── mssql.py
│   ├── mysql.py
│   ├── mongodb.py
│   └── sqlite.py         ← drop new .py here to add a DB
├── templates/
│   ├── index.html        # Dashboard
│   ├── blueprint.html    # Analysis wizard
│   ├── profiles.html     # Profile manager
│   ├── explorer.html     # Schema tree explorer
│   └── diagram.html      # ER diagram viewer
├── data/
│   └── profiles.db       # SQLite — encrypted profiles
└── cache/
    └── *.json            # Cached schema extractions
```

## Phase 2 (roadmap)

- Source code analysis (Python/Django ORM, PHP/Laravel Eloquent, Go/GORM)
- Cross-reference DB schema ↔ ORM models → detect drift
- Oracle, CockroachDB, Redshift, Snowflake, Cassandra, Elasticsearch connectors
- Schema diff — compare two profiles or snapshots
- Export to dbdiagram.io, draw.io, Notion
