# DB Blueprint v2

> Runtime note: use Python **3.11 or 3.12** for local runs. Python 3.13 may break some pinned dependencies in this build. Docker already uses Python 3.11.

AI-powered database reverse engineering. Connects to your real database,
extracts the full schema, runs name-based and data-fingerprint similarity
analysis, optionally builds a Neo4j knowledge graph, then uses a local
Ollama model to generate an ER diagram, table documentation, and a
relation map — all without any data leaving your machine.

---

## Quick start

### Local Python

```bash
cp .env.example .env          # set BLUEPRINT_SECRET at minimum
chmod +x run.sh && ./run.sh
```

Open **http://localhost:8000**

### Docker + Neo4j (recommended)

```bash
cp .env.example .env          # edit BLUEPRINT_SECRET and passwords
docker compose up --build
```

| Service | URL |
|---|---|
| App | http://localhost:8000 |
| Neo4j Browser | http://localhost:7474 |
| Neo4j Bolt | bolt://localhost:7687 |

Ollama must be running on the host machine. On Linux the compose file
already sets `host.docker.internal → host-gateway`. On macOS/Windows
Docker Desktop handles it automatically.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `BLUEPRINT_SECRET` | `db-blueprint-v2-local-key` | **Set this.** Used as the Fernet encryption key for saved credentials. Any change invalidates existing profiles. |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama base URL. In Docker use `http://host.docker.internal:11434` |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j bolt URI. In Docker use `bolt://neo4j:7687` |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `blueprint` | Neo4j password |

> **Security note:** If `BLUEPRINT_SECRET` is left at the default value the
> app logs a warning at startup. The default is a publicly known string —
> set a real secret before storing production credentials.

---

## Pages

| URL | What it does |
|---|---|
| `/` | Dashboard — server health, Ollama model list, recent profiles |
| `/profiles` | Create, edit, test, duplicate, and favourite connection profiles |
| `/blueprint` | 5-step analysis wizard: connect → context → extract → analyze → results |
| `/explorer` | 3-pane schema tree: table list → column inspector → live data preview |
| `/dedup` | Similarity and deduplication results viewer |
| `/diagram` | ER diagram viewer with filter controls and SVG / Mermaid export |
| `/graph` | Neo4j knowledge graph — D3 force layout, impact analysis, path finder |

---

## Connector plugin system

Each database engine is a self-contained plugin in `connectors/`.
The registry auto-discovers any `.py` file dropped into that folder
at startup — no changes to `main.py` needed.

### Built-in connectors

| Engine | `db_type` | Default port | Driver |
|---|---|---|---|
| PostgreSQL | `postgresql` | 5432 | `psycopg2-binary` |
| MS SQL Server | `mssql` | 1433 | `pymssql` |
| MySQL / MariaDB | `mysql` | 3306 | `pymysql` |
| MongoDB | `mongodb` | 27017 | `pymongo` |
| SQLite | `sqlite` | — | built-in |

### Writing a new connector

Create `connectors/my_db.py`:

```python
from connectors.base import BaseConnector, ConnectorField, ConnectorRegistry

@ConnectorRegistry.register
class MyDBConnector(BaseConnector):
    name         = "my_db"          # used in API calls and profile storage
    display_name = "My Database"    # shown in the UI
    icon         = "🔌"
    default_port = 1234
    category     = "relational"     # relational | nosql | warehouse | cache

    @classmethod
    def fields(cls):
        return [
            ConnectorField("host",     "Host",     default="localhost", width="half"),
            ConnectorField("port",     "Port",     type="number", default="1234", width="third"),
            ConnectorField("dbname",   "Database", width="full"),
            ConnectorField("user",     "Username", width="half"),
            ConnectorField("password", "Password", type="password", width="half"),
        ]

    def test_connection(self, host, port, dbname, user, password, **kw):
        try:
            # connect and count tables/collections
            return {"ok": True, "table_count": n}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def extract_schema(self, host, port, dbname, user, password, **kw):
        # build tables dict
        return self.canonical_schema("my_db", dbname, tables, fks, indexes)

    def get_table_list(self, **kw): ...
    def get_table_detail(self, table, **kw): ...
    def get_sample_rows(self, table, limit=10, **kw): ...
```

Restart the server — the connector appears in all UIs automatically.

`BaseConnector` shared helpers:

| Helper | Purpose |
|---|---|
| `_safe(v)` | Stringify any value, truncate at 120 chars |
| `_sanitize_rows(rows)` | Apply `_safe` to every cell in a sample row list |
| `_bounded_limit(n)` | Clamp row limit between 1 and 200 |
| `_validate_identifier(s)` | Reject SQL-unsafe table/column name characters |
| `canonical_schema(...)` | Build the standard schema dict all routes expect |

---

## Connection profiles

Profiles are saved in `data/profiles.db` (SQLite, local only).
All connection parameters including passwords are encrypted with
**Fernet** (AES-128-CBC + HMAC-SHA256) before writing.

The encryption key is derived exclusively from `BLUEPRINT_SECRET` —
hostname is not included, so the key is stable across Docker restarts,
machine renames, and environment moves as long as `BLUEPRINT_SECRET`
does not change.

Profile operations: create · edit · test · duplicate · favourite ·
delete · quick-launch to Blueprint or Explorer.

---

## Domain dictionary system

Lets you define what abbreviated column and table names mean, scoped
by industry and region. This feeds three places simultaneously:

1. **Dedup / similarity engine** — token expansion before fuzzy matching,
   so `txn` and `transaction` score as identical rather than ~60% similar.
2. **AI analysis prompt** — injected as a vocabulary prior section,
   reducing hallucination on cryptic or abbreviated column names.
3. **Dictionary candidates** — after every analysis, the engine scans
   the schema and returns a list of tokens it found that are not yet
   defined in any dictionary, giving you a fill-in list.

### Scope resolution order

Dictionaries are resolved in three layers and merged top-to-bottom,
with more-specific scopes overriding less-specific ones:

```
shared / global                             applies everywhere
    ↓ overrides
industry / banking                          applies to all banking schemas
    ↓ overrides
context / banking|retail banking|indonesia  most specific, highest priority
```

### Built-in domain lexicons

Active automatically when you select an industry on the analysis wizard.
Defined in `dedup.py` and always available without any database setup.

| Industry | Example expansions |
|---|---|
| Banking | `txn`→transaction, `acct`→account, `cust`→customer, `amt`→amount, `bal`→balance, `gl`→ledger, `ccy`→currency |
| Healthcare | `pt`→patient, `adm`→admission, `dx`→diagnosis, `rx`→prescription, `mrn`→medical_record |
| Logistics | `awb`→air_waybill, `pod`→proof_of_delivery, `wh`→warehouse, `eta`→estimated_arrival |
| E-commerce | `ord`→order, `sku`→stock_keeping_unit, `qty`→quantity, `inv`→invoice, `pmt`→payment |
| Manufacturing | `bom`→bill_of_materials, `wip`→work_in_progress, `wo`→work_order, `po`→purchase_order |
| Telco | `msisdn`→subscriber_number, `cdr`→call_detail_record, `subs`→subscriber |
| Insurance | `pol`→policy, `prm`→premium, `clm`→claim, `cov`→coverage, `ben`→beneficiary |

Generic aliases always active: `id`→identifier, `no`→number, `cd`→code,
`ref`→reference, `dt`→date, `ts`→timestamp.

### Managing dictionaries

```bash
# Save a global dictionary (applies everywhere)
POST /api/dictionaries/shared/global
{"dictionary": {"cif": "customer_information_file", "rtgs": "real_time_gross_settlement"}}

# Save an industry-scoped dictionary
POST /api/dictionaries/industry/banking
{"dictionary": {"nostro": "correspondent_account", "vostro": "foreign_bank_account"}, "industry": "banking"}

# Save a context-specific dictionary (most specific)
POST /api/dictionaries/context/banking|retail banking|indonesia
{"dictionary": {"ojk": "financial_services_authority"}, "industry": "banking", "subdomain": "retail banking", "region": "indonesia"}

# Preview what resolves for a given context
GET /api/dictionaries/resolve?industry=banking&subdomain=retail+banking&region=indonesia

# List all saved dictionaries
GET /api/dictionaries
```

Hint syntax is also supported — pass `"hints": ["abbr=expansion"]` in any
analysis request to inject one-off definitions without saving them.

---

## Similarity and deduplication engine

Runs automatically before every AI analysis. Four passes:

| Pass | Method | Finds |
|---|---|---|
| Name similarity | RapidFuzz ratio + Jaccard token set + normalised form, domain-expanded | Duplicate/alias tables, implicit FK columns |
| Value overlap | Jaccard on sampled value sets between FK candidates and PK columns | Relations confirmed by actual data |
| Structural fingerprint | Jaccard on column-type distribution | Partition tables, clones, archives |
| Dictionary candidates | Scan all schema tokens against active lexicon | Abbreviations not yet defined |

Name matching uses **domain-grounded token expansion** before scoring.
`txn_id` expands to `transaction_identifier` before being compared with
`transactions.id`, giving a near-perfect score instead of ~40%.

`run_all()` returns:

```json
{
  "inferred_fks":          [...],
  "similar_tables":        [...],
  "structural_duplicates": [...],
  "value_overlaps":        [...],
  "dictionary_found":      [...],
  "effective_dictionary":  {...},
  "domain_context":        {...},
  "summary":               {...}
}
```

---

## Fuzzy analytic engine

Name-blind relation discovery — finds FK candidates purely from
**what the data looks like**, with zero reliance on column naming.

Seven signals fused into a weighted composite score per column pair:

| Signal | Weight | What it measures |
|---|---|---|
| Value overlap | 35% | Jaccard intersection of sampled values |
| Format fingerprint | 25% | UUID / date / phone / email / prefixed-ID format match |
| Cardinality match | 15% | Both near-unique (IDs) or both low-cardinality (status codes) |
| Numeric range overlap | 10% | Min/max range intersection for numeric columns |
| Null density | 5% | Similar null % suggests same usage pattern |
| Length distribution | 5% | p50 and p95 string length similarity |
| Prefix cluster | 5% | Shared value prefixes such as ORD-, TXN-, INV- |

Format patterns detected: `uuid`, `uuid_nodash`, `email`, `phone_id`,
`date_iso`, `date_compact`, `integer_id`, `alpha_code`, `prefixed_id`,
`hex_hash`, `ip_address`.

Confidence bands: `high` ≥ 0.80 · `medium` ≥ 0.55 · `low` ≥ 0.35.

Table-level duplicate detection uses cosine similarity of each table's
column-format vector combined with row-count magnitude class (tiny /
small / medium / large).

**Example — banking schema, no explicit FKs, abbreviated names:**
`txn.src_acct` (varchar 10, 100% unique, pattern `NNN-NNNNNNN`) and
`acct.acct_no` (varchar 10, PK, same pattern, 94% value overlap) →
composite score 0.97, confidence `high`, without any name matching.

---

## Relation curation

After analysis you can approve, reject, or annotate any inferred
relation. Approved relations are stored in `data/curation.db` per
profile and flow back into all subsequent analyses and exports —
they appear in the AI prompt as confirmed ground truth, in the ER
diagram, and in DBML / Mermaid exports with `source: user_approved`.

```bash
GET  /api/graph/approvals/{profile_id}    # list approved relations
POST /api/graph/approve                   # approve / annotate (upserts)
DELETE /api/graph/approve/{relation_id}   # remove an approval
```

---

## Knowledge graph (Neo4j — optional)

Neo4j is fully optional. If it is not reachable all graph endpoints
return graceful fallbacks and the rest of the app works normally.

### Node and relationship model

```
(:Schema   {id, name, db_type, profile_id, analyzed_at})
(:Table    {id, name, schema_id, row_count})
(:Column   {id, name, table_id, data_type, is_pk, is_nullable})

(Schema)-[:HAS_TABLE]  ->(Table)
(Table) -[:HAS_COLUMN] ->(Column)
(Table) -[:RELATES_TO  {confidence, type, source, from_col, to_col}]->(Table)
(Table) -[:FUZZY_MATCH {score, dominant_signal, evidence, from_col, to_col}]->(Table)
(Column)-[:FUZZY_LINKS {composite_score, confidence, signals_json}]->(Column)
(Table) -[:LIKELY_CLONE{score, reason}]->(Table)
```

### What the graph enables beyond flat JSON

- **Multi-hop traversal** — `MATCH path=(t)-[:RELATES_TO|FUZZY_MATCH*1..3]-(n) RETURN n`
  in one Cypher call. Equivalent in Python requires recursive loops.
- **Impact analysis** — what tables and columns are affected if you
  rename or drop a given table/column, traced through all relation types.
- **Path finding** — shortest relation chain between any two tables.
- **GraphRAG context injection** — for large schemas (100+ tables) the
  app extracts only the relevant 2-hop subgraph and injects that into
  the Ollama prompt instead of the full schema, keeping the context
  window tight and response quality high.
- **Cross-schema linking** — ingest multiple profiles and link equivalent
  tables across schemas with `SAME_AS` edges (future roadmap).

### Graph API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/graph/status` | Neo4j health + node/edge counts + APOC availability |
| POST | `/api/graph/ingest/profile` | Extract + fuzzy + name dedup + write to Neo4j |
| POST | `/api/graph/neighbors` | Multi-hop traversal from a table |
| POST | `/api/graph/impact` | What breaks if this table/column changes |
| POST | `/api/graph/path` | Shortest path between two tables |
| POST | `/api/graph/fuzzy-relations` | All fuzzy column links above a score threshold |
| POST | `/api/graph/relations/profile` | Full merged relation set (explicit + inferred + fuzzy + approved) |
| POST | `/api/graph/subgraph` | Focused subgraph for LLM context injection |
| POST | `/api/graph/nl-query` | NL question → relevant subgraph context string |
| POST | `/api/graph/export/dbml` | Export schema + relations as DBML |
| POST | `/api/graph/export/mermaid` | Export schema + relations as Mermaid erDiagram |

---

## AI analysis prompt structure

The prompt sent to Ollama is built in six layers:

```
Layer 1 — Domain context      industry · sub-domain · region · user hints
Layer 2 — Neo4j subgraph      pre-confirmed relations from knowledge graph (if available)
Layer 3 — Dedup hints         pre-computed FK candidates + similar tables
Layer 4 — Dictionary prior    merged vocabulary from all matching dictionary scopes
Layer 5 — Full schema         tables · columns · types · row counts · 3 sample rows each
Layer 6 — Explicit FKs        any FK constraints the DB actually declared
```

Expected output structure from the model:

```json
{
  "domain_detected":      "banking",
  "domain_confidence":    "high",
  "executive_summary":    "...",
  "tables": [
    {
      "name": "txn",
      "description": "Core transaction ledger ...",
      "purpose": "core",
      "estimated_importance": "high",
      "columns": [
        {"name": "src_acct", "description": "...", "likely_fk_to": "acct.acct_no", "notes": "..."}
      ]
    }
  ],
  "relations": [
    {"from_table": "txn", "from_col": "src_acct", "to_table": "acct", "to_col": "acct_no",
     "type": "many-to-one", "confidence": "high", "explicit_fk": false, "reason": "..."}
  ],
  "missing_tables":       [{"name": "audit_log", "reason": "..."}],
  "design_observations":  ["No FK constraints enforced — application-level integrity only"],
  "mermaid_erd":          "erDiagram\n  txn }o--|| acct : src_acct\n  ..."
}
```

---

## Full API reference

### Core

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | Server, Ollama, Neo4j, connector count |
| GET | `/api/connectors` | List registered connector plugins with form metadata |
| GET | `/api/models` | List downloaded Ollama models with size and family |

### Profiles

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/profiles` | List all profiles (credentials masked) |
| POST | `/api/profiles` | Create profile |
| GET | `/api/profiles/{id}` | Get one profile |
| PUT | `/api/profiles/{id}` | Update profile |
| DELETE | `/api/profiles/{id}` | Delete profile |
| POST | `/api/profiles/{id}/duplicate` | Clone profile |
| POST | `/api/profiles/{id}/favourite` | Toggle favourite |

### Extraction and analysis

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/test-conn` | Test `{db_type, params}` |
| POST | `/api/test-conn/profile` | Test by profile ID |
| POST | `/api/extract` | Extract schema from `{db_type, params}` |
| POST | `/api/extract/profile` | Extract by profile ID (cached, optional auto-ingest to Neo4j) |
| POST | `/api/analyze/stream` | SSE streaming AI analysis |

### Explorer

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/explorer/tables` | Fast table list |
| POST | `/api/explorer/table-detail` | Columns + indexes for one table |
| POST | `/api/explorer/sample` | Sample rows (max 200) |

### Dedup and dictionaries

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/dedup` | Run similarity analysis on a raw schema |
| POST | `/api/dedup/profile` | Run similarity analysis by profile ID |
| GET | `/api/dictionaries` | List all saved dictionaries |
| GET | `/api/dictionaries/resolve` | Preview merged dictionary for a context |
| GET | `/api/dictionaries/{scope_type}/{scope_key}` | Get one dictionary |
| POST | `/api/dictionaries/{scope_type}/{scope_key}` | Save / update a dictionary |
| DELETE | `/api/dictionaries/{scope_type}/{scope_key}` | Delete a dictionary |

### Graph, fuzzy, curation, and export

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/graph/status` | Neo4j health, node counts, APOC availability |
| POST | `/api/graph/fuzzy` | Run name-blind fuzzy fingerprint analysis |
| POST | `/api/graph/fuzzy/column` | Fingerprint a single column's sample values |
| POST | `/api/graph/fuzzy/compare` | Score two pre-computed column fingerprints |
| POST | `/api/graph/ingest/profile` | Extract + analyse + write all edges to Neo4j |
| POST | `/api/graph/neighbors` | Multi-hop table traversal |
| POST | `/api/graph/impact` | Impact analysis for a table or column |
| POST | `/api/graph/path` | Shortest relation path between two tables |
| POST | `/api/graph/fuzzy-relations` | All fuzzy column links above a score threshold |
| POST | `/api/graph/relations/profile` | Merged relation set (all four sources) |
| POST | `/api/graph/subgraph` | Focused subgraph for LLM context injection |
| POST | `/api/graph/nl-query` | Natural language question → subgraph context string |
| GET | `/api/graph/approvals/{profile_id}` | List approved/curated relations |
| POST | `/api/graph/approve` | Approve or annotate a relation (upserts) |
| DELETE | `/api/graph/approve/{relation_id}` | Remove a curation approval |
| POST | `/api/graph/export/dbml` | Export full schema + relations as DBML |
| POST | `/api/graph/export/mermaid` | Export full schema + relations as Mermaid erDiagram |

---

## Recommended Ollama models

```bash
ollama pull qwen2.5-coder:7b     # best for schema / structured JSON output
ollama pull llama3.2:3b          # fast, low RAM, good quality
ollama pull mistral:7b           # solid general purpose
ollama pull deepseek-coder:6.7b  # strong on technical / code tasks
```

---

## Project structure

```
work_v24/
├── main.py               # FastAPI app + all core API routes
├── settings.py           # All config (paths, env vars) centralised
├── dedup.py              # Name similarity + domain-grounded dedup engine
├── prompts.py            # LLM prompt builder — 6-layer context injection
├── profile_manager.py    # Encrypted profile CRUD (SQLAlchemy + Fernet)
├── curation.py           # Approved relations + domain dictionaries (SQLite)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── .env.example
├── run.sh / run.bat
│
├── connectors/
│   ├── base.py           # BaseConnector + ConnectorRegistry
│   ├── postgresql.py
│   ├── mssql.py
│   ├── mysql.py
│   ├── mongodb.py
│   └── sqlite.py         ← drop a new .py here to add a DB engine
│
├── graph/
│   ├── fuzzy_engine.py   # Name-blind data-fingerprint relation discovery
│   ├── graph_store.py    # Neo4j read/write layer (optional)
│   └── routes.py         # /api/graph/* endpoints
│
├── templates/
│   ├── index.html        # Dashboard
│   ├── blueprint.html    # 5-step analysis wizard
│   ├── profiles.html     # Profile manager
│   ├── explorer.html     # 3-pane schema tree explorer
│   ├── dedup.html        # Dedup and similarity results
│   ├── diagram.html      # ER diagram viewer
│   └── graph.html        # Neo4j knowledge graph explorer
│
├── data/                 # created at runtime
│   ├── profiles.db       # Encrypted connection profiles
│   └── curation.db       # Approved relations + domain dictionaries
│
└── cache/                # created at runtime
    └── *.json            # Cached schema extractions
```

---

## Phase 2 — source code analyser

**Yes, source code analysis is possible**, and it is a natural next step.
The idea: parse ORM model definitions from application source code and
cross-reference them against the live database schema to find drift,
undocumented relations, and naming mismatches.

### What it solves

ORM models (Django, SQLAlchemy, Laravel Eloquent, GORM, TypeORM) define
the same entity/relation structure as the database, but expressed in code.
Parsing them gives you a second independent view of the schema to diff
against the live DB.

Three concrete outputs:

**ORM → DB drift detection.** "This model defines `user_id` as a FK to
`users`, but the DB has no such constraint and the live column is named
`usr_id`." Surfaces migration leftovers, documentation rot, and soft-FK
patterns that only live in application code.

**Relation confidence boost.** When both the fuzzy fingerprint engine
*and* the ORM model independently agree on a relation, that relation is
promoted from `medium` to `high` confidence automatically, and its
provenance records both sources.

**Missing model detection.** Tables in the DB with no corresponding ORM
model are flagged — typically legacy tables, shadow tables, or audit logs
the application never touches directly.

### What is parseable without an LLM

| Language / ORM | Parse method | What we can extract |
|---|---|---|
| Python / SQLAlchemy | `ast` (stdlib) | Table name, column names + types, `ForeignKey()`, `relationship()` |
| Python / Django | `ast` (stdlib) | Model → table, `ForeignKey`, `ManyToManyField`, `related_name` |
| PHP / Laravel Eloquent | regex + optional `nikic/php-parser` | `$table`, `hasMany`, `belongsTo`, `belongsToMany` |
| Go / GORM | `go/ast` or regex on struct tags | `gorm:"column:..."`, `ForeignKey`, `References` tags |
| TypeScript / TypeORM | `ts-morph` or regex | `@Entity`, `@Column`, `@ManyToOne`, `@JoinColumn` |
| Java / Hibernate | `javalang` or regex | `@Table`, `@Column`, `@ManyToOne`, `@JoinColumn` |

The LLM is only needed for ambiguous cases — deep inheritance, dynamic
table names, custom base classes — and for generating human-readable
documentation. Structural extraction is deterministic AST parsing.

### Planned architecture

```
source_analyzer/
├── base.py                   # BaseSourceAnalyzer (same plugin pattern as connectors)
├── python_sqlalchemy.py      # ast-based, zero extra deps
├── python_django.py          # ast-based, zero extra deps
├── php_eloquent.py           # regex-based MVP, optional parser upgrade
├── go_gorm.py
└── typescript_typeorm.py

New API routes:
  POST /api/source/upload       # upload a zip of source files
  POST /api/source/analyze      # parse ORM models from uploaded or local path
  POST /api/source/diff         # cross-reference ORM models vs live DB schema
  POST /api/source/boost        # merge source-confirmed relations into curation.db

New page:
  /source   upload zip or point at local dir → parsed model list → diff view
```

The `diff` output feeds directly into `curation.py` — source-confirmed
relations are stored with `source: orm_model` provenance and flow into
all subsequent analysis runs and exports automatically.

### Implementation scope

A Python-only MVP (SQLAlchemy + Django, `ast` stdlib, zero new
dependencies) can be built in one session and already covers the most
common backend stack for the banking and logistics domains this tool
targets. Full coverage across all five ORMs is the complete Phase 2.

## Phase D1 — Stack Cartographer (Backend ↔ API ↔ DB)

This build adds a first-pass **Stack Cartographer** for Python backends. It parses uploaded source bundles, extracts ORM models and API routes, then maps backend handlers to touched models and tables. When Neo4j is available it can ingest this lineage so the graph can answer questions like:

- which API routes touch `transactions`
- which backend handlers use `accounts`
- which tables tend to travel together in one endpoint

### Implemented in D1

- Python AST backend parsing for **FastAPI** and **Flask-style** route decorators
- ORM model extraction from the existing Source Hunter
- Route → Handler → ORM Model → Table lineage stitching
- Optional Neo4j ingestion with these node/edge types:
  - `(:APIRoute)-[:HANDLED_BY]->(:BackendFunction)`
  - `(:BackendFunction)-[:USES_MODEL]->(:ORMModel)`
  - `(:BackendFunction)-[:TOUCHES_TABLE]->(:Table)`
  - `(:APIRoute)-[:USES_TABLE]->(:Table)`
  - `(:ORMModel)-[:MAPS_TO]->(:Table)`

### API

```
POST /api/cartographer/analyze
```

Request body:

```json
{
  "upload_id": "your_uploaded_source_id",
  "profile_id": "optional_db_profile_id",
  "ingest_to_graph": true
}
```

### UI

Use **Source Code DB Hunter** and click **Run cartographer**. The Cartography tab shows:

- top tables touched by routes
- route count per table
- API → handler → table mapping
- Neo4j ingestion status

### Current scope limits

- D1 is **backend-only lineage**. It does not yet map frontend pages/components to APIs.
- Python route parsing is AST-based; non-Python stacks are still outside the D1 cartography path.
- Table usage inference is strongest when handlers reference known ORM model names directly.

### Next planned phases

- **D2** frontend ↔ API mapping for Jinja, fetch, axios, and TypeScript clients
- **D3** derived full-stack graph so pages/components can inherit `USES_TABLE` edges transitively
- richer explainers: file excerpts, line links, and confidence per edge

## Phase D2 — Frontend ↔ API mapping

This build extends Stack Cartographer from backend-only lineage into the first pass of **full-stack cartography**. It scans frontend templates and client code, links them to API calls, then derives which tables each page or component probably touches through those API routes.

### Implemented in D2

- Jinja / HTML template scanning for:
  - inline `fetch(...)`
  - form `action="/api/..."`
  - HTMX style `hx-get`, `hx-post`, `hx-put`, `hx-delete`, `hx-patch`
  - template includes that help pages inherit component-level table usage
- JS / TS client scanning for:
  - `fetch(...)`
  - `axios.get/post/put/delete/patch(...)`
  - simple `apiClient.get/post/...(...)` style calls
- Frontend node types:
  - `FrontendPage`
  - `FrontendComponent`
- Derived lineage:
  - `(:FrontendPage)-[:CALLS_API]->(:APIRoute)`
  - `(:FrontendComponent)-[:CALLS_API]->(:APIRoute)`
  - `(:FrontendPage)-[:CONTAINS]->(:FrontendComponent)`
  - `(:FrontendPage)-[:USES_TABLE]->(:Table)`
  - `(:FrontendComponent)-[:USES_TABLE]->(:Table)`

### What the Cartography tab shows now

- API → handler → table mappings from D1
- top frontend pages by API activity
- page → API call list
- page/component → derived table usage
- page/component → backend handler hints

### Current scope limits

- D2 uses **heuristic frontend parsing**, not full AST for React or TypeScript component trees yet.
- API-client wrapper indirection is only lightly detected right now.
- Page-to-table lineage is derived through matched API routes, so it is strongest when route paths appear explicitly in templates or client calls.

### Next planned phases

- **D3** richer impact explorer and derived full-stack graph views
- frontend AST parsing for React / TypeScript component trees
- route alias resolution for shared API clients and hooks


## Phase D4 — Visual Graph Explorer

Phase D4 turns Cartography into an interactive map, not just a table dump.

### Implemented in D4

- Cartography now returns a `visual` payload with graph-ready nodes and edges.
- New node kinds in the visual explorer:
  - `FrontendPage`
  - `FrontendComponent`
  - `APIRoute`
  - `BackendFunction`
  - `ORMModel`
  - `Table`
- Source Hunter UI now includes a **Visual Graph** tab with:
  - search filter
  - node-kind filter
  - click-to-highlight
  - double-click focus
  - neighbor and edge detail panels

### Why D4 matters

D1 and D2 produced a lineage map. D4 makes that lineage explorable, so you can visually trace:

- which pages call which APIs
- which APIs map to which backend handlers
- which handlers use which models and tables
- which tables sit at the end of a given screen flow

### Current scope limits

- The visual explorer uses the cartography payload from source analysis, not a live Neo4j browser query surface yet.
- Frontend parsing remains heuristic for JS/TS/HTML at this stage.
- D4 focuses on lineage visualization; blast-radius scoring and impact ranking belong to the next phase.

## Phase D5 — Impact Scoring + Saved Run History

D5 adds two practical layers on top of cartography:

1. **Blast-radius scoring**
   - `POST /api/cartographer/impact-score`
   - ranks nodes by likely change impact using graph degree, two-hop reach, and cross-layer neighbors
   - returns severity tiers: `critical`, `high`, `medium`, `low`

2. **Saved run history**
   - `GET /api/history/runs`
   - `GET /api/history/run/{id}`
   - stores Source Analyze, Source Diff, Fusion, Cartography, and Impact Score payloads in SQLite
   - enables reopening past results without rerunning every analysis immediately

### What is persisted now

These features are persisted and reviewable:
- domain dictionaries
- approved relations
- source analyze runs
- source diff runs
- fusion runs
- cartography runs
- impact-score runs

### Notes on saved runs

- Saved runs are stored as JSON payloads in `data/analysis_history.db`.
- This is meant for internal reviewability and iteration, not yet as a compressed long-term artifact store.
- Neo4j ingestion state is still external to the run payload itself; the saved run stores the analysis result and ingest response, not a Neo4j snapshot.
