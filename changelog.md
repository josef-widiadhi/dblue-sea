# Changelog

## 2026-03-26 - Source Hunter Expansion A
- Added heuristic source-model extraction for TypeORM, Prisma, and Hibernate/JPA.
- Added source-confidence boosting flow so source-confirmed relations can be promoted into curated truth.
- Added `changelog.md` and `ideas.md` tracking files.

## 2026-03-26 - AST Brain Upgrade B
- Upgraded Python source parsing from regex sniffing to real AST parsing for SQLAlchemy/Flask-style models and Django models.
- Added parser-mode reporting in Source Code DB Hunter so each detected model shows whether it came from `ast`, `structured`, or `heuristic` parsing.
- Kept Prisma in structured parsing mode, and kept Laravel, GORM, TypeORM, and Hibernate/JPA in heuristic mode.
- Updated Source Code DB Hunter UI summary, model table, and relation table to show parser mode visibility.
- Prepared the codebase for future deeper AST work on TypeScript and Java.

## 2026-03-26 01:05 UTC — Phase C Fusion Engine
- Added `fusion_engine.py` to combine explicit DB metadata, Dedup signals, fuzzy evidence, source-code evidence, and curated approvals into a ranked final relation list.
- Added `/api/fusion/profile` and `/api/fusion/promote`.
- Updated Source Code DB Hunter UI with a Fusion tab, run button, and promote fused high-confidence relations flow.
- Preserved approval source attribution so `orm_model` and `fusion_engine` stay visible downstream.
- Added small UI fixes for Blueprint scrolling, Graph control overlap, and Dedup panel layout.

## 2026-03-26 01:24 UTC — Phase D1 Stack Cartographer
- Added `stack_cartographer.py` for Python AST-based backend ↔ API ↔ DB lineage mapping.
- Added `/api/cartographer/analyze` to analyze uploaded source bundles and optionally ingest the stack map into Neo4j.
- Updated Source Code DB Hunter UI with a Cartography run action and Cartography results tab.
- Neo4j ingestion now creates `APIRoute`, `BackendFunction`, and `ORMModel` nodes plus `HANDLED_BY`, `USES_MODEL`, `TOUCHES_TABLE`, and `USES_TABLE` edges.
- Expanded `scripts/selftest.py` with a FastAPI cartography smoke test.

## 2026-03-26 01:44 UTC — Phase D2 Stack Cartographer
- Extended `stack_cartographer.py` to map frontend pages/components to API calls for Jinja/HTML, fetch, axios, HTMX-style attributes, and JS/TS client calls.
- Added derived page/component → table lineage so Cartography can show which screens likely touch which tables.
- Expanded Neo4j ingestion with `FrontendPage`, `FrontendComponent`, `CALLS_API`, `CONTAINS`, and frontend `USES_TABLE` edges.
- Updated Source Hunter UI summary and Cartography tab to show frontend pages, components, API links, and page-to-table usage.
- Expanded self-test coverage with template and TSX frontend fixtures.


## 2026-03-26 02:20 UTC — Phase D4 Stack Cartographer
- Added a visual graph payload builder in `stack_cartographer.py` so Cartography returns ready-to-draw nodes and edges for frontend pages/components, API routes, backend handlers, ORM models, and tables.
- Upgraded `templates/source.html` with a Visual Graph tab using an interactive graph explorer for filtering, click-to-highlight, neighborhood inspection, and lineage detail.
- Kept the Source Hunter summary + cartography tables intact while layering the visual explorer on top.
- Continued append-only updates for `README.md`, `changelog.md`, and `ideas.md`.

## 2026-03-26 03:20 UTC — Phase D5 Impact Scoring + Saved Runs
- Added `impact_engine.py` to score blast radius across FrontendPage, FrontendComponent, APIRoute, BackendFunction, ORMModel, and Table nodes from the cartography graph.
- Added `analysis_store.py` with SQLite-backed saved run history so source analysis, diffs, fusion results, cartography runs, and impact scoring can be reopened later.
- Added `/api/cartographer/impact-score`, `/api/history/runs`, and `/api/history/run/{id}`.
- Updated `templates/source.html` with a Saved Runs loader and an Impact tab showing ranked hotspots with severity badges and signal reasons.
- Expanded `scripts/selftest.py` to cover impact scoring and saved-run persistence.
