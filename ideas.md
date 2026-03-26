# Ideas / Progress Tracker

## Completed
- [x] Split Dedup and Knowledge Graph into separate product concepts.
- [x] Add reusable lint definition library shared across Blueprint, Dedup, and Knowledge Graph.
- [x] Add Source Code DB Hunter Phase 2 starter.
- [x] Expansion A: add TypeORM, Prisma, and Hibernate/JPA support using heuristic or structured parsing.
- [x] Expansion B: upgrade Python SQLAlchemy and Django model extraction to real AST parsing.

## In Progress
- [ ] Add `source_confirmed` chips and filters inside Knowledge Graph UI.
- [ ] Push source-confirmed relations into Neo4j as first-class edge types.
- [ ] Build a DB-vs-source mismatch matrix with severity badges.

## Backlog
- [ ] TypeScript AST parser for TypeORM beyond decorator heuristics.
- [ ] Java AST parser for Hibernate/JPA beyond annotation heuristics.
- [ ] Laravel migration parser.
- [ ] Prisma relation typing deep pass.
- [ ] Fusion engine to blend DB metadata, fuzzy data evidence, and source-confirmed evidence into one confidence score.
- [ ] Bulk approve/reject workflow for inferred relations.
- [ ] Graph-side review panel for approved relations.
- [ ] Export mismatch report to Markdown / PDF.

## Notes
- This file should be appended to over time, not replaced.
- Treat it as a running project memory ledger for implementation progress.

## 2026-03-26 01:05 UTC
- [x] Phase C Fusion Engine: unify DB metadata + fuzzy + source code + curated truth into one scoring surface.
- [x] Add fusion promotion endpoint so high-confidence merged relations can become curated truth.
- [ ] Push fusion edges directly into Neo4j as first-class relations with provenance badges.
- [ ] Add side-by-side disagreement explainer with why one target beat another.
- [ ] Upgrade TypeORM and Hibernate parsers from heuristic to AST/structured parsing.

## 2026-03-26 01:24 UTC
- [x] Phase D1 Stack Cartographer: backend ↔ API ↔ DB map for Python FastAPI/Flask source bundles.
- [x] Ingest stack lineage into Neo4j so graph view can answer which APIs and handlers touch which tables.
- [ ] Phase D2: frontend ↔ API mapping for Jinja, fetch, axios, and TypeScript clients.
- [ ] Phase D3: derived full-stack `USES_TABLE` impact view from page/component down to table.
- [ ] Add line-number deep links and file excerpts in Cartography results.

## 2026-03-26 01:44 UTC
- [x] Phase D2: frontend ↔ API mapping for Jinja, fetch, axios, and TypeScript clients.
- [x] Derive page/component → table lineage from matched API routes.
- [ ] Add frontend file excerpt preview and line anchors in Cartography UI.
- [ ] Upgrade frontend parsing from heuristic scanning to AST for React/TypeScript trees.
- [ ] Add route-name alias resolution for shared API client wrappers.


## 2026-03-26 02:20 UTC
- [x] Phase D4: visual graph explorer for full-stack lineage.
- [x] Add filterable node graph for FrontendPage / FrontendComponent / APIRoute / BackendFunction / ORMModel / Table.
- [x] Add click-to-highlight neighborhood inspection and node detail panel in Source Hunter UI.
- [ ] D5: impact scoring and blast-radius ranking by table / API / page.
- [ ] D5: color-coded change simulation with severity tiers.
- [ ] D5: line-level evidence preview and deep-link jumpouts from graph nodes.

## 2026-03-26 03:20 UTC
- [x] D5: impact scoring and blast-radius ranking by table / API / page.
- [x] Persist source-hunter/cartography/fusion/impact runs so results can be reviewed again later.
- [x] Add a saved-run loader in Source Code DB Hunter UI.
- [ ] D5.1: add per-node explainers directly in the visual graph side panel, not only in the impact table.
- [ ] D5.2: add change simulation presets, e.g. rename field / drop column / alter API contract.
- [ ] D5.3: allow comparing two saved runs to see architecture drift over time.
