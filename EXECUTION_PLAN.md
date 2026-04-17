# Bookmaker Mistake Detector Execution Plan

## 1. Planning Basis
This execution plan is based on:
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_srs_v_2_clean.md`
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_sdd_v_2_clean.md`

This version replaces the older phase-status plan in the repository. The current planning target is not "finish Phase 5 polish." It is to bring the repository into the clean release baseline described by the new SRS/SDD.

## 2. Goal
Deliver a clean release candidate that:
- preserves the existing NBA ingestion, analytics, modeling, opportunity, and backtest capabilities
- separates analyst, admin, and development concerns
- removes demo-driven behavior from stable runtime contracts
- removes runtime schema creation from normal production paths
- decomposes oversized modules into maintainable service and repository boundaries
- leaves the repository looking like a product system rather than an accumulated workshop state

## 3. Current-State Findings
Observed repository shape on April 17, 2026:
- the API currently mounts only health and one large admin router under `/api/v1`
- `backend/src/bookmaker_detector_api/api/admin_routes.py` is extremely large and currently mixes stable admin APIs, demo endpoints, orchestration helpers, and mutation-heavy workflows
- `backend/src/bookmaker_detector_api/repositories/ingestion.py` is oversized and carries multiple persistence concerns in one file
- `backend/src/bookmaker_detector_api/services/models.py` contains runtime schema creation helpers such as `ensure_model_tables(...)`, which conflicts with the migration-owned schema rule in the SDD
- the root `README.md` still behaves like a long validation journal and endpoint inventory rather than a concise portable entry point
- the repo already has strong functional coverage across ingestion, features, models, backtests, frontend, and runbooks, so the highest-value work is now structural cleanup and production hardening rather than net-new MVP capability

## 4. Delivery Strategy
Execute the work in five tracks:
- `Track A: API surface cleanup`
- `Track B: persistence and schema cleanup`
- `Track C: service decomposition and orchestration cleanup`
- `Track D: documentation and repo cleanup`
- `Track E: regression, acceptance, and release hardening`

Critical path:
`route separation -> side-effect removal -> schema ownership cleanup -> repository decomposition -> regression and acceptance`

## 5. Execution Phases

### Phase 1: Baseline and Gap Closure
Objective:
- freeze the clean-release scope and convert the SRS/SDD into a concrete backlog

Tasks:
- inventory all current routes and classify each as `analyst`, `admin`, `dev-only`, or `delete`
- inventory query parameters that trigger seeding, auto-training, auto-selection, auto-materialization, auto-refresh, or other hidden mutations
- inventory every runtime DDL entry point
- inventory large modules and map them to target package boundaries from the SDD
- crosswalk current tests and docs against SRS acceptance criteria

Deliverables:
- route inventory
- cleanup backlog with priorities
- acceptance checklist mapped to real code areas

Exit criteria:
- every public endpoint and major module has an explicit keep/move/remove decision
- the team has a ranked backlog instead of a general cleanup intention

### Phase 2: API Surface Separation
Objective:
- reshape the runtime surface so stable product behavior is explicit and narrow

Tasks:
- split the current admin mega-router into focused modules:
  - `health`
  - `analyst_opportunities`
  - `analyst_patterns`
  - `analyst_trends`
  - `analyst_backtests`
  - `admin_ingestion`
  - `admin_features`
  - `admin_models`
  - `admin_backtests`
  - `admin_maintenance`
- create explicit `/api/v1/analyst/...` and `/api/v1/admin/...` namespaces
- move demo and fixture flows out of stable runtime contracts into scripts, fixtures, tests, or dev-only routers
- remove development toggles from stable GET endpoints
- add typed request/response schemas for stable routes
- enforce that analyst GET endpoints are read-only and side-effect-free

Deliverables:
- decomposed route package
- stable analyst surface
- explicit admin mutation surface
- isolated dev/demo entry points

Exit criteria:
- stable analyst endpoints no longer accept demo-oriented mutation toggles
- admin routes are smaller, responsibility-focused, and easier to test
- ordinary reads do not trigger training, seeding, scoring, or materialization

### Phase 3: Persistence and Schema Ownership Cleanup
Objective:
- make production persistence Postgres-first, explicit, and migration-owned

Tasks:
- break up `repositories/ingestion.py` into:
  - `contracts.py`
  - `records.py`
  - `postgres/*`
  - `in_memory/*`
  - `reporting_queries.py`
  - `quality_helpers.py`
- move any remaining runtime DDL helpers out of normal service/repository execution paths
- make migrations and init SQL the only production schema owners
- confine in-memory repositories to tests, fixtures, and controlled development flows
- centralize repository construction behind clearer contracts/factories
- tighten transaction boundaries for mutation workflows

Deliverables:
- decomposed repository layer
- removal of runtime schema creation from production paths
- clearer repository contract boundary

Exit criteria:
- production services can assume schema existence
- no normal production request path performs table creation or alteration
- persistence code is organized by responsibility rather than by feature phase

### Phase 4: Service and Job Decomposition
Objective:
- narrow service responsibilities and make heavy workflows explicit

Tasks:
- split oversized analytics/service modules by responsibility:
  - ingestion
  - canonicalization
  - metrics
  - features
  - patterns
  - model registry/training/evaluation/selection/scoring
  - opportunities
  - backtesting
  - diagnostics
- consolidate orchestration logic into explicit job or service entry points
- remove repeated demo branches from business logic
- ensure scheduled and admin-triggered jobs follow the explicit dependency chain:
  - ingestion
  - canonicalization
  - metrics
  - features
  - patterns
  - training
  - scoring
  - opportunities
- preserve artifact traceability across features, models, scoring runs, opportunities, and backtests

Deliverables:
- smaller service modules
- explicit job entry points
- reduced branching between demo and production logic

Exit criteria:
- heavy workflows are triggered only through jobs, scripts, or explicit admin mutations
- services are understandable without phase-history context

### Phase 5: Documentation and Repository Cleanup
Objective:
- make the repository portable and release-oriented

Tasks:
- rewrite the root `README.md` into a concise entry point
- move operational depth into `docs/operations`, `docs/release`, and `docs/architecture`
- remove machine-local paths and stale phase narrative from production-facing docs
- classify leftover scaffolding into:
  - keep as production
  - move to admin/internal
  - move to scripts/tests/fixtures
  - delete
- remove dead imports, duplicate helpers, and obsolete development leftovers

Deliverables:
- concise README
- cleaned supporting docs
- reduced repository noise

Exit criteria:
- a new engineer can understand what the project is and how to run it without reading a long phase log
- production-facing docs no longer depend on one developer's local environment

### Phase 6: Regression, Acceptance, and Release Gate
Objective:
- prove that cleanup did not break the product and that the repository meets the clean baseline

Tasks:
- expand automated regression around:
  - parser/canonicalization correctness
  - feature time-safety
  - model and backtest reproducibility
  - analyst/admin route separation
  - side-effect-free stable reads
  - absence of runtime DDL in production flows
- run backend tests, frontend validation, and release smoke checks
- validate SRS acceptance criteria one by one
- record residual risks and known issues
- produce a final release checklist outcome

Deliverables:
- passing regression evidence
- completed acceptance matrix
- release recommendation with known gaps if any remain

Exit criteria:
- the clean baseline in the SRS/SDD is demonstrably satisfied
- remaining gaps, if any, are explicit and small enough for a controlled release decision

## 6. Priority Backlog

### P0: Must Do First
- split `admin_routes.py`
- create `/analyst` and `/admin` route boundaries
- remove demo mutation toggles from stable GET contracts
- remove runtime DDL from production service paths
- decompose the ingestion repository layer
- rewrite the main README
- add regression coverage for side-effect-free reads and route separation

### P1: Should Do During Cleanup
- decompose oversized service modules, especially modeling/orchestration paths
- move demo flows into scripts/tests/fixtures or dev-only mounts
- standardize schema models and response contracts
- tighten job/service naming and package structure
- improve transaction and failure handling around admin mutation workflows

### P2: Follow-Up Hardening
- refine worker/job orchestration boundaries
- improve admin operational dashboards after cleanup settles
- add more focused test utilities around separated repository contracts

## 7. Suggested Order of Execution
1. Freeze endpoint and module inventory.
2. Refactor route composition and create analyst/admin/dev boundaries.
3. Remove hidden side effects from stable reads.
4. Eliminate runtime DDL from production paths.
5. Decompose repository and service layers behind clear contracts.
6. Clean documentation and remove repo leftovers.
7. Run full regression and acceptance validation.

## 8. Risks and Mitigations

### Risk: cleanup breaks current demo-driven flows
Mitigation:
- preserve demo coverage in scripts/tests before removing route-level shortcuts
- migrate behavior first, then delete old entry points

### Risk: route splitting causes frontend/API drift
Mitigation:
- add typed schemas and contract tests while splitting routes
- migrate frontend calls behind stable analyst endpoints before deleting old ones

### Risk: runtime DDL removal exposes missing migration coverage
Mitigation:
- inventory all tables touched by `ensure_*` helpers
- add missing SQL init or migration scripts before removing runtime creation logic

### Risk: large-file decomposition creates temporary duplication
Mitigation:
- split by responsibility in small slices
- keep regression coverage running after each extraction step

## 9. Definition of Done
This execution plan is complete when the repository satisfies all of the following:
- analyst, admin, and development concerns are clearly separated
- stable read endpoints are side-effect-free
- demo helpers no longer shape stable product contracts
- runtime schema creation is not part of normal production execution
- route and repository responsibilities are decomposed into maintainable modules
- README and supporting docs are concise, portable, and clean
- regression and acceptance evidence support the clean release claim

## 10. Recommended Immediate Next Steps
1. Create a route inventory from `backend/src/bookmaker_detector_api/api/admin_routes.py` and classify each endpoint into keep/move/delete buckets.
2. Identify every `seed_demo`, `auto_*`, and runtime materialization toggle that currently affects request behavior.
3. Extract the first stable analyst router from the current admin surface, starting with opportunity and backtest reads.
4. Replace runtime schema creation in modeling flows with migration-owned setup.
5. Rewrite `README.md` after the new route and runtime boundaries are in place.
