# SDD Refactor Execution Plan

## 1. Purpose
This execution plan translates the recommendations from `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_sdd_refactor_plan.docx` into a repo-specific delivery plan for the current codebase.

The goal is to preserve the existing MVP capabilities while executing the design-consolidation cycle described in the SDD:
- reduce oversized modules and service sprawl
- remove duplicated in-memory/Postgres orchestration paths from application logic
- harden backend and frontend contracts
- separate demo behavior from operator-facing production behavior
- improve release confidence through better testing, logging, and runbooks

## 2. Current Repo Snapshot

### Confirmed hotspots
- Backend orchestration is still concentrated in [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py).
- Repository responsibilities are still concentrated in [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py).
- Frontend routing, data loading, and composition are still concentrated in [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx).
- The frontend API layer still embeds demo-oriented defaults in [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts).
- Route files are already split by area under [backend/src/bookmaker_detector_api/api](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api), but they still return large manual dictionaries and still branch between stable-read Postgres and seeded in-memory/demo flows.

### Existing assets to build on
- Route grouping already exists in [backend/src/bookmaker_detector_api/api/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\__init__.py).
- Repository construction already exists in [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py).
- Backend regression tests already cover core modeling and backtest behavior in [backend/tests/test_models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_models.py).
- Operational release docs already exist in [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md) and [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md).
- Frontend smoke scaffolding already exists in [frontend/e2e/phase5-smoke.spec.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\e2e\phase5-smoke.spec.ts).

## 3. Planning Principles
These principles come directly from the SDD and should be treated as change-control rules during implementation:

1. Keep business logic independent from storage details.
2. Prefer one shared orchestration path with adapters over twin in-memory/Postgres flows.
3. Keep API contracts typed and intentional.
4. Separate demo utilities from operator-facing production behavior.
5. Keep UI routes, data loading, and presentation components decoupled.
6. Preserve provenance and auditability without letting serialization dominate service code.
7. Stay incremental: modular monolith, no premature microservice split.

## 4. Target Outcomes

### Backend
- No single backend module should dominate unrelated workflow areas.
- High-value workflows should route through thin FastAPI handlers into focused application services.
- Domain rules should live in storage-agnostic modules with plain-object inputs and outputs.
- Repository interfaces should hide the storage adapter choice from most application code.
- Production schema ownership should remain outside runtime business logic.

### Frontend
- `App.tsx` should become a route shell.
- Data loading should move into page hooks and typed API modules.
- Demo defaults should be explicit and mode-gated rather than silently embedded in fetch wrappers.
- Backend response types should be aligned with backend schemas.

### Operations and release
- Long-running workflows should emit structured logs with correlation IDs.
- Release smoke steps should be concise, workflow-based, and repeatable.
- Acceptance checks should explicitly prove route contracts, orchestration behavior, and separation between demo and operator flows.

## 5. Workstreams

### Workstream A: Backend route and contract hardening
Scope:
- [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_opportunity_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py)
- [backend/src/bookmaker_detector_api/api/analyst_backtests.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_backtests.py)
- [backend/src/bookmaker_detector_api/api/analyst_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_opportunities.py)
- New schema package under `backend/src/bookmaker_detector_api/api/schemas/`

Objectives:
- replace large anonymous response dictionaries with Pydantic request/response models
- centralize repeated query parameters into shared request schema objects
- keep analyst routes read-only and side-effect-free
- isolate demo presets behind explicit demo-only endpoints or fixtures

Execution tasks:
1. Inventory every route that currently accepts repeated model, opportunity, scoring, or backtest query parameters.
2. Define shared request models for common filters:
   - feature/task filters
   - train/validation split controls
   - backtest controls
   - opportunity/game context filters
   - market-board refresh and cadence controls
3. Define nested response models for:
   - model history/detail
   - evaluation snapshots
   - selection snapshots
   - scoring runs and previews
   - opportunities and opportunity detail
   - backtest history and fold detail
   - market-board operations and history
4. Refactor route handlers so they only:
   - validate input
   - invoke one application service method
   - return one typed schema
5. Remove stable-read branching logic from route bodies wherever possible and move adapter selection into dependency wiring or service construction.
6. Move demo-oriented entry points to an explicit `admin_demo_routes` boundary or test/script helpers only.

Dependencies:
- Workstream C for service extraction
- Workstream D for repository interface stabilization

Verification:
- route-level tests for schema validation and error handling
- contract snapshots for high-value routes
- no analyst `GET` should seed, train, materialize, or refresh state

### Workstream B: Backend service decomposition
Scope:
- [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py)
- [backend/src/bookmaker_detector_api/services/model_records.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_records.py)
- [backend/src/bookmaker_detector_api/services/model_market_board_store.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_market_board_store.py)
- [backend/src/bookmaker_detector_api/services/model_market_board_views.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_market_board_views.py)
- [backend/src/bookmaker_detector_api/services/model_market_board_sources.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_market_board_sources.py)
- [backend/src/bookmaker_detector_api/services/feature_evidence_scoring.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\feature_evidence_scoring.py)

Objectives:
- split the oversized modeling module into workflow-focused services
- make orchestration explicit and easier to test
- reduce accidental coupling between training, scoring, opportunities, market-board, and backtesting concerns

Target service split:
- `application/models/training_service.py`
- `application/models/evaluation_service.py`
- `application/models/selection_service.py`
- `application/models/scoring_service.py`
- `application/opportunities/service.py`
- `application/market_board/service.py`
- `application/backtesting/service.py`
- `application/diagnostics/service.py`

Execution tasks:
1. Map every public function in `services/models.py` to one target workflow area.
2. Extract shared DTOs or dataclasses for service input/output payloads.
3. Move transport formatting logic out of services and into schema mappers if needed.
4. Consolidate orchestration entry points so each workflow has one main service surface instead of separate in-memory/Postgres function families.
5. Keep compatibility wrappers temporarily so routes can migrate incrementally.
6. Delete wrappers only after route consumers and tests fully move.

Dependencies:
- Workstream D for repository contracts
- Workstream F for test coverage freeze before deep extraction

Verification:
- existing model tests stay green throughout extraction
- service tests use fake repositories, not HTTP or real DB connections
- workflow ownership becomes obvious from import paths and filenames

### Workstream C: Domain extraction
Scope:
- New domain packages under `backend/src/bookmaker_detector_api/domain/`

Objectives:
- isolate storage-agnostic business rules called out by the SDD
- improve unit-test speed and clarity

Priority domain modules:
- `domain/selection/policies.py`
- `domain/scoring/evidence.py`
- `domain/opportunities/status.py`
- `domain/backtesting/folds.py`
- `domain/scoring/scenario_keys.py`
- `domain/models/fallbacks.py`
- `domain/markets/edge_buckets.py`

Execution tasks:
1. Extract pure functions for:
   - candidate model ranking
   - fallback behavior and fallback reason selection
   - edge bucket derivation
   - evidence strength scoring
   - opportunity status evaluation
   - scenario key generation
   - walk-forward fold summarization
2. Ensure extracted functions accept plain objects or dataclasses only.
3. Remove DB connections, request context, and FastAPI types from domain code.
4. Build critical unit tests around deterministic edge cases before wider rewiring.

Dependencies:
- Workstream B

Verification:
- pure unit tests for the deterministic rule set named in the SDD
- mutation coverage or branch coverage focus on fallback and ranking logic

### Workstream D: Persistence and repository-interface cleanup
Scope:
- [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py)
- [backend/src/bookmaker_detector_api/repositories/ingestion_types.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_types.py)
- [backend/src/bookmaker_detector_api/repositories/ingestion_in_memory_support.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_in_memory_support.py)
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_support.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_support.py)
- [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py)

Objectives:
- hide storage mode behind interfaces
- reduce repeated twin-path orchestration
- organize persistence by responsibility rather than feature history

Target structure:
- `persistence/interfaces/`
- `persistence/adapters/in_memory/`
- `persistence/adapters/postgres/`
- `persistence/reporting_queries/`
- `persistence/records/`
- `persistence/factories/`

Execution tasks:
1. Split `ingestion.py` by repository responsibility:
   - contracts/interfaces
   - in-memory adapter
   - Postgres adapter
   - reporting/query helpers
   - quality helpers
   - record definitions
2. Introduce repository protocols for:
   - model artifacts
   - scoring artifacts
   - opportunities
   - market-board operations
   - feature dataset access
3. Replace route- and service-level storage branching with injected adapter instances.
4. Keep in-memory implementations available for tests, fixtures, and demo-only flows.
5. Tighten transaction boundaries around admin mutation workflows.

Dependencies:
- Workstream B
- Workstream E for schema-ownership rules

Verification:
- adapter tests for Postgres upsert semantics and filter correctness
- fake repository service tests
- reduced duplication across in-memory/Postgres orchestration paths

### Workstream E: Data and schema ownership hardening
Scope:
- [backend/src/bookmaker_detector_api/db/postgres.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\db\postgres.py)
- any runtime DDL helpers referenced from services or repositories
- future migration tooling area under `backend/alembic/` or equivalent

Objectives:
- keep schema ownership out of normal runtime business logic
- preserve current bootstrap/readiness strengths
- formalize the path toward migration tooling

Execution tasks:
1. Inventory every place that creates or mutates schema at request time.
2. Remove runtime DDL from normal service and repository flows.
3. Keep startup readiness checks fail-fast, but non-mutating.
4. Decide when to introduce Alembic:
   - now, if schema churn is still active
   - next milestone, if schema is mostly stable
5. Review JSON payload usage and classify fields as:
   - structured queryable columns
   - provenance/audit JSON payloads
6. Preserve queryable provenance links across training, evaluation, selection, scoring, opportunity, and backtest artifacts.

Dependencies:
- Workstream D

Verification:
- no production request path performs table creation or alteration
- schema bootstrap path is explicit and documented
- tests prove runtime flows work against an already-prepared schema

### Workstream F: Frontend shell, API client, and mode separation
Scope:
- [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx)
- [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts)
- [frontend/src/appTypes.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appTypes.ts)
- [frontend/src/appSharedComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appSharedComponents.tsx)
- new `frontend/src/pages/`, `frontend/src/hooks/`, `frontend/src/api/`, and `frontend/src/components/`

Objectives:
- convert the frontend into a page-driven structure
- remove hardcoded demo filters from the API client
- create explicit demo mode vs operator mode behavior
- align TypeScript models with backend response schemas

Target structure:
- `pages/BacktestsPage.tsx`
- `pages/BacktestDetailPage.tsx`
- `pages/OpportunityQueuePage.tsx`
- `pages/OpportunityDetailPage.tsx`
- `pages/ArtifactComparePage.tsx`
- `hooks/useBacktests.ts`
- `hooks/useOpportunities.ts`
- `hooks/useArtifactCompare.ts`
- `api/backtests.ts`
- `api/opportunities.ts`
- `api/models.ts`
- `components/backtests/*`
- `components/opportunities/*`
- `components/provenance/*`

Execution tasks:
1. Turn `App.tsx` into route composition plus top-level providers only.
2. Move fetch and mutation logic out of component bodies and into hooks.
3. Replace hardcoded query defaults in `appApi.ts` with:
   - route params
   - controlled filter state
   - explicit demo presets
4. Introduce a mode layer:
   - demo mode can inject defaults
   - operator mode requires explicit user-selected or URL-provided filters
5. Split large page sections into reusable components.
6. Align TS response types to backend schemas either manually or via generation.

Dependencies:
- Workstream A for typed backend contracts

Verification:
- App route shell remains small and readable
- browser smoke still passes
- operator mode can run without hidden `LAL`, `2024-2025`, or `canonical_game_id=3` defaults

### Workstream G: Testing and regression strategy
Scope:
- [backend/tests](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests)
- [frontend/e2e/phase5-smoke.spec.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\e2e\phase5-smoke.spec.ts)
- CI commands referenced in repo config and docs

Objectives:
- freeze behavior before deeper extraction
- shift most new tests toward domain and service layers
- preserve confidence in the key workflows named by the SDD

Execution tasks:
1. Add or strengthen pure unit tests for:
   - selection ranking
   - fallback behavior
   - opportunity status evaluation
   - edge bucket derivation
   - fold summarization
2. Add service tests with fake repositories for:
   - train model
   - score future scenario
   - materialize opportunities
   - refresh market board
   - run backtest
3. Add adapter tests for:
   - Postgres upsert behavior
   - query filters
   - transaction boundaries
4. Add API contract tests for:
   - request validation
   - response schema shape
   - side-effect-free analyst reads
5. Expand frontend tests for empty/error states and comparison flows.
6. Keep the E2E smoke focused on golden paths:
   - run backtest
   - inspect backtest detail and fold detail
   - materialize/view opportunities
   - compare fold vs opportunity

Priority regression gates from the SDD:
- selection policy
- scoring classification
- backtest fold summaries
- market-board orchestration

Dependencies:
- all workstreams

Verification:
- backend tests green
- frontend lint, typecheck, and build green
- E2E smoke green or explicitly waived with reason

### Workstream H: Observability, release, and runbook hardening
Scope:
- [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md)
- [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md)
- [docs/release_candidate_runbook.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_candidate_runbook.md)
- logging/config modules in backend infrastructure

Objectives:
- add structured workflow logging
- keep release validation short, task-oriented, and repeatable
- make environment modes explicit

Execution tasks:
1. Add correlation IDs or run IDs across:
   - refreshes
   - scoring batches
   - opportunity materialization
   - backtests
2. Log counts, durations, and statuses for long-running workflows.
3. Update runbooks to reflect the post-refactor route and service boundaries.
4. Shorten smoke steps so they verify stable workflows rather than phase-era demo terminology.
5. Label demo fixtures clearly in docs and tooling.
6. Define explicit behavior by environment mode:
   - demo
   - development
   - production

Dependencies:
- Workstream A for stable route naming
- Workstream F for demo/operator mode distinction

Verification:
- workflow logs support traceability from request to persisted artifacts
- release docs match the actual route and execution model

## 6. Delivery Phases

### Phase 0: Baseline freeze
Goal:
- lock behavior before structural work starts

Tasks:
- capture API surface inventory by route group
- capture current smoke path for backtests, opportunities, scoring, and market-board flows
- identify runtime DDL entry points
- classify all demo-only routes and seeded helper paths

Outputs:
- route inventory
- refactor backlog
- baseline regression evidence

Exit criteria:
- every high-value workflow has a test or smoke reference before code movement begins

### Phase 1: Contract stabilization
Goal:
- satisfy the SDD recommendation to add typed request/response schemas for high-value workflows

Tasks:
- create shared request models
- add response models for model, backtest, opportunity, and scoring flows
- add contract tests
- reduce manual dict assembly in routes

Outputs:
- initial `api/schemas` package
- typed contracts for the first wave of routes

Exit criteria:
- target routes return stable typed payloads

### Phase 2: Core module split
Goal:
- extract focused services from oversized backend modules without changing behavior

Tasks:
- split `services/models.py`
- extract domain rules
- keep compatibility wrappers during migration

Outputs:
- smaller workflow-oriented service modules

Exit criteria:
- no single service module dominates unrelated workflow areas

### Phase 3: Repository-interface adoption
Goal:
- satisfy the SDD recommendation to hide in-memory/Postgres variants behind interfaces

Tasks:
- split `repositories/ingestion.py`
- define repository protocols
- push adapter selection into factories/dependency wiring

Outputs:
- shared orchestration path with swappable adapters

Exit criteria:
- twin-path duplication is materially reduced

### Phase 4: Frontend shell refactor
Goal:
- satisfy the SDD recommendation to turn `App.tsx` into a route shell and remove hardcoded demo values

Tasks:
- move API functions into domain-specific modules
- add pages/hooks/components
- add explicit demo/operator mode behavior
- align TS types to backend contracts

Outputs:
- route-based UI structure

Exit criteria:
- `App.tsx` is a shell, and operator workflows no longer rely on hidden demo constants

### Phase 5: Operational hardening
Goal:
- satisfy the SDD recommendations on logging, smoke checks, and release readiness

Tasks:
- add structured workflow logging
- update runbooks and smoke docs
- tighten release checklist around stable workflows

Outputs:
- safer release process

Exit criteria:
- release validation is repeatable and reflects the new architecture

## 7. Detailed Sequencing
Recommended order:

1. Freeze regression and contract baselines.
2. Add backend schemas for model, opportunity, backtest, and scoring flows.
3. Refactor route handlers to thin-controller form while preserving existing paths.
4. Extract domain rules from `services/models.py`.
5. Split service orchestration into workflow-focused modules.
6. Split repository responsibilities and introduce adapter interfaces.
7. Remove remaining runtime DDL from normal execution paths.
8. Refactor the frontend API client and explicit mode handling.
9. Break `App.tsx` into pages, hooks, and reusable components.
10. Add structured logging and update release docs.
11. Run full regression, smoke, and acceptance validation.

## 8. Recommendation Coverage Matrix

| SDD recommendation | Planned implementation |
| --- | --- |
| Keep business logic independent from storage details | Workstreams B, C, and D extract domain logic and hide adapters behind interfaces. |
| Prefer one shared domain workflow over duplicated in-memory/Postgres orchestration | Workstream D moves adapter choice into factories and repository protocols. |
| Keep API contracts intentional and typed | Workstream A adds Pydantic request/response schemas for high-value routes. |
| Separate demo utilities from operator-facing production behavior | Workstreams A and F create explicit demo boundaries and mode handling. |
| Keep UI routes, data loading, and presentation decoupled | Workstream F splits `App.tsx` into pages, hooks, API modules, and components. |
| Make provenance and auditability first-class | Workstreams B, E, and H preserve queryable artifact links and add structured logging. |
| Thin route handlers by bounded workflow | Workstream A reorganizes handlers around ingestion, diagnostics, features, models, opportunities, market-board, and backtests. |
| Extract storage-agnostic domain rules | Workstream C creates pure modules for ranking, fallback, scoring, status, keys, and fold summaries. |
| Hide in-memory and Postgres variants behind interfaces | Workstream D introduces adapter contracts and shared orchestration paths. |
| Remove hardcoded frontend demo filters | Workstream F removes embedded `LAL`, `2024-2025`, and `canonical_game_id=3` assumptions. |
| Support explicit demo mode and operator mode | Workstream F introduces mode-aware defaults and user-driven filters. |
| Align frontend types to backend schemas | Workstreams A and F align TS models with backend responses. |
| Keep schema ownership outside runtime business logic | Workstream E inventories and removes runtime DDL from normal flows. |
| Be deliberate about structured columns vs JSON payloads | Workstream E classifies queryable columns and provenance-rich JSON data. |
| Expand testing toward domain and service layers | Workstream G prioritizes pure unit tests and service tests with fake repositories. |
| Add structured logging for long-running workflows | Workstream H adds run IDs, durations, counts, and statuses. |
| Introduce a small release checklist tied to stable workflows | Workstream H updates smoke and release docs accordingly. |

## 9. Acceptance Criteria Mapping

| SDD acceptance criterion | Execution-plan proof point |
| --- | --- |
| No single backend service module dominates unrelated workflow areas | Phase 2 complete, `services/models.py` decomposed, imports shifted to focused modules. |
| Core workflows run through shared orchestration regardless of storage adapter | Phase 3 complete, services depend on repository interfaces rather than twin function families. |
| High-value routes use typed request/response models | Phase 1 complete with route-level contract tests. |
| Frontend API calls no longer rely on hardcoded team, season, or scenario demo constants | Phase 4 complete, operator mode validated via UI and API tests. |
| `App.tsx` reduced to a route shell with extracted pages/hooks/components | Phase 4 complete, file size materially reduced and route/page split in place. |
| Regression tests cover selection policy, scoring classification, backtest fold summaries, and market-board orchestration | Phase 5 gate includes explicit test evidence for each area. |

## 10. Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Refactor breaks current flows | Freeze contract tests and golden-path smoke before deeper extraction. |
| Over-design slows momentum | Keep the modular monolith approach and extract only where the SDD points to recurring pain. |
| Frontend/backend drift during contract changes | Ship backend schemas first, then align TS models and browser smoke to them. |
| Demo assumptions keep leaking into production paths | Introduce explicit demo mode boundaries and reject hidden defaults in operator mode. |

## 11. Immediate Priority Backlog

### P1
1. Split [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py) into focused workflow modules.
2. Introduce Pydantic response models for model, opportunity, backtest, and scoring flows.
3. Remove hardcoded demo filters from [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts).

### P2
1. Refactor [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) into pages, hooks, and components.
2. Add repository interfaces over in-memory/Postgres flows and route all major services through them.

### P3
1. Formalize structured workflow logging.
2. Refresh smoke and release checklists to match the post-refactor architecture.

## 12. Definition of Done
This execution plan is complete when all of the following are true:

1. The backend uses typed contracts for high-value operator workflows.
2. The frontend no longer depends on hidden demo constants in its API layer.
3. Oversized backend and frontend hotspots are decomposed into focused modules.
4. Shared orchestration paths exist across storage adapters.
5. Runtime schema mutation is removed from normal production flows.
6. Regression, smoke, and release checks prove the architecture remains functional and auditable.
