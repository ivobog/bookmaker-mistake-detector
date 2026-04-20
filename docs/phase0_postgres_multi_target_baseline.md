# Phase 0 Baseline

## Purpose
This document records the Phase 0 baseline for the Postgres-only, multi-target SDD dated `2026-04-20`.

Phase 0 goals:
- inventory runtime storage-mode branching
- inventory self-seeding and in-memory preparation paths
- inventory DTOs and frontend types that expose `repository_mode`
- inventory frontend spread-centric and demo-mode assumptions
- create an acceptance checklist for the SDD cutover

## Baseline Summary
Current repository state:
- runtime API composition still mounts demo routes through [api/__init__.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/__init__.py)
- backend runtime configuration still exposes `api_repository_mode` and `use_postgres_stable_read_mode` in [config.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/config.py)
- repository construction still supports both Postgres and in-memory mode in [repository_factory.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/repository_factory.py)
- analyst routes still branch on `settings.use_postgres_stable_read_mode`
- admin modeling, scoring, opportunity, feature, and market-board routes still branch on storage mode and often return `repository_mode`
- several admin mutation paths still self-seed, auto-train, auto-promote, or auto-materialize in-memory state before serving the request
- backend response schemas still model `RepositoryMode = Literal["in_memory", "postgres"]`
- frontend shared types and model-admin types still require `repository_mode`
- frontend runtime still supports demo mode and still hardcodes `spread_error_regression` in several key workflows

## Runtime Branch Inventory

### Core runtime control points
| File | Current issue | Phase 2 implication |
| --- | --- | --- |
| [config.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/config.py) | Defines `api_repository_mode` and `use_postgres_stable_read_mode` | Remove mode switching and keep one persisted runtime assumption |
| [repository_factory.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/repository_factory.py) | `build_ingestion_repository(repository_mode)` builds both `PostgresIngestionRepository` and `InMemoryIngestionRepository` | Replace with persisted-only construction on runtime paths |
| [api/__init__.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/__init__.py) | Still mounts `admin_demo_router` | Remove runtime demo router from API composition |

### Analyst route inventory
| File | Endpoints | Current pattern | Phase 2 action |
| --- | --- | --- | --- |
| [analyst_backtests.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/analyst_backtests.py) | `GET /api/v1/analyst/backtests`, `GET /api/v1/analyst/backtests/{backtest_run_id}` | Branches on `settings.use_postgres_stable_read_mode` and falls back to `build_in_memory_phase_three_modeling_store()` | Remove mode branch and return persisted-only responses |
| [analyst_opportunities.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/analyst_opportunities.py) | `GET /api/v1/analyst/opportunities`, `GET /api/v1/analyst/opportunities/{opportunity_id}` | Branches on `settings.use_postgres_stable_read_mode` and falls back to in-memory modeling store | Remove mode branch and keep persisted queue/detail loading only |
| [analyst_patterns.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/analyst_patterns.py) | `GET /api/v1/analyst/patterns`, `GET /api/v1/analyst/comparables`, `GET /api/v1/analyst/evidence` | Branches on `settings.use_postgres_stable_read_mode` and falls back to `build_in_memory_feature_dataset_store()` | Remove mode branch and keep persisted feature access only |
| [analyst_trends.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/analyst_trends.py) | `GET /api/v1/analyst/trends/summary` | Branches on `settings.use_postgres_stable_read_mode` and falls back to in-memory feature dataset store | Remove mode branch and keep persisted feature summary only |

### Admin route inventory
| File | Endpoints | Current pattern | Phase 2 action |
| --- | --- | --- | --- |
| [admin_feature_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_feature_routes.py) | `GET /phase-2-feature-demo`, plus feature snapshot, dataset, profile, analysis, and training-view endpoints | Runtime branching across all feature endpoints; demo endpoint remains first-class | Remove demo endpoint from runtime surface; remove branching from feature endpoints |
| [admin_model_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_routes.py) | model train, backtests run/history, registry, runs, summary, history, evaluations, selections | Imports both `*_in_memory` and `*_postgres` service functions and branches per request | Replace with persisted-only service calls and task-aware validation |
| [admin_scoring_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_scoring_routes.py) | score preview, future-game preview/materialize, future-slate preview/materialize, scoring run history/detail | Branches per request and uses in-memory prep helpers for materialization paths | Replace with persisted-only scoring workflow services |
| [admin_opportunity_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py) | future preview opportunity materialize, opportunity materialize, opportunity history | Branches per request and uses in-memory prep helpers before materialization | Replace with persisted-only opportunity workflow services |
| [admin_market_board_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_market_board_routes.py) | market-board refresh, history, queue, orchestration, materialize, detail, operations, score | Large mixed runtime branch surface with several in-memory orchestration helpers | Replace with persisted-only market-board services and remove route-level self-preparation |
| [admin_demo_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_demo_routes.py) | provider demo and multiple phase-1 demo endpoints | Dedicated runtime demo router | Delete or quarantine from runtime API package |

## Self-Seeding And Auto-Preparation Inventory

### In-memory preparation helpers
| File | Helper | Current behavior | Why it matters |
| --- | --- | --- | --- |
| [admin_model_support.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_support.py) | `_prepare_in_memory_phase_three_model_repository(...)` | Seeds phase-two feature data, trains in-memory models, optionally promotes best model | This is the clearest runtime self-seeding path that must disappear from Phase 2 runtime code |
| [admin_model_support.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_support.py) | `_prepare_in_memory_future_game_scoring_repository(...)` | Builds an in-memory modeling store and can materialize a future-game preview before the route result is returned | Route behavior differs materially from persisted production behavior |
| [admin_model_support.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_support.py) | `_prepare_in_memory_future_slate_repository(...)` | Builds an in-memory modeling store and can materialize a slate before the route result is returned | Keeps demo/runtime mutation shortcuts inside the HTTP layer |
| [admin_feature_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_feature_routes.py) | `_prepare_in_memory_feature_repository()` | Seeds phase-two feature data on demand | Runtime feature analysis mutation paths can fabricate state in-memory |
| [admin_market_board_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_market_board_routes.py) | `_prepare_in_memory_market_board_refresh_repository(...)` | Refreshes an in-memory market board before serving orchestration requests | Route-level hidden mutation |
| [admin_market_board_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_market_board_routes.py) | `_prepare_in_memory_market_board_score_repository(...)` | Seeds features, materializes a board, trains models, and promotes best model before scoring | This combines several hidden steps in one helper |
| [admin_market_board_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_market_board_routes.py) | `_prepare_in_memory_market_board_orchestration_repository(...)` | Seeds features, refreshes the board, trains models, promotes best model, and optionally orchestrates refresh/scoring/cadence | Highest-risk demo-style runtime shortcut in the admin surface |

### Routes that currently rely on self-preparation
| Endpoint | Current hidden work before returning |
| --- | --- |
| `POST /api/v1/admin/models/train` | In-memory repository preparation before training when not in Postgres mode |
| `POST /api/v1/admin/models/select` | In-memory repository preparation and optional promotion path when not in Postgres mode |
| `POST /api/v1/admin/models/future-game-preview/materialize` | In-memory prep repository for preview materialization |
| `POST /api/v1/admin/models/future-slate/materialize` | In-memory prep repository for slate materialization |
| `POST /api/v1/admin/models/future-game-preview/opportunities/materialize` | In-memory repo preparation plus best-model promotion |
| `POST /api/v1/admin/models/opportunities/materialize` | In-memory repo preparation plus best-model promotion |
| `POST /api/v1/admin/features/analysis/materialize` | In-memory feature repository seeding |
| `POST /api/v1/admin/models/market-board/orchestrate-refresh` | In-memory market-board refresh preparation |
| `POST /api/v1/admin/models/market-board/orchestrate-score` | In-memory market-board orchestration preparation |
| `POST /api/v1/admin/models/market-board/orchestrate-cadence` | In-memory market-board orchestration preparation |
| `POST /api/v1/admin/models/market-board/{board_id}/score` | In-memory board score preparation including training and promotion |

## DTO Contract Inventory

### Backend schema inventory
Current backend schema pattern:
- [admin.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/schemas/admin.py) defines `RepositoryMode = Literal["in_memory", "postgres"]`
- [analyst.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/schemas/analyst.py) defines `RepositoryMode = Literal["in_memory", "postgres"]`

Response models in backend schemas that currently expose `repository_mode`:

Admin schema responses:
- `AdminBacktestHistoryResponse`
- `AdminModelRegistryResponse`
- `AdminModelRunsResponse`
- `AdminModelHistoryResponse`
- `AdminModelSummaryResponse`
- `AdminModelRunDetailResponse`
- `AdminModelEvaluationsResponse`
- `AdminEvaluationHistoryResponse`
- `AdminEvaluationDetailResponse`
- `AdminModelSelectionsResponse`
- `AdminSelectionHistoryResponse`
- `AdminSelectionDetailResponse`
- `AdminScoringPreviewResponse`
- `AdminFutureGamePreviewResponse`
- `AdminScoringRunsResponse`
- `AdminScoringRunDetailResponse`
- `AdminScoringHistoryResponse`
- `AdminOpportunityHistoryResponse`

Analyst schema responses:
- `AnalystBacktestListResponse`
- `AnalystBacktestDetailResponse`
- `AnalystOpportunityListResponse`
- `AnalystOpportunityDetailResponse`
- `AnalystTrendResponse`
- `AnalystPatternResponse`
- `AnalystComparableResponse`
- `AnalystEvidenceResponse`

### Frontend type inventory
Current frontend type pattern:
- [modelAdminTypes.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminTypes.ts) defines `ModelAdminRepositoryMode = "in_memory" | "postgres"`
- [appTypes.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/appTypes.ts) models multiple API payloads with `repository_mode: string`

Frontend model-admin response types that currently expose `repository_mode`:
- `ModelAdminRegistryResponse`
- `ModelAdminRunsResponse`
- `ModelAdminRunDetailResponse`
- `ModelAdminSummaryResponse`
- `ModelAdminHistoryResponse`
- `ModelAdminEvaluationsResponse`
- `ModelAdminEvaluationHistoryResponse`
- `ModelAdminEvaluationDetailResponse`
- `ModelAdminSelectionsResponse`
- `ModelAdminSelectionHistoryResponse`
- `ModelAdminSelectionDetailResponse`
- `ModelAdminTrainResponse`
- `ModelAdminSelectResponse`

Frontend shared app response types that currently expose `repository_mode`:
- `BacktestHistoryResponse`
- `BacktestRunResponse`
- `OpportunityHistoryResponse`
- `OpportunityListResponse`
- `OpportunityDetailResponse`
- `OpportunityMaterializeResponse`
- `ModelHistoryResponse`
- `ScoringRunDetailResponse`
- `ModelRunDetailResponse`
- `SelectionDetailResponse`
- `EvaluationDetailResponse`

## Spread-Centric And Demo-Mode Assumption Inventory

### Backend defaults that remain spread-centric
| File | Current default or coupling | Phase 1 to Phase 4 implication |
| --- | --- | --- |
| [api/schemas/admin.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/schemas/admin.py) | Several filter DTOs still default `target_task` to `spread_error_regression` | Move list/history DTOs toward `target_task = None` and task-aware defaults from capability resolution |
| [api/schemas/analyst.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/schemas/analyst.py) | `AnalystPatternFilters`, `AnalystComparableFilters`, and `AnalystEvidenceFilters` default to `spread_error_regression` | Replace hidden defaults with capability-driven or explicit task selection |
| [admin_model_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_routes.py) | Training and backtest routes default `target_task` to `spread_error_regression` and selection policy to `validation_mae_candidate_v1` | Replace with explicit task requirement and task-aware policy validation |
| [admin_scoring_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_scoring_routes.py) | Future-game preview still falls back to `spread_error_regression` when `target_task` is null | Stop using silent spread fallback |

### Frontend runtime assumptions
| File | Current assumption | Phase 4 implication |
| --- | --- | --- |
| [frontend/src/api/mode.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/mode.ts) | Defines `FrontendAppMode = "operator" | "demo"` and `appendDemoScope(...)` | Replace with app-defaults only; remove runtime demo mode |
| [frontend/src/api/mode.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/mode.ts) | Default target task is `spread_error_regression` | Replace with capability-driven default selection |
| [frontend/src/modelAdminWorkspace.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminWorkspace.tsx) | Training, evaluation, and selection filters all initialize to `spread_error_regression` | Replace with capability-provided initial task or no task until chosen |
| [frontend/src/App.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/App.tsx) | Still creates in-memory fallback promises for selection and evaluation detail loading | Remove in-memory runtime fallback branches |
| [frontend/src/vite-env.d.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/vite-env.d.ts) | Still declares `VITE_APP_MODE?: "operator" | "demo"` | Remove demo-mode environment contract |

### Frontend tests currently tied to old contracts
| File | Current coupling |
| --- | --- |
| [frontend/src/modelAdminWorkspace.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminWorkspace.test.tsx) | Repeatedly asserts `repository_mode: "in_memory"` and spread-centric defaults |
| [frontend/src/modelAdminComponents.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminComponents.test.tsx) | Uses `spread_error_regression` as the only default task in several expectations |
| [frontend/src/modelAdminActionValidation.test.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminActionValidation.test.ts) | Validation tests are written around spread-only task input |
| [frontend/src/App.opportunities.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/App.opportunities.test.tsx) | Uses `repository_mode: "in_memory"` payloads and spread-centric opportunity fixtures |

## Phase 0 Decisions
These decisions are now explicit:
- runtime demo endpoints are removal candidates, not compatibility commitments
- `repository_mode` is a cleanup target across backend and frontend contracts
- in-memory prep helpers are runtime debt, not acceptable production-faithful behavior
- `spread_error_regression` is a legacy default that must be replaced by explicit task capability handling
- list and history APIs should prefer `target_task = None` where the SDD calls for broader task awareness

## Acceptance Checklist
Status key:
- `Open`: not yet implemented
- `Ready to verify`: implementation exists and needs end-to-end confirmation
- `Done`: verified against runtime behavior

| SDD acceptance criterion | Status | Evidence target |
| --- | --- | --- |
| No runtime API route branches on in-memory vs Postgres mode | Open | Route code no longer references `use_postgres_stable_read_mode`, `build_in_memory_*`, or `*_in_memory` runtime paths |
| No runtime frontend code exposes demo/operator mode | Open | `frontend/src/api/mode.ts` removed or replaced and `VITE_APP_MODE` removed from runtime typing |
| Demo/admin seed endpoints are removed from runtime routing | Open | [api/__init__.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/__init__.py) no longer mounts `admin_demo_router` and feature demo endpoints are removed or quarantined |
| Response DTOs no longer expose `repository_mode` | Open | Backend schemas and frontend response types remove `repository_mode` entirely |
| The UI can train, select, score, backtest, and materialize opportunities for all Phase A target tasks | Open | Frontend flow tests and smoke checks pass for the four Phase A regression tasks |
| `spread_error_regression` is no longer the hidden default assumption across the whole stack | Open | Route defaults, schema defaults, and frontend filter defaults are capability-driven or explicit |
| Target-task capabilities are discoverable from one backend source of truth | Open | `GET /api/v1/admin/model-capabilities` exists and frontend consumes it |
| PostgreSQL-backed integration tests pass across the full Phase A workflow surface | Open | Backend integration suite and smoke checklist use persisted fixtures only |

## Recommended Phase 1 Entry Points
Recommended next changes after this baseline:
1. Add `target_task_definition`, `model_family_capability`, and generalized evaluation snapshot fields.
2. Introduce a backend task registry service and the `GET /api/v1/admin/model-capabilities` endpoint.
3. Simplify configuration and repository construction to eliminate runtime storage-mode switching.
4. Remove runtime demo router mounting and delete route-level in-memory prep helpers.
5. Replace frontend demo mode and spread-only defaults with capability-driven initialization.
