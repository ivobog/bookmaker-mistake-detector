# Bookmaker Mistake Detector Execution Plan

## 1. Planning Basis
This execution plan is based on:
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_sdd_postgres_only_multi_target.md`

Planning date:
- `2026-04-20`

This plan replaces the older broad cleanup plan in the repository. The active delivery target is now the SDD-defined cutover to:
- one PostgreSQL-backed runtime path
- no frontend or backend demo execution mode in runtime flows
- explicit multi-target model/task capabilities across training, selection, scoring, backtesting, and opportunity generation

## 2. Goal
Deliver an implementation branch that:
- removes runtime `in_memory` vs `postgres` branching from business workflows
- removes runtime demo behavior from the frontend and API surface
- introduces an explicit task/capability model for Phase A regression targets
- generalizes model evaluation and selection away from a hidden MAE-only global rule
- keeps the current product surface usable while making target-task behavior explicit and extensible

## 3. Current-State Findings
Observed in the repository on April 20, 2026:
- backend runtime is Postgres-only across the active admin and analyst route surfaces, and the demo router/mode flags are removed from runtime code
- the capability registry is live through [task_registry.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/task_registry.py) and `GET /api/v1/admin/model-capabilities`
- Phase A regression tasks now share task-aware training, selection, scoring, opportunity, and backtest semantics, including raw-value tasks that score against market edge rather than raw magnitude alone
- selection-policy handling now defaults and normalizes to `validation_regression_candidate_v1` while still accepting `validation_mae_candidate_v1` as a temporary compatibility alias
- [frontend/src/api/defaults.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/defaults.ts) has replaced the old mode-based client plumbing, and model-admin task/policy controls now resolve from backend capabilities
- frontend model-admin forms and tests now use canonical capability-driven defaults instead of legacy spread-only or alias-only assumptions
- the automated Phase 5 regression gate is green at `main / faacd2c`, including backend Ruff, backend pytest, Python compile checks, frontend typecheck, frontend lint, and frontend build
- the main remaining work is Phase 5 manual smoke, acceptance signoff, and residual-risk closeout rather than Phase 1 to Phase 4 cutover work

## 4. Delivery Strategy
Execute the SDD in six controlled phases:
- `Phase 0: inventory and acceptance baseline`
- `Phase 1: schema and capability foundation`
- `Phase 2: backend persisted-only cutover`
- `Phase 3: task-aware modeling lifecycle refactor`
- `Phase 4: frontend task-aware cutover`
- `Phase 5: regression, migration, and release gate`

Critical path:
`task registry foundation -> persisted-only backend cutover -> task-aware selection/scoring normalization -> frontend capability adoption -> end-to-end regression`

Current execution status:
- `Phase 0`: complete
- `Phase 1`: complete
- `Phase 2`: complete
- `Phase 3`: complete
- `Phase 4`: complete
- `Phase 5`: in progress
  - automated regression gate: complete
  - PostgreSQL-backed smoke pass: partial complete
  - acceptance closeout and residual-risk resolution: pending

## 5. Workstreams

### Workstream A: Schema and capability model
Objective:
- establish one source of truth for target tasks and supported model families

Primary files and areas:
- migrations and SQL init assets under the backend persistence layer
- [model_records.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_records.py)
- [model_training_lifecycle.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_training_lifecycle.py)
- [model_training_views.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_training_views.py)

Tasks:
- add `target_task_definition`
- add `model_family_capability`
- extend evaluation snapshot persistence with `primary_metric_direction`, `selection_score`, and `selection_score_name`
- backfill the four Phase A tasks:
  - `spread_error_regression`
  - `total_error_regression`
  - `point_margin_regression`
  - `total_points_regression`
- backfill model-family capability rows for current regression families:
  - `linear_feature`
  - `tree_stump`

Exit criteria:
- the backend can resolve enabled tasks and valid task/model combinations without hidden constants alone
- evaluation snapshots can represent generalized ranking metadata

### Workstream B: Postgres-only runtime cutover
Objective:
- remove runtime storage-mode branching from the API and service entry points

Primary files and areas:
- [config.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/config.py)
- [api/__init__.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/__init__.py)
- [admin_model_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_routes.py)
- [admin_scoring_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_scoring_routes.py)
- [admin_opportunity_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py)
- [admin_market_board_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_market_board_routes.py)
- [admin_model_support.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_model_support.py)
- [repository_factory.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/repository_factory.py)

Tasks:
- remove `api_repository_mode`
- remove `use_postgres_stable_read_mode`
- replace repository construction that branches on mode with persisted-only construction
- delete runtime use of `_prepare_in_memory_*` helpers
- remove `repository_mode` from response DTOs and payload shaping
- unregister [admin_demo_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/admin_demo_routes.py) from the runtime API

Exit criteria:
- no runtime route chooses `in_memory` vs `postgres`
- no runtime API response exposes `repository_mode`
- no admin workflow self-seeds in-memory state to satisfy a request

### Workstream C: Task registry and policy registry
Objective:
- make task support and ranking semantics explicit and extensible

Primary files and areas:
- new `services/task_registry.py`
- [model_training_algorithms.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_training_algorithms.py)
- [model_training_lifecycle.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_training_lifecycle.py)
- [model_training_views.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_training_views.py)
- [model_opportunities.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_opportunities.py)
- [model_scoring_runs.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_scoring_runs.py)
- [model_scoring_previews.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_scoring_previews.py)
- [model_backtest_workflows.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_backtest_workflows.py)

Tasks:
- introduce a task registry contract for metadata, policy resolution, and enablement
- introduce a model-family capability registry for allowed task/model combinations
- generalize selection policy resolution beyond `validation_mae_candidate_v1`
- keep `validation_mae_candidate_v1` as a temporary regression alias during migration
- define task-aware scoring and opportunity policy lookup
- enforce that active model selection, scoring, and materialization are task-compatible

Exit criteria:
- `spread_error_regression` is no longer a hidden platform default
- all Phase A tasks use the same lifecycle contracts with task-aware metadata
- unsupported task/model/policy combinations fail validation early

### Workstream D: Modeling service decomposition
Objective:
- shrink the dual-mode facade and separate workflow responsibilities

Primary files and areas:
- [models.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/models.py)
- [model_market_board_orchestration.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_market_board_orchestration.py)
- [model_backtest_runs.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_backtest_runs.py)
- [model_market_board_views.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/model_market_board_views.py)

Tasks:
- extract persisted-only orchestration into focused services:
  - `model_training_service`
  - `model_selection_service`
  - `model_scoring_service`
  - `model_opportunity_service`
  - `model_backtest_service`
  - `model_market_board_service`
- move task registry and policy registry concerns out of the facade
- keep pure algorithm modules separate from persistence and route concerns
- leave classification hooks in place without enabling unsupported tasks in Phase A

Exit criteria:
- [models.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/models.py) is no longer the dual-mode control center
- each major workflow has one persisted orchestration path

### Workstream E: Frontend task-aware cutover
Objective:
- remove demo mode and make UI workflows capability-driven

Primary files and areas:
- [frontend/src/api/mode.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/mode.ts)
- [frontend/src/api/modelAdmin.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/modelAdmin.ts)
- [frontend/src/api/models.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/models.ts)
- [frontend/src/modelAdminTypes.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminTypes.ts)
- [frontend/src/modelAdminPages.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminPages.tsx)
- [frontend/src/modelAdminWorkspace.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminWorkspace.tsx)
- [frontend/src/App.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/App.tsx)

Tasks:
- replace [mode.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/api/mode.ts) with a simpler app-defaults module
- remove `FrontendAppMode = "demo"` and `appendDemoScope(...)`
- add capability loading from `GET /api/v1/admin/model-capabilities`
- render target-task choices, labels, and compatible selection policies from backend data
- remove implicit `spread_error_regression` defaults from list and mutation flows where the backend should accept `target_task = None`
- stop reading or writing `repository_mode` in UI types, mocks, and tests

Exit criteria:
- the UI has no runtime demo/operator split
- target-task options are loaded from backend capabilities rather than hidden constants
- Phase A tasks are visible across training, selection, scoring, and backtest flows

### Workstream F: Test and migration hardening
Objective:
- update the suite so it proves the new runtime truth rather than the old demo truth

Primary files and areas:
- backend route tests and service tests
- [frontend/src/modelAdminWorkspace.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminWorkspace.test.tsx)
- [frontend/src/modelAdminComponents.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminComponents.test.tsx)
- [frontend/src/modelAdminActionValidation.test.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/modelAdminActionValidation.test.ts)
- [frontend/src/App.opportunities.test.tsx](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/src/App.opportunities.test.tsx)

Tasks:
- replace in-memory expectations with PostgreSQL-backed contracts
- add contract coverage for the capability endpoint
- add coverage for all four Phase A tasks across:
  - training
  - selection
  - scoring preview
  - backtest preview/materialization
  - opportunity materialization
- add rejection tests for disabled tasks and invalid task/model/policy combinations
- update smoke flows so they use persisted fixtures rather than route-level self-seeding shortcuts

Exit criteria:
- the regression suite validates persisted-only behavior
- acceptance is measured against Phase A end-to-end workflows rather than demo-mode convenience paths

## 6. Execution Phases

### Phase 0: Inventory and acceptance baseline
Objective:
- lock scope and capture the exact removals and contract changes before code churn begins

Tasks:
- inventory every runtime route that branches on storage mode or self-seeds state
- inventory every response schema that returns `repository_mode`
- inventory every frontend type and test that expects demo behavior or spread-only defaults
- produce an acceptance checklist for the eight SDD acceptance criteria

Deliverables:
- runtime branch inventory
- DTO contract inventory
- acceptance checklist

### Phase 1: Schema and capability foundation
Objective:
- make target-task support first-class before removing the old runtime shortcuts

Tasks:
- add the new capability tables and backfills
- extend evaluation snapshot schema
- create the backend task registry abstraction
- define the initial model-family capability mapping

Deliverables:
- migrations
- task registry module
- seeded Phase A task definitions

Dependency note:
- this phase must land before broad route validation changes so the backend has a source of truth to validate against

### Phase 2: Backend persisted-only cutover
Objective:
- remove the storage-mode split from the API and repository entry points

Tasks:
- simplify configuration to one runtime storage assumption
- replace repository factory branching
- remove in-memory route helpers
- remove demo route mounting
- remove `repository_mode` from DTOs

Deliverables:
- persisted-only route layer
- simplified configuration
- updated admin schemas

Dependency note:
- this phase should finish before frontend cutover so the UI can be migrated against final backend contracts

### Phase 3: Task-aware modeling lifecycle refactor
Objective:
- make the train-select-score-materialize path task-aware without changing product intent

Tasks:
- generalize training contracts and evaluation metadata
- introduce selection policy registry logic
- move opportunity interpretation behind task-aware policies
- refactor persisted orchestration into smaller services

Deliverables:
- task-aware training and selection flow
- smaller workflow services
- legacy MAE alias support during migration

Dependency note:
- this phase can overlap partially with frontend capability work once the capability endpoint contract is stable

### Phase 4: Frontend task-aware cutover
Objective:
- replace runtime demo behavior with backend-driven task capability UX

Tasks:
- replace `mode.ts`
- load model capabilities on entry
- update form defaults and filters
- remove `repository_mode` fields from types and mocks
- update all affected admin workspace tests

Deliverables:
- capability-driven UI
- simplified API client layer
- updated frontend contracts

### Phase 5: Regression, migration, and release gate
Objective:
- prove the cutover works across the full Phase A workflow surface

Tasks:
- run backend tests
- run frontend unit tests
- run type checks and build validation
- run PostgreSQL-backed workflow smoke checks for all Phase A tasks
- validate each SDD acceptance criterion explicitly

Deliverables:
- regression evidence
- acceptance checklist completion
- residual risk log

## 7. Priority Backlog

### P0: Must complete first
- add `target_task_definition` and `model_family_capability`
- remove config and route storage-mode branching
- remove `repository_mode` from backend DTOs and frontend types
- unregister runtime demo routes
- add the capability endpoint
- replace frontend demo mode plumbing

### P1: Required to finish the SDD cleanly
- generalize selection metadata and policy resolution
- refactor persisted modeling workflows out of the dual-mode facade
- validate all four Phase A tasks end to end
- update smoke and integration tests to use persisted fixtures only

### P2: Follow-up hardening after the cutover
- split additional large modeling modules beyond the minimum needed for Phase A
- persist selection policy definitions if the team wants runtime-configurable policies
- stage classification task enablement behind disabled registry entries

## 8. Sequencing Rules
Apply these rules while implementing:
- do not start deleting frontend demo mode until the backend capability endpoint contract is stable
- do not remove legacy selection aliases until persisted selection tests pass for all Phase A tasks
- do not enable any Phase B task in the registry until trainer, scorer, and opportunity logic exist
- do not merge route simplification without simultaneous schema and test updates for DTO changes

## 9. Risks and Mitigations

### Risk: in-memory code removal breaks developer workflows
Mitigation:
- provide PostgreSQL-backed seed scripts and fixtures before deleting convenience helpers
- keep test-specific fixtures separate from runtime routes

### Risk: hidden spread-centric assumptions survive the refactor
Mitigation:
- grep-driven inventory of `spread_error_regression` defaults before and after each phase
- require capability-derived task options in the UI

### Risk: generalized selection changes ranking behavior unexpectedly
Mitigation:
- keep `validation_mae_candidate_v1` as a temporary regression alias
- add snapshot-level ranking tests for all Phase A tasks before deleting legacy branches

### Risk: DTO cleanup causes large frontend breakage
Mitigation:
- land backend contract changes and capability endpoint first
- update frontend types and mocks immediately after backend DTO changes

## 10. Definition of Done
This execution plan is complete when all of the following are true:
- no runtime backend route branches on repository mode
- no runtime frontend code exposes demo/operator mode
- runtime demo routes are removed
- `repository_mode` is gone from backend payloads and frontend types
- enabled target-task capabilities come from one backend source of truth
- the UI can train, select, score, backtest, and materialize opportunities for all four Phase A regression tasks
- integration and smoke coverage use PostgreSQL-backed data only

## 11. Recommended Immediate Next Steps
1. Add the schema and seed/backfill for `target_task_definition`, `model_family_capability`, and generalized evaluation snapshot fields.
2. Implement `GET /api/v1/admin/model-capabilities` and wire it to a new backend task registry service.
3. Remove `api_repository_mode`, `use_postgres_stable_read_mode`, and repository-mode branching from the backend entry points.
4. Delete runtime in-memory prep paths and unregister `admin_demo_routes`.
5. Replace the frontend demo mode module with app defaults plus capability loading.
6. Run the full Phase A workflow matrix against PostgreSQL-backed fixtures and close any remaining spread-centric defaults.
