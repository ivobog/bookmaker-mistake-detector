# Model Admin / Training Workspace Execution Plan

## 1. Planning Basis
This execution plan is based on the following source documents supplied by the user:
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_srs_model_admin_training_workspace.docx`
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_sdd_model_admin_training_workspace.docx`

Document date in both sources: `2026-04-18`

## 2. Delivery Goal
Deliver a dedicated `Model Admin / Training` workspace in the existing web application that lets an admin:
- open a first-class `#/models` workspace
- review registry, runs, evaluations, and selections
- train models from the frontend
- select the best model from the frontend
- inspect artifact detail and active selection status without leaving the UI

Success definition:
- the train-to-select lifecycle is fully operable from the frontend
- existing `Backtests` and `Opportunities` workspaces continue to work without route regressions
- the implementation is modular and does not push more workspace-specific logic into `frontend/src/App.tsx`

## 2.1 Current Delivery Status
Status as of `2026-04-19`:
- Phase 0 is complete: contracts, routes, and UX ownership were frozen in `docs/phase0_model_admin_contract_and_ux_mapping.md`
- Phase 1 is complete: the `#/models` route family and dedicated workspace shell are live
- Phase 2 is complete: the frontend now uses a dedicated model-admin API and type layer
- Phase 3 is complete: dashboard, registry, runs, evaluations, and selections read surfaces are live
- Phase 4 is complete: `Train model` and `Select best model` are live from the frontend workspace
- Phase 5 is complete: cross-links, artifact summaries, rationale handling, and browser smoke coverage are in place
- Phase 6 is partially complete in the current repo: browser smoke is green and route-contract unit tests are now part of the frontend stack
- Phase 6 now also includes unit coverage for `Train model` and `Select best model` input normalization and validation rules
- backend model-admin and backtest route validation now passes against the in-memory contract suite used by the frontend-facing API tests
- the broader backend regression suite now passes across the full `backend/tests` surface
- frontend component-level tests now cover Model Admin action-panel interaction states and list empty-state rendering
- frontend component-level tests now also cover Model Admin workspace loading/error banners and core run/selection detail-card rendering
- frontend component-level tests now also cover a successful training mutation banner path plus registry and evaluation detail-card variants
- frontend component-level tests now also cover a successful selection mutation banner path and null-detail placeholders across all four Model Admin artifact-card variants
- frontend workspace tests now also cover cross-route state transitions, selection-filter reset behavior, and post-mutation read-side refresh calls
- browser smoke now also covers train/select mutation flows and verifies browser-level refresh behavior against stateful mocked admin APIs

Remaining Phase 6 gap:
- no blocking Phase 6 gap remains in the current repo; any further coverage would be additive hardening beyond the current release gate

## 3. Current-State Repo Assessment

### Frontend
Current relevant files:
- `frontend/src/App.tsx`
- `frontend/src/appUtils.ts`
- `frontend/src/appTypes.ts`
- `frontend/src/api/models.ts`
- `frontend/src/backtestsWorkspace.tsx`
- `frontend/src/opportunitiesWorkspace.tsx`
- `frontend/src/appArtifactDetailComponents.tsx`
- `frontend/src/appSharedComponents.tsx`

Observed state:
- the app already uses hash-based routing and route parsing through `parseRouteFromHash(...)` and `routeHash(...)`
- `App.tsx` is the routing and data-loading hub for Backtests and Opportunities and is already large
- `frontend/src/api/models.ts` already consumes read-side model endpoints for history and artifact detail
- there is no dedicated `Model Admin` workspace module, no model-admin route family, and no frontend mutation flow for train or select
- there is no current frontend unit/component test harness beyond typecheck, lint, and Playwright smoke coverage

### Backend
Current relevant files:
- `backend/src/bookmaker_detector_api/api/admin_model_routes.py`
- `backend/src/bookmaker_detector_api/api/schemas/admin.py`
- `backend/src/bookmaker_detector_api/services/models.py`
- `backend/src/bookmaker_detector_api/services/model_training_views.py`
- `backend/src/bookmaker_detector_api/services/model_training_lifecycle.py`
- `backend/tests/test_admin_routes.py`
- `backend/tests/test_models.py`

Observed state:
- the backend already exposes the required v1 endpoint families for `train`, `select`, `registry`, `runs`, `summary`, `history`, `evaluations`, and `selections`
- existing tests already cover much of the model-admin contract surface
- backend work for v1 should be limited to response-contract tightening, human-readable field support, and optional dashboard optimization

## 4. Execution Strategy
Ship the feature in six phases, with frontend-first delivery and minimal backend changes unless contract gaps appear.

Critical path:
`route shell -> workspace extraction -> api/types expansion -> read-only pages -> mutation flows -> detail polish -> browser regression`

Working principle:
- reuse existing backend endpoints first
- extract new frontend modules instead of growing `App.tsx`
- make the dashboard and artifact views readable before optimizing data aggregation

## 5. Phase Plan

### Phase 0: Contract Freeze and UX Mapping
Objective:
- convert the SRS and SDD into a repo-grounded backlog before implementation starts

Tasks:
- confirm the exact `#/models` route map and modal-vs-panel interaction pattern
- map each SRS requirement to an existing endpoint or a backend gap
- inventory all model-admin response shapes currently returned by `admin_model_routes.py`
- document reusable UI primitives already present in `appSharedComponents.tsx` and artifact detail components
- define which existing artifact cards can be reused versus which model-admin-specific cards are required

Target files:
- `frontend/src/App.tsx`
- `frontend/src/appUtils.ts`
- `frontend/src/appTypes.ts`
- `frontend/src/api/models.ts`
- `backend/src/bookmaker_detector_api/api/admin_model_routes.py`
- `backend/src/bookmaker_detector_api/api/schemas/admin.py`

Exit criteria:
- every SRS screen and action has a mapped route, component owner, and endpoint dependency
- all backend gaps are classified as `required for v1`, `nice to have`, or `post-v1`

### Phase 1: Route Shell and Workspace Extraction
Objective:
- add the Model Admin workspace without increasing shell coupling

Tasks:
- extend `AppRoute` to include `models`, `models/registry`, `models/runs`, `models/runs/:id`, `models/evaluations`, `models/evaluations/:id`, `models/selections`, and `models/selections/:id`
- extend `parseRouteFromHash(...)` and `routeHash(...)` for the new route family
- add a `Model Admin / Training` navigation destination alongside `Backtests` and `Opportunities`
- extract model-admin rendering into dedicated modules instead of adding more inline JSX to `App.tsx`
- keep shell ownership limited to global navigation, top-level route selection, and shared error/loading behavior

Recommended new frontend modules:
- `frontend/src/modelAdminWorkspace.tsx`
- `frontend/src/modelAdminPages.tsx`
- `frontend/src/modelAdminDetailComponents.tsx`
- `frontend/src/modelAdminActions.tsx`
- `frontend/src/modelAdminTypes.ts`

Existing files to modify:
- `frontend/src/App.tsx`
- `frontend/src/appUtils.ts`
- `frontend/src/appTypes.ts`

Exit criteria:
- users can navigate to the new workspace and between all list/detail routes
- the new workspace is rendered through dedicated modules, not via another large `App.tsx` branch

### Phase 2: API Layer and Type Contracts
Objective:
- create a dedicated frontend integration surface for model-admin reads and mutations

Tasks:
- split model-admin API helpers out of generic model history usage into a dedicated module
- add frontend types for dashboard summary, registry entries, run list/detail, evaluation list/detail, selection list/detail, and mutation responses
- add `trainModels(...)` and `selectBestModel(...)` helpers
- add list helpers for `registry`, `runs`, `evaluations`, and `selections`
- add dashboard helpers for `summary`, `history`, `evaluations/history`, and `selections/history`
- normalize field naming where the backend still exposes verbose or uneven shapes

Recommended frontend API structure:
- keep `frontend/src/api/models.ts` for existing provenance consumers if needed
- add `frontend/src/api/modelAdmin.ts` for the new workspace
- export the new helpers from `frontend/src/api/index.ts`

Possible backend support changes if needed:
- tighten schema typing in `backend/src/bookmaker_detector_api/api/schemas/admin.py`
- return additive summary fields if the current payloads are too awkward for the UI

Exit criteria:
- every Model Admin screen and action can be backed by a typed frontend API call
- no screen relies on ad hoc JSON access from `App.tsx`

### Phase 3: Read-Only Workspace Delivery
Objective:
- ship a usable read-only Model Admin console before mutations

Tasks:
- build the `#/models` dashboard with summary cards, recent activity, and quick links
- build the registry list screen with target-task filtering
- build the runs list and run detail views with readable split metrics and artifact summaries
- build the evaluations list and detail views with selected feature, fallback strategy, and metric visibility
- build the selections list and detail views with active-state visibility and rationale rendering
- add loading, empty, and error states for every list/detail screen

Priority UI outcomes:
- artifact summaries should be readable without opening raw JSON first
- timestamps, active indicators, selected feature, fallback strategy, and primary metrics should be visible in the first screenful

Primary files:
- `frontend/src/modelAdminWorkspace.tsx`
- `frontend/src/modelAdminPages.tsx`
- `frontend/src/modelAdminDetailComponents.tsx`
- `frontend/src/styles.css`

Exit criteria:
- a user can inspect registry, runs, evaluations, and selections from dedicated pages
- the workspace already satisfies the non-mutation acceptance criteria from the SRS

### Phase 4: Mutation Flows
Objective:
- make the frontend operational for model training and model selection

Tasks:
- build the `Train model` form with defaults from the SRS:
  - `feature_key=baseline_team_features_v1`
  - `target_task=spread_error_regression`
  - `train_ratio=0.70`
  - `validation_ratio=0.15`
- build the `Select best model` form with `target_task` and `selection_policy_name`
- validate ratios and required fields before submit
- disable repeat submit while requests are in flight
- refresh affected resources after success:
  - train: summary, runs, evaluations
  - select: summary, evaluations, selections, active indicators
- provide direct navigation to the newest run or newest selection from the success state

Primary files:
- `frontend/src/modelAdminActions.tsx`
- `frontend/src/modelAdminWorkspace.tsx`
- `frontend/src/api/modelAdmin.ts`

Potential backend follow-up only if needed:
- ensure mutation responses consistently include the latest created artifact id
- ensure backend error payloads are human-readable enough for inline UI display

Exit criteria:
- an admin can train a model and select the best model from the UI without using manual API calls

### Phase 5: Cross-Workspace Interoperability and Detail Polish
Objective:
- make the console feel integrated with the rest of the product

Tasks:
- add links from training artifacts into existing provenance views where identifiers line up
- reuse artifact detail rendering patterns already present in the app where appropriate
- highlight active selection status consistently across dashboard and selection detail
- surface rationale, fallback behavior, and split summaries as labeled UI sections instead of buried JSON blobs
- ensure medium and large screens preserve the intended master-detail behavior
- verify the workspace remains usable on mobile-sized layouts

Likely touched files:
- `frontend/src/modelAdminDetailComponents.tsx`
- `frontend/src/appArtifactDetailComponents.tsx`
- `frontend/src/backtestsWorkspace.tsx`
- `frontend/src/opportunitiesWorkspace.tsx`
- `frontend/src/styles.css`

Exit criteria:
- artifact inspection feels coherent across Model Admin, Backtests, and Opportunities
- model lifecycle details are understandable without backend knowledge

### Phase 6: Test Coverage, Smoke Validation, and Release Gate
Objective:
- prove that the new workspace works and does not break existing routes

Tasks:
- add frontend route tests for `parseRouteFromHash(...)` and `routeHash(...)`
- add frontend component or interaction tests for:
  - dashboard rendering
  - train form validation
  - select form validation
  - list empty/error states
- extend Playwright smoke coverage to include:
  - opening `#/models`
  - navigating to runs, evaluations, and selections
  - opening a detail route
  - verifying no regression in existing Backtests and Opportunities navigation
- add or update backend contract tests only where the frontend exposes a contract gap
- run `npm run lint`, `npm run typecheck`, `npm run build`, `npm run test:smoke`, and backend `pytest`

Notes:
- the frontend currently has Playwright smoke coverage but no dedicated unit/component harness in `package.json`
- if route and component tests are required for v1, add a small test stack such as `vitest` plus React Testing Library instead of relying only on browser smoke tests

Exit criteria:
- the new workspace passes smoke coverage
- existing workspaces still function
- residual risks are documented explicitly

Current status:
- route-contract unit coverage now exists for `parseRouteFromHash(...)` and `routeHash(...)`
- unit coverage now exists for train/select input validation and payload normalization
- component-level coverage now exists for action-panel interaction behavior and core list empty states
- component-level coverage now exists for workspace loading/error banners and core run/selection detail rendering
- component-level coverage now exists for training success-banner rendering and registry/evaluation detail variants
- component-level coverage now exists for selection success-banner rendering and null-detail placeholders across all Model Admin artifact cards
- Playwright smoke covers all three workspaces and the core Model Admin detail routes
- Playwright smoke also covers train/select mutation flows with browser-level refresh verification through stateful API stubs
- backend `test_admin_routes.py` model-admin/backtest slice and the related `test_models.py` service slice both pass
- the broader backend regression suite also passes across `backend/tests`
- the remaining optional improvements are additive coverage or operational hardening outside the current release gate

## 6. Backlog by Priority

### P0: Must Have for v1
- add the full `#/models` route family
- extract model-admin workspace modules out of `App.tsx`
- implement dashboard, registry, runs, evaluations, and selections screens
- implement train and select mutation flows
- provide readable loading, empty, success, and error states
- preserve existing Backtests and Opportunities route behavior
- add smoke coverage for model-admin navigation and regression coverage for existing workspaces

### P1: Strongly Recommended
- add frontend route/unit tests
- improve artifact cards so selected feature, fallback strategy, and rationale are first-class fields
- tighten backend response schemas for smoother frontend typing
- add direct links from mutation success states to the created artifact detail

### P2: Optional Optimization
- add a single overview aggregator endpoint if dashboard latency becomes a problem
- add richer recent-activity rollups or comparison cards
- refine filter state persistence beyond simple route-local state

## 7. Dependencies and Integration Points

### Frontend dependencies
- route changes depend on `frontend/src/appTypes.ts` and `frontend/src/appUtils.ts`
- workspace extraction depends on reducing direct model-history assumptions in `frontend/src/App.tsx`
- model-admin pages depend on new typed API helpers rather than direct reuse of provenance detail fetches

### Backend dependencies
- current v1 delivery depends on these existing endpoints remaining stable:
  - `POST /api/v1/admin/models/train`
  - `POST /api/v1/admin/models/select`
  - `GET /api/v1/admin/models/registry`
  - `GET /api/v1/admin/models/runs`
  - `GET /api/v1/admin/models/runs/{run_id}`
  - `GET /api/v1/admin/models/summary`
  - `GET /api/v1/admin/models/history`
  - `GET /api/v1/admin/models/evaluations`
  - `GET /api/v1/admin/models/evaluations/history`
  - `GET /api/v1/admin/models/evaluations/{snapshot_id}`
  - `GET /api/v1/admin/models/selections`
  - `GET /api/v1/admin/models/selections/history`
  - `GET /api/v1/admin/models/selections/{selection_id}`

## 8. Risks and Mitigations

### Risk: `App.tsx` grows further and becomes the third workspace bottleneck
Mitigation:
- keep `App.tsx` limited to route dispatch and shared shell concerns
- move all model-admin rendering, fetching orchestration, and mutation handling into dedicated modules

### Risk: backend payloads are technically correct but not UI-friendly
Mitigation:
- define frontend mapper/types early
- add small additive backend fields only when the UI cannot cleanly derive them

### Risk: stale UI after train/select mutations creates operator confusion
Mitigation:
- refresh affected resources immediately after each mutation
- highlight the newest artifact and expose a direct deep link from the success state

### Risk: dashboard over-fetching causes slow first paint
Mitigation:
- use concurrent requests first
- introduce an overview aggregator endpoint only if measured latency justifies it

### Risk: model-admin work regresses existing route behavior
Mitigation:
- add route parsing tests
- extend Playwright smoke coverage to cover all three workspaces before release

## 9. Acceptance Mapping
The implementation is done when the following are true:
- the main shell exposes a `Model Admin / Training` navigation destination
- `#/models` dashboard works and links to registry, runs, evaluations, and selections
- a user can submit `Train model` from the frontend and reach the resulting run detail
- a user can submit `Select best model` from the frontend and verify active selection status
- registry, runs, evaluations, and selections each have dedicated list and detail screens
- artifact fields and metrics are readable without relying on raw JSON alone
- Backtests and Opportunities continue to function without regression

## 10. Suggested Delivery Order
1. Freeze routes, contracts, and component ownership.
2. Add the route family and extract the workspace shell.
3. Build the dedicated model-admin API and type layer.
4. Deliver dashboard plus read-only list/detail pages.
5. Add train and select mutation flows.
6. Add cross-links, responsive polish, and success-state navigation.
7. Finish route tests, smoke tests, and release validation.

## 11. Out of Scope for This Plan
The following remain out of scope for the first release unless product direction changes:
- role-based auth redesign
- model editing or deletion workflows
- multi-user approval flows for promotion
- advanced experiment comparison dashboards beyond artifact detail views
