# Phase 0: Model Admin Contract and UX Mapping

## Purpose
This document completes Phase 0 for the `Model Admin / Training` workspace by translating the supplied SRS and SDD into repo-specific implementation decisions, file ownership, and a ranked backlog.

Source documents:
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_srs_model_admin_training_workspace.docx`
- `C:\Users\Ivica\Downloads\bookmaker_mistake_detector_sdd_model_admin_training_workspace.docx`

Date of analysis:
- `2026-04-19`

## 1. Current Baseline

### Frontend baseline
Relevant current files:
- `frontend/src/App.tsx`
- `frontend/src/appTypes.ts`
- `frontend/src/appUtils.ts`
- `frontend/src/api/models.ts`
- `frontend/src/backtestsWorkspace.tsx`
- `frontend/src/opportunitiesWorkspace.tsx`
- `frontend/src/appSharedComponents.tsx`
- `frontend/src/appArtifactDetailComponents.tsx`
- `frontend/src/styles.css`

Observed behavior:
- the app already uses hash-based routing through `parseRouteFromHash(...)` and `routeHash(...)`
- supported workspaces today are `Backtests` and `Opportunities`
- `App.tsx` already owns route parsing, initial data loading, route-driven detail fetching, and top-level rendering
- the frontend already consumes model-admin read endpoints for provenance detail, but only as supporting detail inside other workspaces
- no `Model Admin / Training` route family exists today
- no modal, dialog, drawer, or side-panel UI primitive appears to exist in the current frontend

### Backend baseline
Relevant current files:
- `backend/src/bookmaker_detector_api/api/admin_model_routes.py`
- `backend/src/bookmaker_detector_api/api/schemas/admin.py`
- `backend/src/bookmaker_detector_api/services/models.py`
- `backend/tests/test_admin_routes.py`
- `backend/tests/test_models.py`

Observed behavior:
- backend route coverage already exists for `train`, `select`, `registry`, `runs`, `summary`, `history`, `evaluations`, `evaluations/history`, `selections`, `selections/history`, and detail routes
- backend tests already cover much of the model-admin read and mutation surface
- backend schemas still use broad `dict[str, Any]` payloads for several summary and history responses, which is workable but not ideal for frontend typing

## 2. Confirmed Route Model for v1
This phase locks the route map for implementation.

### New route family
- `#/models`
- `#/models/registry`
- `#/models/runs`
- `#/models/runs/:runId`
- `#/models/evaluations`
- `#/models/evaluations/:id`
- `#/models/selections`
- `#/models/selections/:id`

### Explicit route decisions
- `Train model` and `Select best model` will not get dedicated routes in v1
- action flows should open inside the workspace shell as lightweight inline action panels or newly added dialog components
- because the repo does not currently contain a modal/dialog system, the lowest-risk v1 path is an inline panel or card-based action surface inside the dashboard header area
- registry detail will be handled as inline expansion or a detail card on `#/models/registry`, not a separate route

## 3. Screen-to-Code Mapping

| SRS screen or action | Locked route | Frontend owner | Backend dependency | Status |
| --- | --- | --- | --- | --- |
| Workspace landing dashboard | `#/models` | new `modelAdminWorkspace.tsx` | `GET /api/v1/admin/models/summary`, `GET /api/v1/admin/models/history`, `GET /api/v1/admin/models/evaluations/history`, `GET /api/v1/admin/models/selections/history` | missing frontend |
| Registry list | `#/models/registry` | new `modelAdminPages.tsx` | `GET /api/v1/admin/models/registry` | missing frontend |
| Registry inline detail | `#/models/registry` | new `modelAdminDetailComponents.tsx` | `GET /api/v1/admin/models/registry` payload | missing frontend |
| Runs list | `#/models/runs` | new `modelAdminPages.tsx` | `GET /api/v1/admin/models/runs` | missing frontend |
| Run detail | `#/models/runs/:runId` | new `modelAdminDetailComponents.tsx` | `GET /api/v1/admin/models/runs/{run_id}` | missing frontend |
| Evaluations list | `#/models/evaluations` | new `modelAdminPages.tsx` | `GET /api/v1/admin/models/evaluations` | missing frontend |
| Evaluation detail | `#/models/evaluations/:id` | new `modelAdminDetailComponents.tsx` | `GET /api/v1/admin/models/evaluations/{snapshot_id}` | missing frontend |
| Selections list | `#/models/selections` | new `modelAdminPages.tsx` | `GET /api/v1/admin/models/selections` | missing frontend |
| Selection detail | `#/models/selections/:id` | new `modelAdminDetailComponents.tsx` | `GET /api/v1/admin/models/selections/{selection_id}` | missing frontend |
| Train model action | inline workspace action | new `modelAdminActions.tsx` | `POST /api/v1/admin/models/train` | missing frontend |
| Select best model action | inline workspace action | new `modelAdminActions.tsx` | `POST /api/v1/admin/models/select` | missing frontend |

## 4. Reuse Plan

### Existing frontend pieces to reuse
- `StatTile` from `frontend/src/appSharedComponents.tsx`
- `ProvenanceRibbon` from `frontend/src/appSharedComponents.tsx`
- artifact presentation patterns from `frontend/src/appArtifactDetailComponents.tsx`
- master-detail layout patterns from `frontend/src/backtestsWorkspace.tsx`
- list/detail split patterns from `frontend/src/opportunitiesWorkspace.tsx`
- current global styling tokens and layout classes in `frontend/src/styles.css`

### Existing backend pieces to reuse
- all current admin model endpoints in `backend/src/bookmaker_detector_api/api/admin_model_routes.py`
- current admin schema models in `backend/src/bookmaker_detector_api/api/schemas/admin.py`
- existing route contract tests in `backend/tests/test_admin_routes.py`

### Reuse boundaries
- existing provenance detail components should be reused for formatting ideas and shared sub-sections, not copied wholesale into `App.tsx`
- the new workspace should get its own API and type layer rather than stretching `frontend/src/api/models.ts` and `frontend/src/appTypes.ts` into another mixed-purpose surface

## 5. Contract Findings

### Confirmed endpoint support for SRS v1
- dashboard data can be assembled from existing `summary`, `history`, `evaluations/history`, and `selections/history` endpoints
- registry list is supported
- runs list and run detail are supported
- evaluations list and evaluation detail are supported
- selections list and selection detail are supported
- train mutation is supported
- select mutation is supported

### Frontend and backend contract mismatches

#### Required for v1
- `frontend/src/appTypes.ts` models `SelectionSnapshot.rationale` as `string | null`, but the backend schema defines `rationale` as an object payload
- current frontend types do not include registry list response shapes
- current frontend types do not include dedicated dashboard summary/history shapes for model admin
- current frontend route union in `frontend/src/appTypes.ts` has no `models` route family
- current frontend route parser and serializer in `frontend/src/appUtils.ts` have no `models` support
- current frontend API layer has no dedicated helpers for:
  - `fetchModelRegistry`
  - `fetchModelRuns`
  - `fetchModelSummary`
  - `fetchEvaluations`
  - `fetchEvaluationHistory`
  - `fetchSelections`
  - `fetchSelectionHistory`
  - `trainModels`
  - `selectBestModel`

#### Important but not blocking
- backend summary and history responses are exposed as broad dictionaries, which will require frontend mappers or local types
- the backend exposes more filter fields than the SRS requires for some list endpoints, so the v1 frontend should intentionally limit visible filters to the product scope
- the current frontend types allow some nullable IDs where backend schema models mark them as required; this is not an immediate blocker but should be normalized in the dedicated model-admin type layer

#### Nice to have
- dashboard could benefit from a single aggregated overview endpoint later if concurrent requests prove too chatty
- mutation responses would be easier to consume if they guaranteed explicit `latest_run_id` or `latest_selection_id` fields rather than requiring derivation from payload ordering

## 6. UX Decisions Locked in Phase 0

### Navigation
- `Model Admin / Training` becomes a first-class shell destination alongside `Backtests` and `Opportunities`
- the landing route for the application remains unchanged
- model-admin list and detail pages must be deep-linkable

### Detail behavior
- runs, evaluations, and selections get dedicated detail routes
- registry does not get a dedicated detail route in v1
- medium and large layouts should preserve a master-detail feel where practical

### Action behavior
- `Train model` and `Select best model` stay within the dashboard shell instead of becoming routes
- because no modal system exists today, Phase 1 should implement an inline action panel first unless a dialog primitive is added intentionally
- submit buttons must be disabled while mutation requests are in flight

### Filter scope for v1
The visible v1 filters should match the SRS, even if more are technically supported by the backend:
- registry: `target_task`
- runs: `target_task`, `team_code`, `season_label`
- evaluations: `target_task`, `model_family`
- selections: `target_task`, `active_only`

## 7. Recommended File Ownership

### New frontend files
- `frontend/src/modelAdminWorkspace.tsx`
- `frontend/src/modelAdminPages.tsx`
- `frontend/src/modelAdminDetailComponents.tsx`
- `frontend/src/modelAdminActions.tsx`
- `frontend/src/modelAdminTypes.ts`
- `frontend/src/api/modelAdmin.ts`

### Existing frontend files to change
- `frontend/src/App.tsx`
- `frontend/src/appTypes.ts`
- `frontend/src/appUtils.ts`
- `frontend/src/api/index.ts`
- `frontend/src/styles.css`

### Existing backend files to watch
- `backend/src/bookmaker_detector_api/api/admin_model_routes.py`
- `backend/src/bookmaker_detector_api/api/schemas/admin.py`
- `backend/tests/test_admin_routes.py`

## 8. Ranked Phase 1 Backlog

### P0
- add the `models` route family to `AppRoute`
- extend `parseRouteFromHash(...)` and `routeHash(...)`
- add shell navigation for `Model Admin / Training`
- create `modelAdminWorkspace.tsx` and move all model-admin rendering there
- create `frontend/src/api/modelAdmin.ts`
- add dedicated model-admin types for dashboard, registry, runs, evaluations, selections, and mutation payloads
- normalize `selection.rationale` typing in the new model-admin type layer

### P1
- add read-only dashboard sections backed by concurrent calls
- add registry, runs, evaluations, and selections list pages
- add detail cards for runs, evaluations, and selections
- add inline empty, loading, and error states

### P2
- add train and select action panels
- add direct post-success navigation links
- add cross-links back to Backtests and Opportunities provenance where identifiers line up

## 9. Test Impact

### Existing coverage to preserve
- backend admin route tests in `backend/tests/test_admin_routes.py`
- backend model behavior tests in `backend/tests/test_models.py`
- browser smoke in `frontend/e2e/phase5-smoke.spec.ts`

### New tests implied by the SRS/SDD
- route parsing tests for the new `models` route family
- smoke coverage for `#/models`, `#/models/runs`, `#/models/evaluations`, and `#/models/selections`
- mutation smoke for `Train model` and `Select best model`

### Tooling note
- the frontend currently has Playwright smoke coverage but no unit/component test runner in `package.json`
- Phase 1 can proceed without adding a new test runner immediately, but Phase 6 should either add one or keep the acceptance bar explicitly browser-smoke-based

## 10. Phase 0 Exit Check
Phase 0 is considered complete because the following are now explicit:
- the exact v1 route map is locked
- the non-route handling of `Train model` and `Select best model` is locked
- each SRS screen and action has a frontend owner and backend dependency
- reuse candidates are identified
- v1 blockers and contract mismatches are identified
- the next implementation backlog is ranked

## 11. Recommended Next Step
Start Phase 1 by implementing the route family and extracting the new workspace shell before adding any list or mutation logic. That keeps the routing model stable and prevents more feature-specific branching from being added directly into `frontend/src/App.tsx`.
