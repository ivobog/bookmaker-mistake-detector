# Playwright Real E2E Integration Summary

## Outcome

Integrated a real-stack Playwright suite under `frontend/e2e-real/` and verified it green against the Docker Compose application stack (`postgres`, `backend`, `worker`, `frontend`) using the live GUI, live backend APIs, and direct Postgres assertions.

Verified command:

```bash
cd frontend
npm run test:e2e:real
```

Latest local result: `8 passed`.

## Files changed

### Backend

- `backend/src/bookmaker_detector_api/config.py`
- `backend/src/bookmaker_detector_api/api/__init__.py`
- `backend/src/bookmaker_detector_api/api/test_routes.py`
- `docker-compose.yml`
- `.env.example`

### Frontend app / selectors

- `frontend/src/App.tsx`
- `frontend/src/modelAdminActions.tsx`
- `frontend/src/modelAdminPages.tsx`
- `frontend/src/modelAdminDetailComponents.tsx`
- `frontend/src/backtestsWorkspace.tsx`
- `frontend/src/opportunitiesWorkspace.tsx`
- `frontend/src/appOpportunityDetailComponents.tsx`

### Frontend Playwright

- `frontend/package.json`
- `frontend/package-lock.json`
- `frontend/playwright.config.e2e.ts`
- `frontend/e2e-real/helpers/api.ts`
- `frontend/e2e-real/helpers/db.ts`
- `frontend/e2e-real/helpers/ui.ts`
- `frontend/e2e-real/fixtures/appHarness.ts`
- `frontend/e2e-real/tests/00-stack-health.spec.ts`
- `frontend/e2e-real/tests/10-model-admin-registry.spec.ts`
- `frontend/e2e-real/tests/20-model-admin-train.spec.ts`
- `frontend/e2e-real/tests/30-model-admin-select.spec.ts`
- `frontend/e2e-real/tests/40-backtests-real.spec.ts`
- `frontend/e2e-real/tests/50-opportunities-scope.spec.ts`
- `frontend/e2e-real/tests/60-tree-stump-explainability.spec.ts`
- `frontend/e2e-real/tests/90-after-each-invariants.spec.ts`

## Dependencies added

- Frontend dev dependency: `pg`

## Scripts added

- `test:e2e:real`
- `test:e2e:real:headed`

## Seed / reset strategy

Used guarded non-production backend test helpers, enabled only when:

- `API_ENV != production`
- `API_ENABLE_TEST_HELPERS=true`

Added endpoints:

- `POST /api/v1/test/reset`
- `POST /api/v1/test/seed-minimal-dataset`
- `POST /api/v1/test/seed-e2e-dataset`
- `POST /api/v1/test/materialize-baseline-features`
- `POST /api/v1/test/activate-selection`

The Playwright harness now uses `seed-e2e-dataset`, which deterministically ingests four fixture-backed team pages (`LAL`, `DAL`, `CHI`, `PHX`) for `2024-2025` and materializes baseline features. This richer deterministic dataset is what makes the tree-stump explainability path reliably exercise a real split instead of a fallback stump.

## Test files added / coverage

- `00-stack-health`: frontend, backend, seeded DB, key pages, fatal UI error smoke
- `10-model-admin-registry`: registry page, DB row count match, filtering
- `20-model-admin-train`: GUI train action, persisted `model_training_run` and `model_evaluation_snapshot`
- `30-model-admin-select`: GUI selection action, active `model_selection_snapshot` uniqueness
- `40-backtests-real`: GUI backtest action, persisted `model_backtest_run`, fold rendering, payload consistency
- `50-opportunities-scope`: scoped vs global queue semantics, stale/scoped protection
- `60-tree-stump-explainability`: active `tree_stump` explainability card and repeated leaf predictions
- `90-after-each-invariants`: health plus DB invariants after mutating flows

## DB assertions implemented

Direct SQL checks cover:

- `model_registry`
- `model_training_run`
- `model_evaluation_snapshot`
- `model_selection_snapshot`
- `model_opportunity`
- `model_backtest_run`

After each mutating test, invariants verify:

- backend health remains OK
- no duplicate active selection for the target task
- no orphan evaluation snapshots
- no orphan selection snapshots
- no mixed queue scope metadata within a materialization batch
- operator-scoped opportunities keep `materialization_scope_key = 'operator-wide'`

## Selectors / testids introduced

### App shell / navigation

- `app-shell`
- `model-admin-nav`
- `nav-backtests`
- `nav-opportunities`
- `nav-model-admin`
- `run-backtest-button`
- `open-model-dashboard-button`
- `opportunities-refresh-button`

### Model admin

- `model-admin-train-action`
- `model-admin-select-action`
- `train-model-form`
- `train-feature-key`
- `train-target-task`
- `train-team-code`
- `train-season-label`
- `train-train-ratio`
- `train-validation-ratio`
- `train-submit`
- `select-model-form`
- `select-target-task`
- `select-policy-name`
- `select-submit`
- `model-admin-summary`
- `model-admin-registry-tab`
- `model-admin-runs-tab`
- `model-admin-evaluations-tab`
- `model-admin-selections-tab`
- `registry-table`
- `runs-table`
- `evaluations-table`
- `selections-table`
- row ids for registry / runs / evaluations / selections
- `registry-detail-card`
- `run-detail-card`
- `evaluation-detail-card`
- `selection-detail-card`

### Backtests

- `backtests-page`
- `backtests-history-table`
- `backtest-fold-grid`
- per-fold cards like `backtest-fold-card-<foldIndex>`

### Opportunities / explainability

- `opportunities-page`
- `opportunity-row-<id>`
- `opportunity-detail-card`
- `opportunities-queue-scope-panel`
- `opportunities-queue-scope-badge`
- `opportunities-queue-scope-label`
- `opportunities-queue-batch-id`
- `opportunities-queue-materialized-at`
- `stump-explainability-card`
- `stump-selected-feature`
- `stump-threshold`
- `stump-left-prediction`
- `stump-right-prediction`

## Commands to run the suite

### 1. Start the real stack

```bash
docker compose up -d --build
```

### 2. Run the real Playwright suite

```bash
cd frontend
npm run test:e2e:real
```

### 3. Optional headed run

```bash
cd frontend
npm run test:e2e:real:headed
```

## Environment used by the suite

Frontend / Playwright:

- `E2E_BASE_URL` default: `http://127.0.0.1:5173`
- `E2E_API_BASE_URL` default: `http://127.0.0.1:8000`
- `E2E_FEATURE_KEY` default: `baseline_team_features_v1`
- `E2E_TARGET_TASK` default: `spread_error_regression`
- `E2E_TEAM_CODE` default: `LAL`
- `E2E_SEASON_LABEL` default: `2024-2025`

Postgres helpers:

- `E2E_PGHOST` default: `127.0.0.1`
- `E2E_PGPORT` default: `5433`
- `E2E_PGDATABASE` default: `bookmaker_detector`
- `E2E_PGUSER` default: `bookmaker`
- `E2E_PGPASSWORD` default: `bookmaker`

Backend test-helper gating:

- `API_ENABLE_TEST_HELPERS=true`

## Known limitations

- Backtest fold selection metadata is asserted against the persisted `model_backtest_run.payload_json`, which is the real persisted contract today. The suite does not assume that every fold also creates a standalone `model_evaluation_snapshot` row, because the live app does not currently guarantee that.
- The Docker `worker` service exits cleanly after startup in this environment; the real E2E suite still passes because the tested flows are exercised through the live backend/frontend stack and persisted Postgres state.
