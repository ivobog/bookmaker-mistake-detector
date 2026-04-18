# Phase 0 Baseline Freeze

## Purpose
This document completes Phase 0 from the SDD execution plan: establish a reliable baseline before deeper refactoring.

Phase 0 outputs required by the plan:
- route inventory
- runtime schema-mutation inventory
- demo and hidden-side-effect inventory
- baseline regression signal
- initial backlog for Phase 1 entry

Related artifacts:
- [docs/release/api_surface_inventory.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release\api_surface_inventory.md)
- [docs/sdd_refactor_execution_plan.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\sdd_refactor_execution_plan.md)
- [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md)
- [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md)

## Baseline status

### Completed in this phase
- inventoried currently mounted routes from the split router tree
- identified runtime schema checks versus runtime schema mutation points
- identified non-production seeded/demo side effects in read-shaped routes
- recorded current regression anchors and ran a lightweight baseline validation

### Not changed in this phase
- no business logic refactor
- no route contract changes
- no schema changes
- no service/module extraction

## Current architecture freeze summary

### Route topology
- Current mounted route tree lives in [backend/src/bookmaker_detector_api/api/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\__init__.py).
- Total current route count: `82`
- Health routes: `1`
- Analyst routes: `8`
- Admin read routes: `49`
- Admin mutation routes: `16`
- Dev-only/demo routes: `8`

### Main structural hotspots confirmed
- Oversized backend orchestration module: [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py)
- Oversized repository module with schema mutation behavior: [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py)
- Oversized frontend shell: [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx)
- Frontend API layer with hardcoded demo defaults: [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts)

## Runtime schema ownership findings

### Safe runtime schema check to keep
- [backend/src/bookmaker_detector_api/db/postgres.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\db\postgres.py) performs a fail-fast readiness check through `ensure_required_postgres_schema(...)`.
- This check verifies table existence and does not mutate schema.
- This behavior matches the SDD guidance and should remain.

### Runtime schema mutation to remove from normal flows
- [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) still performs request-time schema mutation in:
  - `_ensure_raw_row_source_identity_schema()`
  - `_ensure_data_quality_issue_identity_schema()`
- Current runtime DDL includes:
  - `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
  - `UPDATE ...` data backfill coupled to schema evolution
  - `CREATE UNIQUE INDEX IF NOT EXISTS`
  - duplicate-row cleanup before index creation

### Phase 0 decision
- Keep readiness verification in `db/postgres.py`.
- Treat the `ingestion.py` schema helpers as Phase 1/2 removal targets.
- Move all schema evolution into bootstrap SQL or formal migrations before calling the architecture hardened.

## Hidden-side-effect inventory

### Analyst routes that are not yet side-effect-free outside production
- [backend/src/bookmaker_detector_api/api/analyst_backtests.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_backtests.py)
  - seeds in-memory feature data
  - runs a backtest on `GET` outside production
- [backend/src/bookmaker_detector_api/api/analyst_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_opportunities.py)
  - seeds in-memory data
  - trains models
  - promotes a best model
  - materializes opportunities or future opportunities on `GET` outside production

### Admin read routes with seeded fallback behavior outside production
- [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py)
  - `GET` diagnostics routes call `get_admin_diagnostics(..., seed_demo=True)` outside production
- [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py)
  - multiple `GET` routes prepare seeded in-memory feature repositories outside production
- [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py)
  - history and detail reads still rely on helper-prepared in-memory repositories outside production
- [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py)
  - queue/history/detail reads still use materialized seeded repositories outside production

### Explicit demo-only surfaces
- [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py)
- `GET /api/v1/admin/phase-2-feature-demo` in [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py)
- Demo market-board sources in [backend/src/bookmaker_detector_api/services/model_market_board_sources.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_market_board_sources.py)

### Frontend demo coupling
- [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts) still hardcodes:
  - `team_code=LAL`
  - `season_label=2024-2025`
  - `canonical_game_id=3`
  - scenario defaults such as `LAL` vs `BOS`
- The client also removes `auto_*_demo` flags before some mutations, which is a sign that demo coupling is still shaping the transport layer.

## Baseline validation executed

### Executed now
- `pytest backend/tests/test_health.py backend/tests/test_models.py -q`
  - result: `43 passed in 4.22s`
- `npm run typecheck` in [frontend](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend)
  - result: passed

### Existing smoke anchors retained
- [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md)
- [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md)
- [frontend/e2e/phase5-smoke.spec.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\e2e\phase5-smoke.spec.ts)

## Phase 1 entry backlog

### Contract hardening
1. Add typed request/response schemas for backtest, opportunity, scoring, and model-history flows.
2. Replace manual dict assembly in route files with schema-backed responses.
3. Add contract tests for high-value analyst/admin routes.

### Side-effect removal
1. Make analyst `GET` routes read-only in every environment.
2. Move seeded/demo preparation out of admin read routes.
3. Keep demo fixtures behind explicit demo-only boundaries.

### Schema ownership
1. Remove runtime DDL helpers from [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py).
2. Decide whether the next step is bootstrap-SQL-only cleanup or immediate Alembic adoption.

### Frontend stabilization
1. Remove hardcoded demo filters from [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts).
2. Define explicit demo mode versus operator mode behavior.

## Phase 0 exit assessment
- Route inventory: complete
- Runtime schema-mutation inventory: complete
- Demo/side-effect inventory: complete
- Baseline regression signal: complete
- Ready to enter Phase 1: yes

## Recommended next implementation slice
The lowest-risk next step is to begin Phase 1 with typed schemas and analyst-route side-effect removal for:
1. `/api/v1/analyst/backtests`
2. `/api/v1/analyst/backtests/{backtest_run_id}`
3. `/api/v1/analyst/opportunities`
4. `/api/v1/analyst/opportunities/{opportunity_id}`

These routes are already the clearest operator-facing surface, and they currently carry the most obvious gap versus the SDD acceptance criteria.
