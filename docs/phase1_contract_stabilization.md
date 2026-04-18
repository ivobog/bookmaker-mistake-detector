# Phase 1 Contract Stabilization

## Purpose
This document records the current completion state of Phase 1 from the SDD execution plan: typed contract hardening and removal of hidden read-side effects from operator-facing routes.

Related artifacts:
- [docs/sdd_refactor_execution_plan.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\sdd_refactor_execution_plan.md)
- [docs/phase0_baseline_freeze.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\phase0_baseline_freeze.md)

## Phase 1 outcome

### Completed objectives
- added typed request/response schemas for the main analyst, admin model, admin scoring, and admin opportunity read surfaces
- removed hidden non-production seed/train/materialize/refresh behavior from operator-facing `GET` routes
- converted route tests to prove empty-by-default read behavior plus existing-record coverage via monkeypatched service calls
- reduced dead prep-helper surface left behind after the read-route hardening

### Current status
- analyst read routes are now read-only in non-production
- admin model/history/detail reads are now read-only in non-production
- admin scoring preview/history/detail reads are now read-only in non-production
- admin feature dataset/history/artifact reads are now read-only in non-production
- admin market-board history/queue/detail/dashboard reads are now read-only in non-production

## Route areas completed

### Analyst
- [backend/src/bookmaker_detector_api/api/analyst_backtests.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_backtests.py)
- [backend/src/bookmaker_detector_api/api/analyst_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_opportunities.py)
- [backend/src/bookmaker_detector_api/api/analyst_patterns.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_patterns.py)
- [backend/src/bookmaker_detector_api/api/analyst_trends.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_trends.py)

### Admin contracts
- [backend/src/bookmaker_detector_api/api/schemas/analyst.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\schemas\analyst.py)
- [backend/src/bookmaker_detector_api/api/schemas/admin.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\schemas\admin.py)
- [backend/src/bookmaker_detector_api/api/schemas/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\schemas\__init__.py)

### Admin model and opportunity reads
- [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_opportunity_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py)
- [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py)

## Remaining Phase 1-adjacent cleanup

### Still intentionally left in place
- explicit demo routes in [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py)
- mutation/orchestration prep helpers that are still required by `POST` routes in:
  - [backend/src/bookmaker_detector_api/api/admin_model_support.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_support.py)
  - [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py)

### Next recommended step
- move from read-surface hardening into Phase 2 service decomposition, starting with [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py)
- in parallel, remove hardcoded frontend demo defaults from [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts)

## Verification snapshot
- `python -m pytest backend/tests/test_admin_routes.py -q` -> `152 passed`
- `python -m pytest backend/tests/test_health.py backend/tests/test_models.py -q` -> `43 passed`
- `cmd /c npm run typecheck` -> passed

## Phase 1 exit assessment
- typed operator-facing contracts: complete for the major analyst/admin read surfaces
- hidden read-side effects outside production: removed for the major analyst/admin read surfaces
- route-level contract regression coverage: in place
- ready to enter Phase 2 service decomposition: yes
