# Phase 5 Operational Hardening

## Slice 1: Structured Workflow Logging Baseline

This first Phase 5 slice starts Workstream H from [docs/sdd_refactor_execution_plan.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\sdd_refactor_execution_plan.md) by adding a shared structured logging surface for the highest-value operator workflows.

Implementation notes:
- Added a shared workflow logging helper in [backend/src/bookmaker_detector_api/services/workflow_logging.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\workflow_logging.py).
- The helper emits JSON log lines through the `bookmaker_detector_api.workflow` logger with:
  - `workflow_name`
  - `workflow_run_id`
  - `event`
  - workflow-specific filter/context fields
  - `duration_ms` on success or failure
  - error type/message on failure
- Added first-wave instrumentation in [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py) for:
  - `model_backtest.run`
  - `model_opportunities.materialize`
  - `model_market_board.refresh`
  - `model_market_board.refresh_orchestration`
  - `model_market_board.scoring_orchestration`
  - `model_market_board.cadence_orchestration`

Why this slice comes first:
- These are the release-candidate golden paths that already anchor the smoke checklist.
- They benefit immediately from stable run IDs, status markers, and duration logging when a smoke pass or demo-maintenance run goes wrong.
- The change stays incremental: no logging stack migration, just standard-library JSON logs on the highest-value workflows.

Verification added in this slice:
- [backend/tests/test_models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_models.py) now proves the backtest, opportunity materialization, and market-board cadence paths emit structured workflow events with the expected counts and IDs.

Current Phase 5 status after this slice:
- Structured workflow logging exists for the main predictive/operator execution chain.
- Release docs can now reference a real logger and payload shape instead of Phase 5 intent only.
- Deeper request-correlation wiring and expanded smoke/release doc cleanup still remain for the next slices.

Next recommended Phase 5 slice:
1. Extend workflow logging to training, selection promotion, scoring preview/materialization, and ingestion/bootstrap jobs.
2. Refresh [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md) and [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md) so they validate stable workflows and expected log evidence instead of older phase/demo phrasing.
