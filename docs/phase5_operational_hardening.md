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

## Slice 2: Expanded Training, Scoring, and Ingestion Traceability

This slice extends the same structured logging boundary across the remaining high-value operator and maintenance workflows that still needed stable run IDs.

Implementation notes:
- Added workflow spans in [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py) for:
  - `model_training.train`
  - `model_training.promote`
  - `model_scoring.preview`
  - `model_scoring.future_game_preview`
  - `model_scoring.future_game_materialize`
  - `model_scoring.future_slate_preview`
  - `model_scoring.future_slate_materialize`
- Added workflow spans in:
  - [backend/src/bookmaker_detector_api/services/fetch_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\fetch_ingestion_runner.py) for `ingestion.fetch_and_ingest`
  - [backend/src/bookmaker_detector_api/services/fixture_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\fixture_ingestion_runner.py) for `ingestion.fixture_ingestion`
  - [backend/src/bookmaker_detector_api/services/initial_dataset_load.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\initial_dataset_load.py) for `ingestion.initial_dataset_load`
- Success events now capture key workflow outputs such as persisted run counts, selected snapshot IDs, prediction/materialization counts, target counts, and ingestion job/page IDs.
- Failure events now exist for fetch/fixture/bootstrap ingestion paths as well, so smoke and release triage can recover a `workflow_run_id` even when the workflow returns a handled failure payload.

Verification added in this slice:
- [backend/tests/test_models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_models.py) now covers training, promotion, scoring preview, future-game materialization, and future-slate flows.
- [backend/tests/test_fetch_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_fetch_ingestion_runner.py) now covers both success and failure workflow events.
- [backend/tests/test_initial_dataset_load.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_initial_dataset_load.py) now covers the bootstrap dataset-load workflow event.
- Added [backend/tests/test_fixture_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_fixture_ingestion_runner.py) for fixture-ingestion success/failure workflow events.

Current Phase 5 status after Slice 2:
- Structured workflow logging now covers the main model lifecycle, future scoring/materialization flows, ingestion maintenance flows, and the earlier backtest/opportunity/market-board golden paths.
- Manual smoke and release-candidate runs can rely on a consistent `workflow_run_id` across both operator workflows and bootstrap/demo-maintenance jobs.
- The remaining Phase 5 work is now more about correlation/runbook tightening than basic workflow visibility.

Next recommended Phase 5 slice:
1. Add request-correlation or header-propagated trace IDs so route-level retries and backend workflow spans can be tied together more directly.
2. Expand release/runbook guidance from "capture the workflow run ID" to "capture the expected workflow family and success/failure evidence" for the full release-candidate checklist.

## Slice 3: Request-Correlated Workflow Tracing

This slice connects HTTP requests to the existing workflow spans so operator retries and release triage can move from "find the right workflow" to "follow this exact request."

Implementation notes:
- Added lightweight request-correlation middleware in [backend/src/bookmaker_detector_api/main.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\main.py).
- Each HTTP request now:
  - reuses an inbound `X-Request-ID` header when present
  - generates a new request trace ID when the header is absent
  - echoes the effective `X-Request-ID` back on the response
- Added request-context propagation in [backend/src/bookmaker_detector_api/services/workflow_logging.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\workflow_logging.py) using a request-scoped context variable.
- Workflow events emitted during API requests now automatically include:
  - `request_trace_id`
  - `request_method`
  - `request_path`

Verification added in this slice:
- [backend/tests/test_admin_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_admin_routes.py) now proves an admin route can inject `X-Request-ID`, receive it back on the response, and observe the same trace ID on the emitted `model_training.train` workflow event.

Current Phase 5 status after Slice 3:
- Workflow spans are now correlated across operator routes, scoring/training services, and maintenance ingestion jobs.
- API-level smoke failures can be triaged using both `X-Request-ID` and `workflow_run_id`.
- The remaining Phase 5 work is mostly runbook/operability refinement rather than missing traceability primitives.

Next recommended Phase 5 slice:
1. Tighten release/runbook guidance around expected workflow families and response headers for each smoke step.
2. Add a small diagnostic endpoint or log-search aid only if operators need a simpler way to pivot from `X-Request-ID` to workflow evidence during manual smoke passes.

## Slice 4: Runbook Evidence Hardening

This slice turns the new tracing primitives into concrete operating guidance, so release-candidate runs have a standard evidence trail instead of ad hoc note taking.

Implementation notes:
- Updated [docs/release_candidate_runbook.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_candidate_runbook.md) with:
  - current workflow-coverage scope
  - a workflow-evidence map from route/trigger to expected workflow family
  - an explicit failure-capture sequence using both `X-Request-ID` and `workflow_run_id`
- Updated [docs/manual_smoke_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\manual_smoke_checklist.md) with:
  - a reusable failure-evidence template
  - expected workflow families on the highest-value smoke steps
- Updated [docs/release_acceptance_checklist.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_acceptance_checklist.md) so the manual smoke path now explicitly depends on workflow-family and trace-evidence capture.

Current Phase 5 status after Slice 4:
- Operator-facing and maintenance workflows are instrumented.
- API requests are correlated with workflow spans through `X-Request-ID`.
- The runbook now tells operators exactly what evidence to capture for the main release-candidate routes.

Next recommended Phase 5 slice:
1. Add a lightweight diagnostic or support surface that helps operators pivot from `X-Request-ID` to the related workflow log lines during manual smoke.
2. If that is unnecessary, Phase 5 is close to a clean closeout and the next move becomes commit/push plus any final release-candidate execution pass.
