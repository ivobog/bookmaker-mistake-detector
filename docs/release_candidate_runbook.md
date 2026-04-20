# Release Candidate Runbook

## Purpose
This runbook is the Phase 5 starting point for validating the MVP as a release candidate.

It focuses on the workflows that now span:
- historical ingestion
- analytical feature generation
- predictive model selection
- market-board refresh and scoring cadence
- Phase 4 analyst backtest and opportunity review UX

Use it together with:
- [docs/release_acceptance_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/release_acceptance_checklist.md)
- [docs/manual_smoke_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/manual_smoke_checklist.md)
- [docs/known_issues.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/known_issues.md)
- [docs/phase5_operational_hardening.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/phase5_operational_hardening.md)

## Workflow Logging
- High-value operator workflows now emit structured JSON log lines through the `bookmaker_detector_api.workflow` logger.
- Current coverage includes:
  - model training and promotion
  - scoring preview, future-game preview/materialization, and future-slate preview/materialization
  - backtest execution
  - opportunity materialization
  - market-board refresh and refresh/scoring/cadence orchestration
  - fetch ingestion, fixture ingestion, and initial dataset bootstrap
- Each workflow log line includes:
  - `workflow_name`
  - `workflow_run_id`
  - `event`
  - `request_trace_id` for API-driven flows
  - `request_method`
  - `request_path`
  - key filter/context fields
  - `duration_ms` on success/failure
- When a release-candidate pass fails, capture both the response `X-Request-ID` and the matching `workflow_run_id` before retrying the workflow.

## Workflow Evidence Map
Use this map during manual smoke and release-candidate triage.

| Workflow area | Typical route / trigger | Response evidence to capture | Expected workflow family |
| --- | --- | --- | --- |
| Model training | `POST /api/v1/admin/models/train` | `X-Request-ID`, HTTP status, target task | `model_training.train` |
| Model promotion | `POST /api/v1/admin/models/select` | `X-Request-ID`, HTTP status, selection policy | `model_training.promote` |
| Score preview | `GET /api/v1/admin/models/score-preview` | `X-Request-ID`, HTTP status, filters | `model_scoring.preview` |
| Future game preview | `GET /api/v1/admin/models/future-game-preview` | `X-Request-ID`, HTTP status, scenario filters | `model_scoring.future_game_preview` |
| Future game materialization | `POST /api/v1/admin/models/future-game-preview/materialize` | `X-Request-ID`, HTTP status, scenario filters | `model_scoring.future_game_materialize` |
| Future slate preview | `POST /api/v1/admin/models/future-slate/preview` | `X-Request-ID`, HTTP status, slate label | `model_scoring.future_slate_preview` |
| Future slate materialization | `POST /api/v1/admin/models/future-slate/materialize` | `X-Request-ID`, HTTP status, slate label | `model_scoring.future_slate_materialize` |
| Opportunity materialization | `POST /api/v1/admin/models/opportunities/materialize` | `X-Request-ID`, HTTP status, target task | `model_opportunities.materialize` |
| Backtest run | `POST /api/v1/admin/models/backtests/run` | `X-Request-ID`, HTTP status, target task | `model_backtest.run` |
| Market-board refresh | `POST /api/v1/admin/models/market-board/refresh` | `X-Request-ID`, HTTP status, source name | `model_market_board.refresh` |
| Market-board refresh orchestration | `POST /api/v1/admin/models/market-board/orchestrate-refresh` | `X-Request-ID`, HTTP status, source name | `model_market_board.refresh_orchestration` |
| Market-board scoring orchestration | `POST /api/v1/admin/models/market-board/orchestrate-score` | `X-Request-ID`, HTTP status, source name | `model_market_board.scoring_orchestration` |
| Market-board cadence | `POST /api/v1/admin/models/market-board/orchestrate-cadence` | `X-Request-ID`, HTTP status, source name | `model_market_board.cadence_orchestration` |
| Fetch ingestion maintenance | `run_fetch_and_ingest(...)` or Phase 1 fetch demo | `X-Request-ID` when route-driven, job/page IDs | `ingestion.fetch_and_ingest` |
| Fixture ingestion maintenance | `run_fixture_ingestion(...)` | job/page IDs | `ingestion.fixture_ingestion` |
| Initial dataset bootstrap | `run_initial_production_dataset_load(...)` | team/target counts, final status | `ingestion.initial_dataset_load` |

## Failure Capture
When a smoke or release step fails:
1. Record the route or command that was executed.
2. Record the response HTTP status if applicable.
3. Record the response `X-Request-ID` header if applicable.
4. Record the expected workflow family from the map above.
5. Capture the matching `workflow_run_id` plus the `workflow_failed` or slow `workflow_succeeded` log event.
6. Only retry after that evidence is written into the smoke checklist or known-issues file.

## Schema Contract
- PostgreSQL schema ownership lives in `infra/postgres/init/`.
- The current migration authority is the ordered bootstrap SQL chain in `infra/postgres/init/`; Alembic is not in the repo yet.
- Normal API and worker execution must assume the schema already exists.
- Runtime DDL is no longer part of request handling or worker execution.
- `POSTGRES_ALLOW_RUNTIME_SCHEMA_MUTATION` now defaults to disabled in every environment.
- Only explicit bootstrap or demo-maintenance flows should opt into runtime schema mutation, and that opt-in should stay exceptional.
- `docker compose up --build` applies the init SQL only when the Postgres data directory is empty.
- If you point the stack at an existing database or reused volume, apply the SQL in `infra/postgres/init/` before starting postgres-backed services.
- Production backend startup now fails fast when required tables are missing, with an error that points back to `infra/postgres/init`.

## Default Regression Pass
From the repository root, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_phase5_regression.ps1
```

What it covers:
- backend Ruff checks
- backend pytest suite
- Python compile checks for backend and worker
- frontend typecheck
- frontend lint
- frontend production build

Current status:
- this automated regression pass is green on `main / faacd2c`
- treat that pass as necessary but not sufficient for Phase 5 closeout

Optional browser-route smoke:

```powershell
cd .\frontend
npm run test:smoke
```

This Playwright smoke pass is intended for the Phase 4 hash-routed analyst workflow. It is useful for
release-candidate validation, but it may be explicitly waived if the operator chooses to skip browser
execution for a given pass.

Initial production dataset bootstrap:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_initial_production_dataset_load.ps1 -SourceUrlTemplate "<provider-template>"
```

The loader reads the team and season scope from the reference tables by default, uses the last four
completed seasons when no explicit season filter is provided, and records one fetch-and-ingest job
per team-season target in Postgres.

Schema preflight before production-oriented validation:
- confirm `infra/postgres/init/` matches the database you are about to use
- if you are reusing a Docker volume or external Postgres instance, apply the SQL manually before boot
- start the backend and verify startup succeeds before running ingestion, scoring, or bootstrap flows
- treat a schema-readiness startup failure as an environment/setup issue, not as an application bug in the feature/model routes

## Acceptance Sequence
Run Phase 5 in this order:
1. Execute the regression script.
2. Complete the manual smoke checklist.
3. Record blockers or waivers in the known-issues file.
4. Make the release recommendation from the acceptance checklist.

## Suggested Manual Smoke Pass
After the regression script passes:

1. Start the stack with `docker compose up --build`.
2. Open the frontend at `http://localhost:5173`.
3. Confirm the API health route at `http://localhost:8000/api/v1/health`.
4. Confirm `GET /api/v1/admin/model-capabilities` returns the four Phase A regression tasks.
5. Confirm one persisted ingestion/diagnostics route such as `GET /api/v1/admin/ingestion/stats` or a diagnostics listing route.
6. Run one market-board refresh and one cadence/orchestration flow.
7. Run one backtest workflow and one opportunity-materialization workflow against the current Postgres-backed runtime.
8. Open:
   - one backtest run
   - one fold
   - one opportunity
   - one comparable case
   - one compare route
9. Confirm the compare route shows:
   - alignment summary
   - mismatch review
   - decision summary
10. If you want browser-backed route verification instead of manual clicking, run `npm run test:smoke`
   from [frontend](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend) after the stack is up.

As you complete the smoke pass:
- record pass/fail in `docs/manual_smoke_checklist.md`
- record `X-Request-ID`, workflow family, and `workflow_run_id` for any failed or unexpectedly slow step
- move blockers into `docs/known_issues.md`
- update the release decision section in `docs/release_acceptance_checklist.md`

## External Source Readiness
Before validating the live external source path:

- set `THE_ODDS_API_KEY`
- verify `THE_ODDS_API_*` values in `.env`
- confirm backend starts cleanly with those values present or absent

If no live key is available, treat the file-backed source and demo providers as the required MVP fallback.

## Release Gate Notes
The release candidate should not be marked ready until:
- regression script is green
- manual smoke pass is green or explicitly waived with documented rationale
- acceptance checklist is reviewed against the SRS
- no blocking ingestion, scoring, or UI navigation issues remain
- known issues are captured in `docs/known_issues.md`
