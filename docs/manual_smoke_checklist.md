# Manual Smoke Checklist

## Status Note
- This checklist was refreshed on `2026-04-20` for the current Postgres-only runtime on `main / faacd2c`.
- The previous recorded smoke pass predated the Phase 1 to Phase 4 cutover and referenced removed demo/runtime-mode routes.
- Treat this document as the current Phase 5 checklist template until a fresh operator pass fills in the result columns.

## Run Metadata
- Date: `2026-04-20 04:58:23 +02:00`
- Operator: `Codex`
- Branch / commit: `main / faacd2c`
- Environment: `local Docker Compose on Windows with reused Postgres volume on port 5433`
- Notes: `Applied 024_phase1_multi_target_capability_schema.sql manually to the reused Postgres volume before restarting the backend. Seeded a persisted smoke dataset through /api/v1/test/reset and /api/v1/test/seed-e2e-dataset. Worker startup still fails because it imports retired ingestion-runner modules.`

## Result Keys
- `pass`
- `fail`
- `waived`
- `not-run`

## Workflow Log Capture
- For any failed or slow golden-path workflow, capture both the response `X-Request-ID` header and the matching `workflow_run_id` from the `bookmaker_detector_api.workflow` logger before retrying.
- Current structured-log coverage includes model training/promotion, scoring previews and future-slate materialization, backtests, opportunity materialization, market-board refresh/orchestration, and ingestion/bootstrap maintenance flows.

## Failure Evidence Template
When a step fails or is unexpectedly slow, append these notes in the step row:
- `route_or_command=<value>`
- `x_request_id=<value or n/a>`
- `workflow_family=<expected workflow family>`
- `workflow_run_id=<value or n/a>`
- `log_event=workflow_failed|slow_success`

## 1. Environment Bring-up
| Step | Result | Notes |
| --- | --- | --- |
| `docker compose up --build` completes | `pass` | Images rebuilt successfully and the stack started. A reused Postgres volume needed the missing Phase 1 capability SQL applied before the backend could come up. |
| frontend loads at `http://localhost:5173` | `pass` | HTTP 200 from the frontend root. |
| backend health route responds | `pass` | `GET /api/v1/health` returned HTTP 200 with `{\"status\":\"ok\",\"service\":\"bookmaker-detector-api\"}`. |
| backend starts cleanly with current `.env` | `pass` | Backend started after the documented schema preflight for the reused local database volume. |
| worker process starts cleanly | `fail` | `docker compose up --build` leaves `bookmaker-worker` exited because `worker/src/bookmaker_detector_worker/main.py` still imports removed `fetch_ingestion_runner` and `fixture_ingestion_runner` modules. |

## 2. Historical Ingestion and Diagnostics
| Step | Result | Notes |
| --- | --- | --- |
| ingestion stats endpoint returns expected payload shape | `pass` | `GET /api/v1/admin/ingestion/stats` returned HTTP 200. |
| data-quality issue view returns issues and filters correctly | `pass` | `GET /api/v1/admin/data-quality/issues` returned HTTP 200. |
| initial production dataset bootstrap can be invoked or explicitly waived | `waived` | No live production source-url template was configured for this local smoke pass. Persisted smoke data was seeded through `/api/v1/test/reset` and `/api/v1/test/seed-e2e-dataset` instead. Expected workflow family remains `ingestion.initial_dataset_load`. |
| capability endpoint returns the four Phase A tasks | `pass` | `GET /api/v1/admin/model-capabilities` returned HTTP 200 with the four enabled Phase A regression tasks. |

## 3. Analytical Core
| Step | Result | Notes |
| --- | --- | --- |
| feature dataset/profile endpoints respond | `pass` | `GET /api/v1/admin/features/dataset?team_code=LAL` and `GET /api/v1/admin/features/dataset/profile` both returned HTTP 200. |
| patterns endpoint returns grouped results | `pass` | `GET /api/v1/analyst/patterns?target_task=spread_error_regression` returned HTTP 200 with grouped patterns. |
| comparables endpoint returns ranked examples | `pass` | `GET /api/v1/analyst/comparables?target_task=spread_error_regression&canonical_game_id=1&team_code=LAL` returned HTTP 200. Omitting `team_code` still produced a 500 instead of a validation response; tracked in `docs/known_issues.md`. |
| evidence endpoint returns strength and recommendation | `pass` | `GET /api/v1/analyst/evidence?target_task=spread_error_regression&canonical_game_id=1&team_code=LAL` returned HTTP 200 with strength and recommendation payloads. Omitting `team_code` triggers the same known validation gap as comparables. |

## 4. Predictive Workflow
| Step | Result | Notes |
| --- | --- | --- |
| model training route completes for one Phase A task | `pass` | Exceeded the checklist: `POST /api/v1/admin/models/train` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_training.train`. |
| model selection route promotes an active model | `pass` | Exceeded the checklist: `POST /api/v1/admin/models/select` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_training.promote`. |
| score preview returns scored cases | `pass` | Exceeded the checklist: `GET /api/v1/admin/models/score-preview` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_scoring.preview`. |
| future game or future slate preview responds | `pass` | Exceeded the checklist: `GET /api/v1/admin/models/future-game-preview` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_scoring.future_game_preview`. |
| opportunity materialization returns persisted opportunities | `pass` | Exceeded the checklist: `POST /api/v1/admin/models/opportunities/materialize?team_code=LAL&limit=3` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_opportunities.materialize`. |
| opportunity history returns surfaced artifacts | `pass` | `GET /api/v1/admin/models/opportunities/history?target_task=spread_error_regression` returned HTTP 200. |

## 5. Market Board and Cadence
| Step | Result | Notes |
| --- | --- | --- |
| market-board source catalog loads | `pass` | `GET /api/v1/admin/models/market-board/sources` returned HTTP 200. |
| file-backed refresh completes | `pass` | `POST /api/v1/admin/models/market-board/refresh` returned HTTP 200 for `spread_error_regression` and `total_points_regression` using `source_name=demo_daily_lines_v1`. Expected workflow family: `model_market_board.refresh`. |
| refresh history records change summary and source run | `pass` | `GET /api/v1/admin/models/market-board/history?target_task=spread_error_regression&source_name=demo_daily_lines_v1` returned HTTP 200. |
| scoring queue shows a board lifecycle | `pass` | `GET /api/v1/admin/models/market-board/queue?target_task=spread_error_regression` returned HTTP 200. |
| cadence/orchestration run completes end to end | `pass` | `POST /api/v1/admin/models/market-board/orchestrate-cadence?target_task=spread_error_regression&source_name=demo_daily_lines_v1` returned HTTP 200. Expected workflow family: `model_market_board.cadence_orchestration`. |
| board operations page returns combined state | `not-run` | The smoke pass did not materialize a standalone board record for an operations-detail drill-through. Cadence dashboard and queue endpoints were exercised instead. |

## 6. Backtesting
| Step | Result | Notes |
| --- | --- | --- |
| backtest run route completes for one Phase A task | `pass` | Exceeded the checklist: `POST /api/v1/admin/models/backtests/run` returned HTTP 200 for all four Phase A tasks. Expected workflow family: `model_backtest.run`. |
| backtest history returns recent runs | `pass` | `GET /api/v1/analyst/backtests?target_task=spread_error_regression` returned HTTP 200. |
| fold summaries are present in run detail | `pass` | `GET /api/v1/analyst/backtests/1` returned HTTP 200 and the payload contained 11 fold summaries. |
| strategy metrics and ROI fields are visible | `pass` | Backtest detail exposed `review_threshold` and `candidate_threshold` strategy payloads, including an ROI field in the strategy result structure. |

## 7. Frontend Analyst Workflow
| Step | Result | Notes |
| --- | --- | --- |
| backtests dashboard loads | `pass` | Covered by Playwright `Phase 5 browser smoke › loads the core analyst routes in a real browser`. |
| one backtest run route loads | `pass` | Covered by the same Playwright smoke test. |
| one fold route loads | `pass` | Covered by the same Playwright smoke test. |
| opportunity queue loads | `pass` | Covered by the existing Playwright opportunity queue scope tests plus the core analyst smoke test. |
| one opportunity detail route loads | `pass` | Covered by the core analyst Playwright smoke test. |
| one comparable case route loads | `pass` | Covered by the core analyst Playwright smoke test. |
| one provenance artifact route loads | `pass` | Covered by the model admin artifact-detail Playwright smoke test. |
| compare route loads | `pass` | Covered by the core analyst Playwright smoke test. |
| compare route shows alignment summary, mismatch review, and decision summary | `pass` | Covered by the core analyst Playwright smoke test. |

## 8. External Source Validation
Use this section only if a real The Odds API key is configured.

| Step | Result | Notes |
| --- | --- | --- |
| backend starts with `THE_ODDS_API_*` configured | `waived` | No live API key was configured for this local smoke pass. |
| external source appears in market-board sources | `waived` | Live external-source validation was not exercised without credentials. |
| refresh using `the_odds_api_v4_nba` completes | `waived` | Live external-source validation was not exercised without credentials. |
| source run is persisted | `waived` | Live external-source validation was not exercised without credentials. |
| resulting board can be scored and surfaced | `waived` | Live external-source validation was not exercised without credentials. |

## 9. Release Decision Summary
- Regression script run: `pass` on `main / faacd2c`
- Manual smoke result: `partial`
- Blocking issues logged in `docs/known_issues.md`: `yes`
- External source status: `waived`
- Frontend browser-route status: `partial` (`npm run test:smoke` finished 4/5 passing; one model-admin mutation verification is still failing)
- Release-candidate recommendation: `not yet ready for formal Phase 5 closeout; the Postgres-backed API matrix is strong, but worker startup and the remaining browser mutation smoke failure still need resolution or an explicit waiver.`
