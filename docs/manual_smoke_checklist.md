# Manual Smoke Checklist

## Run Metadata
- Date: `2026-04-17 02:30:00 +02:00`
- Operator: `Codex`
- Branch / commit: `main / c5c8211`
- Environment: `local Docker Compose on Windows`
- Notes: `API-oriented smoke pass completed. Frontend root was validated, and a Playwright browser-smoke harness was added for the Phase 4 routes, but live browser execution was explicitly skipped for this release-candidate pass.`

## Result Keys
- `pass`
- `fail`
- `waived`
- `not-run`

## 1. Environment Bring-up
| Step | Result | Notes |
| --- | --- | --- |
| `docker compose up --build` completes | `pass` | Stack came up after clearing stale created containers left behind by a previous Compose run. |
| frontend loads at `http://localhost:5173` | `pass` | HTTP 200 from the frontend root. |
| backend health route responds | `pass` | `GET /api/v1/health` returned HTTP 200 with `{\"status\":\"ok\"}`. |
| backend starts cleanly with current `.env` | `pass` | Backend started successfully with current environment values. |

## 2. Historical Ingestion
| Step | Result | Notes |
| --- | --- | --- |
| Phase 1 demo route responds | `pass` | `GET /api/v1/admin/phase-1-demo` returned HTTP 200. |
| Phase 1 fetch demo responds | `not-run` | Not exercised in this slice. |
| failed-fetch demo records diagnostics cleanly | `not-run` | Not exercised in this slice. |
| ingestion stats endpoint returns expected payload shape | `not-run` | Not exercised in this slice. |
| data-quality issue view returns issues and filters correctly | `not-run` | Not exercised in this slice. |

## 3. Analytical Core
| Step | Result | Notes |
| --- | --- | --- |
| Phase 2 feature demo responds | `pass` | `GET /api/v1/admin/phase-2-feature-demo?repository_mode=in_memory` returned HTTP 200. |
| feature dataset/profile endpoints respond | `not-run` | Not exercised in this slice. |
| patterns endpoint returns grouped results | `not-run` | Not exercised in this slice. |
| comparables endpoint returns ranked examples | `not-run` | Not exercised in this slice. |
| evidence endpoint returns strength and recommendation | `not-run` | Not exercised in this slice. |

## 4. Predictive Workflow
| Step | Result | Notes |
| --- | --- | --- |
| model training route completes | `pass` | `POST /api/v1/admin/models/train?...` returned HTTP 200. |
| model selection route promotes an active model | `pass` | `POST /api/v1/admin/models/select?...` returned HTTP 200. |
| score preview returns scored cases | `not-run` | Not exercised in this slice. |
| opportunity materialization returns persisted opportunities | `pass` | `POST /api/v1/admin/models/opportunities/materialize?...` returned HTTP 200. |
| opportunity history returns surfaced artifacts | `not-run` | Not exercised in this slice. |

## 5. Market Board and Cadence
| Step | Result | Notes |
| --- | --- | --- |
| market-board source catalog loads | `pass` | `GET /api/v1/admin/models/market-board/sources` returned HTTP 200. |
| file-backed or demo refresh completes | `pass` | `POST /api/v1/admin/models/market-board/refresh?...source_name=demo_daily_lines_v1` returned HTTP 200. |
| refresh history records change summary and source run | `not-run` | Not exercised in this slice. |
| scoring queue shows a board lifecycle | `not-run` | Not exercised in this slice. |
| cadence/orchestration run completes end to end | `pass` | `POST /api/v1/admin/models/market-board/orchestrate-cadence?...` returned HTTP 200. |
| board operations page returns combined state | `not-run` | Not exercised in this slice. |

## 6. Backtesting
| Step | Result | Notes |
| --- | --- | --- |
| backtest run route completes | `pass` | `POST /api/v1/admin/models/backtests/run?...` returned HTTP 200. |
| backtest history returns recent runs | `not-run` | Not exercised in this slice. |
| fold summaries are present in run detail | `not-run` | Not exercised in this slice. |
| strategy metrics and ROI fields are visible | `not-run` | Not exercised in this slice. |

## 7. Frontend Analyst Workflow
| Step | Result | Notes |
| --- | --- | --- |
| backtests dashboard loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| one backtest run route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| one fold route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| opportunity queue loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| one opportunity detail route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| one comparable case route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| one provenance artifact route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| compare route loads | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |
| compare route shows alignment summary, mismatch review, and decision summary | `waived` | Browser validation was skipped for this release-candidate pass at operator request. |

## 8. External Source Validation
Use this section only if a real The Odds API key is configured.

| Step | Result | Notes |
| --- | --- | --- |
| backend starts with `THE_ODDS_API_*` configured | `waived` | No live API key was configured for this smoke pass. |
| external source appears in market-board sources | `waived` | Not validated with live credentials in this slice. |
| refresh using `the_odds_api_v4_nba` completes | `waived` | Not validated with live credentials in this slice. |
| source run is persisted | `waived` | Not validated with live credentials in this slice. |
| resulting board can be scored and surfaced | `waived` | Not validated with live credentials in this slice. |

## 9. Release Decision Summary
- Regression script run: `pass`
- Manual smoke result: `partial`
- Blocking issues logged in `docs/known_issues.md`: `yes`
- External source status: `waived`
- Frontend browser-route status: `waived`
- Release-candidate recommendation: `ready for an internal release candidate with explicit waivers; backend/API smoke is healthy, browser-route validation was skipped by request, and live external-source validation remains waived until a real API key is available.`
