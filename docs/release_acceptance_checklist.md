# Release Acceptance Checklist

## Purpose
This checklist is the Phase 5 acceptance matrix for deciding whether the MVP is ready for internal release-candidate use.

Use it together with:
- [docs/release_candidate_runbook.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/release_candidate_runbook.md)
- [docs/manual_smoke_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/manual_smoke_checklist.md)
- [docs/known_issues.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/known_issues.md)
- [C:/Users/Ivica/Downloads/bookmaker_mistake_detector_srs.md](C:/Users/Ivica/Downloads/bookmaker_mistake_detector_srs.md)

## Status Keys
- `pass`: delivered and verified enough for MVP use
- `manual-check`: implemented, but needs explicit release-candidate smoke validation
- `deferred`: intentionally out of MVP scope
- `issue`: expected for MVP but currently blocked

## Product Scope
| Area | Status | Notes |
| --- | --- | --- |
| NBA-only scope | `pass` | Current sources, features, and UI flows are NBA-focused. |
| Regular season focus | `pass` | Phase 1 ingestion filters regular season rows. |
| Last 4 completed seasons as core dataset | `manual-check` | Implemented in architecture and seed/demo flows; validate the concrete loaded seasons in the release smoke pass. |
| Spread and total market analysis | `pass` | Metrics, features, evidence, opportunities, and backtests all include spread/total context. |
| Decision-support only, not bet execution | `pass` | No bet placement or execution workflow exists. |

## SRS Feature Acceptance Matrix

### 1. Historical ingestion and raw storage
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Team-season retrieval and batch ingestion | `pass` | Phase 1 ingestion pipeline, worker flow, and admin demos. |
| Retrieval metadata and raw payload preservation | `pass` | `page_retrieval`, raw payload snapshots, retrieval trends, source runs. |
| Regular-season section filtering | `pass` | Phase 1 parser and canonical ingestion flow. |
| Row parsing for venue, score, ATS, O/U | `pass` | Raw row parsing and quality issue capture are implemented. |
| Idempotent raw persistence and parse diagnostics | `pass` | Repository-backed ingestion, issue tracking, admin diagnostics. |

### 2. Canonical normalization and metrics
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Reciprocal row matching into canonical games | `pass` | Canonical reconciliation pipeline and conflict statuses. |
| Single canonical game per real-world game | `pass` | Canonical game persistence and admin diagnostics. |
| Spread/total error metrics | `pass` | Game metrics, feature inputs, evidence inputs, and backtests all consume them. |
| Canonical conflict visibility | `pass` | Data-quality issues, trends, and diagnostics endpoints. |

### 3. Feature engineering and pattern discovery
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Time-safe prior-game feature engineering | `pass` | Versioned feature snapshots and flattened datasets. |
| Pattern discovery and comparable-game lookup | `pass` | Pattern endpoints, comparables, evidence bundle, artifact history. |
| Explainable analytical evidence | `pass` | Evidence strength, recommendation, artifact materialization. |

### 4. Predictive modeling and opportunity generation
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Lightweight residual model training | `pass` | Linear/tree baselines, runs, summaries, evaluation snapshots. |
| Time-aware promotion and scoring | `pass` | Selection snapshots, scoring previews, scoring runs, future preview flows. |
| Explainable opportunity surfacing | `pass` | Opportunity artifacts, history, evidence, recommendation, provenance. |
| Upcoming-game workflow | `manual-check` | Manual scenarios, slates, market boards, source-backed refreshes, cadence orchestration. Validate end-to-end on release smoke pass. |

### 5. Backtesting and evaluation
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Leakage-safe walk-forward evaluation | `pass` | Persisted backtest runs, fold summaries, stored strategy results. |
| Threshold-based evaluation metrics | `pass` | ROI, hit rate, push rate, and strategy surfaces are exposed. |
| Stored run inspection and history | `pass` | Backtest run/detail/history APIs and UI routes. |

### 6. Analyst and admin user experience
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Analyst dashboard and opportunity detail UX | `pass` | Phase 4 routes for queue, detail, comparables, provenance, compare. |
| Admin diagnostics and operational visibility | `pass` | Ingestion diagnostics, trends, market-board operations, queues, cadence. |
| Provenance and auditability | `pass` | Selection/evaluation/scoring/training artifact routes and history. |
| Compare workflow for fold vs live opportunity | `pass` | Phase 4 compare route with alignment, drift, and decision guidance. |

### 7. Operational readiness
| Requirement area | Status | Evidence |
| --- | --- | --- |
| Regression path for release candidate | `pass` | [scripts/run_phase5_regression.ps1](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/scripts/run_phase5_regression.ps1). |
| Manual release smoke path | `pass` | [docs/manual_smoke_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/manual_smoke_checklist.md). |
| Structured workflow logging for operator golden paths | `pass` | [backend/src/bookmaker_detector_api/services/workflow_logging.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/services/workflow_logging.py) plus instrumented backtest, opportunity-materialization, and market-board orchestration services. |
| Optional browser-route smoke harness | `pass` | Playwright smoke scaffolding now exists in [frontend/playwright.config.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/playwright.config.ts) and [frontend/e2e/phase5-smoke.spec.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/e2e/phase5-smoke.spec.ts). |
| Known-issues tracking | `pass` | [docs/known_issues.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/known_issues.md). |
| Live external source validation | `manual-check` | External provider exists, but should be validated with a real `THE_ODDS_API_KEY` during release smoke. |

## Deferred or Narrowed MVP Scope
| Area | Status | Notes |
| --- | --- | --- |
| Bet execution / sportsbook integration | `deferred` | Out of scope by SRS. |
| Multi-sport or playoff support | `deferred` | MVP remains NBA regular season only. |
| Production alerting and deep observability stack | `manual-check` | Operational surfaces exist, but alerting/runbooks need further Phase 5 hardening. |

## Release Decision
Mark this section when a real release-candidate pass is completed.

- Regression script: `pass`
- Manual smoke checklist: `partial`
- Known issues reviewed: `pass`
- External source validated or explicitly waived: `waived`
- Frontend browser-route validation: `waived by operator request`
- Release recommendation: `ready for an internal release candidate with explicit waivers for live external-source validation and browser-route execution.`
