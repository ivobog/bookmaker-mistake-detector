# Bookmaker Mistake Detector Execution Plan

## 1. Planning Basis
This execution plan is based on:
- `bookmaker_mistake_detector_vision_document.md`
- `bookmaker_mistake_detector_srs.md`
- `bookmaker_mistake_detector_sdd.md`

Observed local state on April 16, 2026:
- the project is now bootstrapped locally at `C:\Users\Ivica\Downloads\bookmakers-mistake-detector`
- Phase 0 foundation work is complete
- Phase 1 historical data spine work is functionally complete for the backend MVP

This execution plan started as a greenfield build plan and is now updated to reflect current delivery status.

## 2. Delivery Goal
Deliver an MVP that can:
- ingest historical NBA regular season team game pages for the last 4 completed seasons
- normalize team-perspective rows into canonical games
- calculate spread and total line error
- generate time-safe historical features
- discover recurring line-miss patterns
- train lightweight residual models
- score upcoming games and surface explainable opportunities
- expose analyst and admin web experiences
- validate signal quality with walk-forward backtests

## 3. Delivery Strategy
Build in four execution tracks that run in parallel where possible:
- `Track A: Platform and developer foundations`
- `Track B: Data ingestion and canonical analytics`
- `Track C: Intelligence layer: features, patterns, models, backtests`
- `Track D: Product surface: APIs, analyst UI, admin UI`

The critical path is:
`data ingestion -> canonical games -> metrics -> features -> patterns/models -> opportunity generation -> UI`

## 4. Recommended Phases

### Phase 0: Foundation and Project Setup
Duration: 1 week
Status: Complete

Objectives:
- establish repository structure
- choose MVP stack and coding standards
- define environments and local developer workflow
- create a thin vertical skeleton for API, jobs, database, and frontend

Work items:
- initialize monorepo or split repo structure
- create backend service scaffold
- create frontend scaffold
- add Docker Compose for local development
- provision PostgreSQL
- add config management, logging, linting, formatting, and test runners
- create initial CI pipeline for lint, unit tests, and type checks
- define seed reference data for teams and seasons

Deliverables:
- running local stack
- base database migrations
- CI baseline
- repo conventions documented

Exit criteria:
- `frontend`, `api`, `worker`, and `postgres` boot locally
- one sample API route and one sample UI page are working
- migrations and tests run in CI

Completion notes:
- local Docker development stack exists
- backend, frontend, worker, and Postgres scaffolds are in place
- baseline CI and repo conventions are present

### Phase 1: Historical Data Spine
Duration: 2 to 3 weeks
Status: Complete for backend MVP

Objectives:
- build the raw ingestion and canonical game backbone
- make data correctness visible early

Work items:
- implement provider abstraction
- implement page fetcher and retrieval metadata persistence
- implement regular season section parsing
- parse opponent, venue, score, ATS, and O/U fields
- persist raw team-perspective rows with idempotency guards
- implement canonical matching and reconciliation states
- compute final margin, final total, spread error, and total error
- add admin diagnostics for parse failures and reconciliation conflicts
- create parser fixtures and canonicalization regression tests

Deliverables:
- raw historical row store
- canonical game table
- metric computation pipeline
- admin diagnostics for ingestion quality

Exit criteria:
- at least one provider works end to end for a representative sample
- regular season rows are isolated correctly
- canonical game creation is deterministic
- data quality metrics are visible for missing lines and conflicts

Completion notes:
- one provider (`covers`) works end to end across fixture-backed and fetch-backed flows
- raw rows, canonical games, metrics, retrievals, job runs, and data-quality issues persist
- failed fetches and parse/canonicalization issues are recorded and queryable
- admin diagnostics cover jobs, issues, stats, trends, retrieval trends, quality trends, and validation-run comparison
- live Postgres validation has been performed repeatedly across the Phase 1 path
- Phase 1 backend verification is currently backed by a passing test suite

### Phase 2: Analytical Core
Duration: 2 weeks
Status: Complete for backend MVP

Objectives:
- turn historical games into usable analytical context
- establish reproducible feature and pattern pipelines

Work items:
- implement feature versioning
- generate time-safe team, opponent, and matchup features
- support rolling windows for 3, 5, and 10 games
- implement rest, back-to-back, ATS trend, O/U trend, and volatility features
- build pattern discovery engine using bucketed conditions
- compute sample size, mean error, median error, hit rate, and variance
- persist comparable historical matches
- expose pattern and trend APIs

Deliverables:
- versioned feature snapshots
- persisted pattern summaries
- comparable-game lookup
- initial analyst-facing pattern explorer data

Exit criteria:
- features use only prior games
- pattern discovery enforces minimum sample thresholds
- historical comparable cases can be retrieved for a given condition set

Completion notes:
- versioned feature snapshots and time-safe rolling team/matchup features are implemented
- flattened feature datasets, profiles, chronological splits, training views, manifests, bundles, task matrices, and naive benchmark scoring are available
- grouped pattern discovery, comparable-case retrieval, ranked comparables, and unified evidence bundles are implemented
- evidence strength scoring, task-aware recommendation policies, persisted analysis artifacts, and artifact history rollups are available through admin APIs
- Phase 2 backend verification is currently backed by a passing test suite and live Docker smoke tests across the analytical and artifact surfaces

### Phase 3: Predictive and Opportunity Layer
Duration: 2 weeks
Status: Complete for backend MVP

Objectives:
- convert analytics into restrained, explainable recommendations

Work items:
- create training datasets for spread residual and total residual targets
- train baseline models: one linear model and one tree-based model
- implement model registry metadata and artifact storage
- ingest future schedule and available market lines
- score future games
- combine predictions with pattern support
- implement opportunity thresholds and opportunity scoring
- generate evidence objects separating model, pattern, and comparable-game support
- expose opportunity list and detail APIs

Deliverables:
- residual model pipeline
- scheduled game scoring pipeline
- opportunity generation service
- explainable opportunity API responses

Exit criteria:
- model runs are versioned and reproducible
- opportunities are created only when thresholds are met
- each opportunity includes explanation summary and supporting evidence

Completion notes:
- lightweight regression baselines, evaluation snapshots, selection history, scoring previews, opportunities, market boards, refresh queues, scoring queues, cadence runs, source runs, and external-source refresh providers are implemented
- the backend can refresh, score, and materialize explainable opportunities for upcoming boards through persisted operational workflows
- Phase 3 backend verification is currently backed by a passing test suite and live Docker smoke tests across the predictive and market-board surfaces

### Phase 4: Backtesting, UX, and Operations Hardening
Duration: 2 weeks
Status: Complete for MVP

Objectives:
- validate signal quality
- complete analyst and admin workflows
- make the system operable

Work items:
- implement walk-forward backtest engine
- simulate threshold-based spread and totals strategies
- compute ROI, hit rate, push rate, and edge-bucket performance
- build analyst dashboard
- build opportunity detail page
- build backtest results page
- build admin jobs, issue views, and run history
- add job orchestration, retries, and structured audit logging
- add alerting hooks for ingestion failures and zero-output scoring runs

Deliverables:
- backtest results and stored runs
- analyst UI MVP
- admin UI MVP
- production-like job flow and observability

Completion notes:
- persisted walk-forward backtests, fold summaries, and history views are implemented
- the frontend now supports analyst backtest inspection, opportunity review, provenance drill-through, and artifact comparison workflows
- the compare route includes alignment checks, mismatch summaries, and analyst guidance for fold-vs-opportunity review
- Phase 4 verification is backed by passing frontend typecheck, lint, and build checks

Exit criteria:
- walk-forward evaluation runs without leakage
- analyst can inspect opportunities, evidence, and comparables
- admin can inspect jobs, parse issues, and backtest history

### Phase 5: Release Candidate
Duration: 1 week
Status: In progress

Objectives:
- stabilize the MVP for internal use

Work items:
- full regression pass
- parser resilience checks
- performance tuning on key queries
- documentation and runbooks
- seed demo dataset or initial production dataset load
- acceptance review against SRS MVP criteria

Deliverables:
- MVP release candidate
- known issues list
- operating checklist

Current slice notes:
- Phase 5 has started with a runnable regression script
- release-candidate runbook documentation is now present
- known-issues tracking is now explicitly seeded in the repo

Exit criteria:
- MVP acceptance checklist passes
- core jobs run in order reliably
- demo workflow works end to end from ingestion to surfaced opportunities

## 5. Suggested Sprint Breakdown

### Sprint 1
- repository bootstrap
- local infrastructure
- DB migrations
- team/season/provider reference tables
- CI and quality tooling

### Sprint 2
- provider adapter
- page retrieval
- raw parsing
- raw row persistence
- parser fixtures

### Sprint 3
- canonicalization
- metrics
- ingestion diagnostics
- reconciliation diagnostics

### Sprint 4
- feature generation
- feature versioning
- team trend summaries
- pattern discovery

### Sprint 5
- model training
- scheduled game ingestion
- prediction scoring
- opportunity generation

### Sprint 6
- backtesting
- dashboard UI
- opportunity detail UI
- admin job/run pages

### Sprint 7
- hardening
- observability
- acceptance validation
- release prep

## 6. Workstream Backlog by Priority

### P0: Must Have
- repo and environment bootstrap
- Postgres schema and migrations
- provider adapter and parsing
- raw row persistence
- canonical game normalization
- spread and total error metrics
- time-safe feature generation
- residual model training and scoring
- opportunity generation and evidence model
- walk-forward backtesting
- analyst opportunities UI
- admin diagnostics UI

### P1: Should Have
- raw page snapshot storage
- model comparison between linear and tree baselines
- richer comparable-game ranking
- structured logging and alerting
- job retry and rerun controls

### P2: Nice to Have After MVP
- additional providers
- opening vs closing line support
- richer charting and filters
- more advanced model explainability
- more operational dashboards

## 7. Team Roles
Minimum effective team:
- `1 full-stack lead` for architecture, API, and cross-cutting delivery
- `1 data/backend engineer` for ingestion, canonicalization, features, jobs
- `1 frontend engineer` for analyst and admin UX
- `1 data scientist / ML engineer` for pattern logic, models, and backtests
- `1 QA partner or shared test ownership` across the team

If the team is smaller, execution order should prioritize:
1. data spine
2. analytical core
3. backtests
4. UI polish

## 8. Major Risks and Mitigations

### Risk: provider layout changes
Mitigation:
- isolate provider adapters
- store sample fixtures
- build parser regression tests early

### Risk: canonicalization errors poison downstream analytics
Mitigation:
- use explicit reconciliation statuses
- expose conflict diagnostics in admin views
- block questionable records from model training unless reviewed

### Risk: leakage invalidates results
Mitigation:
- centralize time-safe feature logic
- test feature windows and backtest splits explicitly
- version features, models, and strategies

### Risk: attractive patterns are just noise
Mitigation:
- enforce minimum sample size
- require multi-season coverage where possible
- compare performance by edge bucket in walk-forward backtests

### Risk: recommendation quality is too noisy
Mitigation:
- keep thresholds conservative
- separate prediction from surfaced opportunity
- prefer fewer stronger opportunities

## 9. Acceptance Gates

### Gate A: Data Foundation
- can ingest and parse target provider pages
- raw parse quality is measurable
- canonical games are reproducible

### Gate B: Analytical Readiness
- spread error and total error are validated
- features are time-safe
- pattern summaries are queryable and interpretable

### Gate C: Signal Readiness
- residual models train successfully
- opportunity rules produce explainable outputs
- walk-forward backtests complete successfully

### Gate D: Product Readiness
- dashboard, detail, pattern, and admin views work end to end
- jobs are observable and retryable
- MVP acceptance criteria from the SRS are satisfied

## 10. Current Phase Status

### Completed
- Phase 0: Foundation and Project Setup
- Phase 1: Historical Data Spine
- Phase 2: Analytical Core
- Phase 3: Predictive and Opportunity Layer
- Phase 4: Backtesting, UX, and Operations Hardening

### Active recommendation
- continue Phase 5: Release Candidate

### Phase 5 current focus
- use the new regression script as the default release-candidate validation path
- tighten documentation, runbooks, and known-issues handling for internal MVP use
- harden operational edges around cadence orchestration, retries, and alerting

## 11. Immediate Next Steps
1. Expand the Phase 5 regression pass from code checks into a documented Docker/manual smoke checklist.
2. Harden retries, audit logging, and failure handling around ingestion, market-board refresh, and scoring cadence flows.
3. Review acceptance criteria from the SRS against the delivered MVP surfaces and capture any remaining gaps.
4. Produce a fuller internal rollout checklist on top of the new known-issues tracker and runbook.
5. Run the first end-to-end release-candidate validation pass and record findings.

## 12. Recommendation
The best execution approach is to treat the product as a data-quality-first analytics system, not a model-first betting app. If Phase 1 is weak, everything downstream becomes misleading; if Phase 1 is strong, the rest of the roadmap becomes much safer and faster.
