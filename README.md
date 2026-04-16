# Bookmaker Mistake Detector

Phase 0 and Phase 1 backend MVP delivery for the Bookmaker Mistake Detector.

## Stack
- `backend/`: FastAPI API service
- `worker/`: Python batch-worker scaffold
- `frontend/`: React + TypeScript + Vite UI scaffold
- `infra/postgres/init/`: baseline PostgreSQL schema SQL
- `.github/workflows/`: CI baseline

## Quick Start
1. Copy `.env.example` to `.env`.
2. Run `docker compose up --build`.
3. Open the frontend at `http://localhost:5173`.
4. Check the API health route at `http://localhost:8000/api/v1/health`.
5. Inspect the fixture-backed Phase 1 demo at `http://localhost:8000/api/v1/admin/phase-1-demo`.
6. Inspect the fixture-backed persistence flow at `http://localhost:8000/api/v1/admin/phase-1-persistence-demo`.
7. Inspect the worker-shaped ingestion flow at `http://localhost:8000/api/v1/admin/phase-1-worker-demo`.
8. Inspect the fetch-and-ingest flow at `http://localhost:8000/api/v1/admin/phase-1-fetch-demo`.
9. Inspect the failed-fetch diagnostics flow at `http://localhost:8000/api/v1/admin/phase-1-fetch-failure-demo`.
10. Inspect fetch-backed reporting in one shot at `http://localhost:8000/api/v1/admin/phase-1-fetch-reporting-demo?repository_mode=in_memory`.
Use `run_label=phase-1-fetch-reporting-demo` on recent-job and trend endpoints to isolate those validation runs from normal worker traffic.
The same `run_label` filter now works on ingestion stats and data-quality issue views too.
Compare the latest labeled validation runs at `http://localhost:8000/api/v1/admin/validation-runs/compare?repository_mode=in_memory&seed_demo=false&run_label=phase-1-fetch-reporting-demo`.
11. Inspect recent job runs at `http://localhost:8000/api/v1/admin/jobs/recent?repository_mode=in_memory&seed_demo=true`.
12. Inspect recent retrieval issues at `http://localhost:8000/api/v1/admin/ingestion/issues?repository_mode=in_memory&seed_demo=true`.
13. Inspect recent data quality issues at `http://localhost:8000/api/v1/admin/data-quality/issues?repository_mode=in_memory&seed_demo=true`.
14. Inspect ingestion quality stats at `http://localhost:8000/api/v1/admin/ingestion/stats?repository_mode=in_memory&seed_demo=true`.
15. Inspect scoped stats for one team/season at `http://localhost:8000/api/v1/admin/ingestion/stats?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025`.
16. Preview a data-quality taxonomy backfill with `POST /api/v1/admin/data-quality/normalize-taxonomy?repository_mode=postgres&dry_run=true`.
17. Inspect recent run trends at `http://localhost:8000/api/v1/admin/ingestion/trends?repository_mode=in_memory&seed_demo=true`.
18. Inspect a tighter recent trend window at `http://localhost:8000/api/v1/admin/ingestion/trends?repository_mode=in_memory&seed_demo=true&days=2`.
19. Inspect retrieval reliability trends at `http://localhost:8000/api/v1/admin/retrieval/trends?repository_mode=in_memory&seed_demo=true`.
20. Inspect parse and reconciliation quality trends at `http://localhost:8000/api/v1/admin/ingestion/quality-trends?repository_mode=in_memory&seed_demo=true`.

## Phase 0 Outcome
This scaffold gives us:
- a runnable local development stack
- a sample API route and sample UI page
- initial reference and job-run schema
- quality tooling placeholders for backend and frontend

## Phase Status
Phase 0 is complete.
Phase 1 is complete for the backend MVP data spine.
Phase 2 is the recommended next build phase.

## Phase 1 Outcome
The repo now includes:
- a provider abstraction and a fixture-backed `covers` historical-team-page parser
- raw row parsing for date, opponent, venue, score, ATS, and O/U fields
- canonical game normalization for reciprocal team-perspective rows
- metric calculation for spread error, total error, cover, and over/under outcomes
- an ingestion pipeline service that persists a team-season run through a repository interface
- a page fetcher with file and URL retrieval support
- raw payload snapshot storage for fetched pages
- failed-fetch recording that still writes job and retrieval diagnostics
- repository-backed admin diagnostics for recent jobs and retrieval failures
- persisted data-quality issues for canonicalization and parse warnings
- admin aggregation stats for parse status, reconciliation status, and quality issue breakdowns
- scoped admin diagnostics filters for provider, team, and season
- taxonomy normalization for legacy data-quality issue types plus a backfill endpoint
- run-level job summaries with parse, reconciliation, and issue breakdowns
- recent ingestion trend reporting built from persisted job summaries
- date-window filtering for recent jobs and ingestion trends
- repository-backed daily trend summaries for longer-window ingestion reporting
- structured `job_run_reporting_snapshot` persistence for query-friendly trend analytics
- structured `job_run_quality_snapshot` persistence for query-friendly parse and reconciliation quality trend analytics
- structured `page_retrieval_reporting_snapshot` persistence for query-friendly retrieval reliability analytics
- packaged fixture resources in the backend distribution so containerized demo and seed flows behave like local runs
- a fetch-backed reporting demo endpoint that validates ingestion, retrieval trends, and quality trends together
- run-label tagging for validation runs so admin job and trend views can isolate demo traffic from worker traffic
- a validation-run comparison endpoint that highlights deltas between the latest and previous labeled runs
- SQL schema for raw rows, canonical games, game metrics, and data-quality issues
- admin demo endpoints to inspect parsing and normalization behavior quickly
- an in-memory repository path for testing and demo validation flows
- a worker entry point that can execute the same fixture-backed ingestion flow in `in_memory` or `postgres` mode

## Next Phase
Start Phase 2 by building feature versioning, time-safe feature generation, and the first analytical APIs on top of the canonical game spine.
