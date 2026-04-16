# Bookmaker Mistake Detector

Phase 0, Phase 1, and the Phase 2 backend analytical core are complete for the Bookmaker Mistake Detector.

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
21. Inspect the initial Phase 2 feature snapshot demo at `http://localhost:8000/api/v1/admin/phase-2-feature-demo?repository_mode=in_memory`.
22. Query Phase 2 feature snapshots with team and season filters at `http://localhost:8000/api/v1/admin/features/snapshots?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025`.
23. Inspect the aggregated Phase 2 feature summary for one team at `http://localhost:8000/api/v1/admin/features/summary?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025`.
24. Export flattened Phase 2 feature rows for one team at `http://localhost:8000/api/v1/admin/features/dataset?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025`.
25. Profile flattened Phase 2 feature rows at `http://localhost:8000/api/v1/admin/features/dataset/profile?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025`.
26. Explore grouped Phase 2 historical patterns at `http://localhost:8000/api/v1/admin/features/patterns?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&dimensions=venue,days_rest_bucket&min_sample_size=1`.
27. Pull comparable historical cases for one anchor game at `http://localhost:8000/api/v1/admin/features/comparables?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&dimensions=venue,days_rest_bucket&canonical_game_id=3`.
28. Drill down from a pattern directly with `pattern_key` at `http://localhost:8000/api/v1/admin/features/comparables?repository_mode=in_memory&seed_demo=true&season_label=2024-2025&target_task=spread_error_regression&pattern_key=venue=home|days_rest_bucket=unknown_rest`.
29. Build a unified evidence payload for one anchor case at `http://localhost:8000/api/v1/admin/features/evidence?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&canonical_game_id=3&dimensions=venue,days_rest_bucket&comparable_limit=5&min_pattern_sample_size=1&train_ratio=0.5&validation_ratio=0.25`. The response now includes `evidence.strength` with an overall score, rating, component scores, and warnings, plus `evidence.recommendation` with analyst-facing states like `monitor_only`, `review_manually`, or `candidate_signal`. Recommendation thresholds are task-aware and exposed under `evidence.recommendation.policy_profile`.
30. Materialize persisted Phase 2 analysis artifacts at `http://localhost:8000/api/v1/admin/features/analysis/materialize?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&canonical_game_id=3&dimensions=venue,days_rest_bucket&min_sample_size=1&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25` using `POST`.
31. List persisted Phase 2 analysis artifacts at `http://localhost:8000/api/v1/admin/features/analysis/artifacts?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&canonical_game_id=3&dimensions=venue,days_rest_bucket&min_sample_size=1&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25`.
32. Review persisted Phase 2 artifact history at `http://localhost:8000/api/v1/admin/features/analysis/history?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&canonical_game_id=3&dimensions=venue,days_rest_bucket&min_sample_size=1&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25`.
30. Build chronological train/validation/test splits for flattened Phase 2 rows at `http://localhost:8000/api/v1/admin/features/dataset/splits?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&train_ratio=0.5&validation_ratio=0.25`.
31. Project a task-ready training view for one target at `http://localhost:8000/api/v1/admin/features/dataset/training-view?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression`.
32. Build a split-aware training bundle for one target at `http://localhost:8000/api/v1/admin/features/dataset/training-bundle?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
33. Inspect the training manifest for one target at `http://localhost:8000/api/v1/admin/features/dataset/training-manifest?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression`.
34. Score naive baselines for one target at `http://localhost:8000/api/v1/admin/features/dataset/training-benchmark?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
35. Compare training readiness across all supported targets at `http://localhost:8000/api/v1/admin/features/dataset/training-task-matrix?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025&train_ratio=0.5&validation_ratio=0.25`.

## Phase 0 Outcome
This scaffold gives us:
- a runnable local development stack
- a sample API route and sample UI page
- initial reference and job-run schema
- quality tooling placeholders for backend and frontend

## Phase Status
Phase 0 is complete.
Phase 1 is complete for the backend MVP data spine.
Phase 2 is complete for the backend analytical core.

## Phase 2 Outcome
The repo now includes:
- versioned feature snapshots and time-safe rolling team/matchup feature generation
- flattened feature datasets, dataset profiles, and chronological train/validation/test splits
- target-specific training views, manifests, bundles, task matrices, and naive benchmark scoring
- grouped pattern discovery with drill-down pattern keys
- comparable historical case retrieval with ranking
- unified evidence bundles with strength scoring and task-aware recommendation policies
- persisted Phase 2 analysis artifacts for pattern summaries and evidence bundles
- artifact catalog and artifact history rollups for persisted analytical outputs

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
Start Phase 3 by building model training, model artifact/version management, future-game scoring, and explainable opportunity generation on top of the completed analytical core.
