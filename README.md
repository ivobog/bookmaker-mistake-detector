# Bookmaker Mistake Detector

Phase 0, Phase 1, and the Phase 2 backend analytical core are complete, and Phase 3 has started with the first backend model-training slice.

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
36. Run the first Phase 3 baseline model training slice at `http://localhost:8000/api/v1/admin/models/train?repository_mode=in_memory&seed_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25` using `POST`.
37. Inspect the Phase 3 model registry at `http://localhost:8000/api/v1/admin/models/registry?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
38. Inspect persisted Phase 3 model runs at `http://localhost:8000/api/v1/admin/models/runs?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
39. Inspect the Phase 3 model summary view at `http://localhost:8000/api/v1/admin/models/summary?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
40. Inspect the Phase 3 model history view at `http://localhost:8000/api/v1/admin/models/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`.
41. Inspect persisted Phase 3 model evaluation snapshots at `http://localhost:8000/api/v1/admin/models/evaluations?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25`.
42. Inspect Phase 3 model evaluation history at `http://localhost:8000/api/v1/admin/models/evaluations/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`.
43. Promote the best currently evaluated Phase 3 candidate at `http://localhost:8000/api/v1/admin/models/select?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25` using `POST`.
44. Inspect active or historical Phase 3 model selections at `http://localhost:8000/api/v1/admin/models/selections?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25&active_only=true`.
45. Inspect Phase 3 model selection history at `http://localhost:8000/api/v1/admin/models/selections/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`.
46. Preview Phase 3 active-model scoring on pregame feature rows at `http://localhost:8000/api/v1/admin/models/score-preview?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25`. The response carries the active promoted model, scored predictions, and a compact Phase 2 evidence summary for each scored case.
47. Preview a manual upcoming-game scenario at `http://localhost:8000/api/v1/admin/models/future-game-preview?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25`. The response scores both team perspectives for the future matchup and includes an opportunity preview derived from evidence-aware policy rules.
48. Materialize Phase 3 opportunity artifacts at `http://localhost:8000/api/v1/admin/models/opportunities/materialize?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25` using `POST`.
49. Inspect the current Phase 3 opportunity list at `http://localhost:8000/api/v1/admin/models/opportunities?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025&canonical_game_id=3&status=review_manually`.
50. Inspect Phase 3 opportunity history at `http://localhost:8000/api/v1/admin/models/opportunities/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025&canonical_game_id=3&recent_limit=5`.
51. Materialize a persisted Phase 3 future-scenario scoring run at `http://localhost:8000/api/v1/admin/models/future-game-preview/materialize?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25` using `POST`.
52. Inspect persisted Phase 3 future-scenario scoring runs at `http://localhost:8000/api/v1/admin/models/future-game-preview/runs?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&season_label=2025-2026&team_code=LAL&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25`.
53. Inspect one persisted Phase 3 future-scenario scoring run at `http://localhost:8000/api/v1/admin/models/future-game-preview/runs/1?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&season_label=2025-2026&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25`.
54. Inspect Phase 3 future-scenario scoring history at `http://localhost:8000/api/v1/admin/models/future-game-preview/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&season_label=2025-2026&team_code=LAL&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`.
55. Materialize persisted Phase 3 future-scenario opportunities at `http://localhost:8000/api/v1/admin/models/future-game-preview/opportunities/materialize?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25` using `POST`.
56. Inspect future-scenario opportunities through the shared opportunity list at `http://localhost:8000/api/v1/admin/models/opportunities?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&source_kind=future_scenario&team_code=LAL&season_label=2025-2026&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25`.
57. Inspect future-scenario opportunity history through the shared opportunity history view at `http://localhost:8000/api/v1/admin/models/opportunities/history?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&auto_materialize_demo=true&target_task=spread_error_regression&source_kind=future_scenario&team_code=LAL&season_label=2025-2026&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`.
58. Preview a batch upcoming slate at `http://localhost:8000/api/v1/admin/models/future-slate/preview?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25` using `POST` with a JSON body containing `slate_label` and a `games` array. The response returns per-game future previews plus slate-level prediction and opportunity-preview counts.
59. Materialize a batch upcoming slate at `http://localhost:8000/api/v1/admin/models/future-slate/materialize?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25` using `POST` with the same JSON body. The response persists one scoring run per future game, materializes future-scenario opportunities, and returns a slate summary across the batch.
60. Persist a reusable market board at `http://localhost:8000/api/v1/admin/models/market-board/materialize?repository_mode=in_memory&target_task=spread_error_regression` using `POST` with the same future-slate JSON body. The response stores the upcoming slate as a first-class board artifact before scoring.
61. Inspect persisted market boards at `http://localhost:8000/api/v1/admin/models/market-board?repository_mode=in_memory&target_task=spread_error_regression&season_label=2025-2026&auto_materialize_demo=true`.
62. Score a persisted market board at `http://localhost:8000/api/v1/admin/models/market-board/1/score?repository_mode=in_memory&seed_demo=true&auto_materialize_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&season_label=2025-2026&train_ratio=0.5&validation_ratio=0.25` using `POST`. The response reuses the saved board inputs, materializes slate scoring runs, and persists future-scenario opportunities from the stored board.
63. Inspect available market-board refresh sources at `http://localhost:8000/api/v1/admin/models/market-board/sources`. The catalog now includes built-in demo providers, the file-backed `file_market_board_v1` source for operator-supplied boards, and the external `the_odds_api_v4_nba` source backed by The Odds API.
64. Refresh a source-backed market board at `http://localhost:8000/api/v1/admin/models/market-board/refresh?repository_mode=in_memory&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2` using `POST`. The response generates source-backed upcoming games for the date, persists them as a reusable board, and now includes a structured change summary for added, removed, changed, and unchanged games. If the source provider fails, the response returns `status=FAILED`, `error_message`, and a persisted failed `source_run` without creating a board or refresh event. If the provider returns mixed-quality rows, the response returns `validation_summary` and can surface `status=SUCCESS_WITH_WARNINGS` while still materializing the valid normalized games. Refresh responses also include `source_payload_fingerprints`, comparison metadata, and `source_request_context` so operators can tell when the upstream raw payload changed even though the normalized board stayed the same. For file-backed refreshes, pass `source_name=file_market_board_v1&source_path=fixture://demo_market_board_file_source.json` or a real local JSON/CSV path. For the live external source, set `THE_ODDS_API_KEY` and call `source_name=the_odds_api_v4_nba`.
65. Inspect market-board refresh history at `http://localhost:8000/api/v1/admin/models/market-board/history?repository_mode=in_memory&auto_refresh_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&recent_limit=5`. The response summarizes refresh events, status counts like `created` or `unchanged`, and recent refresh activity, including stored change summaries.
66. Inspect persisted market-board source runs at `http://localhost:8000/api/v1/admin/models/market-board/source-runs?repository_mode=in_memory&auto_refresh_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&recent_limit=5`. The response summarizes source-run history, generated game counts, validation counts for invalid or warning rows, status counts like `SUCCESS`, `SUCCESS_WITH_WARNINGS`, or `FAILED`, and the stored source request payload behind each refresh. Each source run now carries raw-vs-normalized payload hashes for drift diagnostics.
66. Inspect the market-board refresh queue at `http://localhost:8000/api/v1/admin/models/market-board/refresh-queue?repository_mode=in_memory&auto_refresh_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&pending_only=false`. The response shows which source-backed boards are refreshable, already current, or due for refresh.
67. Orchestrate market-board refreshes at `http://localhost:8000/api/v1/admin/models/market-board/orchestrate-refresh?repository_mode=in_memory&auto_refresh_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&pending_only=false` using `POST`. The response refreshes eligible boards in one batch and returns queue state before and after the run.
68. Inspect market-board refresh orchestration history at `http://localhost:8000/api/v1/admin/models/market-board/refresh-orchestration-history?repository_mode=in_memory&auto_refresh_demo=true&auto_orchestrate_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&pending_only=false&recent_limit=5`. The response summarizes persisted refresh batches, daily rollups, and the latest refresh cadence across source-backed boards.
69. Run a full market-board cadence cycle at `http://localhost:8000/api/v1/admin/models/market-board/orchestrate-cadence?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&refresh_freshness_status=stale&refresh_pending_only=false&scoring_freshness_status=fresh&scoring_pending_only=true&train_ratio=0.5&validation_ratio=0.25` using `POST`. The response runs refresh orchestration first, scoring orchestration second, and persists one combined cadence batch for the full cycle.
70. Inspect cadence batch history at `http://localhost:8000/api/v1/admin/models/market-board/cadence-history?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&refresh_freshness_status=stale&refresh_pending_only=false&scoring_freshness_status=fresh&scoring_pending_only=true&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`. The response summarizes persisted end-to-end cadence batches, daily rollups, and the latest combined refresh-plus-score runs.
66. Inspect the market-board scoring queue at `http://localhost:8000/api/v1/admin/models/market-board/queue?repository_mode=in_memory&auto_refresh_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&freshness_status=fresh&pending_only=true`. The response shows which boards are fresh and still need scoring versus boards that are already current.
67. Orchestrate scoring for the pending market-board queue at `http://localhost:8000/api/v1/admin/models/market-board/orchestrate-score?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25` using `POST`. The response scores each queued board, materializes scoring runs and opportunities, and returns queue state before and after orchestration.
68. Inspect market-board orchestration history at `http://localhost:8000/api/v1/admin/models/market-board/orchestration-history?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`. The response summarizes persisted orchestration batches, daily rollups, and the latest scoring cadence across queued boards.
69. Inspect one board’s operational summary at `http://localhost:8000/api/v1/admin/models/market-board/1/operations?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`. The response combines refresh, queue, scoring, opportunity, and orchestration state for one persisted board.
70. Inspect the market-board cadence dashboard at `http://localhost:8000/api/v1/admin/models/market-board/cadence?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true&target_task=spread_error_regression&source_name=demo_daily_lines_v1&season_label=2025-2026&game_date=2026-04-20&game_count=2&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25&recent_limit=5`. The response classifies boards into actionable cadence states like `ready_to_score`, `needs_refresh`, and `recently_scored`.

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
Phase 3 is in progress, now covering baseline training, evaluation, and explicit model selection.

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

## Phase 3 Progress
The repo now includes the first predictive-layer slice:
- baseline model registry metadata for reproducible model families
- persisted model training runs linked to feature versions
- lightweight regression-first baselines for `linear_feature` and `tree_stump`
- fallback-aware baseline behavior so tiny datasets still produce scorable model runs
- model summary rollups for latest runs, best runs, family counts, and feature-selection coverage
- richer training artifacts with selection metrics and split target summaries
- model history rollups with daily buckets and recent-run inspection
- persisted model evaluation snapshots with dedicated list and history APIs
- persisted model selection snapshots with active-model promotion and selection-history APIs
- policy-based model promotion that prefers non-fallback candidates and records selection rationale
- active-model scoring previews that use promoted selections to score pregame feature rows and attach evidence summaries
- manual future-game scenario previews that synthesize upcoming matchup features and score both perspectives through the active model
- batch future-slate preview and materialization endpoints that score multiple upcoming games in one request
- persisted market-board artifacts so manual upcoming slates can be stored, inspected, and rescored later
- built-in market-board refresh sources so upcoming boards can be repopulated automatically by date instead of only posted manually
- persisted market-board source-run artifacts so each refresh stores the upstream request and generated input snapshot
- board freshness metadata and refresh-event history so repeated source refreshes are auditable over time
- structured market-board refresh change summaries so refresh events show what matchups or lines actually changed
- market-board refresh queue and refresh-orchestration batches so source-backed boards can be refreshed in a consistent, auditable batch flow
- market-board scoring queue and orchestration APIs so fresh-but-unscored boards can be identified and pushed through a consistent scoring workflow
- end-to-end market-board cadence batches so refresh-plus-score runs can be executed and audited as one combined operational cycle
- persisted market-board orchestration batches and history rollups so scoring cadence is auditable over time
- board-level operational summaries that combine refresh, queue, scoring, opportunity, and orchestration state in one API
- market-board cadence dashboard that turns board freshness and scoring state into operator-facing priority signals
- persisted future-scenario scoring runs with list, detail, and history APIs for auditability over repeated previews
- future-scenario opportunity artifacts that link back to scoring runs and share the same review/history surfaces as historical opportunities
- persisted Phase 3 opportunity artifacts generated from scored predictions plus evidence-aware threshold policies
- opportunity list, detail, and history APIs for the first analyst-facing scoring workflow
- admin APIs to train models, inspect the model registry, review persisted model runs, promote active selections, preview scored cases, and materialize opportunities
- Phase 3 verification backed by a passing backend test suite

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
Continue Phase 3 by expanding from built-in market-board refresh and queue orchestration into true external schedule and line ingestion, plus stronger recurring scoring cadence on top of the current predictive workflow.
