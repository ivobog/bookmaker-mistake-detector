# API Surface Inventory

## Purpose
This document is the first cleanup slice from the clean-release execution plan. It inventories every route currently defined in `backend/src/bookmaker_detector_api/api/admin_routes.py` and assigns each route to one of the clean-release target zones:

- `analyst`: stable analyst-facing product surface
- `admin`: privileged operational or mutation surface
- `dev-only`: move to scripts, fixtures, tests, or non-production route mounts
- `delete`: remove instead of preserving as a route

## Current-State Summary
- Current source file: `backend/src/bookmaker_detector_api/api/admin_routes.py`
- Current route count in that file: `81`
- Current mounted API surfaces: `health` plus one large `/admin` router
- Dominant cleanup smells:
  - stable read endpoints accept `repository_mode`
  - many read endpoints accept `seed_demo`
  - many read endpoints also accept `auto_*` toggles that can trigger hidden mutations
  - demo workflows are mixed into the same module as production-oriented APIs

## Parameter Families To Remove From Stable Contracts
- `repository_mode`
  - keep only for dev-only tooling or test harnesses
- `seed_demo`
  - move to scripts, fixtures, or dev-only entry points
- `auto_run_demo`
- `auto_train_demo`
- `auto_select_demo`
- `auto_materialize_demo`
- `auto_refresh_demo`
- `auto_orchestrate_demo`

These parameters are the main source of side-effect risk in otherwise read-shaped APIs.

## Inventory

### Demo and Bootstrap Routes
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `GET /api/v1/admin/providers` | `list_supported_providers` | `admin` | `admin_ingestion` | Keep as an admin capability catalog, but remove any implication that fixture-backed providers are production-default. |
| `GET /api/v1/admin/phase-1-demo` | `phase_one_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. This is phase/demo scaffolding. |
| `GET /api/v1/admin/phase-1-persistence-demo` | `phase_one_persistence_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |
| `GET /api/v1/admin/phase-1-worker-demo` | `phase_one_worker_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |
| `GET /api/v1/admin/phase-1-fetch-demo` | `phase_one_fetch_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |
| `GET /api/v1/admin/phase-1-fetch-failure-demo` | `phase_one_fetch_failure_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |
| `GET /api/v1/admin/phase-1-fetch-reporting-demo` | `phase_one_fetch_reporting_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |
| `GET /api/v1/admin/phase-2-feature-demo` | `phase_two_feature_demo` | `dev-only` | `dev_routes` or scripts | Move out of stable runtime. |

### Model Training, Evaluation, and Backtests
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `POST /api/v1/admin/models/train` | `phase_three_model_train` | `admin` | `admin_models` | Keep as explicit mutation. Remove `repository_mode` and `seed_demo` from the production contract. |
| `POST /api/v1/admin/models/backtests/run` | `phase_four_model_backtest_run` | `admin` | `admin_backtests` | Keep as explicit backtest execution. |
| `GET /api/v1/admin/models/backtests` | `phase_four_model_backtests` | `analyst` | `analyst_backtests` | Move stable summary listing here. Keep admin history separately. Remove `auto_run_demo`. |
| `GET /api/v1/admin/models/backtests/history` | `phase_four_model_backtest_history` | `admin` | `admin_backtests` | Keep as operational run history. |
| `GET /api/v1/admin/models/backtests/{backtest_run_id}` | `phase_four_model_backtest_detail` | `analyst` | `analyst_backtests` | Keep as analyst detail if the contract is stable and read-only. |
| `GET /api/v1/admin/models/registry` | `phase_three_model_registry` | `admin` | `admin_models` | Keep as model registry admin view. Remove auto-training behavior from reads. |
| `GET /api/v1/admin/models/runs` | `phase_three_model_runs` | `admin` | `admin_models` | Keep as admin run listing. |
| `GET /api/v1/admin/models/runs/{run_id}` | `phase_three_model_run_detail` | `admin` | `admin_models` | Keep as admin run detail. |
| `GET /api/v1/admin/models/summary` | `phase_three_model_summary` | `admin` | `admin_models` | Keep as admin summary view. |
| `GET /api/v1/admin/models/history` | `phase_three_model_history` | `admin` | `admin_models` | Keep as admin history view. |
| `GET /api/v1/admin/models/evaluations` | `phase_three_model_evaluations` | `admin` | `admin_models` | Keep as evaluation snapshot listing. |
| `GET /api/v1/admin/models/evaluations/history` | `phase_three_model_evaluation_history` | `admin` | `admin_models` | Keep as operational history. |
| `GET /api/v1/admin/models/evaluations/{snapshot_id}` | `phase_three_model_evaluation_detail` | `admin` | `admin_models` | Keep as evaluation detail. |
| `POST /api/v1/admin/models/select` | `phase_three_model_select` | `admin` | `admin_models` | Keep as explicit active-model selection mutation. |
| `GET /api/v1/admin/models/selections` | `phase_three_model_selections` | `admin` | `admin_models` | Keep as selection listing. |
| `GET /api/v1/admin/models/selections/history` | `phase_three_model_selection_history` | `admin` | `admin_models` | Keep as admin history. |
| `GET /api/v1/admin/models/selections/{selection_id}` | `phase_three_model_selection_detail` | `admin` | `admin_models` | Keep as selection detail. |
| `GET /api/v1/admin/models/score-preview` | `phase_three_model_score_preview` | `admin` | `admin_models` | Keep as internal scoring preview, not analyst surface. Remove hidden auto-training and auto-selection. |

### Future Scenario and Slate Modeling
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `GET /api/v1/admin/models/future-game-preview` | `phase_three_model_future_game_preview` | `admin` | `admin_models` | Keep as internal preview tooling; do not expose as stable analyst contract yet. |
| `POST /api/v1/admin/models/future-game-preview/materialize` | `phase_three_model_future_game_preview_materialize` | `admin` | `admin_models` | Keep as explicit mutation. |
| `GET /api/v1/admin/models/future-game-preview/runs` | `phase_three_model_future_game_preview_runs` | `admin` | `admin_models` | Keep as admin run listing. |
| `GET /api/v1/admin/models/future-game-preview/runs/{scoring_run_id}` | `phase_three_model_future_game_preview_run_detail` | `admin` | `admin_models` | Keep as admin run detail. |
| `GET /api/v1/admin/models/future-game-preview/history` | `phase_three_model_future_game_preview_history` | `admin` | `admin_models` | Keep as admin history. |
| `POST /api/v1/admin/models/future-slate/preview` | `phase_three_model_future_slate_preview` | `admin` | `admin_models` | Keep as internal scenario-preview tooling. |
| `POST /api/v1/admin/models/future-slate/materialize` | `phase_three_model_future_slate_materialize` | `admin` | `admin_models` | Keep as explicit mutation. |
| `POST /api/v1/admin/models/future-game-preview/opportunities/materialize` | `phase_three_model_future_opportunity_materialize` | `admin` | `admin_models` | Keep as explicit mutation. |

### Market Board Operations
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `GET /api/v1/admin/models/market-board/sources` | `phase_three_model_market_board_sources` | `admin` | `admin_models` | Keep as admin source catalog. |
| `POST /api/v1/admin/models/market-board/refresh` | `phase_three_model_market_board_refresh` | `admin` | `admin_models` | Keep as explicit refresh mutation. |
| `GET /api/v1/admin/models/market-board/history` | `phase_three_model_market_board_history` | `admin` | `admin_models` | Keep as admin refresh history. |
| `GET /api/v1/admin/models/market-board/source-runs` | `phase_three_model_market_board_source_runs` | `admin` | `admin_models` | Keep as admin source-run history. |
| `GET /api/v1/admin/models/market-board/refresh-queue` | `phase_three_model_market_board_refresh_queue` | `admin` | `admin_models` | Keep as admin queue view. |
| `GET /api/v1/admin/models/market-board/queue` | `phase_three_model_market_board_queue` | `admin` | `admin_models` | Keep as admin scoring queue view. |
| `POST /api/v1/admin/models/market-board/orchestrate-refresh` | `phase_three_model_market_board_orchestrate_refresh` | `admin` | `admin_models` | Keep as explicit orchestration mutation. |
| `POST /api/v1/admin/models/market-board/orchestrate-score` | `phase_three_model_market_board_orchestrate_score` | `admin` | `admin_models` | Keep as explicit orchestration mutation. |
| `POST /api/v1/admin/models/market-board/orchestrate-cadence` | `phase_three_model_market_board_orchestrate_cadence` | `admin` | `admin_models` | Keep as explicit orchestration mutation. |
| `GET /api/v1/admin/models/market-board/refresh-orchestration-history` | `phase_three_model_market_board_refresh_orchestration_history` | `admin` | `admin_models` | Keep as admin orchestration history. |
| `GET /api/v1/admin/models/market-board/cadence-history` | `phase_three_model_market_board_cadence_history` | `admin` | `admin_models` | Keep as cadence history. |
| `GET /api/v1/admin/models/market-board/orchestration-history` | `phase_three_model_market_board_orchestration_history` | `admin` | `admin_models` | Keep as orchestration history. |
| `GET /api/v1/admin/models/market-board/cadence` | `phase_three_model_market_board_cadence` | `admin` | `admin_models` | Keep as admin cadence dashboard. |
| `POST /api/v1/admin/models/market-board/materialize` | `phase_three_model_market_board_materialize` | `admin` | `admin_models` | Keep as explicit board creation mutation. |
| `GET /api/v1/admin/models/market-board` | `phase_three_model_market_boards` | `admin` | `admin_models` | Keep as board listing. |
| `GET /api/v1/admin/models/market-board/{board_id}` | `phase_three_model_market_board_detail` | `admin` | `admin_models` | Keep as board detail. |
| `GET /api/v1/admin/models/market-board/{board_id}/operations` | `phase_three_model_market_board_operations` | `admin` | `admin_models` | Keep as admin operations summary. |
| `POST /api/v1/admin/models/market-board/{board_id}/score` | `phase_three_model_market_board_score` | `admin` | `admin_models` | Keep as explicit score mutation. |

### Opportunity Surface
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `POST /api/v1/admin/models/opportunities/materialize` | `phase_three_model_opportunity_materialize` | `admin` | `admin_models` | Keep as explicit opportunity materialization mutation. |
| `GET /api/v1/admin/models/opportunities` | `phase_three_model_opportunities` | `analyst` | `analyst_opportunities` | Promote to stable analyst list API after removing `seed_demo`, `auto_train_demo`, `auto_select_demo`, and `auto_materialize_demo`. |
| `GET /api/v1/admin/models/opportunities/history` | `phase_three_model_opportunity_history` | `admin` | `admin_models` | Keep as admin history and audit view. |
| `GET /api/v1/admin/models/opportunities/{opportunity_id}` | `phase_three_model_opportunity_detail` | `analyst` | `analyst_opportunities` | Promote to stable analyst detail API after cleanup. |

### Feature, Pattern, and Evidence Surface
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `GET /api/v1/admin/features/snapshots` | `feature_snapshots` | `admin` | `admin_features` | Keep as artifact/admin visibility, not stable analyst surface. |
| `GET /api/v1/admin/features/summary` | `feature_summary` | `analyst` | `analyst_trends` | Promote as stable team trend summary once it is detached from demo seeding. |
| `GET /api/v1/admin/features/dataset` | `feature_dataset` | `admin` | `admin_features` | Keep as internal analytical export/admin view. |
| `GET /api/v1/admin/features/dataset/profile` | `feature_dataset_profile` | `admin` | `admin_features` | Keep as internal diagnostic/admin view. |
| `GET /api/v1/admin/features/patterns` | `feature_patterns` | `analyst` | `analyst_patterns` | Promote as stable analyst pattern exploration. |
| `GET /api/v1/admin/features/comparables` | `feature_comparables` | `analyst` | `analyst_patterns` | Promote as analyst comparable-case retrieval. |
| `GET /api/v1/admin/features/evidence` | `feature_evidence` | `analyst` | `analyst_patterns` | Promote as analyst evidence/comparable detail. |
| `POST /api/v1/admin/features/analysis/materialize` | `materialize_feature_analysis` | `admin` | `admin_features` | Keep as explicit artifact materialization mutation. |
| `GET /api/v1/admin/features/analysis/artifacts` | `feature_analysis_artifacts` | `admin` | `admin_features` | Keep as admin artifact listing. |
| `GET /api/v1/admin/features/analysis/history` | `feature_analysis_history` | `admin` | `admin_features` | Keep as admin artifact history. |
| `GET /api/v1/admin/features/dataset/splits` | `feature_dataset_splits` | `admin` | `admin_features` | Keep as internal data-science/admin view. |
| `GET /api/v1/admin/features/dataset/training-view` | `feature_dataset_training_view` | `admin` | `admin_features` | Keep as internal training/admin view. |
| `GET /api/v1/admin/features/dataset/training-manifest` | `feature_dataset_training_manifest` | `admin` | `admin_features` | Keep as admin training metadata. |
| `GET /api/v1/admin/features/dataset/training-bundle` | `feature_dataset_training_bundle` | `admin` | `admin_features` | Keep as internal training/admin view. |
| `GET /api/v1/admin/features/dataset/training-benchmark` | `feature_dataset_training_benchmark` | `admin` | `admin_features` | Keep as internal benchmark/admin view. |
| `GET /api/v1/admin/features/dataset/training-task-matrix` | `feature_dataset_training_task_matrix` | `admin` | `admin_features` | Keep as internal task-readiness/admin view. |

### Ingestion, Quality, and Maintenance
| Route | Handler | Zone | Target module | Recommended action |
| --- | --- | --- | --- | --- |
| `GET /api/v1/admin/jobs/recent` | `recent_job_runs` | `admin` | `admin_ingestion` | Keep as admin operational view. |
| `GET /api/v1/admin/ingestion/issues` | `recent_ingestion_issues` | `admin` | `admin_ingestion` | Keep as admin issue listing. |
| `GET /api/v1/admin/data-quality/issues` | `recent_data_quality_issues` | `admin` | `admin_ingestion` | Keep as admin issue listing. |
| `GET /api/v1/admin/ingestion/stats` | `ingestion_stats` | `admin` | `admin_ingestion` | Keep as admin stats surface. |
| `GET /api/v1/admin/validation-runs/compare` | `compare_validation_runs` | `admin` | `admin_maintenance` | Keep as admin validation comparison. |
| `GET /api/v1/admin/ingestion/trends` | `ingestion_trends` | `admin` | `admin_ingestion` | Keep as admin trend surface. |
| `GET /api/v1/admin/retrieval/trends` | `retrieval_trends` | `admin` | `admin_ingestion` | Keep as admin retrieval trend surface. |
| `GET /api/v1/admin/ingestion/quality-trends` | `ingestion_quality_trends` | `admin` | `admin_ingestion` | Keep as admin quality-trend surface. |
| `POST /api/v1/admin/data-quality/normalize-taxonomy` | `normalize_data_quality_issue_taxonomy` | `admin` | `admin_maintenance` | Keep as explicit maintenance mutation. |

## Recommended First Refactor Cut
The lowest-risk first router split is:

1. Create `analyst_opportunities.py`
2. Move:
   - `GET /models/opportunities`
   - `GET /models/opportunities/{opportunity_id}`
3. Create `analyst_backtests.py`
4. Move:
   - `GET /models/backtests`
   - `GET /models/backtests/{backtest_run_id}`
5. Create `analyst_patterns.py`
6. Move:
   - `GET /features/patterns`
   - `GET /features/comparables`
   - `GET /features/evidence`
7. Create `analyst_trends.py`
8. Move:
   - `GET /features/summary`

These are the clearest analyst-facing reads in the current file.

## Follow-On Cleanup Rules
- No analyst route should accept `seed_demo`, `repository_mode`, or any `auto_*` parameter.
- No analyst GET route should perform training, selection, refresh, scoring, or materialization.
- Admin mutation routes should remain explicit `POST` operations.
- Demo and phase-specific routes should not ship inside the production router tree.
