# API Surface Inventory

## Purpose
This inventory captures the currently mounted API surface for the modularized router tree under [backend/src/bookmaker_detector_api/api/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\__init__.py).

It replaces the older inventory that referenced the pre-split `admin_routes.py` mega-router.

## Current mounted route groups
- `health`
- `analyst_backtests`
- `analyst_opportunities`
- `analyst_patterns`
- `analyst_trends`
- `admin_demo`
- `admin_diagnostics`
- `admin_features`
- `admin_market_board`
- `admin_models`
- `admin_opportunities`
- `admin_scoring`

## Route count summary

| Surface | Count | Notes |
| --- | --- | --- |
| Health | `1` | Root operational health check |
| Analyst | `8` | Intended stable read surface, but several routes still trigger seeded/demo workflows outside production |
| Admin operational + artifact reads | `49` | Includes diagnostics, model history, feature exports, and market-board operational views |
| Admin explicit mutations | `16` | Training, selection, materialization, orchestration, maintenance |
| Demo/dev-only | `8` | Phase/demo scaffolding and provider catalog |
| Total | `82` | Current route count under `/api/v1` |

## Classification rules
- `health`: always-on readiness or liveness checks
- `analyst`: read-oriented operator-facing workflow routes
- `admin-read`: operational, audit, diagnostics, and artifact retrieval routes
- `admin-mutation`: explicit state-changing routes
- `dev-only`: phase/demo scaffolding that should not be part of the stable operator surface

## Health

| Method | Path | Zone | Source file |
| --- | --- | --- | --- |
| `GET` | `/api/v1/health` | `health` | [backend/src/bookmaker_detector_api/api/routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\routes.py) |

## Analyst

| Method | Path | Zone | Source file | Current caution |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/analyst/backtests` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_backtests.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_backtests.py) | Seeds in-memory data and runs a backtest outside production |
| `GET` | `/api/v1/analyst/backtests/{backtest_run_id}` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_backtests.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_backtests.py) | Seeds in-memory data and runs a backtest outside production |
| `GET` | `/api/v1/analyst/opportunities` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_opportunities.py) | Can train, select, and materialize in-memory data outside production |
| `GET` | `/api/v1/analyst/opportunities/{opportunity_id}` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_opportunities.py) | Can train, select, and materialize in-memory data outside production |
| `GET` | `/api/v1/analyst/patterns` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_patterns.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_patterns.py) | Review for seeded in-memory preparation before phase 1 |
| `GET` | `/api/v1/analyst/comparables` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_patterns.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_patterns.py) | Review for seeded in-memory preparation before phase 1 |
| `GET` | `/api/v1/analyst/evidence` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_patterns.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_patterns.py) | Review for seeded in-memory preparation before phase 1 |
| `GET` | `/api/v1/analyst/trends/summary` | `analyst` | [backend/src/bookmaker_detector_api/api/analyst_trends.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\analyst_trends.py) | Review for seeded in-memory preparation before phase 1 |

## Admin Read

### Demo and provider catalog
| Method | Path | Zone | Source file | Recommended direction |
| --- | --- | --- | --- | --- |
| `GET` | `/api/v1/admin/providers` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Keep only if clearly labeled as development/provider catalog |
| `GET` | `/api/v1/admin/phase-1-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |
| `GET` | `/api/v1/admin/phase-1-persistence-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |
| `GET` | `/api/v1/admin/phase-1-worker-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |
| `GET` | `/api/v1/admin/phase-1-fetch-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |
| `GET` | `/api/v1/admin/phase-1-fetch-failure-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |
| `GET` | `/api/v1/admin/phase-1-fetch-reporting-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_demo_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_demo_routes.py) | Move to dev-only mounts or scripts |

### Diagnostics
| Method | Path | Zone | Source file |
| --- | --- | --- | --- |
| `GET` | `/api/v1/admin/jobs/recent` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/ingestion/issues` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/data-quality/issues` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/ingestion/stats` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/validation-runs/compare` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/ingestion/trends` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/retrieval/trends` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `GET` | `/api/v1/admin/ingestion/quality-trends` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |

### Feature artifacts and datasets
| Method | Path | Zone | Source file |
| --- | --- | --- | --- |
| `GET` | `/api/v1/admin/phase-2-feature-demo` | `dev-only` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/snapshots` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/profile` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/analysis/artifacts` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/analysis/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/splits` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/training-view` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/training-manifest` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/training-bundle` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/training-benchmark` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `GET` | `/api/v1/admin/features/dataset/training-task-matrix` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |

### Model, opportunity, scoring, and market-board reads
| Method | Path | Zone | Source file |
| --- | --- | --- | --- |
| `GET` | `/api/v1/admin/models/backtests/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/registry` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/runs` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/runs/{run_id}` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/summary` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/evaluations` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/evaluations/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/evaluations/{snapshot_id}` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/selections` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/selections/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/selections/{selection_id}` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `GET` | `/api/v1/admin/models/opportunities/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_opportunity_routes.py) |
| `GET` | `/api/v1/admin/models/score-preview` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `GET` | `/api/v1/admin/models/future-game-preview` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `GET` | `/api/v1/admin/models/future-game-preview/runs` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `GET` | `/api/v1/admin/models/future-game-preview/runs/{scoring_run_id}` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `GET` | `/api/v1/admin/models/future-game-preview/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/sources` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/source-runs` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/refresh-queue` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/queue` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/refresh-orchestration-history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/cadence-history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/orchestration-history` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/cadence` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/{board_id}` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `GET` | `/api/v1/admin/models/market-board/{board_id}/operations` | `admin-read` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |

## Admin Mutation

| Method | Path | Zone | Source file |
| --- | --- | --- | --- |
| `POST` | `/api/v1/admin/data-quality/normalize-taxonomy` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_diagnostics_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_diagnostics_routes.py) |
| `POST` | `/api/v1/admin/features/analysis/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_feature_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_feature_routes.py) |
| `POST` | `/api/v1/admin/models/train` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `POST` | `/api/v1/admin/models/backtests/run` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `POST` | `/api/v1/admin/models/select` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_model_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_model_routes.py) |
| `POST` | `/api/v1/admin/models/future-game-preview/opportunities/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_opportunity_routes.py) |
| `POST` | `/api/v1/admin/models/opportunities/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_opportunity_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_opportunity_routes.py) |
| `POST` | `/api/v1/admin/models/future-game-preview/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `POST` | `/api/v1/admin/models/future-slate/preview` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `POST` | `/api/v1/admin/models/future-slate/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_scoring_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_scoring_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/refresh` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/orchestrate-refresh` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/orchestrate-score` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/orchestrate-cadence` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/materialize` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |
| `POST` | `/api/v1/admin/models/market-board/{board_id}/score` | `admin-mutation` | [backend/src/bookmaker_detector_api/api/admin_market_board_routes.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\api\admin_market_board_routes.py) |

## Immediate findings for the refactor
- Analyst routes are mounted under their own namespace, but some still perform seed/train/materialize work when `settings.api_env != "production"`.
- Admin diagnostics and feature routes still use in-memory seeded fallback behavior in non-production, which means many `GET` responses are not truly side-effect-free in development.
- Demo routes are already isolated into `admin_demo_routes.py`, which is a good boundary for future dev-only mounting.
- The stable route topology is far better than the older mega-router state, but the contract-hardening and side-effect cleanup work described in the SDD is still required.
