# Configuration Reference

This document lists the configuration parameters that can be set for the app and the defaults currently implemented in the checked-in code.

## Environment Variables

These values are typically provided through `.env` and consumed by Docker Compose, the backend, the worker, or the frontend build.

| Parameter | Default in code/runtime | Example in `.env.example` | Notes |
| --- | --- | --- | --- |
| `POSTGRES_DB` | Compose-managed | `bookmaker_detector` | Used by Docker Compose to build the Postgres connection string. |
| `POSTGRES_USER` | Compose-managed | `bookmaker` | Used by Docker Compose to build the Postgres connection string. |
| `POSTGRES_PASSWORD` | Compose-managed | `bookmaker` | Used by Docker Compose to build the Postgres connection string. |
| `POSTGRES_PORT` | `5432` in Compose | `5432` | Host port mapped to Postgres. |
| `BACKEND_PORT` | `8000` in Compose | `8000` | Host port mapped to the backend container. |
| `FRONTEND_PORT` | `5173` in Compose | `5173` | Host port mapped to the frontend container. |
| `API_ENV` | `development` | `development` | Backend environment mode. |
| `API_ENABLE_TEST_HELPERS` | `false` | `true` | Test helpers are only enabled when `API_ENV` is not `production`. |
| `API_HOST` | `0.0.0.0` | `0.0.0.0` | Backend bind host. |
| `API_PORT` | `8000` | `8000` | Backend bind port inside the container/process. |
| `API_CORS_ORIGINS` | `http://localhost:5173,http://127.0.0.1:5173,http://localhost:4173,http://127.0.0.1:4173` | same | Comma-separated backend CORS allowlist. |
| `DATABASE_URL` | `postgresql://bookmaker:bookmaker@localhost:5432/bookmaker_detector` | `postgresql://bookmaker:bookmaker@postgres:5432/bookmaker_detector` | Backend default is localhost for non-Compose execution; `.env.example` targets the Compose service name. |
| `RAW_PAYLOAD_DIR` | `artifacts/raw-pages` | `artifacts/raw-pages` | Backend and worker payload storage location. |
| `PARSER_SNAPSHOT_DIR` | `artifacts/parser-output` | not set | Backend parser snapshot output directory. |
| `THE_ODDS_API_KEY` | `null` | not set | Required for The Odds API-backed market-board refresh flows. |
| `THE_ODDS_API_BASE_URL` | `https://api.the-odds-api.com/v4` | not set | Base URL for The Odds API. |
| `THE_ODDS_API_SPORT_KEY` | `basketball_nba` | not set | The Odds API sport key. |
| `THE_ODDS_API_REGIONS` | `us` | not set | The Odds API regions filter. |
| `THE_ODDS_API_MARKETS` | `spreads,totals` | not set | The Odds API markets filter. |
| `THE_ODDS_API_ODDS_FORMAT` | `american` | not set | The Odds API odds format. |
| `THE_ODDS_API_BOOKMAKERS` | `null` | not set | Optional The Odds API bookmaker filter. |
| `THE_ODDS_API_TIMEOUT_SECONDS` | `10.0` | not set | Timeout for The Odds API calls. |
| `POSTGRES_ALLOW_RUNTIME_SCHEMA_MUTATION` | `false` | not set | Runtime schema mutation is disabled by default in every environment. |
| `WORKER_ENV` | `development` | `development` | Worker environment mode. |
| `WORKER_POLL_INTERVAL_SECONDS` | `60` | `60` | Worker loop interval. Set to `0` or below to run once and exit. |
| `WORKER_JOB_MODE` | `idle` | `fixture_ingestion` | Supported values in code are `idle`, `fixture_ingestion`, `fetch_and_ingest`, and `production_dataset_load`. |
| `WORKER_TEAM_CODE` | `LAL` | `LAL` | Default team scope for worker jobs that operate on one team. |
| `WORKER_SEASON_LABEL` | `2024-2025` | `2024-2025` | Default season scope for worker jobs that operate on one season. |
| `WORKER_SOURCE_URL` | `https://example.com/covers/lal/2024-2025` | same | Source URL for one-team worker ingestion. |
| `WORKER_DATASET_SOURCE_URL_TEMPLATE` | `null` | empty | Required when `WORKER_JOB_MODE=production_dataset_load`. |
| `WORKER_DATASET_TEAM_CODES` | `null` | empty | CSV list consumed by `production_dataset_load`. |
| `WORKER_DATASET_SEASON_LABELS` | `null` | empty | CSV list consumed by `production_dataset_load`. |
| `WORKER_DATASET_REQUESTED_BY` | `worker-initial-production-dataset-load` | same | Audit label for production dataset loads. |
| `WORKER_DATASET_RUN_LABEL` | `initial-production-dataset-load` | same | Run label for production dataset loads. |
| `WORKER_DATASET_CONTINUE_ON_ERROR` | `true` | `true` | Continue when one target load fails. |
| `WORKER_DATASET_PERSIST_PAYLOAD` | `true` | `true` | Persist fetched raw payloads during dataset load. |
| `VITE_API_BASE_URL` | `http://localhost:8000` | `http://localhost:8000` | Frontend API base URL. |
| `VITE_DEFAULT_TARGET_TASK` | unset; frontend falls back to backend capabilities | not set | Overrides the backend-provided default target task in the UI. |
| `VITE_DEFAULT_TRAIN_RATIO` | `0.7` | not set | Frontend default training ratio. |
| `VITE_DEFAULT_VALIDATION_RATIO` | `0.15` | not set | Frontend default validation ratio. |
| `VITE_DEFAULT_MINIMUM_TRAIN_GAMES` | `2000` | not set | Frontend default backtest train-window floor. |
| `VITE_DEFAULT_TEST_WINDOW_GAMES` | `200` | not set | Frontend default backtest test-window size. |
| `VITE_DEFAULT_SEASON_LABEL` | `null` | not set | Frontend default manual future-scenario season. |
| `VITE_DEFAULT_HOME_TEAM_CODE` | `null` | not set | Frontend default manual future-scenario home team. |
| `VITE_DEFAULT_AWAY_TEAM_CODE` | `null` | not set | Frontend default manual future-scenario away team. |
| `VITE_DEFAULT_GAME_DATE` | `null` | not set | Frontend default manual future-scenario game date. |

## Frontend Runtime Defaults

These defaults are applied by the frontend when it builds query strings for API calls.

| Parameter | Default |
| --- | --- |
| `apiBaseUrl` | `http://localhost:8000` |
| `train_ratio` | `0.7` |
| `validation_ratio` | `0.15` |
| `minimum_train_games` | `2000` |
| `test_window_games` | `200` |
| `opportunity list limit` | `25` |
| `history recent_limit` | `6` |
| `canonical selection policy name` | `validation_regression_candidate_v1` |

## Backend Capability Defaults

These defaults are exposed through the backend model-capabilities payload and are used when a client does not send an explicit override.

| Parameter | Default |
| --- | --- |
| `default_feature_key` | `baseline_team_features_v1` |
| `default_train_ratio` | `0.7` |
| `default_validation_ratio` | `0.15` |
| `default_selection_policy_name` | `validation_regression_candidate_v1` |
| `legacy_selection_policy_name` | `validation_mae_candidate_v1` |
| `default_target_task` | `spread_error_regression` |

## Bootstrapped Target Tasks

The PostgreSQL bootstrap seeds the following enabled target tasks:

| Task Key | Default UI Task | Opportunity Policy |
| --- | --- | --- |
| `spread_error_regression` | `true` | `spread_edge_policy_v1` |
| `total_error_regression` | `false` | `total_edge_policy_v1` |
| `point_margin_regression` | `false` | `margin_signal_policy_v1` |
| `total_points_regression` | `false` | `totals_signal_policy_v1` |

All four target tasks default to:

| Setting | Default |
| --- | --- |
| `default_selection_policy_name` | `validation_regression_candidate_v1` |
| `selection_policy_names` | `validation_regression_candidate_v1`, `validation_mae_candidate_v1` |
| `workflow_support.training` | `true` |
| `workflow_support.selection` | `true` |
| `workflow_support.scoring` | `true` |
| `workflow_support.opportunity_materialization` | `true` |
| `workflow_support.market_board` | `true` |
| `workflow_support.backtesting` | `true` |

## Endpoint-Level Defaults

These are the main workflow defaults exposed by the current admin API routes.

### Training and Backtesting

| Parameter | Default |
| --- | --- |
| `feature_key` | `baseline_team_features_v1` |
| `target_task` | backend default target task |
| `team_code` | `null` |
| `season_label` | `null` |
| `train_ratio` | `0.7` |
| `validation_ratio` | `0.15` |
| `selection_policy_name` | `validation_regression_candidate_v1` |
| `minimum_train_games` | `1` on the backend route; frontend currently sends `2000` |
| `test_window_games` | `1` on the backend route; frontend currently sends `200` |

## Training Parameters Explained

This section focuses on the parameters that most directly affect model training, model selection, and walk-forward evaluation quality.

### `feature_key`

- Default: `baseline_team_features_v1`
- Current practical values:
  - `baseline_team_features_v1`
- What it controls:
  - Chooses the feature snapshot version used to build the training dataset.
- Why it matters:
  - This is the foundation of the model. If the feature set changes, the learned coefficients, thresholds, and downstream scoring behavior can change significantly.
- How to think about values:
  - Use the default feature key for normal operation.
  - Introduce a new feature key only when you intentionally materialize and validate a new feature-engineering version.
- Risks:
  - Changing this without regenerating the corresponding feature snapshots can lead to empty or misleading training runs.

### `target_task`

- Default: backend capability default, currently `spread_error_regression`
- Supported values:
  - `spread_error_regression`
  - `total_error_regression`
  - `point_margin_regression`
  - `total_points_regression`
- What it controls:
  - Defines the prediction target the models are trained against.
- Why it matters:
  - Each task changes the meaning of the label, the interpretation of the output, the opportunity policy, and the backtesting semantics.
- How to think about values:
  - `spread_error_regression`: best when you want the model to learn edge relative to the spread directly.
  - `total_error_regression`: best when you want edge relative to the total directly.
  - `point_margin_regression`: predicts raw margin, then compares it with the market spread.
  - `total_points_regression`: predicts raw total points, then compares it with the market total.
- Risks:
  - Comparing metrics across different target tasks is usually not apples-to-apples, because the label definitions differ.

### `team_code`

- Default: `null`
- Practical values:
  - `null` for league-wide training
  - a single team code such as `LAL`, `BOS`, `DAL`
- What it controls:
  - Scopes the dataset to one team when provided.
- Why it matters:
  - This is a bias-variance tradeoff.
- How to think about values:
  - `null`: more data, broader patterns, usually more stable model fitting.
  - one team code: less data, more team-specific behavior, more sensitive to noise.
- Importance to training:
  - Smaller scoped datasets can overfit more easily, especially with tree-stump feature splits and backtests with too many folds.
- Good use cases:
  - Team-specific diagnostics or experiments.
- Risks:
  - Too little data can make validation unstable or produce weak model comparisons.

### `season_label`

- Default: `null`
- Practical values:
  - `null` for all available seasons
  - a season label such as `2024-2025`
- What it controls:
  - Filters the training data to a single season when set.
- Why it matters:
  - Another data-scope tradeoff: recency versus sample size.
- How to think about values:
  - `null`: more observations, usually better statistical stability.
  - one season: more recent context, but less data.
- Importance to training:
  - Restricting to one season may help when the environment shifts over time, but it can also reduce the amount of usable training history too much.
- Risks:
  - Very narrow seasonal scopes can make train, validation, and test slices too small to compare models reliably.

### `train_ratio`

- Default: `0.7`
- Allowed values:
  - greater than `0` and less than `1`
- What it controls:
  - Share of the chronological dataset allocated to the training split.
- Why it matters:
  - Training data volume strongly affects model stability and how well the model can learn patterns.
- How to think about values:
  - lower values like `0.5` to `0.6`: more room for validation/test, but less data to learn from.
  - moderate values like `0.7`: balanced default.
  - higher values like `0.8` to `0.9`: more training data, but less room to judge generalization.
- Importance to training:
  - Too low: the model may underfit because it does not see enough history.
  - Too high: model comparison becomes less trustworthy because validation and test slices get small.
- Recommended starting point:
  - `0.7` is a sensible default for most runs in this codebase.

### `validation_ratio`

- Default: `0.15`
- Allowed values:
  - greater than or equal to `0`
  - less than `1`
- What it controls:
  - Share of the dataset reserved for validation after the training split.
- Why it matters:
  - Validation is used to compare model candidates and decide which snapshot wins selection.
- How to think about values:
  - `0`: no dedicated validation split; selection falls back more heavily on train behavior, which is weaker.
  - `0.1` to `0.2`: common practical range.
  - higher values: stronger holdout for model comparison, but less training data.
- Importance to training:
  - This is one of the key anti-overfitting controls in the workflow.
- Recommended starting point:
  - `0.15` is a good default because it leaves a distinct validation window while still preserving a reasonable train set.

### `selection_policy_name`

- Default: `validation_regression_candidate_v1`
- Supported values in current task configuration:
  - `validation_regression_candidate_v1`
  - `validation_mae_candidate_v1`
- What it controls:
  - Decides how candidate evaluation snapshots are ranked during model selection.
- Why it matters:
  - The best model is not just a function of raw fit; it depends on the selection policy used to compare candidates.
- How to think about values:
  - `validation_regression_candidate_v1`: the canonical current policy; use this unless you are reproducing legacy behavior.
  - `validation_mae_candidate_v1`: legacy-compatible name retained for compatibility.
- Importance to training:
  - Different selection policies can choose different winning models even on the same dataset.
- Recommended starting point:
  - Use `validation_regression_candidate_v1`.

### `minimum_train_games`

- Backend route default: `1`
- Frontend default for backtests: `2000`
- Allowed values:
  - integer `>= 1`
- What it controls:
  - In walk-forward backtests, the minimum number of games required before the first fold can train.
- Why it matters:
  - This is one of the main levers controlling fold count, training-set maturity, and runtime.
- How to think about values:
  - very low values like `1` to `50`: mostly useful for toy/demo data, not for realistic synchronous runs.
  - moderate to high values like `1000` to `2000`: much safer for production-like datasets because each fold trains on a more mature sample.
- Importance to training:
  - Low values can make early folds very noisy and unrepresentative.
  - Higher values make each fold more credible but reduce the number of folds.
- Operational importance:
  - Too small a value can explode fold count and make backtests appear to hang.

### `test_window_games`

- Backend route default: `1`
- Frontend default for backtests: `200`
- Allowed values:
  - integer `>= 1`
- What it controls:
  - Number of games evaluated in each walk-forward backtest fold.
- Why it matters:
  - This determines how many folds the backtest creates and how stable each fold-level metric is.
- How to think about values:
  - very small values like `1` to `10`: very granular but computationally expensive and noisy.
  - medium values like `50` to `200`: much more practical for synchronous execution.
  - large values: fewer folds, faster runtime, but less resolution across time.
- Importance to training:
  - Small windows make fold metrics jump around more.
  - Larger windows smooth variance and reduce runtime substantially.
- Operational importance:
  - This is the second major runtime-control parameter for backtests, together with `minimum_train_games`.

### Training-Scope Interaction Notes

- `team_code=null` and `season_label=null` generally produce the most stable training runs because they maximize sample size.
- `train_ratio` and `validation_ratio` should be chosen together, not independently. Increasing one necessarily reduces the remainder available to the other splits.
- `minimum_train_games` and `test_window_games` matter only for walk-forward backtests, not for basic training runs.
- `target_task`, `feature_key`, and `selection_policy_name` define the training problem itself. The rest mostly define scope, split behavior, or evaluation strategy.

### Historical Opportunity Materialization

| Parameter | Default |
| --- | --- |
| `feature_key` | `baseline_team_features_v1` |
| `target_task` | backend default target task |
| `team_code` | `null` |
| `season_label` | `null` |
| `canonical_game_id` | `null` |
| `limit` | `10` |
| `include_evidence` | `true` |
| `dimensions` | `venue`, `days_rest_bucket` |
| `comparable_limit` | `5` |
| `min_pattern_sample_size` | `1` |
| `train_ratio` | `0.7` |
| `validation_ratio` | `0.15` |

### Future Game Preview and Opportunity Materialization

| Parameter | Default |
| --- | --- |
| `feature_key` | `baseline_team_features_v1` |
| `target_task` | backend default target task |
| `season_label` | `2025-2026` |
| `game_date` | `2026-04-20` in the checked-in route defaults |
| `home_team_code` | `LAL` |
| `away_team_code` | `BOS` |
| `home_spread_line` | `null` |
| `total_line` | `null` |
| `include_evidence` | `true` |
| `dimensions` | `venue`, `days_rest_bucket` |
| `comparable_limit` | `5` |
| `min_pattern_sample_size` | `1` |
| `train_ratio` | `0.7` |
| `validation_ratio` | `0.15` |

## Variables Present But Currently Unused

These names still appear in `.env.example` or `docker-compose.yml`, but the checked-in Python and frontend code does not currently read them directly:

- `API_REPOSITORY_MODE`
- `WORKER_REPOSITORY_MODE`

They are safe to leave in local env files for now, but they do not currently change application behavior.
