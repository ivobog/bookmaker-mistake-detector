# Phase 4 Schema Ownership Hardening

## Slice 1: Non-Mutating Runtime by Default

This first Phase 4 slice starts Workstream E from [docs/sdd_refactor_execution_plan.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\sdd_refactor_execution_plan.md) by removing the last permissive default that allowed normal Postgres runtime flows to mutate schema outside production.

Implementation notes:
- [backend/src/bookmaker_detector_api/config.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\config.py) now resolves `postgres_allow_runtime_schema_mutation` to `false` by default in every environment.
- Normal Postgres repository construction through [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py) therefore defaults to verification-only behavior unless an explicit override is supplied.
- Explicit bootstrap/demo flows still retain the supported opt-in path by calling `PostgresIngestionRepository(..., allow_runtime_schema_mutation=True)` through bootstrap helpers.
- [backend/tests/test_repository_policy.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_policy.py) now verifies both sides of that boundary:
  - normal runtime defaults to non-mutating
  - explicit bootstrap helpers still enable runtime mutation when intentionally requested

Why this matters:
- Schema ownership is now aligned with the runbook in every environment, not just production.
- Local development and reused Docker volumes now fail fast when the schema is stale instead of silently drifting it during normal API or worker execution.
- The remaining runtime schema mutation path is clearly isolated to bootstrap/demo maintenance flows, which is the right base for a later migration-tooling decision.

Current explicit schema-mutation entry points:
- [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py) via `build_bootstrap_postgres_ingestion_repository(...)`
- [backend/src/bookmaker_detector_api/services/initial_dataset_load.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\initial_dataset_load.py)
- [backend/src/bookmaker_detector_api/services/fixture_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\fixture_ingestion_runner.py)
- [backend/src/bookmaker_detector_api/demo.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\demo.py)

What this slice does not do yet:
- It does not introduce Alembic or another migration runner.
- It does not move the identity-schema SQL/backfill/index logic into versioned migrations yet.
- It does not classify all JSON payload fields into structured-column versus provenance-only buckets yet.

Next recommended Phase 4 slice:
1. Inventory the remaining identity-schema mutation SQL in [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_schema.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_schema.py) and map it to bootstrap SQL or migration-owned changes.
2. Document the structured-column versus provenance-JSON split for ingestion, scoring, opportunity, and backtest artifacts so migration planning has a concrete target.

## Slice 2: Runtime-Mutation Ownership Inventory

This follow-up slice turns the remaining runtime schema-mutation helpers into an explicit ownership inventory so the next migration-planning step can work from a concrete, tested map.

Implementation notes:
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_schema.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_schema.py) now exposes a `RuntimeSchemaMutationOwnership` inventory for the two remaining mutation helpers:
  - `ensure_raw_row_source_identity_schema`
  - `ensure_data_quality_issue_identity_schema`
- Each inventory entry records:
  - the helper name
  - the owning bootstrap SQL file under `infra/postgres/init/`
  - the schema/data-shape operations it performs
  - the current migration target state
- [backend/tests/test_repository_schema_ownership.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_schema_ownership.py) now verifies that the inventory points to real bootstrap SQL files.

Ownership mapping captured in this slice:
- `ensure_raw_row_source_identity_schema`
  - bootstrap owner: [infra/postgres/init/019_phase5_raw_row_source_identity_schema.sql](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\infra\postgres\init\019_phase5_raw_row_source_identity_schema.sql)
  - operations: source identity columns, backfill, unique coordinate index
- `ensure_data_quality_issue_identity_schema`
  - bootstrap owner: [infra/postgres/init/020_phase7_data_quality_issue_identity_schema.sql](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\infra\postgres\init\020_phase7_data_quality_issue_identity_schema.sql)
  - operations: duplicate cleanup, unique identity index

Why this matters:
- The remaining mutation helpers are now explicitly tied to bootstrap-owned SQL instead of implicitly drifting between runtime code and infra scripts.
- Migration planning can now start from named ownership records rather than a code search.
- The inventory is tested, so future edits that move or remove the owning SQL file will fail fast in CI.

Next recommended Phase 4 slice:
1. Classify the major JSON payload fields across ingestion, training, scoring, opportunity, and backtest artifacts into queryable-column versus provenance-only buckets.
2. Decide whether to introduce Alembic now or keep using the bootstrap SQL chain until that classification work is complete.

## Slice 3: JSON Column Ownership Classification

This slice classifies the major Postgres JSON columns across ingestion, features, modeling, scoring, opportunities, backtests, and market-board orchestration so future schema work has a concrete rule set.

Implementation notes:
- Added a code-backed inventory in [backend/src/bookmaker_detector_api/repositories/postgres_json_ownership.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\postgres_json_ownership.py).
- Added regression coverage in [backend/tests/test_repository_json_ownership.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_json_ownership.py) so the inventory remains unique and continues to cover the key workflow tables.
- The inventory records, for each JSON column:
  - classification
  - the structured columns that should remain the query/join surface
  - rationale for keeping the nested payload in JSON
  - promotion candidates where future schema pressure is likely

Classification rules captured in this slice:
- `workflow_request_provenance` and `workflow_result_provenance`
  - keep raw request/result context in JSON, but query through reporting snapshots or explicit columns
- `versioned_config`
  - keep feature/model version configuration in JSON because it is sparse version metadata
- `derived_feature_payload`
  - keep wide feature vectors in JSON; slice by feature version, teams, seasons, and games via columns
- `*_provenance` and `*_payload`
  - keep nested predictions, evidence, fold summaries, operation diffs, and source payloads in JSON
  - do not build new operator filters on nested JSON keys; promote new keys into explicit columns first

High-value conclusions:
- Ingestion:
  - keep `raw_team_game_row.parse_warning_codes_json`, `canonical_game.source_row_indexes_json`, and `data_quality_issue.details_json` as provenance/audit JSON
  - continue querying by structured row/game/issue identity columns
- Features and modeling:
  - keep `feature_payload_json`, `artifact_json`, `metrics_json`, `snapshot_json`, and `rationale_json` as JSON
  - continue querying by feature version, task, model family, team/season scope, and promoted evaluation metrics
- Scoring, opportunities, and backtests:
  - keep `model_scoring_run.payload_json`, `model_opportunity.payload_json`, and `model_backtest_run.payload_json` as provenance payloads
  - continue querying by scenario/opportunity identity, status, ratings, counts, task, scope, and execution knobs already stored as columns
  - if future operator workflows need direct filtering on ROI or other headline backtest outcomes, promote those to columns instead of querying inside JSON

Next recommended Phase 4 slice:
1. Decide whether Alembic should be introduced now that both mutation ownership and JSON-column ownership are explicitly mapped.
2. If migration tooling is deferred, document the bootstrap-SQL chain as the temporary migration authority and note the first promotion candidates for later schema changes.

## Slice 4: Migration Authority Decision

This slice makes the current migration decision explicit: the repo continues to use the ordered bootstrap SQL chain as the temporary migration authority, and Alembic remains intentionally deferred for now.

Implementation notes:
- Added a code-backed authority record in [backend/src/bookmaker_detector_api/repositories/postgres_migration_authority.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\postgres_migration_authority.py).
- The authority record captures:
  - the current migration authority
  - Alembic status
  - rationale for deferring migration tooling
  - concrete triggers for introducing it
- The ordered bootstrap chain is now explicitly recorded there as well, covering `001_reference_schema.sql` through `021_phase3_market_board_source_run_schema.sql`.
- Added regression coverage in [backend/tests/test_repository_migration_authority.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_migration_authority.py) so the chain remains ordered and all referenced files exist.
- Updated [backend/README.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\README.md) and [docs/release_candidate_runbook.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\release_candidate_runbook.md) so operational docs match the decision.

Decision captured in this slice:
- Current authority: bootstrap SQL chain in `infra/postgres/init/`
- Alembic status: deferred
- Why it is deferred:
  - there is no existing Alembic footprint in the repo
  - runtime schema mutation is already disabled by default
  - mutation ownership and JSON-column ownership are now explicitly mapped
- Triggers to introduce Alembic:
  - a populated environment needs forward-only schema evolution
  - a JSON promotion candidate needs multi-step backfill work
  - overlapping branches start evolving Postgres schema in parallel release windows

Why this matters:
- The repo now has an explicit answer to “what is our migration authority today?”
- Operators and future contributors no longer need to infer whether bootstrap SQL or a hidden migration runner is authoritative.
- The eventual Alembic adoption point is now criteria-based rather than vague.

Next recommended Phase 4 slice:
1. Promote the first concrete JSON-to-column candidates only if an operator-facing filter or dashboard now depends on them.
2. Otherwise Phase 4 is in a good state to close and hand off to the next workstream.

## Slice 5: Deferred JSON Promotion Backlog

This slice checks whether any current operator path already depends on nested JSON fields strongly enough to justify an immediate column promotion. The answer today is no, so the result is an explicit deferred promotion backlog instead of speculative schema churn.

Implementation notes:
- Added a code-backed backlog in [backend/src/bookmaker_detector_api/repositories/postgres_json_promotion_candidates.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\postgres_json_promotion_candidates.py).
- Added regression coverage in [backend/tests/test_repository_json_promotion_candidates.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_json_promotion_candidates.py).
- Every candidate records:
  - source table and JSON column
  - JSON path
  - proposed column name
  - priority
  - current action
  - trigger for promotion
  - rationale

Current conclusion:
- No immediate JSON-to-column promotion is required for existing operator workflows.
- Current operator-facing filters already use structured columns for:
  - evaluation metrics
  - evidence rating
  - recommendation status
  - scoring/opportunity counts
  - scenario, team, season, and task identity

Deferred promotion shortlist:
1. `model_backtest_run.payload_json -> strategy_results.candidate_threshold.roi`
2. `model_backtest_run.payload_json -> strategy_results.candidate_threshold.profit_units`
3. `model_backtest_run.payload_json -> strategy_results.candidate_threshold.hit_rate`
4. `model_scoring_run.payload_json -> prediction_summary.top_signal_strength`
5. `model_market_board_source_run.payload_json -> validation_summary.invalid_row_count`

Why this matters:
- The backlog makes the next schema promotions intentional and evidence-based.
- Phase 4 closes without introducing premature columns that no current query path needs.
- The highest-value future promotion target is now explicit: backtest headline metrics if SQL-side ranking or filtering becomes necessary.

Phase 4 closeout state:
- Runtime schema mutation is disabled by default.
- Remaining mutation helpers are mapped to bootstrap-owned SQL.
- JSON column ownership is classified.
- Migration authority is explicit.
- First promotion candidates are named and intentionally deferred.

Next recommended step:
1. Close Phase 4 and move to the frontend shell/API-client workstream.
