# Phase 3 Repository Interface Cleanup

## Slice 1: Focused Modeling Store Protocols

This first Phase 3 slice starts Workstream D from [docs/sdd_refactor_execution_plan.md](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\docs\sdd_refactor_execution_plan.md) by introducing focused repository protocols for the extracted model and feature services.

Added protocol boundaries:
- `FeatureDatasetStore`
- `ModelTrainingArtifactStore`
- `ModelScoringArtifactStore`
- `ModelOpportunityStore`
- `ModelBacktestArtifactStore`
- `MarketBoardOperationStore`
- `ModelingRepositoryStore`

Implementation notes:
- New contracts live in [backend/src/bookmaker_detector_api/repositories/modeling_protocols.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\modeling_protocols.py).
- Extracted service modules now type against those protocols instead of the concrete in-memory adapter:
  - [backend/src/bookmaker_detector_api/services/features.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\features.py)
  - [backend/src/bookmaker_detector_api/services/model_training_views.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_training_views.py)
  - [backend/src/bookmaker_detector_api/services/model_training_lifecycle.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_training_lifecycle.py)
  - [backend/src/bookmaker_detector_api/services/model_scoring_runs.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_scoring_runs.py)
  - [backend/src/bookmaker_detector_api/services/model_opportunities.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_opportunities.py)
  - [backend/src/bookmaker_detector_api/services/model_backtest_runs.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_backtest_runs.py)
  - [backend/src/bookmaker_detector_api/services/model_market_board_store.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\model_market_board_store.py)

Why this slice comes first:
- It creates an adapter boundary without changing storage behavior.
- It narrows each workflow module to the exact repository surface it consumes.
- It prepares the repo for the deeper `repositories/ingestion.py` split without forcing that larger move into the same patch.

What this slice does not do:
- It does not change runtime storage selection.
- It does not split [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py).
- It does not remove the compatibility glue still living in [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py).

Next recommended Phase 3 slice:
1. Move adapter construction and twin-path selection behind repository factory surfaces that return protocol-typed stores.
2. Start carving [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) into contract, in-memory adapter, Postgres adapter, and reporting/query helpers.

## Slice 2: Protocol-Typed Factory Wiring

This follow-up slice moves the common in-memory adapter construction behind repository-factory helpers and updates the model-service facade to type against a combined Phase 3 store contract.

Added factory helpers:
- `build_in_memory_feature_dataset_store()`
- `build_in_memory_phase_three_modeling_store()`

Additional implementation notes:
- [backend/src/bookmaker_detector_api/services/models.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\models.py) now annotates in-memory workflow functions with `PhaseThreeModelingStore` instead of the concrete in-memory repository.
- Read-side admin and analyst routes now construct in-memory stores via [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py) instead of directly instantiating the concrete adapter.
- Seeded demo prep helpers keep their existing behavior, but now return protocol-typed stores where possible.

What still remains:
- `backend/src/bookmaker_detector_api/repositories/ingestion.py` is still the concrete monolith.
- Postgres and in-memory mutation/query code are still co-located under the same repository module.
- Admin diagnostics and ingestion maintenance still rely on the ingestion-specific factory path rather than the newer modeling-store helpers.

## Slice 3: In-Memory Adapter Extraction

This slice performs the first concrete carve-out of `backend/src/bookmaker_detector_api/repositories/ingestion.py` by moving the in-memory adapter into its own module.

Implementation notes:
- The full `InMemoryIngestionRepository` implementation now lives in [backend/src/bookmaker_detector_api/repositories/ingestion_in_memory_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_in_memory_repository.py).
- [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) now focuses on the Postgres adapter plus compatibility exports for the ingestion repository contract and JSON serialization helper.
- [backend/src/bookmaker_detector_api/repositories/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\__init__.py) now exports `InMemoryIngestionRepository` from the extracted module directly.

Why this matters:
- It turns the repository split from a planning boundary into a real module boundary.
- It reduces the blast radius for future Postgres-specific changes in `ingestion.py`.
- It sets up the next extraction, which can now focus purely on Postgres/reporting concerns.

Next recommended Phase 3 slice:
1. Extract Postgres reporting/query helpers or the Postgres adapter itself out of [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py).
2. Move shared serialization helpers such as `_json_dumps` into a small repository utility module so services no longer import them from the Postgres adapter module.

## Slice 4: Postgres Adapter and JSON Utility Extraction

This slice completes the matching Postgres-side move by extracting the Postgres adapter into its own module and separating the shared JSON serialization helper from the adapter modules.

Implementation notes:
- The full `PostgresIngestionRepository` implementation now lives in [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py).
- Shared JSON serialization now lives in [backend/src/bookmaker_detector_api/repositories/ingestion_json.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_json.py).
- [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) is now a compatibility shim that re-exports the ingestion contract, record types, adapter classes, and `_json_dumps`.
- Modeling and market-board services now import `_json_dumps` from the shared JSON utility module instead of reaching through the repository adapter module.

Why this matters:
- `ingestion.py` is no longer a concrete implementation hotspot.
- The in-memory and Postgres adapters now have separate module ownership.
- Shared serialization no longer depends on whichever adapter module happened to define it first.

Next recommended Phase 3 slice:
1. Split Postgres reporting/query helpers out of [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py).
2. Start routing ingestion-specific callers away from the compatibility shim and onto the direct module paths where appropriate.

## Slice 5: Postgres Reporting Helper Extraction

This slice extracts the Postgres reporting and query surface out of the adapter class into a dedicated helper module, leaving the repository class as a thinner adapter facade.

Implementation notes:
- Read/query helpers now live in [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_reporting.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_reporting.py).
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py) now delegates its reporting, counts, and taxonomy-normalization methods to that module.
- The remaining `PostgresIngestionRepository` hotspot is now concentrated around write-path persistence and runtime schema-identity helpers rather than the entire read/query surface.

Why this matters:
- It separates mutation-heavy adapter logic from reporting/query logic.
- It makes the reporting surface easier to test and to move again later if a dedicated reporting-query layer is introduced.
- It reduces the size and cognitive load of the main Postgres adapter module without changing the public repository contract.

Next recommended Phase 3 slice:
1. Start routing ingestion-specific callers away from [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) onto the direct module paths where appropriate.
2. Extract or isolate the runtime schema-identity helpers in [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py) as the next step toward Workstream E.

## Slice 6: Shim Cleanup and Schema Helper Isolation

This slice removes the remaining active service dependence on the legacy `repositories.ingestion` shim and isolates the Postgres runtime schema-identity helpers into their own module.

Implementation notes:
- Ingestion services now import repository contracts and record types from direct modules:
  - [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py)
  - [backend/src/bookmaker_detector_api/services/admin_diagnostics.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\admin_diagnostics.py)
  - [backend/src/bookmaker_detector_api/services/fetch_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\fetch_ingestion_runner.py)
  - [backend/src/bookmaker_detector_api/services/initial_dataset_load.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\initial_dataset_load.py)
  - [backend/src/bookmaker_detector_api/services/ingestion_pipeline.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\ingestion_pipeline.py)
- Runtime schema-identity helpers now live in [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_schema.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_schema.py).
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py) now delegates to that schema module while retaining its readiness flags.

Why this matters:
- The compatibility shim is now mostly for backwards compatibility rather than active internal usage.
- Runtime DDL-related behavior is now isolated and easier to review as part of the Workstream E schema-ownership hardening.
- The Postgres adapter is narrower and more explicit about which concerns still remain inside it.

Next recommended Phase 3 slice:
1. Trim or remove unused compatibility exports from [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) where safe.
2. Start the Workstream E crossover: review whether the schema-identity helpers should stay runtime-reachable or move behind explicit maintenance/bootstrap flows only.

## Slice 7: Explicit Runtime Schema-Mutation Policy

This slice turns the remaining runtime schema-mutation behavior into an explicit policy decision instead of an unconditional side effect of Postgres-backed write flows.

Implementation notes:
- [backend/src/bookmaker_detector_api/config.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\config.py) now exposes `postgres_allow_runtime_schema_mutation` with an environment-aware default:
  - defaults to `false` in production
  - defaults to `true` outside production unless explicitly overridden
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_schema.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_schema.py) now provides non-mutating verification helpers for the raw-row identity schema and data-quality identity schema.
- [backend/src/bookmaker_detector_api/repositories/ingestion_postgres_repository.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_postgres_repository.py) now:
  - accepts `allow_runtime_schema_mutation`
  - mutates schema only when that flag is enabled
  - otherwise fails fast with a clear readiness error if the identity schema is missing
- Explicit bootstrap/demo flows still opt in to runtime schema mutation:
  - [backend/src/bookmaker_detector_api/services/initial_dataset_load.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\initial_dataset_load.py)
  - [backend/src/bookmaker_detector_api/services/fixture_ingestion_runner.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\fixture_ingestion_runner.py)
  - [backend/src/bookmaker_detector_api/demo.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\demo.py)

Why this matters:
- Production runtime behavior is now aligned with the SDD direction: fail fast on missing schema instead of silently mutating it.
- Maintenance/bootstrap flows still have a supported path for legacy identity-schema preparation.
- The remaining schema-mutation behavior is now explicit and reviewable at construction time.

Next recommended Phase 3 slice:
1. Trim the remaining compatibility surface in [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) where safe.
2. Add targeted tests for the new `allow_runtime_schema_mutation` policy and verification-failure paths on the Postgres adapter.

## Slice 8: Repository Policy Test Coverage

This slice adds direct regression coverage around the new repository policy boundary and catches a missing config import in the factory wiring.

Implementation notes:
- Added targeted policy tests in [backend/tests/test_repository_policy.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_policy.py) covering:
  - config default resolution for `postgres_allow_runtime_schema_mutation`
  - `build_ingestion_repository(...)` policy propagation
  - legacy-constructor fallback for bootstrap repository construction
  - Postgres adapter behavior when runtime schema mutation is enabled
  - Postgres adapter behavior when runtime schema mutation is disabled and schema verification passes or fails
- Fixed [backend/src/bookmaker_detector_api/services/repository_factory.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\services\repository_factory.py) so it imports `settings` explicitly instead of relying on an undeclared global.

Why this matters:
- The new Workstream E crossover policy is now protected by direct tests instead of only indirect route coverage.
- The fallback path for older constructor shapes remains intentional and verified.
- The repository factory wiring now has explicit config ownership.

Next recommended Phase 3 slice:
1. Trim the remaining compatibility exports in [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) only after confirming there are no external/thread-local consumers that still rely on them.
2. If you want to keep moving immediately, Phase 4 is now a clean transition point.

## Slice 9: Compatibility Surface Finalization

This final Phase 3 slice closes the repository-interface cleanup by making the package-level ingestion contract export come from its canonical protocol module instead of the Postgres adapter alias.

Implementation notes:
- [backend/src/bookmaker_detector_api/repositories/__init__.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\__init__.py) now exports `IngestionRepository` directly from [backend/src/bookmaker_detector_api/repositories/ingestion_types.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion_types.py).
- [backend/src/bookmaker_detector_api/repositories/ingestion.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\src\bookmaker_detector_api\repositories\ingestion.py) remains in place only as a minimal legacy compatibility module and now clearly imports the protocol from `ingestion_types` instead of routing through the Postgres adapter.
- [backend/tests/test_repository_policy.py](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\backend\tests\test_repository_policy.py) now verifies that both the package export and the legacy shim resolve to the same canonical `IngestionRepository` protocol object.

Why this matters:
- The repository contract now has a single authoritative home.
- Package consumers no longer depend on the Postgres adapter module just to type against the ingestion contract.
- The legacy shim is still available for compatibility, but it is no longer part of the internal architectural path.

Phase 3 exit state:
- Modeling services type against focused repository protocols.
- In-memory and Postgres ingestion adapters live in dedicated modules.
- Postgres reporting, schema policy, and shared JSON serialization have their own module boundaries.
- Runtime schema mutation is an explicit policy with direct regression coverage.
- Internal code now uses direct module paths or package exports rooted in canonical contract modules, not the old `repositories.ingestion` monolith.

Next recommended step:
1. Start Phase 4 from the schema-ownership and migration hardening backlog, using the new repository/module boundaries as the base.
