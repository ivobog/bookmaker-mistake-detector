# Phase 2 Service Decomposition

## Completed Slices

The first Phase 2 extraction moved the model training query/read-model logic out of `backend/src/bookmaker_detector_api/services/models.py` into `backend/src/bookmaker_detector_api/services/model_training_views.py`.

This slice now owns:

- model registry listing
- model training run listing and detail lookup
- evaluation snapshot listing and detail lookup
- selection snapshot listing and detail lookup
- training summary and training history rollups
- evaluation history rollups
- selection history rollups

`backend/src/bookmaker_detector_api/services/models.py` now delegates those public read functions to the extracted module, preserving the existing import surface for routes and tests.

The next Phase 2 extraction moved the remaining model training lifecycle write logic into `backend/src/bookmaker_detector_api/services/model_training_lifecycle.py`.

This slice now owns:

- model registry ensure/upsert flows
- model training run persistence
- evaluation snapshot persistence
- model promotion and active selection creation
- selection snapshot persistence

`backend/src/bookmaker_detector_api/services/models.py` now delegates both the training read surface and the training write/promotion surface, leaving the legacy module as a compatibility facade for this workflow area.

The next Phase 2 extraction moved the scoring-run persistence/history boundary into `backend/src/bookmaker_detector_api/services/model_scoring_runs.py`.

This slice now owns:

- scoring run record construction
- scoring run persistence
- scoring run listing and detail lookup
- scoring history rollups

`backend/src/bookmaker_detector_api/services/models.py` still owns scoring preview generation and future-game/future-slate preview orchestration, but now delegates the scoring run store/history layer to the extracted module.

The next Phase 2 extraction moved the scoring preview computation boundary into `backend/src/bookmaker_detector_api/services/model_scoring_previews.py`.

This slice now owns:

- active selection and evaluation snapshot resolution helpers for scoring
- scored prediction serialization and summary logic
- scoring preview computation
- future-game preview computation
- scenario serialization and preview status shaping

`backend/src/bookmaker_detector_api/services/models.py` still owns the repository-loading wrappers and the higher-level future-slate/opportunity orchestration, but now delegates the scoring computation layer to the extracted module.

The next Phase 2 extraction moved the future-scenario orchestration layer into `backend/src/bookmaker_detector_api/services/model_future_scenarios.py`.

This slice now owns:

- future-game materialization orchestration
- future-opportunity materialization orchestration
- future-slate preview orchestration
- future-slate materialization orchestration
- shared future-game input/date/slate response helpers

`backend/src/bookmaker_detector_api/services/models.py` still owns the underlying repository loaders and opportunity-building persistence internals, but now delegates the future scenario orchestration layer to the extracted module.

The next Phase 2 extraction moved the opportunity workflow into `backend/src/bookmaker_detector_api/services/model_opportunities.py`.

This slice now owns:

- historical opportunity materialization shaping
- opportunity record construction and status evaluation
- opportunity persistence
- opportunity list/detail reads
- opportunity history rollups

`backend/src/bookmaker_detector_api/services/models.py` now delegates the opportunity workflow as a compatibility facade, while the surrounding scoring and market-board flows continue to call the same public functions.

The next Phase 2 extraction moved the backtest run store/history boundary into `backend/src/bookmaker_detector_api/services/model_backtest_runs.py`.

This slice now owns:

- backtest run persistence
- backtest run listing and detail lookup
- backtest history rollups
- backtest run serialization

`backend/src/bookmaker_detector_api/services/models.py` still owns the walk-forward training/orchestration helpers, but now delegates the backtest run storage/history layer to the extracted module.

The next Phase 2 extraction moved the walk-forward backtest orchestration and evaluation helpers into `backend/src/bookmaker_detector_api/services/model_backtest_workflows.py`.

This slice now owns:

- walk-forward backtest orchestration
- ordered game-window construction
- per-fold snapshot training and selection
- fold summary generation
- backtest prediction metric summaries
- strategy bet construction and ROI/bucket rollups

`backend/src/bookmaker_detector_api/services/models.py` now delegates the backtest workflow layer as well, leaving the legacy module with thinner wrappers around dataset loading and the remaining shared utility helpers.

The next Phase 2 slice collapsed the remaining private scoring/opportunity/future-scenario wrapper layer inside `backend/src/bookmaker_detector_api/services/models.py`.

This slice now:

- rewires in-file callers to use `model_scoring_previews`, `model_future_scenarios`, and `model_opportunities` directly
- removes obsolete private facade helpers that were no longer referenced outside `models.py`
- narrows the legacy module to higher-level orchestration and compatibility entrypoints instead of duplicate helper pass-throughs

This was a cleanup/debt-reduction slice rather than a new module extraction, but it materially reduces the size and indirection of the legacy service hotspot before the market-board orchestration split.

The next Phase 2 extraction moved the market-board workflow/orchestration layer into `backend/src/bookmaker_detector_api/services/model_market_board_orchestration.py`.

This slice now owns:

- market-board materialization orchestration
- source-driven board refresh orchestration
- refresh and scoring queue assembly
- refresh, scoring, and cadence orchestration batches
- market-board scoring orchestration
- market-board operations summary assembly
- market-board cadence dashboard assembly

`backend/src/bookmaker_detector_api/services/models.py` now delegates the market-board workflow layer to the extracted module, while keeping the backend-specific repository wiring and the existing public import surface stable for routes and tests.

The next Phase 2 extraction moved the remaining shared training/model utility logic into `backend/src/bookmaker_detector_api/services/model_training_algorithms.py`.

This slice now owns:

- linear-feature training
- tree-stump training
- numeric feature candidate discovery
- regression training-pair assembly
- simple linear regression fitting
- tree-threshold candidate generation
- constant-mean fallback computation
- target-value summary helpers
- linear and tree-stump prediction helpers
- shared regression metric scoring and candidate ranking helpers

This slice also removed now-dead duplicate training-view helper implementations from `backend/src/bookmaker_detector_api/services/models.py`, so the legacy module is now primarily compatibility glue and repository-specific wrappers rather than dense workflow or utility implementations.

## Why This Slice First

This was the safest Phase 2 starting point because it:

- has direct route and service test coverage
- does not require changing API modules
- does not alter write orchestration, persistence side effects, or feature/materialization flows
- creates a real service boundary that later slices can build on

## Remaining Work In `services/models.py`

Phase 2 removed the major workflow and shared utility hotspots from `backend/src/bookmaker_detector_api/services/models.py`.

The remaining code in the legacy module is now mostly:

- repository-specific dataset loading wrappers
- stable compatibility entrypoints consumed by routes and tests
- thin delegation shims around extracted workflow modules

## Verification

Validated after this extraction:

- `python -m pytest backend/tests/test_models.py -q`
- `python -m pytest backend/tests/test_admin_routes.py -q`
- `cmd /c npm run typecheck`
