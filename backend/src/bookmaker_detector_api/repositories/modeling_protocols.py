from __future__ import annotations

from typing import Any, Protocol

RecordList = list[dict[str, Any]]


class FeatureDatasetStore(Protocol):
    feature_versions: RecordList
    feature_snapshots: RecordList
    feature_analysis_artifacts: RecordList
    canonical_games: RecordList
    metrics: RecordList


class ModelTrainingArtifactStore(Protocol):
    model_registries: RecordList
    model_training_runs: RecordList
    model_evaluation_snapshots: RecordList
    model_selection_snapshots: RecordList


class ModelScoringArtifactStore(Protocol):
    model_scoring_runs: RecordList


class ModelOpportunityStore(Protocol):
    model_opportunities: RecordList


class ModelBacktestArtifactStore(Protocol):
    model_backtest_runs: RecordList


class MarketBoardOperationStore(Protocol):
    model_market_boards: RecordList
    model_market_board_source_runs: RecordList
    model_market_board_refresh_events: RecordList
    model_market_board_refresh_batches: RecordList
    model_market_board_scoring_batches: RecordList
    model_market_board_cadence_batches: RecordList


class ModelingRepositoryStore(
    ModelTrainingArtifactStore,
    ModelScoringArtifactStore,
    ModelOpportunityStore,
    ModelBacktestArtifactStore,
    MarketBoardOperationStore,
    Protocol,
): ...


class PhaseThreeModelingStore(
    FeatureDatasetStore,
    ModelingRepositoryStore,
    Protocol,
): ...
