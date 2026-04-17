import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_backend_src() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "backend" / "src"
        if candidate.exists():
            return candidate
    raise RuntimeError("Could not locate backend/src for worker imports.")


if importlib.util.find_spec("bookmaker_detector_api") is None:
    BACKEND_SRC = _find_backend_src()
    if str(BACKEND_SRC) not in sys.path:
        sys.path.insert(0, str(BACKEND_SRC))


def _load_worker_jobs():
    from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest
    from bookmaker_detector_api.services.fixture_ingestion_runner import run_fixture_ingestion
    from bookmaker_detector_api.services.initial_dataset_load import (
        parse_csv_values,
        run_initial_production_dataset_load,
    )

    return {
        "fetch_and_ingest": run_fetch_and_ingest,
        "fixture_ingestion": run_fixture_ingestion,
        "parse_csv_values": parse_csv_values,
        "production_dataset_load": run_initial_production_dataset_load,
    }


class WorkerSettings(BaseSettings):
    worker_env: str = "development"
    worker_poll_interval_seconds: int = 60
    worker_job_mode: str = "fixture_ingestion"
    worker_repository_mode: str = "in_memory"
    worker_team_code: str = "LAL"
    worker_season_label: str = "2024-2025"
    worker_source_url: str = "https://example.com/covers/lal/2024-2025"
    worker_dataset_source_url_template: str | None = None
    worker_dataset_team_codes: str | None = None
    worker_dataset_season_labels: str | None = None
    worker_dataset_requested_by: str = "worker-initial-production-dataset-load"
    worker_dataset_run_label: str = "initial-production-dataset-load"
    worker_dataset_continue_on_error: bool = True
    worker_dataset_persist_payload: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )


def main() -> None:
    settings = WorkerSettings()
    jobs = _load_worker_jobs()
    started_at = datetime.now(UTC).isoformat()
    print(
        f"[worker] started env={settings.worker_env} "
        f"poll_interval={settings.worker_poll_interval_seconds}s at {started_at}"
    )

    if settings.worker_job_mode == "fixture_ingestion":
        result = jobs["fixture_ingestion"](
            repository_mode=settings.worker_repository_mode,
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
        )
    elif settings.worker_job_mode == "fetch_and_ingest":
        result = jobs["fetch_and_ingest"](
            repository_mode=settings.worker_repository_mode,
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
            persist_payload=True,
        )
    elif settings.worker_job_mode == "production_dataset_load":
        if not settings.worker_dataset_source_url_template:
            raise ValueError(
                "WORKER_DATASET_SOURCE_URL_TEMPLATE is required for production_dataset_load."
            )
        result = jobs["production_dataset_load"](
            source_url_template=settings.worker_dataset_source_url_template,
            team_codes=jobs["parse_csv_values"](settings.worker_dataset_team_codes),
            season_labels=jobs["parse_csv_values"](settings.worker_dataset_season_labels),
            requested_by=settings.worker_dataset_requested_by,
            run_label=settings.worker_dataset_run_label,
            continue_on_error=settings.worker_dataset_continue_on_error,
            persist_payload=settings.worker_dataset_persist_payload,
        )
    else:
        raise ValueError(f"Unsupported worker job mode: {settings.worker_job_mode}")

    print(f"[worker] ingestion result {result}")


if __name__ == "__main__":
    main()
