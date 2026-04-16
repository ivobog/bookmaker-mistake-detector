from datetime import UTC, datetime
import importlib.util
from pathlib import Path
import sys

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

from bookmaker_detector_api.services.fixture_ingestion_runner import run_fixture_ingestion
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest


class WorkerSettings(BaseSettings):
    worker_env: str = "development"
    worker_poll_interval_seconds: int = 60
    worker_job_mode: str = "fixture_ingestion"
    worker_repository_mode: str = "in_memory"
    worker_team_code: str = "LAL"
    worker_season_label: str = "2024-2025"
    worker_source_url: str = "https://example.com/covers/lal/2024-2025"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )


def main() -> None:
    settings = WorkerSettings()
    started_at = datetime.now(UTC).isoformat()
    print(
        f"[worker] started env={settings.worker_env} "
        f"poll_interval={settings.worker_poll_interval_seconds}s at {started_at}"
    )

    if settings.worker_job_mode == "fixture_ingestion":
        result = run_fixture_ingestion(
            repository_mode=settings.worker_repository_mode,
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
        )
    elif settings.worker_job_mode == "fetch_and_ingest":
        result = run_fetch_and_ingest(
            repository_mode=settings.worker_repository_mode,
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
            persist_payload=True,
        )
    else:
        raise ValueError(f"Unsupported worker job mode: {settings.worker_job_mode}")

    print(f"[worker] ingestion result {result}")


if __name__ == "__main__":
    main()
