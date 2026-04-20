import importlib.util
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

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


def _fixture_dir() -> Path:
    return _find_backend_src() / "bookmaker_detector_api" / "fixtures"


def _load_worker_jobs():
    from bookmaker_detector_api.config import settings as api_settings
    from bookmaker_detector_api.db.postgres import postgres_connection
    from bookmaker_detector_api.fetching import fetch_page, store_raw_payload
    from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
    from bookmaker_detector_api.services.ingestion_pipeline import (
        HistoricalIngestionRequest,
        ingest_historical_team_page,
    )
    from bookmaker_detector_api.services.initial_dataset_load import (
        parse_csv_values,
        run_initial_production_dataset_load,
    )
    from bookmaker_detector_api.services.repository_factory import (
        build_bootstrap_postgres_ingestion_repository,
    )
    from bookmaker_detector_api.services.workflow_logging import start_workflow_span

    def _is_placeholder_source_url(source_url: str) -> bool:
        parsed = urlparse(source_url)
        return parsed.netloc.strip().lower() == "example.com"

    def _load_source_html(
        *,
        provider: CoversHistoricalTeamPageProvider,
        source_url: str,
    ) -> tuple[str, str, int | None]:
        parsed = urlparse(source_url)
        if parsed.scheme == "fixture":
            fixture_name = f"{parsed.netloc}{parsed.path}".lstrip("/")
            if not fixture_name:
                raise ValueError("Fixture ingestion requires a fixture filename.")
            html = provider.load_fixture(_fixture_dir() / fixture_name)
            return html, "SUCCESS", 200

        fetched_page = fetch_page(source_url)
        return fetched_page.content, fetched_page.status, fetched_page.http_status

    def _run_postgres_ingestion_job(
        *,
        workflow_name: str,
        team_code: str,
        season_label: str,
        source_url: str,
        requested_by: str,
        persist_payload: bool,
    ) -> dict[str, object]:
        if _is_placeholder_source_url(source_url):
            return {
                "status": "SKIPPED",
                "reason": "placeholder_source_url",
                "source_url": source_url,
            }

        provider = CoversHistoricalTeamPageProvider()
        span = start_workflow_span(
            workflow_name=workflow_name,
            provider_name=provider.provider_name,
            storage_mode="postgres",
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            requested_by=requested_by,
            persist_payload=persist_payload,
        )

        try:
            html, retrieval_status, retrieval_http_status = _load_source_html(
                provider=provider,
                source_url=source_url,
            )
            payload_storage_path = None
            if persist_payload:
                payload_storage_path = str(
                    store_raw_payload(
                        root_dir=api_settings.raw_payload_path,
                        provider_name=provider.provider_name,
                        team_code=team_code,
                        season_label=season_label,
                        source_url=source_url,
                        content=html,
                    )
                )

            with postgres_connection() as connection:
                repository = build_bootstrap_postgres_ingestion_repository(connection)
                result = ingest_historical_team_page(
                    request=HistoricalIngestionRequest(
                        provider_name=provider.provider_name,
                        team_code=team_code,
                        season_label=season_label,
                        source_url=source_url,
                        requested_by=requested_by,
                        html=html,
                        retrieval_status=retrieval_status,
                        retrieval_http_status=retrieval_http_status,
                        payload_storage_path=payload_storage_path,
                        source_page_url=source_url,
                        persist_parser_snapshot=persist_payload,
                        parser_snapshot_root_dir=api_settings.parser_snapshot_path,
                    ),
                    provider=provider,
                    repository=repository,
                )
        except Exception as exc:
            span.failure(exc)
            raise

        response = {
            "status": "COMPLETED",
            "job_id": result.job_id,
            "page_retrieval_id": result.page_retrieval_id,
            "raw_rows_saved": result.raw_rows_saved,
            "canonical_games_saved": result.canonical_games_saved,
            "metrics_saved": result.metrics_saved,
            "parser_snapshot_path": result.parser_snapshot_path,
        }
        span.success(
            status=response["status"],
            job_id=result.job_id,
            page_retrieval_id=result.page_retrieval_id,
            raw_rows_saved=result.raw_rows_saved,
            canonical_games_saved=result.canonical_games_saved,
            metrics_saved=result.metrics_saved,
        )
        return response

    def run_fetch_and_ingest(
        *,
        team_code: str,
        season_label: str,
        source_url: str,
        requested_by: str,
        persist_payload: bool,
    ) -> dict[str, object]:
        return _run_postgres_ingestion_job(
            workflow_name="ingestion.fetch_and_ingest",
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            requested_by=requested_by,
            persist_payload=persist_payload,
        )

    def run_fixture_ingestion(
        *,
        team_code: str,
        season_label: str,
        source_url: str,
        requested_by: str,
    ) -> dict[str, object]:
        return _run_postgres_ingestion_job(
            workflow_name="ingestion.fixture_ingestion",
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            requested_by=requested_by,
            persist_payload=True,
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
    worker_job_mode: str = "idle"
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


def _run_worker_job(settings: WorkerSettings, jobs: dict[str, object]) -> dict[str, object]:
    if settings.worker_job_mode == "idle":
        return {"status": "IDLE", "job_mode": settings.worker_job_mode}

    if settings.worker_job_mode == "fixture_ingestion":
        return jobs["fixture_ingestion"](
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
        )
    if settings.worker_job_mode == "fetch_and_ingest":
        return jobs["fetch_and_ingest"](
            team_code=settings.worker_team_code,
            season_label=settings.worker_season_label,
            source_url=settings.worker_source_url,
            requested_by="worker",
            persist_payload=True,
        )
    if settings.worker_job_mode == "production_dataset_load":
        if not settings.worker_dataset_source_url_template:
            raise ValueError(
                "WORKER_DATASET_SOURCE_URL_TEMPLATE is required for production_dataset_load."
            )
        return jobs["production_dataset_load"](
            source_url_template=settings.worker_dataset_source_url_template,
            team_codes=jobs["parse_csv_values"](settings.worker_dataset_team_codes),
            season_labels=jobs["parse_csv_values"](settings.worker_dataset_season_labels),
            requested_by=settings.worker_dataset_requested_by,
            run_label=settings.worker_dataset_run_label,
            continue_on_error=settings.worker_dataset_continue_on_error,
            persist_payload=settings.worker_dataset_persist_payload,
        )
    raise ValueError(f"Unsupported worker job mode: {settings.worker_job_mode}")


def main() -> None:
    settings = WorkerSettings()
    jobs = _load_worker_jobs()
    started_at = datetime.now(UTC).isoformat()
    print(
        f"[worker] started env={settings.worker_env} "
        f"poll_interval={settings.worker_poll_interval_seconds}s at {started_at}"
    )

    while True:
        cycle_started_at = datetime.now(UTC).isoformat()
        try:
            result = _run_worker_job(settings, jobs)
            print(f"[worker] cycle result {result}")
        except Exception as exc:
            print(
                f"[worker] cycle failed mode={settings.worker_job_mode} "
                f"at {cycle_started_at}: {type(exc).__name__}: {exc}"
            )

        if settings.worker_poll_interval_seconds <= 0:
            break
        time.sleep(settings.worker_poll_interval_seconds)


if __name__ == "__main__":
    main()
