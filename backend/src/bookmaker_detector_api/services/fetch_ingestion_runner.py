from __future__ import annotations

from dataclasses import asdict

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.fetching import store_raw_payload
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import IngestionRepository, InMemoryIngestionRepository
from bookmaker_detector_api.repositories.ingestion_types import PageRetrievalRecord
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.repository_factory import build_ingestion_repository


def run_fetch_and_ingest(
    *,
    repository_mode: str,
    team_code: str,
    season_label: str,
    source_url: str,
    ingestion_source_url: str | None = None,
    requested_by: str,
    run_label: str | None = None,
    persist_payload: bool = True,
    repository_override: IngestionRepository | None = None,
) -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    effective_source_url = ingestion_source_url or source_url
    repository_context = None
    if repository_override is not None:
        repository = repository_override
    else:
        repository, repository_context = build_ingestion_repository(repository_mode)
    try:
        fetched_page = provider.fetch_page(url=source_url)
        payload_storage_path = None
        if persist_payload:
            payload_storage_path = str(
                store_raw_payload(
                    root_dir=settings.raw_payload_path,
                    provider_name=provider.provider_name,
                    team_code=team_code,
                    season_label=season_label,
                    source_url=effective_source_url,
                    content=fetched_page.content,
                )
            )

        request = HistoricalIngestionRequest(
            provider_name=provider.provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=effective_source_url,
            source_page_url=source_url,
            requested_by=requested_by,
            run_label=run_label,
            html=fetched_page.content,
            retrieval_status=fetched_page.status,
            retrieval_http_status=fetched_page.http_status,
            payload_storage_path=payload_storage_path,
            persist_parser_snapshot=persist_payload,
            parser_snapshot_root_dir=settings.parser_snapshot_path,
        )

        result = ingest_historical_team_page(
            request=request,
            provider=provider,
            repository=repository,
        )

        response: dict[str, object] = {
            "repository_mode": repository_mode,
            "payload_storage_path": payload_storage_path,
            "parser_snapshot_path": result.parser_snapshot_path,
            "fetch_http_status": fetched_page.http_status,
            "result": asdict(result),
            "status": "COMPLETED",
        }
        if isinstance(repository, InMemoryIngestionRepository):
            response["job_runs"] = repository.job_runs
            response["page_retrievals"] = _serialize_page_retrievals(repository.page_retrievals)
        return response
    except Exception as exc:
        failure = _record_fetch_failure(
            repository=repository,
            provider_name=provider.provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=effective_source_url,
            requested_by=requested_by,
            run_label=run_label,
            error_message=str(exc),
        )
        if isinstance(repository, InMemoryIngestionRepository):
            failure["job_runs"] = repository.job_runs
            failure["page_retrievals"] = _serialize_page_retrievals(repository.page_retrievals)
        return failure
    finally:
        if repository_context is not None:
            repository_context.__exit__(None, None, None)


def _record_fetch_failure(
    *,
    repository: IngestionRepository,
    provider_name: str,
    team_code: str,
    season_label: str,
    source_url: str,
    requested_by: str,
    run_label: str | None,
    error_message: str,
) -> dict[str, object]:
    job_id = repository.create_job_run(
        job_name="historical_team_page_fetch",
        requested_by=requested_by,
        payload={
            "provider": provider_name,
            "team_code": team_code,
            "season_label": season_label,
            "source_url": source_url,
            "run_label": run_label,
        },
    )
    page_retrieval_id = repository.create_page_retrieval(
        job_id=job_id,
        record=PageRetrievalRecord(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            status="FAILED",
            http_status=None,
            error_message=error_message,
        ),
    )
    repository.complete_job_run(
        job_id=job_id,
        summary={
            "raw_rows_saved": 0,
            "canonical_games_saved": 0,
            "metrics_saved": 0,
            "error_message": error_message,
        },
        status="FAILED",
    )
    return {
        "status": "FAILED",
        "error_message": error_message,
        "job_id": job_id,
        "page_retrieval_id": page_retrieval_id,
        "payload_storage_path": None,
        "parser_snapshot_path": None,
        "fetch_http_status": None,
    }


def _serialize_page_retrievals(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    serialized: list[dict[str, object]] = []
    for entry in entries:
        record = entry["record"]
        serialized.append(
            {
                **entry,
                "record": {
                    "provider_name": record.provider_name,
                    "team_code": record.team_code,
                    "season_label": record.season_label,
                    "source_url": record.source_url,
                    "status": record.status,
                    "http_status": record.http_status,
                    "error_message": record.error_message,
                    "payload_storage_path": record.payload_storage_path,
                },
            }
        )
    return serialized
