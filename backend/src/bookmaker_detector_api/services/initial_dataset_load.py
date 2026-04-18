from __future__ import annotations

from dataclasses import asdict, dataclass

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.fetching import store_raw_payload
from bookmaker_detector_api.ingestion.providers import (
    CoversHistoricalTeamPageProvider,
    DiscoveredTeamPage,
)
from bookmaker_detector_api.repositories import PostgresIngestionRepository
from bookmaker_detector_api.repositories.ingestion_types import PageRetrievalRecord
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.workflow_logging import start_workflow_span
from bookmaker_detector_api.team_normalization import (
    slugify_team_name,
    team_identity_keys,
)

DEFAULT_COVERS_NBA_TEAMS_INDEX_URL = "https://www.covers.com/sport/basketball/nba/teams"


def _build_bootstrap_repository(connection) -> PostgresIngestionRepository:
    try:
        return PostgresIngestionRepository(
            connection,
            allow_runtime_schema_mutation=True,
        )
    except TypeError:
        return PostgresIngestionRepository(connection)


@dataclass(frozen=True)
class DatasetLoadTarget:
    team_code: str
    team_name: str
    team_slug: str
    team_main_page_url: str
    season_labels: tuple[str, ...]
    provider_team_slug: str | None = None
    provider_team_key: str | None = None


def parse_csv_values(raw_value: str | None) -> list[str] | None:
    if raw_value is None:
        return None
    values = [entry.strip() for entry in raw_value.split(",") if entry.strip()]
    return values or None


def run_initial_production_dataset_load(
    *,
    base_url: str = DEFAULT_COVERS_NBA_TEAMS_INDEX_URL,
    team_codes: list[str] | None = None,
    season_labels: list[str] | None = None,
    requested_by: str = "initial-production-dataset-load",
    run_label: str = "initial-production-dataset-load",
    continue_on_error: bool = True,
    persist_payload: bool = True,
    browser_fallback: bool = False,
) -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    span = start_workflow_span(
        workflow_name="ingestion.initial_dataset_load",
        provider_name=provider.provider_name,
        base_url=base_url,
        requested_by=requested_by,
        run_label=run_label,
        continue_on_error=continue_on_error,
        persist_payload=persist_payload,
        browser_fallback=browser_fallback,
        team_codes=team_codes,
        season_labels=season_labels,
    )

    try:
        with postgres_connection() as connection:
            targets = build_initial_dataset_load_targets(
                connection,
                provider=provider,
                base_url=base_url,
                team_codes=team_codes,
                season_labels=season_labels,
            )
            repository = _build_bootstrap_repository(connection)

            results: list[dict[str, object]] = []
            stopped_early = False

            for target in targets:
                target_results, target_stopped_early = _run_target_load(
                    repository=repository,
                    provider=provider,
                    target=target,
                    requested_by=requested_by,
                    run_label=run_label,
                    continue_on_error=continue_on_error,
                    persist_payload=persist_payload,
                    browser_fallback=browser_fallback,
                )
                results.extend(target_results)
                if target_stopped_early:
                    stopped_early = True
                    break

        failed_count = sum(1 for result in results if result["status"] == "FAILED")
        completed_count = sum(1 for result in results if result["status"] == "COMPLETED")

        response = {
            "status": "FAILED" if failed_count else "COMPLETED",
            "provider_name": provider.provider_name,
            "base_url": base_url,
            "requested_by": requested_by,
            "run_label": run_label,
            "continue_on_error": continue_on_error,
            "persist_payload": persist_payload,
            "browser_fallback": browser_fallback,
            "team_page_target_count": len(targets),
            "target_count": sum(len(target.season_labels) for target in targets),
            "processed_target_count": len(results),
            "completed_target_count": completed_count,
            "failed_target_count": failed_count,
            "stopped_early": stopped_early,
            "targets": [asdict(target) for target in targets],
            "results": results,
        }
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        status=response["status"],
        team_page_target_count=response["team_page_target_count"],
        target_count=response["target_count"],
        processed_target_count=response["processed_target_count"],
        completed_target_count=response["completed_target_count"],
        failed_target_count=response["failed_target_count"],
        stopped_early=response["stopped_early"],
    )
    return response


def build_initial_dataset_load_targets(
    connection,
    *,
    provider: CoversHistoricalTeamPageProvider,
    base_url: str,
    team_codes: list[str] | None = None,
    season_labels: list[str] | None = None,
) -> list[DatasetLoadTarget]:
    selected_teams = _load_team_scope(
        connection,
        provider_name=provider.provider_name,
        team_codes=team_codes,
    )
    selected_seasons = _load_season_scope(connection, season_labels=season_labels)
    discovered_team_pages = provider.discover_team_pages(index_url=base_url)
    targets = _match_discovered_team_pages(
        selected_teams=selected_teams,
        discovered_team_pages=discovered_team_pages,
        season_labels=[season["label"] for season in selected_seasons],
        provider=provider,
    )

    selected_team_codes = {team["code"] for team in selected_teams}
    discovered_team_codes = {target.team_code for target in targets}
    missing_team_codes = sorted(selected_team_codes - discovered_team_codes)
    if missing_team_codes:
        raise ValueError(
            f"Unable to resolve Covers team pages for team codes: {', '.join(missing_team_codes)}."
        )

    return targets


def _run_target_load(
    *,
    repository: PostgresIngestionRepository,
    provider: CoversHistoricalTeamPageProvider,
    target: DatasetLoadTarget,
    requested_by: str,
    run_label: str,
    continue_on_error: bool,
    persist_payload: bool,
    browser_fallback: bool,
) -> tuple[list[dict[str, object]], bool]:
    try:
        fetch_result = provider.fetch_team_main_page(
            url=target.team_main_page_url,
            requested_season_labels=target.season_labels,
            browser_fallback=browser_fallback,
        )
        fetched_page = fetch_result.fetched_page
    except Exception as exc:
        failure_results = [
            _record_initial_load_failure(
                repository=repository,
                provider_name=provider.provider_name,
                team_code=target.team_code,
                season_label=season_label,
                source_url=target.team_main_page_url,
                requested_by=requested_by,
                run_label=run_label,
                error_message=str(exc),
                diagnostics=["team_page_fetch_failed"],
            )
            for season_label in target.season_labels
        ]
        return failure_results, not continue_on_error

    results: list[dict[str, object]] = []
    stopped_early = False

    for season_label in target.season_labels:
        payload_storage_path = None
        if persist_payload:
            payload_storage_path = str(
                store_raw_payload(
                    root_dir=settings.raw_payload_path,
                    provider_name=provider.provider_name,
                    team_code=target.team_code,
                    season_label=season_label,
                    source_url=target.team_main_page_url,
                    content=fetched_page.content,
                )
            )

        diagnostics = list(fetch_result.diagnostics)
        if season_label in fetch_result.missing_season_labels:
            diagnostics.append("browser_fallback_season_still_missing")

        try:
            result = ingest_historical_team_page(
                request=HistoricalIngestionRequest(
                    provider_name=provider.provider_name,
                    team_code=target.team_code,
                    season_label=season_label,
                    source_url=target.team_main_page_url,
                    source_page_url=target.team_main_page_url,
                    requested_by=requested_by,
                    run_label=run_label,
                    html=fetched_page.content,
                    retrieval_status=fetched_page.status,
                    retrieval_http_status=fetched_page.http_status,
                    payload_storage_path=payload_storage_path,
                    diagnostics=diagnostics,
                    persist_parser_snapshot=persist_payload,
                    parser_snapshot_root_dir=settings.parser_snapshot_path,
                ),
                provider=provider,
                repository=repository,
            )
            results.append(
                {
                    "team_code": target.team_code,
                    "team_name": target.team_name,
                    "season_label": season_label,
                    "source_url": target.team_main_page_url,
                    "status": "COMPLETED",
                    "job_id": result.job_id,
                    "page_retrieval_id": result.page_retrieval_id,
                    "raw_rows_saved": result.raw_rows_saved,
                    "canonical_games_saved": result.canonical_games_saved,
                    "metrics_saved": result.metrics_saved,
                    "error_message": None,
                    "payload_storage_path": payload_storage_path,
                    "parser_snapshot_path": result.parser_snapshot_path,
                    "diagnostics": result.diagnostics,
                }
            )
        except Exception as exc:
            _rollback_repository_transaction(repository)
            results.append(
                _record_initial_load_failure(
                    repository=repository,
                    provider_name=provider.provider_name,
                    team_code=target.team_code,
                    season_label=season_label,
                    source_url=target.team_main_page_url,
                    requested_by=requested_by,
                    run_label=run_label,
                    error_message=str(exc),
                    payload_storage_path=payload_storage_path,
                    diagnostics=diagnostics,
                )
            )
            if not continue_on_error:
                stopped_early = True
                break

    return results, stopped_early


def _rollback_repository_transaction(repository: PostgresIngestionRepository) -> None:
    connection = getattr(repository, "connection", None)
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def _record_initial_load_failure(
    *,
    repository: PostgresIngestionRepository,
    provider_name: str,
    team_code: str,
    season_label: str,
    source_url: str,
    requested_by: str,
    run_label: str | None,
    error_message: str,
    payload_storage_path: str | None = None,
    diagnostics: list[str] | None = None,
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
            payload_storage_path=payload_storage_path,
        ),
    )
    repository.complete_job_run(
        job_id=job_id,
        summary={
            "raw_rows_saved": 0,
            "canonical_games_saved": 0,
            "metrics_saved": 0,
            "error_message": error_message,
            "diagnostic_count": len(diagnostics or []),
            "diagnostics": diagnostics or [],
        },
        status="FAILED",
    )
    return {
        "team_code": team_code,
        "season_label": season_label,
        "source_url": source_url,
        "status": "FAILED",
        "job_id": job_id,
        "page_retrieval_id": page_retrieval_id,
        "raw_rows_saved": None,
        "canonical_games_saved": None,
        "metrics_saved": None,
        "error_message": error_message,
        "payload_storage_path": payload_storage_path,
        "parser_snapshot_path": None,
        "diagnostics": diagnostics or [],
    }


def _load_team_scope(
    connection,
    *,
    provider_name: str,
    team_codes: list[str] | None,
) -> list[dict[str, object]]:
    query = """
        SELECT
            t.code,
            t.name,
            tpm.provider_team_key,
            tpm.provider_team_slug
        FROM team t
        LEFT JOIN team_provider_mapping tpm
            ON tpm.team_id = t.id
           AND tpm.active = TRUE
           AND tpm.provider_id = (
                SELECT id
                FROM provider
                WHERE name = %s
            )
    """
    params: list[object] = [provider_name]
    if team_codes is not None:
        query += " WHERE t.code = ANY(%s)"
        params.append(team_codes)
    query += " ORDER BY t.code"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        raise ValueError("No teams matched the requested initial dataset scope.")

    return [
        {
            "code": row[0],
            "name": row[1],
            "team_slug": slugify_team_name(row[1]),
            "provider_team_key": row[2],
            "provider_team_slug": row[3],
        }
        for row in rows
    ]


def _load_season_scope(
    connection,
    *,
    season_labels: list[str] | None,
) -> list[dict[str, object]]:
    query = """
        SELECT label, start_date, end_date
        FROM season
    """
    params: list[object] = []
    if season_labels is not None:
        query += " WHERE label = ANY(%s)"
        params.append(season_labels)
        query += " ORDER BY start_date"
    else:
        query += " WHERE is_completed = TRUE ORDER BY start_date DESC LIMIT 4"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        if season_labels is not None:
            raise ValueError("No seasons matched the requested initial dataset scope.")
        raise ValueError("No completed seasons matched the requested initial dataset scope.")

    ordered_rows = rows if season_labels is not None else list(reversed(rows))
    return [{"label": row[0], "start_date": row[1], "end_date": row[2]} for row in ordered_rows]


def _match_discovered_team_pages(
    *,
    selected_teams: list[dict[str, object]],
    discovered_team_pages: list[DiscoveredTeamPage],
    season_labels: list[str],
    provider: CoversHistoricalTeamPageProvider,
) -> list[DatasetLoadTarget]:
    selected_team_lookup = {team["code"]: team for team in selected_teams}
    matched_team_codes: set[str] = set()
    targets: list[DatasetLoadTarget] = []

    for discovered_team_page in discovered_team_pages:
        matched_team = _match_team(discovered_team_page=discovered_team_page, teams=selected_teams)
        if matched_team is None:
            continue
        team_code = str(matched_team["code"])
        if team_code in matched_team_codes:
            continue
        matched_team_codes.add(team_code)
        canonical_team = selected_team_lookup[team_code]
        targets.append(
            DatasetLoadTarget(
                team_code=team_code,
                team_name=str(canonical_team["name"]),
                team_slug=str(canonical_team["team_slug"]),
                team_main_page_url=provider.resolve_team_main_page_url(
                    team_page=discovered_team_page
                ),
                season_labels=tuple(season_labels),
                provider_team_slug=_string_or_none(canonical_team.get("provider_team_slug")),
                provider_team_key=_string_or_none(canonical_team.get("provider_team_key")),
            )
        )

    return targets


def _match_team(
    *,
    discovered_team_page: DiscoveredTeamPage,
    teams: list[dict[str, object]],
) -> dict[str, object] | None:
    discovered_keys = team_identity_keys(
        discovered_team_page.team_name,
        discovered_team_page.team_slug,
    )
    matches = [team for team in teams if discovered_keys & _team_record_identity_keys(team)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(
            "Ambiguous team identity match for discovered Covers page "
            f"{discovered_team_page.team_main_page_url}."
        )

    return None


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    string_value = str(value).strip()
    return string_value or None


def _team_record_identity_keys(team: dict[str, object]) -> set[str]:
    return team_identity_keys(
        str(team["name"]),
        _string_or_none(team.get("team_slug")),
        _string_or_none(team.get("provider_team_key")),
        _string_or_none(team.get("provider_team_slug")),
        team_code=str(team["code"]),
    )
