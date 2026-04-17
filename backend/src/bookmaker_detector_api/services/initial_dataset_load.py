from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest


@dataclass(frozen=True)
class DatasetLoadTarget:
    team_code: str
    team_name: str
    team_slug: str
    season_label: str
    season_start_date: date
    season_end_date: date
    source_url: str


def parse_csv_values(raw_value: str | None) -> list[str] | None:
    if raw_value is None:
        return None
    values = [entry.strip() for entry in raw_value.split(",") if entry.strip()]
    return values or None


def run_initial_production_dataset_load(
    *,
    source_url_template: str,
    team_codes: list[str] | None = None,
    season_labels: list[str] | None = None,
    requested_by: str = "initial-production-dataset-load",
    run_label: str = "initial-production-dataset-load",
    continue_on_error: bool = True,
    persist_payload: bool = True,
) -> dict[str, object]:
    with postgres_connection() as connection:
        targets = build_initial_dataset_load_targets(
            connection,
            source_url_template=source_url_template,
            team_codes=team_codes,
            season_labels=season_labels,
        )

    results: list[dict[str, object]] = []
    stopped_early = False

    for target in targets:
        response = run_fetch_and_ingest(
            repository_mode="postgres",
            team_code=target.team_code,
            season_label=target.season_label,
            source_url=target.source_url,
            requested_by=requested_by,
            run_label=run_label,
            persist_payload=persist_payload,
        )
        results.append(
            {
                "team_code": target.team_code,
                "season_label": target.season_label,
                "source_url": target.source_url,
                "status": str(response.get("status", "UNKNOWN")),
                "job_id": response.get("result", {}).get("job_id")
                if isinstance(response.get("result"), dict)
                else response.get("job_id"),
                "page_retrieval_id": response.get("result", {}).get("page_retrieval_id")
                if isinstance(response.get("result"), dict)
                else response.get("page_retrieval_id"),
                "raw_rows_saved": response.get("result", {}).get("raw_rows_saved")
                if isinstance(response.get("result"), dict)
                else None,
                "canonical_games_saved": response.get("result", {}).get("canonical_games_saved")
                if isinstance(response.get("result"), dict)
                else None,
                "metrics_saved": response.get("result", {}).get("metrics_saved")
                if isinstance(response.get("result"), dict)
                else None,
                "error_message": response.get("error_message"),
                "payload_storage_path": response.get("payload_storage_path"),
            }
        )

        if response.get("status") == "FAILED" and not continue_on_error:
            stopped_early = True
            break

    failed_count = sum(1 for result in results if result["status"] == "FAILED")
    completed_count = sum(1 for result in results if result["status"] == "COMPLETED")

    return {
        "status": "FAILED" if failed_count else "COMPLETED",
        "requested_by": requested_by,
        "run_label": run_label,
        "continue_on_error": continue_on_error,
        "persist_payload": persist_payload,
        "target_count": len(targets),
        "processed_target_count": len(results),
        "completed_target_count": completed_count,
        "failed_target_count": failed_count,
        "stopped_early": stopped_early,
        "targets": [asdict(target) for target in targets],
        "results": results,
    }


def build_initial_dataset_load_targets(
    connection,
    *,
    source_url_template: str,
    team_codes: list[str] | None = None,
    season_labels: list[str] | None = None,
) -> list[DatasetLoadTarget]:
    selected_teams = _load_team_scope(connection, team_codes=team_codes)
    selected_seasons = _load_season_scope(connection, season_labels=season_labels)

    targets: list[DatasetLoadTarget] = []
    for season in selected_seasons:
        for team in selected_teams:
            render_context = _build_render_context(
                team_code=team["code"],
                team_name=team["name"],
                season_label=season["label"],
                season_start_date=season["start_date"],
                season_end_date=season["end_date"],
            )
            try:
                rendered_source_url = source_url_template.format(**render_context)
            except KeyError as exc:
                missing_key = str(exc).strip("'")
                raise ValueError(
                    f"Unsupported source URL template placeholder: {missing_key}."
                ) from exc
            targets.append(
                DatasetLoadTarget(
                    team_code=team["code"],
                    team_name=team["name"],
                    team_slug=render_context["team_slug"],
                    season_label=season["label"],
                    season_start_date=season["start_date"],
                    season_end_date=season["end_date"],
                    source_url=rendered_source_url,
                )
            )

    return targets


def _load_team_scope(connection, *, team_codes: list[str] | None) -> list[dict[str, object]]:
    query = "SELECT code, name FROM team"
    params: list[object] = []
    if team_codes is not None:
        query += " WHERE code = ANY(%s)"
        params.append(team_codes)
    query += " ORDER BY code"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        raise ValueError("No teams matched the requested initial dataset scope.")

    return [{"code": row[0], "name": row[1]} for row in rows]


def _load_season_scope(
    connection,
    *,
    season_labels: list[str] | None,
) -> list[dict[str, object]]:
    query = """
        SELECT label, start_date, end_date
        FROM season
        WHERE is_completed = TRUE
    """
    params: list[object] = []
    if season_labels is not None:
        query += " AND label = ANY(%s)"
        params.append(season_labels)
        query += " ORDER BY start_date"
    else:
        query += " ORDER BY start_date DESC LIMIT 4"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        raise ValueError("No completed seasons matched the requested initial dataset scope.")

    ordered_rows = rows if season_labels is not None else list(reversed(rows))
    return [
        {"label": row[0], "start_date": row[1], "end_date": row[2]}
        for row in ordered_rows
    ]


def _build_render_context(
    *,
    team_code: str,
    team_name: str,
    season_label: str,
    season_start_date: date,
    season_end_date: date,
) -> dict[str, str]:
    season_start_year, season_end_year = season_label.split("-", maxsplit=1)
    return {
        "team_code": team_code,
        "team_code_lower": team_code.lower(),
        "team_name": team_name,
        "team_slug": _slugify(team_name),
        "season_label": season_label,
        "season_start_year": season_start_year,
        "season_end_year": season_end_year,
        "season_start_date": season_start_date.isoformat(),
        "season_end_date": season_end_date.isoformat(),
    }


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return re.sub(r"-{2,}", "-", slug)
