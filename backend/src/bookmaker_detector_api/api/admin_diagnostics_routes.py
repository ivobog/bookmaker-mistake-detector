from datetime import date

from fastapi import APIRouter, Query

from bookmaker_detector_api.services.admin_diagnostics import (
    get_admin_diagnostics_postgres,
    resolve_started_window,
)
from bookmaker_detector_api.services.data_quality_maintenance import (
    normalize_data_quality_taxonomy,
)

router = APIRouter(prefix="/admin", tags=["admin"])


def _run_admin_diagnostics(**kwargs) -> dict[str, object]:
    return get_admin_diagnostics_postgres(**kwargs)


@router.get("/jobs/recent")
def recent_job_runs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = _run_admin_diagnostics(
        job_limit=limit,
        job_offset=offset,
        retrieval_limit=20,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "filters": diagnostics["filters"],
        "job_runs": diagnostics["job_runs"],
    }


@router.get("/ingestion/issues")
def recent_ingestion_issues(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str = Query(default="FAILED"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics(
        job_limit=20,
        retrieval_limit=limit,
        retrieval_offset=offset,
        retrieval_status=status,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "filters": diagnostics["filters"],
        "page_retrievals": diagnostics["page_retrievals"],
    }


@router.get("/data-quality/issues")
def recent_data_quality_issues(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    severity: str | None = Query(default=None),
    issue_type: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics(
        quality_issue_limit=limit,
        quality_issue_offset=offset,
        quality_issue_severity=severity,
        quality_issue_type=issue_type,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "filters": diagnostics["filters"],
        "data_quality_issues": diagnostics["data_quality_issues"],
    }


@router.get("/ingestion/stats")
def ingestion_stats(
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "filters": diagnostics["filters"],
        "stats": diagnostics["stats"],
    }


@router.get("/validation-runs/compare")
def compare_validation_runs(
    run_label: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=2, le=50),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = _run_admin_diagnostics(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        validation_compare_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "filters": diagnostics["filters"],
        "validation_run_comparison": diagnostics["validation_run_comparison"],
    }


@router.get("/ingestion/trends")
def ingestion_trends(
    limit: int = Query(default=20, ge=1, le=100),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = _run_admin_diagnostics(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        trend_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "filters": diagnostics["filters"],
        "trends": diagnostics["trends"],
    }


@router.get("/retrieval/trends")
def retrieval_trends(
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = _run_admin_diagnostics(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        retrieval_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "filters": diagnostics["filters"],
        "retrieval_trends": diagnostics["retrieval_trends"],
    }


@router.get("/ingestion/quality-trends")
def ingestion_quality_trends(
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = _run_admin_diagnostics(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "filters": diagnostics["filters"],
        "quality_trends": diagnostics["quality_trends"],
    }


@router.post("/data-quality/normalize-taxonomy")
def normalize_data_quality_issue_taxonomy_endpoint(
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
) -> dict[str, object]:
    return normalize_data_quality_taxonomy(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        dry_run=dry_run,
    )
