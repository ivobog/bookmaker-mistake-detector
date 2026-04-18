from fastapi import APIRouter

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.demo import (
    run_phase_one_demo,
    run_phase_one_fetch_demo,
    run_phase_one_fetch_failure_demo,
    run_phase_one_persistence_demo,
    run_phase_one_worker_demo,
)
from bookmaker_detector_api.demo import (
    run_phase_one_fetch_reporting_demo as run_phase_one_fetch_reporting_demo_job,
)

router = APIRouter(prefix="/admin", tags=["admin"])


def _use_postgres_stable_read_mode() -> bool:
    return settings.use_postgres_stable_read_mode


@router.get("/providers")
def list_supported_providers() -> dict[str, list[dict[str, str]]]:
    return {
        "providers": [
            {
                "name": "covers",
                "type": "historical_team_page",
                "status": "fixture_backed",
            }
        ]
    }


@router.get("/phase-1-demo")
def phase_one_demo() -> dict[str, object]:
    return run_phase_one_demo()


@router.get("/phase-1-persistence-demo")
def phase_one_persistence_demo() -> dict[str, object]:
    return run_phase_one_persistence_demo()


@router.get("/phase-1-worker-demo")
def phase_one_worker_demo() -> dict[str, object]:
    return run_phase_one_worker_demo()


@router.get("/phase-1-fetch-demo")
def phase_one_fetch_demo() -> dict[str, object]:
    return run_phase_one_fetch_demo()


@router.get("/phase-1-fetch-failure-demo")
def phase_one_fetch_failure_demo() -> dict[str, object]:
    return run_phase_one_fetch_failure_demo()


@router.get("/phase-1-fetch-reporting-demo")
def phase_one_fetch_reporting_demo() -> dict[str, object]:
    repository_mode = "postgres" if _use_postgres_stable_read_mode() else "in_memory"
    return run_phase_one_fetch_reporting_demo_job(repository_mode=repository_mode)
