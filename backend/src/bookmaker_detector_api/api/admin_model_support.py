from datetime import date

from fastapi import HTTPException
from pydantic import BaseModel, Field

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.task_registry import (
    get_model_capabilities_postgres,
    get_task_capability,
    is_model_family_supported,
    is_selection_policy_supported,
    is_workflow_supported,
    list_supported_model_families,
)


class FutureSlateGameRequest(BaseModel):
    season_label: str = Field(default="2025-2026")
    game_date: date
    home_team_code: str
    away_team_code: str
    home_spread_line: float | None = None
    total_line: float | None = None


class FutureSlateRequest(BaseModel):
    slate_label: str | None = None
    games: list[FutureSlateGameRequest] = Field(min_length=1, max_length=20)


def _load_model_capabilities_payload() -> dict[str, object]:
    with postgres_connection() as connection:
        return get_model_capabilities_postgres(connection)


def _resolve_target_task(
    target_task: str | None = None,
    *,
    capabilities_payload: dict[str, object] | None = None,
) -> tuple[str, dict[str, object]]:
    resolved_payload = capabilities_payload or _load_model_capabilities_payload()
    resolved_target_task = target_task or (
        resolved_payload.get("ui_defaults", {}) or {}
    ).get("default_target_task")
    if not resolved_target_task:
        raise HTTPException(
            status_code=500,
            detail="No default target_task is configured in model capabilities.",
        )
    return str(resolved_target_task), resolved_payload


def _validate_model_admin_inputs(
    *,
    capabilities_payload: dict[str, object] | None = None,
    target_task: str | None = None,
    selection_policy_name: str | None = None,
    model_family: str | None = None,
    workflow_name: str | None = None,
) -> None:
    resolved_payload = capabilities_payload or _load_model_capabilities_payload()

    if target_task is not None:
        task_capability = get_task_capability(resolved_payload, target_task)
        if task_capability is None or not bool(task_capability.get("is_enabled", False)):
            raise HTTPException(status_code=400, detail=f"Unsupported target_task: {target_task}")
        if not list_supported_model_families(resolved_payload, target_task=target_task):
            raise HTTPException(
                status_code=400,
                detail=f"No enabled model families are configured for target_task={target_task}.",
            )

    if selection_policy_name is not None:
        if target_task is None:
            raise HTTPException(
                status_code=400,
                detail="selection_policy_name validation requires a target_task.",
            )
        if not is_selection_policy_supported(
            resolved_payload,
            target_task=target_task,
            selection_policy_name=selection_policy_name,
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unsupported selection_policy_name for target_task "
                    f"{target_task}: {selection_policy_name}"
                ),
            )

    if model_family is not None and not is_model_family_supported(
        resolved_payload,
        model_family=model_family,
        target_task=target_task,
    ):
        if target_task is None:
            detail = f"Unsupported model_family: {model_family}"
        else:
            detail = f"Unsupported model_family for target_task {target_task}: {model_family}"
        raise HTTPException(status_code=400, detail=detail)

    if workflow_name is not None:
        if target_task is None:
            raise HTTPException(
                status_code=400,
                detail="workflow validation requires a target_task.",
            )
        if not is_workflow_supported(
            resolved_payload,
            target_task=target_task,
            workflow_name=workflow_name,
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Workflow {workflow_name} is not enabled for target_task {target_task}.",
            )
