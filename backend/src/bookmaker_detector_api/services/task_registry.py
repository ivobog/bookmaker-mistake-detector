from __future__ import annotations

from typing import Any

from bookmaker_detector_api.services.features import DEFAULT_FEATURE_KEY
from bookmaker_detector_api.services.model_records import (
    ModelFamilyCapabilityRecord,
    TargetTaskDefinitionRecord,
)

DEFAULT_TRAIN_RATIO = 0.7
DEFAULT_VALIDATION_RATIO = 0.15
LEGACY_SELECTION_POLICY_NAME = "validation_mae_candidate_v1"
DEFAULT_REGRESSION_SELECTION_POLICY_NAME = "validation_regression_candidate_v1"

def normalize_selection_policy_name(selection_policy_name: str) -> str:
    if selection_policy_name == LEGACY_SELECTION_POLICY_NAME:
        return DEFAULT_REGRESSION_SELECTION_POLICY_NAME
    if selection_policy_name == DEFAULT_REGRESSION_SELECTION_POLICY_NAME:
        return DEFAULT_REGRESSION_SELECTION_POLICY_NAME
    raise ValueError(f"Unsupported selection policy: {selection_policy_name}")


def list_target_task_definitions_postgres(
    connection: Any,
    *,
    enabled_only: bool = True,
) -> list[TargetTaskDefinitionRecord]:
    query = """
        SELECT
            task_key,
            task_kind,
            label,
            description,
            market_type,
            primary_metric_name,
            metric_direction,
            opportunity_policy_name,
            is_enabled,
            config_json,
            created_at,
            updated_at
        FROM target_task_definition
    """
    params: list[Any] = []
    if enabled_only:
        query += " WHERE is_enabled = TRUE"
    query += " ORDER BY task_key ASC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        TargetTaskDefinitionRecord(
            task_key=row[0],
            task_kind=row[1],
            label=row[2],
            description=row[3] or "",
            market_type=row[4],
            primary_metric_name=row[5],
            metric_direction=row[6],
            opportunity_policy_name=row[7],
            is_enabled=bool(row[8]),
            config=row[9] or {},
            created_at=row[10],
            updated_at=row[11],
        )
        for row in rows
    ]


def list_model_family_capabilities_postgres(
    connection: Any,
    *,
    enabled_only: bool = True,
) -> list[ModelFamilyCapabilityRecord]:
    query = """
        SELECT
            id,
            model_family,
            target_task,
            is_enabled,
            config_json,
            created_at
        FROM model_family_capability
    """
    params: list[Any] = []
    if enabled_only:
        query += " WHERE is_enabled = TRUE"
    query += " ORDER BY target_task ASC, model_family ASC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelFamilyCapabilityRecord(
            id=int(row[0]),
            model_family=row[1],
            target_task=row[2],
            is_enabled=bool(row[3]),
            config=row[4] or {},
            created_at=row[5],
        )
        for row in rows
    ]


def build_model_capabilities_payload(
    task_definitions: list[TargetTaskDefinitionRecord],
    model_family_capabilities: list[ModelFamilyCapabilityRecord],
) -> dict[str, Any]:
    families_by_task: dict[str, list[str]] = {}
    for capability in model_family_capabilities:
        if not capability.is_enabled:
            continue
        families_by_task.setdefault(capability.target_task, []).append(capability.model_family)

    task_payloads: list[dict[str, Any]] = []
    default_target_task: str | None = None
    for definition in task_definitions:
        supported_model_families = sorted(set(families_by_task.get(definition.task_key, [])))
        config = dict(definition.config)
        selection_policy_names = list(
            config.get(
                "selection_policy_names",
                [
                    config.get(
                        "default_selection_policy_name",
                        DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
                    )
                ],
            )
        )
        default_selection_policy_name = str(
            config.get("default_selection_policy_name", DEFAULT_REGRESSION_SELECTION_POLICY_NAME)
        )
        if default_target_task is None and bool(config.get("is_default_ui_task", False)):
            default_target_task = definition.task_key
        task_payloads.append(
            {
                "task_key": definition.task_key,
                "task_kind": definition.task_kind,
                "label": definition.label,
                "description": definition.description,
                "market_type": definition.market_type,
                "primary_metric_name": definition.primary_metric_name,
                "metric_direction": definition.metric_direction,
                "supported_model_families": supported_model_families,
                "default_selection_policy_name": default_selection_policy_name,
                "valid_selection_policy_names": selection_policy_names,
                "default_opportunity_policy_name": definition.opportunity_policy_name,
                "scoring_output_semantics": config.get("scoring_output_semantics"),
                "signal_strength_interpretation": config.get("signal_strength_interpretation"),
                "workflow_support": dict(config.get("workflow_support", {})),
                "is_enabled": definition.is_enabled,
                "config": config,
            }
        )

    if default_target_task is None and task_payloads:
        default_target_task = str(task_payloads[0]["task_key"])

    return {
        "task_count": len(task_payloads),
        "target_tasks": task_payloads,
        "ui_defaults": {
            "default_feature_key": DEFAULT_FEATURE_KEY,
            "default_target_task": default_target_task,
            "default_train_ratio": DEFAULT_TRAIN_RATIO,
            "default_validation_ratio": DEFAULT_VALIDATION_RATIO,
        },
    }


def get_model_capabilities_postgres(connection: Any) -> dict[str, Any]:
    return build_model_capabilities_payload(
        list_target_task_definitions_postgres(connection),
        list_model_family_capabilities_postgres(connection),
    )


def build_task_capability_map(capabilities_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(task["task_key"]): dict(task)
        for task in capabilities_payload.get("target_tasks", [])
    }


def get_task_capability(
    capabilities_payload: dict[str, Any],
    task_key: str,
) -> dict[str, Any] | None:
    return build_task_capability_map(capabilities_payload).get(task_key)


def list_supported_model_families(
    capabilities_payload: dict[str, Any],
    *,
    target_task: str | None = None,
) -> list[str]:
    task_map = build_task_capability_map(capabilities_payload)
    if target_task is not None:
        task = task_map.get(target_task)
        if task is None:
            return []
        return sorted(set(task.get("supported_model_families", [])))

    supported: set[str] = set()
    for task in task_map.values():
        supported.update(
            str(model_family)
            for model_family in task.get("supported_model_families", [])
        )
    return sorted(supported)


def is_selection_policy_supported(
    capabilities_payload: dict[str, Any],
    *,
    target_task: str,
    selection_policy_name: str,
) -> bool:
    task = get_task_capability(capabilities_payload, target_task)
    if task is None:
        return False
    valid_selection_policy_names = {
        str(policy_name) for policy_name in task.get("valid_selection_policy_names", [])
    }
    return selection_policy_name in valid_selection_policy_names


def is_model_family_supported(
    capabilities_payload: dict[str, Any],
    *,
    model_family: str,
    target_task: str | None = None,
) -> bool:
    return model_family in set(
        list_supported_model_families(capabilities_payload, target_task=target_task)
    )


def is_workflow_supported(
    capabilities_payload: dict[str, Any],
    *,
    target_task: str,
    workflow_name: str,
) -> bool:
    task = get_task_capability(capabilities_payload, target_task)
    if task is None:
        return False
    workflow_support = dict(task.get("workflow_support", {}))
    if workflow_name not in workflow_support:
        return True
    return bool(workflow_support[workflow_name])
