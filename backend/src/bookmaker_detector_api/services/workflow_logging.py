from __future__ import annotations

import json
import logging
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from time import perf_counter
from typing import Any
from uuid import uuid4

WORKFLOW_LOGGER_NAME = "bookmaker_detector_api.workflow"


def start_workflow_span(*, workflow_name: str, **context: Any) -> "WorkflowSpan":
    return WorkflowSpan(workflow_name=workflow_name, context=context)


class WorkflowSpan:
    def __init__(self, *, workflow_name: str, context: dict[str, Any]) -> None:
        self.workflow_name = workflow_name
        self.context = context
        self.workflow_run_id = uuid4().hex
        self._started_at = perf_counter()
        self._emit("workflow_started")

    def success(self, **fields: Any) -> None:
        self._emit(
            "workflow_succeeded",
            duration_ms=round((perf_counter() - self._started_at) * 1000, 2),
            **fields,
        )

    def failure(self, exc: BaseException, **fields: Any) -> None:
        self._emit(
            "workflow_failed",
            duration_ms=round((perf_counter() - self._started_at) * 1000, 2),
            error_type=type(exc).__name__,
            error_message=str(exc),
            **fields,
        )

    def _emit(self, event: str, **fields: Any) -> None:
        payload = {
            "event": event,
            "workflow_name": self.workflow_name,
            "workflow_run_id": self.workflow_run_id,
            **self.context,
            **fields,
        }
        logging.getLogger(WORKFLOW_LOGGER_NAME).info(
            json.dumps(_normalize_value(payload), sort_keys=True)
        )


def _normalize_value(value: Any) -> Any:
    if is_dataclass(value):
        return _normalize_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): _normalize_value(entry) for key, entry in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(entry) for entry in value]
    if isinstance(value, set):
        return sorted(_normalize_value(entry) for entry in value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
