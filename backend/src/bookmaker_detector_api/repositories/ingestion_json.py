from __future__ import annotations

from typing import Any


def _json_dumps(payload: Any) -> str:
    import json

    return json.dumps(payload, default=str)
