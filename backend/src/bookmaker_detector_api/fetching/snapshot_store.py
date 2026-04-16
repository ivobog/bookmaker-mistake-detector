from __future__ import annotations

import re
from pathlib import Path


def store_raw_payload(
    *,
    root_dir: Path,
    provider_name: str,
    team_code: str,
    season_label: str,
    source_url: str,
    content: str,
) -> Path:
    safe_source = _sanitize_filename(source_url)
    target_dir = root_dir / season_label / team_code
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{provider_name}_{safe_source}.html"
    target_path.write_text(content, encoding="utf-8")
    return target_path


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")[:120] or "payload"
