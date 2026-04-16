from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname, urlopen


@dataclass(slots=True)
class FetchedPage:
    source_url: str
    content: str
    status: str
    http_status: int | None
    content_type: str | None = None


def fetch_page(source_url: str) -> FetchedPage:
    parsed = urlparse(source_url)

    if parsed.scheme == "file":
        raw_path = parsed.path
        if parsed.netloc:
            raw_path = f"//{parsed.netloc}{parsed.path}"
        file_path = Path(url2pathname(raw_path))
        if not file_path.exists():
            raise FileNotFoundError(f"File source does not exist: {file_path}")
        return FetchedPage(
            source_url=source_url,
            content=file_path.read_text(encoding="utf-8"),
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        )

    with urlopen(source_url) as response:  # noqa: S310
        content_bytes = response.read()
        return FetchedPage(
            source_url=source_url,
            content=content_bytes.decode("utf-8"),
            status="SUCCESS",
            http_status=response.getcode(),
            content_type=response.headers.get_content_type(),
        )
