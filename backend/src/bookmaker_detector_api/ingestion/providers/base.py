from __future__ import annotations

from pathlib import Path
from typing import Protocol

from bookmaker_detector_api.ingestion.models import RawGameRow


class HistoricalTeamPageProvider(Protocol):
    provider_name: str

    def parse_team_page(
        self,
        *,
        html: str,
        team_code: str,
        season_label: str,
        source_url: str,
    ) -> list[RawGameRow]:
        ...

    def load_fixture(self, fixture_path: Path) -> str:
        ...

