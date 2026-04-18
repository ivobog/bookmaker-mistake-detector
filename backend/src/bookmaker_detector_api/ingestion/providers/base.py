from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from bookmaker_detector_api.fetching import FetchedPage
from bookmaker_detector_api.ingestion.models import RawGameRow


@dataclass(slots=True, frozen=True)
class DiscoveredTeamPage:
    team_name: str
    team_main_page_url: str
    team_slug: str | None = None
    source_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class SeasonBlock:
    season_label: str
    team_main_page_url: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class ProviderRow:
    cells: tuple[str, ...]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class TeamPageFetchResult:
    fetched_page: FetchedPage
    diagnostics: tuple[str, ...] = ()
    used_browser_fallback: bool = False
    missing_season_labels: tuple[str, ...] = ()


class HistoricalTeamPageProvider(Protocol):
    provider_name: str

    def discover_team_pages(self, *, index_url: str) -> list[DiscoveredTeamPage]: ...

    def resolve_team_main_page_url(self, *, team_page: DiscoveredTeamPage) -> str: ...

    def fetch_page(self, *, url: str) -> FetchedPage: ...

    def fetch_team_main_page(
        self,
        *,
        url: str,
        requested_season_labels: tuple[str, ...] | list[str] = (),
        browser_fallback: bool = False,
    ) -> TeamPageFetchResult: ...

    def extract_season_block(
        self,
        *,
        page_content: str,
        season_label: str,
        team_main_page_url: str,
    ) -> SeasonBlock | None: ...

    def extract_regular_season_rows(self, *, season_block: SeasonBlock) -> list[ProviderRow]: ...

    def parse_row(
        self,
        *,
        raw_row: ProviderRow,
        row_index: int,
        team_code: str,
        season_label: str,
        source_url: str,
        source_section: str,
    ) -> RawGameRow: ...

    def normalize_row(self, *, raw_row: RawGameRow) -> RawGameRow: ...

    def parse_team_page(
        self,
        *,
        html: str,
        team_code: str,
        season_label: str,
        source_url: str,
    ) -> list[RawGameRow]: ...

    def load_fixture(self, fixture_path: Path) -> str: ...
