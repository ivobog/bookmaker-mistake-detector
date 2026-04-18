from __future__ import annotations

from pathlib import Path

from bookmaker_detector_api.fetching import FetchedPage
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider

DEFAULT_INDEX_URL = "https://www.covers.com/sport/basketball/nba/teams"
DEFAULT_TEAM_PAGE_URL = "https://www.covers.com/sport/basketball/nba/teams/main/los-angeles-lakers"
DEFAULT_SCHEDULE_FRAGMENT_TOKEN = "getschedule/schedule/175/47854"


def load_covers_fixture(fixture_name: str) -> str:
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / fixture_name
    )
    return fixture_path.read_text(encoding="utf-8")


def build_covers_index_html(
    *,
    team_name: str = "Los Angeles Lakers",
    team_slug: str = "los-angeles-lakers",
) -> str:
    return f"""
<html>
  <body>
    <a href="/sport/basketball/nba/teams/main/{team_slug}">{team_name}</a>
  </body>
</html>
"""


def build_fixture_backed_covers_provider(
    *,
    index_html: str | None = None,
    team_page_html: str | None = None,
    schedule_fragment_html: str | None = None,
    browser_page_html: str | None = None,
):
    resolved_index_html = index_html or build_covers_index_html()
    resolved_team_page_html = team_page_html or load_covers_fixture("covers_live_team_page.html")

    class FixtureBackedCoversProvider(CoversHistoricalTeamPageProvider):
        def fetch_page(self, *, url: str) -> FetchedPage:
            if url == DEFAULT_INDEX_URL:
                return FetchedPage(
                    source_url=url,
                    content=resolved_index_html,
                    status="SUCCESS",
                    http_status=200,
                    content_type="text/html",
                )
            if DEFAULT_SCHEDULE_FRAGMENT_TOKEN in url:
                if schedule_fragment_html is None:
                    raise AssertionError(f"Unexpected fixture URL: {url}")
                return FetchedPage(
                    source_url=url,
                    content=schedule_fragment_html,
                    status="SUCCESS",
                    http_status=200,
                    content_type="text/html",
                )
            if url == DEFAULT_TEAM_PAGE_URL:
                return FetchedPage(
                    source_url=url,
                    content=resolved_team_page_html,
                    status="SUCCESS",
                    http_status=200,
                    content_type="text/html",
                )
            raise AssertionError(f"Unexpected fixture URL: {url}")

        if browser_page_html is not None:

            def _fetch_page_with_browser(
                self,
                *,
                url: str,
                season_labels: tuple[str, ...],
            ) -> FetchedPage:
                return FetchedPage(
                    source_url=url,
                    content=browser_page_html,
                    status="SUCCESS",
                    http_status=200,
                    content_type="text/html",
                )

    return FixtureBackedCoversProvider
