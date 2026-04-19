import pytest

from bookmaker_detector_api.fetching import FetchedPage
from bookmaker_detector_api.ingestion.models import ParseStatus
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from tests.support.covers_fixtures import load_covers_fixture


def _load_fixture(provider: CoversHistoricalTeamPageProvider, fixture_name: str) -> str:
    return load_covers_fixture(fixture_name)


def _load_sample_team_page(provider: CoversHistoricalTeamPageProvider) -> str:
    return _load_fixture(provider, "covers_sample_team_page.html")


def _load_live_team_page(provider: CoversHistoricalTeamPageProvider) -> str:
    return _load_fixture(provider, "covers_live_team_page.html")


def _load_live_schedule_fragment(provider: CoversHistoricalTeamPageProvider) -> str:
    return _load_fixture(provider, "covers_live_schedule_fragment_2023_2024.html")


def test_covers_provider_parses_only_regular_season_rows() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_sample_team_page(provider)
    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )

    assert len(rows) == 3
    assert all(row.source_section == "Regular Season" for row in rows)
    assert rows[0].team_code == "LAL"
    assert rows[0].opponent_code == "BOS"
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "direct_code"
    assert rows[1].is_away is True
    assert rows[2].ats_line == -1.5
    assert rows[2].total_line == 221.5


def test_covers_provider_rejects_team_identity_mismatch() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_sample_team_page(provider)

    with pytest.raises(ValueError, match="identity mismatch"):
        provider.parse_team_page(
            html=html,
            team_code="PHX",
            season_label="2024-2025",
            source_url="https://example.com/covers/phx/2024-2025",
        )


def test_covers_provider_discovers_team_pages_from_index_page(monkeypatch) -> None:
    provider = CoversHistoricalTeamPageProvider()
    index_url = "https://www.covers.com/sport/basketball/nba/teams"
    index_html = """
<html>
  <body>
    <a href="/sport/basketball/nba/teams/main/los-angeles-lakers">Los Angeles Lakers</a>
    <a href="/sport/basketball/nba/teams/main/boston-celtics">Boston Celtics</a>
    <a href="/sport/basketball/nba/teams/main/los-angeles-lakers">Duplicate Lakers</a>
    <a href="/sport/football/nfl/teams/main/chiefs">Ignore NFL</a>
  </body>
</html>
"""

    monkeypatch.setattr(
        provider,
        "fetch_page",
        lambda *, url: FetchedPage(
            source_url=url,
            content=index_html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )

    discovered_pages = provider.discover_team_pages(index_url=index_url)

    assert [page.team_name for page in discovered_pages] == [
        "Los Angeles Lakers",
        "Boston Celtics",
    ]
    assert [page.team_slug for page in discovered_pages] == [
        "los-angeles-lakers",
        "boston-celtics",
    ]
    assert [page.team_main_page_url for page in discovered_pages] == [
        "https://www.covers.com/sport/basketball/nba/teams/main/los-angeles-lakers",
        "https://www.covers.com/sport/basketball/nba/teams/main/boston-celtics",
    ]
    assert all(page.source_url == index_url for page in discovered_pages)


def test_covers_provider_adapter_helpers_extract_regular_season_rows() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_sample_team_page(provider)

    season_block = provider.extract_season_block(
        page_content=html,
        season_label="2024-2025",
        team_main_page_url="https://example.com/covers/lal",
    )

    assert season_block is not None
    assert season_block.metadata["selector_match"] == "page-fallback"

    raw_rows = provider.extract_regular_season_rows(season_block=season_block)

    assert len(raw_rows) == 3
    assert raw_rows[0].cells == (
        "2024-11-01",
        "BOS",
        "W",
        "112-104",
        "W -3.5",
        "O 214.5",
    )
    assert all(row.metadata["source_section"] == "Regular Season" for row in raw_rows)

    parsed_row = provider.parse_row(
        raw_row=raw_rows[1],
        row_index=2,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal",
        source_section="Regular Season",
    )

    normalized_row = provider.normalize_row(raw_row=parsed_row)

    assert normalized_row.source_row_index == 2
    assert normalized_row.opponent_code == "NYK"
    assert normalized_row.is_away is True


def test_covers_provider_marks_invalid_formats_without_raising() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = """
<!doctype html>
<html>
  <body>
    <section data-section="Regular Season">
      <table>
        <tbody>
          <tr>
            <td>2024-11-07</td>
            <td>BOS</td>
            <td>W</td>
            <td>bad-score</td>
            <td>BAD ATS</td>
            <td>BAD OU</td>
          </tr>
        </tbody>
      </table>
    </section>
  </body>
</html>
"""

    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )

    assert len(rows) == 1
    assert rows[0].parse_status == ParseStatus.INVALID
    assert rows[0].team_score == 0
    assert rows[0].opponent_score == 0
    assert "parse.invalid_score_format" in rows[0].warnings
    assert "parse.invalid_ats_format" in rows[0].warnings
    assert "parse.invalid_ou_format" in rows[0].warnings


def test_covers_provider_marks_partial_market_fields_as_warnings() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = """
<!doctype html>
<html>
  <body>
    <section data-section="Regular Season">
      <table>
        <tbody>
          <tr>
            <td>2024-11-07</td>
            <td>@New York Knicks</td>
            <td>W</td>
            <td>103-99</td>
            <td>-2.5</td>
            <td>212</td>
          </tr>
        </tbody>
      </table>
    </section>
  </body>
</html>
"""

    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )

    assert len(rows) == 1
    assert rows[0].parse_status == ParseStatus.VALID_WITH_WARNINGS
    assert rows[0].opponent_code == "NYK"
    assert rows[0].is_away is True
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "alias_name"
    assert rows[0].parser_provenance["ats_parse_mode"] == "line_only"
    assert rows[0].parser_provenance["ou_parse_mode"] == "line_only"
    assert rows[0].ats_result is None
    assert rows[0].ats_line == -2.5
    assert rows[0].ou_result is None
    assert rows[0].total_line == 212.0
    assert "parse.missing_ats_result" in rows[0].warnings
    assert "parse.missing_ou_result" in rows[0].warnings


def test_covers_provider_warns_when_opponent_mapping_is_ambiguous() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = """
<!doctype html>
<html>
  <body>
    <section data-section="Regular Season">
      <table>
        <tbody>
          <tr>
            <td>2024-11-07</td>
            <td>@Mystery Club</td>
            <td>W</td>
            <td>103-99</td>
            <td>W -2.5</td>
            <td>U 212</td>
          </tr>
        </tbody>
      </table>
    </section>
  </body>
</html>
"""

    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )

    assert len(rows) == 1
    assert rows[0].parse_status == ParseStatus.VALID_WITH_WARNINGS
    assert rows[0].opponent_code == "MYSTERYCLUB"
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "ambiguous_text_fallback"
    assert "parse.opponent_team_ambiguous" in rows[0].warnings


def test_covers_provider_extracts_live_table_shape_rows() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_live_team_page(provider)

    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )

    assert len(rows) == 1
    assert rows[0].game_date.isoformat() == "2025-04-09"
    assert rows[0].opponent_code == "GSW"
    assert rows[0].is_away is True
    assert rows[0].result_flag == "W"
    assert rows[0].team_score == 119
    assert rows[0].opponent_score == 103
    assert rows[0].ats_result == "W"
    assert rows[0].ats_line == -4.5
    assert rows[0].ou_result == "U"
    assert rows[0].total_line == 228.5
    assert rows[0].parse_status == ParseStatus.VALID
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "alias_name"


def test_covers_provider_normalizes_live_two_letter_team_abbreviations() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_live_schedule_fragment(provider)

    rows = provider.parse_team_page(
        html=html,
        team_code="LAL",
        season_label="2023-2024",
        source_url="https://example.com/covers/lal/2023-2024",
    )

    assert len(rows) == 1
    assert rows[0].opponent_code == "NOP"
    assert rows[0].is_away is False
    assert rows[0].parse_status == ParseStatus.VALID
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "alias_name"


def test_covers_provider_normalizes_historical_pho_abbreviation() -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = """
<!doctype html>
<html>
  <body>
    <section data-section="Regular Season">
      <table>
        <tbody>
          <tr>
            <td>2025-03-24</td>
            <td>PHO</td>
            <td>L</td>
            <td>106-108</td>
            <td>L 2.5</td>
            <td>U 223.5</td>
          </tr>
        </tbody>
      </table>
    </section>
  </body>
</html>
"""

    rows = provider.parse_team_page(
        html=html,
        team_code="MIL",
        season_label="2024-2025",
        source_url="https://example.com/covers/mil/2024-2025",
    )

    assert len(rows) == 1
    assert rows[0].opponent_code == "PHX"
    assert rows[0].parse_status == ParseStatus.VALID
    assert rows[0].parser_provenance["opponent_resolution"]["mode"] == "alias_name"


def test_covers_provider_uses_static_fetch_when_requested_seasons_are_present(
    monkeypatch,
) -> None:
    provider = CoversHistoricalTeamPageProvider()
    html = _load_sample_team_page(provider)

    monkeypatch.setattr(
        provider,
        "fetch_page",
        lambda *, url: FetchedPage(
            source_url=url,
            content=html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )

    fetch_result = provider.fetch_team_main_page(
        url="https://example.com/covers/lal",
        requested_season_labels=("2023-2024",),
        browser_fallback=True,
    )

    assert fetch_result.fetched_page.content == html
    assert fetch_result.diagnostics == ()
    assert fetch_result.used_browser_fallback is False
    assert fetch_result.missing_season_labels == ()


def test_covers_provider_falls_back_to_browser_when_static_html_misses_season(
    monkeypatch,
) -> None:
    provider = CoversHistoricalTeamPageProvider()
    static_html = """
<!doctype html>
<html>
  <body>
    <div id="2024-2025">
      <section data-section="Regular Season">
        <table>
          <tbody>
            <tr>
              <td>2024-11-01</td>
              <td>BOS</td>
              <td>W</td>
              <td>112-104</td>
              <td>W -3.5</td>
              <td>O 214.5</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>
  </body>
</html>
"""
    browser_html = """
<!doctype html>
<html>
  <body>
    <div id="2021-2022">
      <section data-section="Regular Season">
        <table>
          <tbody>
            <tr>
              <td>2022-01-01</td>
              <td>NYK</td>
              <td>W</td>
              <td>101-99</td>
              <td>W -2.5</td>
              <td>U 211.5</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>
  </body>
</html>
"""

    monkeypatch.setattr(
        provider,
        "fetch_page",
        lambda *, url: FetchedPage(
            source_url=url,
            content=static_html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_page_with_browser",
        lambda *, url, season_labels: FetchedPage(
            source_url=url,
            content=browser_html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )

    fetch_result = provider.fetch_team_main_page(
        url="https://example.com/covers/lal",
        requested_season_labels=("2021-2022",),
        browser_fallback=True,
    )

    assert fetch_result.fetched_page.content == browser_html
    assert fetch_result.diagnostics == (
        "browser_fallback_requested",
        "browser_fallback_used",
    )
    assert fetch_result.used_browser_fallback is True
    assert fetch_result.missing_season_labels == ()


def test_covers_provider_falls_back_when_season_block_exists_but_has_no_rows(
    monkeypatch,
) -> None:
    provider = CoversHistoricalTeamPageProvider()
    static_html = """
<!doctype html>
<html>
  <body>
    <section
      id="2024-2025"
      class="previousSeasonContainer"
      data-id="47983"
      data-season-name="2024-2025"
    >
      <div class="seasonHeader" data-id="47983" data-season-name="2024-2025">
        <h2 class="tabHeader" data-id="47983" data-season-name="2024-2025">2024-2025 Season</h2>
        <button
          id="47983-btn"
          class="seasonExpanderBtn"
          data-id="47983"
          data-season-name="2024-2025"
        ></button>
      </div>
      <div
        id="47983-content"
        class="seasonContent hidden"
        data-id="47983"
        data-season-name="2024-2025"
      ></div>
    </section>
  </body>
</html>
"""
    browser_html = """
<!doctype html>
<html>
  <body>
    <section
      id="2024-2025"
      class="previousSeasonContainer"
      data-id="47983"
      data-season-name="2024-2025"
    >
      <div class="seasonHeader" data-id="47983" data-season-name="2024-2025">
        <h2 class="tabHeader" data-id="47983" data-season-name="2024-2025">2024-2025 Season</h2>
      </div>
      <div id="47983-content" class="seasonContent" data-id="47983" data-season-name="2024-2025">
        <table>
          <thead>
            <tr><th colspan="5"><h2>Regular Season</h2></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Apr 9</td>
              <td><a href="/sport/basketball/nba/teams/main/golden-state-warriors">@ GS</a></td>
              <td><a href="https://example.com/matchup">W 119-103</a></td>
              <td><span>W</span> -4.5</td>
              <td><span>U</span> 228.5</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </body>
</html>
"""

    monkeypatch.setattr(
        provider,
        "fetch_page",
        lambda *, url: FetchedPage(
            source_url=url,
            content=static_html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_page_with_browser",
        lambda *, url, season_labels: FetchedPage(
            source_url=url,
            content=browser_html,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        ),
    )

    fetch_result = provider.fetch_team_main_page(
        url="https://example.com/covers/lal",
        requested_season_labels=("2024-2025",),
        browser_fallback=True,
    )
    rows = provider.parse_team_page(
        html=fetch_result.fetched_page.content,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal",
    )

    assert fetch_result.used_browser_fallback is True
    assert fetch_result.diagnostics == (
        "browser_fallback_requested",
        "browser_fallback_used",
    )
    assert fetch_result.missing_season_labels == ()
    assert len(rows) == 1


def test_covers_provider_fetches_missing_season_content_from_schedule_endpoint(
    monkeypatch,
) -> None:
    provider = CoversHistoricalTeamPageProvider()
    static_html = _load_live_team_page(provider)
    fragment_html = _load_live_schedule_fragment(provider)
    fetched_urls: list[str] = []

    def fake_fetch_page(*, url: str):
        fetched_urls.append(url)
        content = fragment_html if "getschedule/schedule/175/47854" in url else static_html
        return FetchedPage(
            source_url=url,
            content=content,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        )

    monkeypatch.setattr(provider, "fetch_page", fake_fetch_page)

    fetch_result = provider.fetch_team_main_page(
        url="https://example.com/covers/lal",
        requested_season_labels=("2023-2024",),
        browser_fallback=True,
    )
    rows = provider.parse_team_page(
        html=fetch_result.fetched_page.content,
        team_code="LAL",
        season_label="2023-2024",
        source_url="https://example.com/covers/lal",
    )

    assert any("getschedule/schedule/175/47854" in url for url in fetched_urls)
    assert fetch_result.used_browser_fallback is False
    assert fetch_result.diagnostics == ("season_content_fetch_used",)
    assert fetch_result.missing_season_labels == ()
    assert len(rows) == 1
    assert rows[0].opponent_code == "NOP"
    assert rows[0].parse_status == ParseStatus.VALID
