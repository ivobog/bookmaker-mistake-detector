from pathlib import Path

from bookmaker_detector_api.ingestion.models import ParseStatus
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider


def test_covers_provider_parses_only_regular_season_rows() -> None:
    provider = CoversHistoricalTeamPageProvider()
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    )

    html = provider.load_fixture(fixture_path)
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
    assert rows[1].is_away is True
    assert rows[2].ats_line == -1.5
    assert rows[2].total_line == 221.5


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
