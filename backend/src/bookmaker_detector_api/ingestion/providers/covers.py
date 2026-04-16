from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow

ATS_PATTERN = re.compile(r"^(?P<result>[WLP])\s+(?P<line>[+-]?\d+(?:\.\d+)?)$")
OU_PATTERN = re.compile(r"^(?P<result>[OU])\s+(?P<line>\d+(?:\.\d+)?)$")
SCORE_PATTERN = re.compile(r"^(?P<team>\d+)-(?P<opponent>\d+)$")
FALLBACK_DATE = datetime.strptime("1900-01-01", "%Y-%m-%d").date()


class CoversHistoricalTeamPageProvider:
    provider_name = "covers"

    def load_fixture(self, fixture_path: Path) -> str:
        return fixture_path.read_text(encoding="utf-8")

    def parse_team_page(
        self,
        *,
        html: str,
        team_code: str,
        season_label: str,
        source_url: str,
    ) -> list[RawGameRow]:
        document = ET.fromstring(_strip_doctype(html))
        rows: list[RawGameRow] = []

        for section in document.findall(".//section"):
            section_name = (section.attrib.get("data-section") or "").strip()
            if section_name != "Regular Season":
                continue

            tbody_rows = section.findall(".//tbody/tr")
            for row_index, row in enumerate(tbody_rows, start=1):
                rows.append(
                    self._parse_row(
                        row_html=row,
                        row_index=row_index,
                        section_name=section_name,
                        team_code=team_code,
                        season_label=season_label,
                        source_url=source_url,
                    )
                )

        return rows

    def _parse_row(
        self,
        *,
        row_html,
        row_index: int,
        section_name: str,
        team_code: str,
        season_label: str,
        source_url: str,
    ) -> RawGameRow:
        warnings: list[str] = []
        cells = ["".join(cell.itertext()).strip() for cell in row_html.findall("td")]
        if len(cells) < 5:
            return RawGameRow(
                provider_name=self.provider_name,
                team_code=team_code,
                season_label=season_label,
                source_url=source_url,
                source_section=section_name,
                source_row_index=row_index,
                game_date=FALLBACK_DATE,
                opponent_code="UNKNOWN",
                is_away=False,
                result_flag="",
                team_score=0,
                opponent_score=0,
                ats_result=None,
                ats_line=None,
                ou_result=None,
                total_line=None,
                parse_status=ParseStatus.INVALID,
                warnings=["row_too_short"],
            )

        parse_status = ParseStatus.VALID
        game_date = FALLBACK_DATE
        try:
            game_date = datetime.strptime(cells[0], "%Y-%m-%d").date()
        except ValueError:
            warnings.append("parse.invalid_game_date_format")
            parse_status = ParseStatus.INVALID
        opponent_raw = cells[1]
        result_flag = cells[2]
        score_raw = cells[3]
        ats_raw = cells[4] if len(cells) > 4 else ""
        ou_raw = cells[5] if len(cells) > 5 else ""

        is_away = opponent_raw.startswith("@")
        opponent_code = opponent_raw.removeprefix("@").strip()

        score_match = SCORE_PATTERN.match(score_raw)
        if score_match is None:
            warnings.append("parse.invalid_score_format")
            parse_status = ParseStatus.INVALID

        ats_result = None
        ats_line = None
        if ats_raw:
            ats_match = ATS_PATTERN.match(ats_raw)
            if ats_match:
                ats_result = ats_match.group("result")
                ats_line = float(ats_match.group("line"))
            else:
                warnings.append("parse.invalid_ats_format")
                if parse_status != ParseStatus.INVALID:
                    parse_status = ParseStatus.VALID_WITH_WARNINGS

        ou_result = None
        total_line = None
        if ou_raw:
            ou_match = OU_PATTERN.match(ou_raw)
            if ou_match:
                ou_result = ou_match.group("result")
                total_line = float(ou_match.group("line"))
            else:
                warnings.append("parse.invalid_ou_format")
                if parse_status != ParseStatus.INVALID:
                    parse_status = ParseStatus.VALID_WITH_WARNINGS

        return RawGameRow(
            provider_name=self.provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            source_section=section_name,
            source_row_index=row_index,
            game_date=game_date,
            opponent_code=opponent_code,
            is_away=is_away,
            result_flag=result_flag,
            team_score=int(score_match.group("team")) if score_match else 0,
            opponent_score=int(score_match.group("opponent")) if score_match else 0,
            ats_result=ats_result,
            ats_line=ats_line,
            ou_result=ou_result,
            total_line=total_line,
            parse_status=parse_status,
            warnings=warnings,
        )


def _strip_doctype(html: str) -> str:
    return re.sub(r"<!doctype[^>]*>\s*", "", html, flags=re.IGNORECASE)
