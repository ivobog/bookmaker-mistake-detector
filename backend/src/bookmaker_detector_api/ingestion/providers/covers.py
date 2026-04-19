from __future__ import annotations

import re
from datetime import datetime
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from bookmaker_detector_api.fetching import FetchedPage, fetch_page
from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow
from bookmaker_detector_api.ingestion.providers.base import (
    DiscoveredTeamPage,
    ProviderRow,
    SeasonBlock,
    TeamPageFetchResult,
)
from bookmaker_detector_api.team_normalization import resolve_team_code_or_name

ATS_PATTERN = re.compile(r"^(?P<result>[WLP])\s+(?P<line>[+-]?\d+(?:\.\d+)?)$")
OU_PATTERN = re.compile(r"^(?P<result>[OU])\s+(?P<line>\d+(?:\.\d+)?)$")
SCORE_PATTERN = re.compile(r"^(?P<team>\d+)-(?P<opponent>\d+)$")
LINE_ONLY_PATTERN = re.compile(r"^(?P<line>[+-]?\d+(?:\.\d+)?)$")
TOTAL_ONLY_PATTERN = re.compile(r"^(?P<line>\d+(?:\.\d+)?)$")
TEAM_MAIN_PATH_PATTERN = re.compile(r"/sport/basketball/nba/teams/main/(?P<slug>[^/?#]+)")
FALLBACK_DATE = datetime.strptime("1900-01-01", "%Y-%m-%d").date()


class CoversHistoricalTeamPageProvider:
    provider_name = "covers"

    def load_fixture(self, fixture_path: Path) -> str:
        return fixture_path.read_text(encoding="utf-8")

    def discover_team_pages(self, *, index_url: str) -> list[DiscoveredTeamPage]:
        fetched_page = self.fetch_page(url=index_url)
        document = _parse_document(fetched_page.content)
        discovered_pages: list[DiscoveredTeamPage] = []
        seen_urls: set[str] = set()

        for anchor in _find_descendants(document, "a"):
            href = (anchor.attributes.get("href") or "").strip()
            if not href:
                continue
            slug = _extract_team_slug(href)
            if slug is None:
                continue
            team_main_page_url = urljoin(index_url, href)
            if team_main_page_url in seen_urls:
                continue
            seen_urls.add(team_main_page_url)
            team_name = _itertext(anchor).strip() or _team_name_from_slug(slug)
            discovered_pages.append(
                DiscoveredTeamPage(
                    team_name=team_name,
                    team_main_page_url=team_main_page_url,
                    team_slug=slug,
                    source_url=index_url,
                )
            )

        return discovered_pages

    def resolve_team_main_page_url(self, *, team_page: DiscoveredTeamPage) -> str:
        return team_page.team_main_page_url

    def fetch_page(self, *, url: str) -> FetchedPage:
        return fetch_page(url)

    def validate_team_page_identity(
        self,
        *,
        page_content: str,
        team_code: str,
        team_main_page_url: str,
    ) -> None:
        document = _parse_document(page_content)
        declared_team_code = _extract_declared_team_identity_value(
            document,
            attribute_name="data-team-code",
        )
        if declared_team_code is None:
            return
        resolved_expected_team_code, _ = resolve_team_code_or_name(team_code)
        expected_team_code = (resolved_expected_team_code or team_code).strip().upper()
        resolved_declared_team_code, _ = resolve_team_code_or_name(declared_team_code)
        normalized_declared_team_code = (
            resolved_declared_team_code or declared_team_code
        ).strip().upper()
        if normalized_declared_team_code == expected_team_code:
            return
        raise ValueError(
            "Covers team page identity mismatch: "
            f"requested {expected_team_code}, fixture declares {normalized_declared_team_code} "
            f"for {team_main_page_url}."
        )

    def fetch_team_main_page(
        self,
        *,
        url: str,
        requested_season_labels: tuple[str, ...] | list[str] = (),
        browser_fallback: bool = False,
    ) -> TeamPageFetchResult:
        fetched_page = self.fetch_page(url=url)
        requested_labels = tuple(
            label.strip() for label in requested_season_labels if label.strip()
        )
        diagnostics: list[str] = []
        enriched_page = fetched_page
        missing_season_labels = self._missing_season_labels(
            page_content=enriched_page.content,
            team_main_page_url=url,
            season_labels=requested_labels,
        )
        if missing_season_labels:
            enriched_page, fetch_diagnostics = self._try_fetch_missing_season_content(
                fetched_page=enriched_page,
                team_main_page_url=url,
                season_labels=missing_season_labels,
            )
            diagnostics.extend(fetch_diagnostics)
            missing_season_labels = self._missing_season_labels(
                page_content=enriched_page.content,
                team_main_page_url=url,
                season_labels=requested_labels,
            )
        if not browser_fallback or not missing_season_labels or _is_file_url(url):
            return TeamPageFetchResult(
                fetched_page=enriched_page,
                diagnostics=tuple(diagnostics),
                missing_season_labels=missing_season_labels,
            )

        diagnostics.append("browser_fallback_requested")
        try:
            browser_page = self._fetch_page_with_browser(
                url=url,
                season_labels=missing_season_labels,
            )
        except Exception:
            diagnostics.append("browser_fallback_failed")
            return TeamPageFetchResult(
                fetched_page=enriched_page,
                diagnostics=tuple(diagnostics),
                missing_season_labels=missing_season_labels,
            )

        browser_missing_season_labels = self._missing_season_labels(
            page_content=browser_page.content,
            team_main_page_url=url,
            season_labels=requested_labels,
        )
        diagnostics.append("browser_fallback_used")
        if browser_missing_season_labels:
            diagnostics.append("browser_fallback_incomplete")
        return TeamPageFetchResult(
            fetched_page=browser_page,
            diagnostics=tuple(diagnostics),
            used_browser_fallback=True,
            missing_season_labels=browser_missing_season_labels,
        )

    def _try_fetch_missing_season_content(
        self,
        *,
        fetched_page: FetchedPage,
        team_main_page_url: str,
        season_labels: tuple[str, ...],
    ) -> tuple[FetchedPage, list[str]]:
        if _is_file_url(team_main_page_url):
            return fetched_page, []

        team_data_id = _extract_team_data_id(fetched_page.content)
        if team_data_id is None:
            return fetched_page, []

        fragment_payloads: dict[str, str] = {}
        failed_seasons: list[str] = []
        for season_label in season_labels:
            season_data_id = _extract_season_data_id(fetched_page.content, season_label)
            if season_data_id is None:
                failed_seasons.append(season_label)
                continue
            try:
                fragment_page = self.fetch_page(
                    url=_build_schedule_fragment_url(
                        team_main_page_url=team_main_page_url,
                        team_data_id=team_data_id,
                        season_data_id=season_data_id,
                    )
                )
                fragment_payloads[season_label] = fragment_page.content
            except Exception:
                failed_seasons.append(season_label)

        diagnostics: list[str] = []
        if fragment_payloads:
            diagnostics.append("season_content_fetch_used")
        if failed_seasons:
            diagnostics.append("season_content_fetch_failed")
        if not fragment_payloads:
            return fetched_page, diagnostics

        enriched_content = _append_season_fragments(
            page_content=fetched_page.content,
            fragment_payloads=fragment_payloads,
        )
        return (
            FetchedPage(
                source_url=fetched_page.source_url,
                content=enriched_content,
                status=fetched_page.status,
                http_status=fetched_page.http_status,
                content_type=fetched_page.content_type,
            ),
            diagnostics,
        )

    def extract_season_block(
        self,
        *,
        page_content: str,
        season_label: str,
        team_main_page_url: str,
    ) -> SeasonBlock | None:
        document = _parse_document(page_content)
        season_patterns = _season_patterns(season_label)

        for element in document.iter():
            if (element.attributes.get("data-season-fragment") or "").strip() == season_label:
                return SeasonBlock(
                    season_label=season_label,
                    team_main_page_url=team_main_page_url,
                    content=_serialize_node(element),
                    metadata={"selector_match": "season-fragment"},
                )

        season_content_block = _find_season_content_block(document, season_label)
        if season_content_block is not None:
            return SeasonBlock(
                season_label=season_label,
                team_main_page_url=team_main_page_url,
                content=_serialize_node(season_content_block),
                metadata={"selector_match": "season-content"},
            )

        for element in document.iter():
            anchor_values = [
                element.attributes.get("id"),
                element.attributes.get("data-season"),
                element.attributes.get("data-season-label"),
                element.attributes.get("data-season-name"),
                element.attributes.get("data-target"),
                element.attributes.get("href"),
            ]
            matches_requested_season = any(
                value and any(pattern == value for pattern in season_patterns)
                for value in anchor_values
            )
            if matches_requested_season:
                return SeasonBlock(
                    season_label=season_label,
                    team_main_page_url=team_main_page_url,
                    content=_serialize_node(element),
                    metadata={"selector_match": "season-anchor"},
                )

        if any(_looks_like_season_container(element) for element in document.iter()):
            return None

        return SeasonBlock(
            season_label=season_label,
            team_main_page_url=team_main_page_url,
            content=page_content,
            metadata={"selector_match": "page-fallback"},
        )

    def extract_regular_season_rows(self, *, season_block: SeasonBlock) -> list[ProviderRow]:
        block_document = _parse_document(season_block.content)
        rows: list[ProviderRow] = []

        for section in _find_descendants(block_document, "section"):
            section_name = (section.attributes.get("data-section") or "").strip()
            if section_name != "Regular Season":
                continue
            for row in _find_descendant_paths(section, ("tbody", "tr")):
                rows.append(
                    ProviderRow(
                        cells=tuple(_itertext(cell).strip() for cell in _find_children(row, "td")),
                        metadata={
                            "source_section": section_name,
                            "row_html": _serialize_node(row),
                        },
                    )
                )

        if rows:
            return rows

        for table in _find_descendants(block_document, "table"):
            section_name = _table_section_name(table)
            if section_name != "Regular Season":
                continue
            for row in _find_descendant_paths(table, ("tbody", "tr")):
                provider_row = _normalize_live_table_row(
                    season_label=season_block.season_label,
                    row=row,
                )
                if provider_row is not None:
                    rows.append(provider_row)

        return rows

    def _missing_season_labels(
        self,
        *,
        page_content: str,
        team_main_page_url: str,
        season_labels: tuple[str, ...],
    ) -> tuple[str, ...]:
        missing_labels: list[str] = []
        for season_label in season_labels:
            season_block = self.extract_season_block(
                page_content=page_content,
                season_label=season_label,
                team_main_page_url=team_main_page_url,
            )
            if season_block is None:
                missing_labels.append(season_label)
                continue
            if not self.extract_regular_season_rows(season_block=season_block):
                missing_labels.append(season_label)
        return tuple(missing_labels)

    def _fetch_page_with_browser(
        self,
        *,
        url: str,
        season_labels: tuple[str, ...],
    ) -> FetchedPage:
        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright browser fallback requested but playwright is not installed."
            ) from exc

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    page = browser.new_page()
                    page.goto(url, wait_until="domcontentloaded")
                    try:
                        page.wait_for_load_state("networkidle", timeout=10_000)
                    except PlaywrightTimeoutError:
                        pass
                    for season_label in season_labels:
                        _expand_season_in_browser(page=page, season_label=season_label)
                    content = page.content()
                finally:
                    browser.close()
        except PlaywrightError as exc:
            raise RuntimeError(f"Playwright browser fallback failed for {url}: {exc}") from exc

        return FetchedPage(
            source_url=url,
            content=content,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        )

    def parse_row(
        self,
        *,
        raw_row: ProviderRow,
        row_index: int,
        team_code: str,
        season_label: str,
        source_url: str,
        source_section: str,
    ) -> RawGameRow:
        warnings: list[str] = []
        cells = list(raw_row.cells)
        if len(cells) < 5:
            return RawGameRow(
                provider_name=self.provider_name,
                team_code=team_code,
                season_label=season_label,
                source_url=source_url,
                source_section=source_section,
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

        opponent_code, is_away, opponent_warnings, opponent_provenance = _parse_opponent(
            opponent_raw
        )
        warnings.extend(opponent_warnings)
        parse_status = _apply_warnings_parse_status(
            parse_status=parse_status,
            warnings=opponent_warnings,
        )
        if "parse.missing_opponent" in opponent_warnings:
            parse_status = ParseStatus.INVALID

        score_match = SCORE_PATTERN.match(score_raw)
        if score_match is None:
            warnings.append("parse.invalid_score_format")
            parse_status = ParseStatus.INVALID

        ats_result, ats_line, ats_warnings, ats_parse_mode = _parse_ats_value(ats_raw)
        warnings.extend(ats_warnings)
        parse_status = _apply_warnings_parse_status(
            parse_status=parse_status,
            warnings=ats_warnings,
        )

        ou_result, total_line, ou_warnings, ou_parse_mode = _parse_ou_value(ou_raw)
        warnings.extend(ou_warnings)
        parse_status = _apply_warnings_parse_status(
            parse_status=parse_status,
            warnings=ou_warnings,
        )

        return RawGameRow(
            provider_name=self.provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            source_section=source_section,
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
            parser_provenance={
                "opponent_resolution": opponent_provenance,
                "ats_parse_mode": ats_parse_mode,
                "ou_parse_mode": ou_parse_mode,
            },
        )

    def normalize_row(self, *, raw_row: RawGameRow) -> RawGameRow:
        return raw_row

    def parse_team_page(
        self,
        *,
        html: str,
        team_code: str,
        season_label: str,
        source_url: str,
    ) -> list[RawGameRow]:
        self.validate_team_page_identity(
            page_content=html,
            team_code=team_code,
            team_main_page_url=source_url,
        )
        season_block = self.extract_season_block(
            page_content=html,
            season_label=season_label,
            team_main_page_url=source_url,
        )
        if season_block is None:
            return []

        parsed_rows = self.extract_regular_season_rows(season_block=season_block)
        rows: list[RawGameRow] = []
        for row_index, raw_row in enumerate(parsed_rows, start=1):
            parsed_row = self.parse_row(
                raw_row=raw_row,
                row_index=row_index,
                team_code=team_code,
                season_label=season_label,
                source_url=source_url,
                source_section=str(raw_row.metadata.get("source_section", "Regular Season")),
            )
            rows.append(self.normalize_row(raw_row=parsed_row))

        return rows


def _strip_doctype(html: str) -> str:
    return re.sub(r"<!doctype[^>]*>\s*", "", html, flags=re.IGNORECASE)


def _parse_document(html: str) -> "_HtmlNode":
    parser = _HtmlTreeParser()
    parser.feed(_strip_doctype(html))
    parser.close()
    return parser.root


def _extract_team_slug(href: str) -> str | None:
    parsed = urlsplit(href)
    match = TEAM_MAIN_PATH_PATTERN.search(parsed.path)
    if match is None:
        return None
    return match.group("slug").strip()


def _extract_declared_team_identity_value(
    document: "_HtmlNode",
    *,
    attribute_name: str,
) -> str | None:
    for element in document.iter():
        attribute_value = (element.attributes.get(attribute_name) or "").strip()
        if attribute_value:
            return attribute_value
    return None


def _team_name_from_slug(slug: str) -> str:
    return slug.replace("-", " ").title()


def _season_patterns(season_label: str) -> set[str]:
    normalized = season_label.strip()
    season_anchor = f"#{normalized}"
    return {normalized, season_anchor, season_anchor.lower()}


def _looks_like_season_container(element: "_HtmlNode") -> bool:
    candidate_values = [
        element.attributes.get("id"),
        element.attributes.get("data-season"),
        element.attributes.get("data-season-label"),
        element.attributes.get("data-season-name"),
        element.attributes.get("href"),
    ]
    for value in candidate_values:
        if value and re.fullmatch(r"#?\d{4}-\d{4}", value.strip()):
            return True
    return False


def _is_file_url(url: str) -> bool:
    return urlsplit(url).scheme == "file"


def _expand_season_in_browser(*, page, season_label: str) -> None:
    season_anchor = f"#{season_label}"
    selectors = [
        f'section[id="{season_label}"] .seasonExpanderBtn',
        f'button[data-season-name="{season_label}"]',
        f'a[href="{season_anchor}"]',
        f'[data-season="{season_label}"]',
        f'[data-season-label="{season_label}"]',
        f'[id="{season_label}"]',
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count() == 0:
                continue
            locator.scroll_into_view_if_needed(timeout=2_000)
            locator.click(timeout=2_000)
            _wait_for_season_content(page=page, season_label=season_label)
            return
        except Exception:
            continue
    try:
        page.evaluate(
            "(anchor) => { window.location.hash = anchor; }",
            season_anchor,
        )
        _wait_for_season_content(page=page, season_label=season_label)
    except Exception:
        return


def _parse_opponent(opponent_raw: str) -> tuple[str, bool, list[str], dict[str, str]]:
    is_away = opponent_raw.startswith("@")
    cleaned_value = opponent_raw.removeprefix("@").strip()
    if not cleaned_value:
        return (
            "UNKNOWN",
            is_away,
            ["parse.missing_opponent"],
            {
                "input": opponent_raw,
                "normalized_input": "",
                "mode": "missing",
            },
        )

    team_code, resolution_mode = resolve_team_code_or_name(cleaned_value)
    if team_code is not None:
        return (
            team_code,
            is_away,
            [],
            {
                "input": opponent_raw,
                "normalized_input": cleaned_value,
                "mode": resolution_mode or "alias_name",
            },
        )

    fallback_code = re.sub(r"[^A-Z0-9]+", "", cleaned_value.upper())
    if re.fullmatch(r"[A-Z0-9]{2,4}", fallback_code or ""):
        return (
            fallback_code,
            is_away,
            ["parse.opponent_team_ambiguous"],
            {
                "input": opponent_raw,
                "normalized_input": cleaned_value,
                "mode": "ambiguous_code_fallback",
            },
        )

    return (
        fallback_code or "UNKNOWN",
        is_away,
        ["parse.opponent_team_ambiguous"],
        {
            "input": opponent_raw,
            "normalized_input": cleaned_value,
            "mode": "ambiguous_text_fallback",
        },
    )


def _parse_ats_value(raw_value: str) -> tuple[str | None, float | None, list[str], str]:
    stripped_value = raw_value.strip()
    if not stripped_value:
        return None, None, [], "missing"
    ats_match = ATS_PATTERN.match(stripped_value)
    if ats_match is not None:
        return ats_match.group("result"), float(ats_match.group("line")), [], "full"
    line_only_match = LINE_ONLY_PATTERN.match(stripped_value)
    if line_only_match is not None:
        return None, float(line_only_match.group("line")), ["parse.missing_ats_result"], "line_only"
    if stripped_value in {"W", "L", "P"}:
        return stripped_value, None, ["parse.missing_ats_line"], "result_only"
    return None, None, ["parse.invalid_ats_format"], "invalid"


def _parse_ou_value(raw_value: str) -> tuple[str | None, float | None, list[str], str]:
    stripped_value = raw_value.strip()
    if not stripped_value:
        return None, None, [], "missing"
    ou_match = OU_PATTERN.match(stripped_value)
    if ou_match is not None:
        return ou_match.group("result"), float(ou_match.group("line")), [], "full"
    line_only_match = TOTAL_ONLY_PATTERN.match(stripped_value)
    if line_only_match is not None:
        return None, float(line_only_match.group("line")), ["parse.missing_ou_result"], "line_only"
    if stripped_value in {"O", "U"}:
        return stripped_value, None, ["parse.missing_ou_line"], "result_only"
    return None, None, ["parse.invalid_ou_format"], "invalid"


def _apply_warnings_parse_status(
    *,
    parse_status: ParseStatus,
    warnings: list[str],
) -> ParseStatus:
    if parse_status == ParseStatus.INVALID or not warnings:
        return parse_status
    return ParseStatus.VALID_WITH_WARNINGS


class _HtmlNode:
    __slots__ = ("tag", "attributes", "children", "content")

    def __init__(self, tag: str, attributes: dict[str, str]) -> None:
        self.tag = tag.lower()
        self.attributes = attributes
        self.children: list[_HtmlNode] = []
        self.content: list[str | _HtmlNode] = []

    def iter(self):
        yield self
        for child in self.children:
            yield from child.iter()


class _HtmlTreeParser(HTMLParser):
    VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _HtmlNode("document", {})
        self._stack: list[_HtmlNode] = [self.root]

    def handle_starttag(self, tag: str, attrs) -> None:
        self._auto_close_for_starttag(tag.lower())
        node = _HtmlNode(tag, _attrs_to_dict(attrs))
        self._stack[-1].children.append(node)
        self._stack[-1].content.append(node)
        if tag.lower() not in self.VOID_TAGS:
            self._stack.append(node)

    def handle_startendtag(self, tag: str, attrs) -> None:
        node = _HtmlNode(tag, _attrs_to_dict(attrs))
        self._stack[-1].children.append(node)
        self._stack[-1].content.append(node)

    def handle_endtag(self, tag: str) -> None:
        target_tag = tag.lower()
        for index in range(len(self._stack) - 1, 0, -1):
            if self._stack[index].tag == target_tag:
                del self._stack[index:]
                break

    def handle_data(self, data: str) -> None:
        if data:
            self._stack[-1].content.append(data)

    def handle_comment(self, data: str) -> None:
        return

    def error(self, message: str) -> None:
        return

    def _auto_close_for_starttag(self, tag: str) -> None:
        while len(self._stack) > 1:
            current_tag = self._stack[-1].tag
            if current_tag in {"td", "th"} and tag in {"td", "th", "tr"}:
                self._stack.pop()
                continue
            if current_tag == "tr" and tag == "tr":
                self._stack.pop()
                continue
            if current_tag == "li" and tag == "li":
                self._stack.pop()
                continue
            if current_tag == "p" and tag in {
                "address",
                "article",
                "aside",
                "blockquote",
                "div",
                "dl",
                "fieldset",
                "footer",
                "form",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "header",
                "hr",
                "menu",
                "nav",
                "ol",
                "p",
                "pre",
                "section",
                "table",
                "ul",
            }:
                self._stack.pop()
                continue
            break


def _attrs_to_dict(attrs) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in attrs:
        if key is None:
            continue
        normalized[str(key).lower()] = "" if value is None else str(value)
    return normalized


def _find_children(node: _HtmlNode, tag: str) -> list[_HtmlNode]:
    target_tag = tag.lower()
    return [child for child in node.children if child.tag == target_tag]


def _find_descendants(node: _HtmlNode, tag: str) -> list[_HtmlNode]:
    target_tag = tag.lower()
    return [
        candidate
        for candidate in node.iter()
        if candidate is not node and candidate.tag == target_tag
    ]


def _find_descendant_paths(node: _HtmlNode, path: tuple[str, ...]) -> list[_HtmlNode]:
    current_nodes = [node]
    for segment in path:
        next_nodes: list[_HtmlNode] = []
        for current_node in current_nodes:
            next_nodes.extend(_find_descendants(current_node, segment))
        current_nodes = next_nodes
    return current_nodes


def _itertext(node: _HtmlNode) -> str:
    text_parts: list[str] = []
    for item in node.content:
        if isinstance(item, str):
            text_parts.append(item)
        else:
            text_parts.append(_itertext(item))
    return "".join(text_parts)


def _serialize_node(node: _HtmlNode) -> str:
    if node.tag == "document":
        return "".join(
            item if isinstance(item, str) else _serialize_node(item) for item in node.content
        )
    serialized_attributes = "".join(
        f' {key}="{escape(value, quote=True)}"' if value != "" else f" {key}"
        for key, value in node.attributes.items()
    )
    inner_html = "".join(
        escape(item) if isinstance(item, str) else _serialize_node(item) for item in node.content
    )
    return f"<{node.tag}{serialized_attributes}>{inner_html}</{node.tag}>"


def _find_season_content_block(document: _HtmlNode, season_label: str) -> _HtmlNode | None:
    for element in document.iter():
        if (element.attributes.get("data-season-name") or "").strip() != season_label:
            continue
        class_value = element.attributes.get("class", "")
        element_id = element.attributes.get("id", "")
        if "seasoncontent" in class_value.lower() or element_id.endswith("-content"):
            return element
    return None


def _table_section_name(table: _HtmlNode) -> str | None:
    for header in _find_descendant_paths(table, ("thead", "h2")):
        header_text = _normalize_whitespace(_itertext(header))
        if header_text.endswith("Regular Season"):
            return "Regular Season"
        if header_text in {"Pre Season", "Playoffs"}:
            return header_text
    return None


def _normalize_live_table_row(
    *,
    season_label: str,
    row: _HtmlNode,
) -> ProviderRow | None:
    cells = _find_children(row, "td")
    if len(cells) < 5:
        return None

    normalized_date = _normalize_live_date(_itertext(cells[0]), season_label)
    opponent_value = _normalize_live_opponent(cells[1])
    score_value = _normalize_whitespace(_itertext(cells[2]))
    ats_value = _normalize_whitespace(_itertext(cells[3]))
    ou_value = _normalize_whitespace(_itertext(cells[4]))
    result_flag, normalized_score = _split_score_value(score_value)

    return ProviderRow(
        cells=(
            normalized_date,
            opponent_value,
            result_flag,
            normalized_score,
            ats_value,
            ou_value,
        ),
        metadata={
            "source_section": "Regular Season",
            "row_html": _serialize_node(row),
            "source_shape": "covers-live-table",
        },
    )


def _normalize_live_date(raw_value: str, season_label: str) -> str:
    cleaned_value = _normalize_whitespace(raw_value)
    date_match = re.search(r"([A-Z][a-z]{2})\s+(\d{1,2})", cleaned_value)
    if date_match is None:
        return cleaned_value
    month_value = datetime.strptime(date_match.group(1), "%b").month
    day_value = int(date_match.group(2))
    start_year, end_year = season_label.split("-", maxsplit=1)
    year_value = int(start_year) if month_value >= 10 else int(end_year)
    return f"{year_value:04d}-{month_value:02d}-{day_value:02d}"


def _normalize_live_opponent(cell: _HtmlNode) -> str:
    anchor = next(iter(_find_descendants(cell, "a")), None)
    opponent_value = _normalize_whitespace(_itertext(anchor or cell))
    return opponent_value


def _split_score_value(score_value: str) -> tuple[str, str]:
    normalized_score = _normalize_whitespace(score_value)
    score_match = re.match(r"^(W|L|P)\s+(.*)$", normalized_score)
    if score_match is None:
        return "", normalized_score
    return score_match.group(1), score_match.group(2).strip()


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _wait_for_season_content(*, page, season_label: str) -> None:
    content_selector = (
        f'section[id="{season_label}"] .seasonContent:not(.hidden), '
        f'div[data-season-name="{season_label}"].seasonContent:not(.hidden)'
    )
    try:
        page.wait_for_selector(content_selector, timeout=5_000)
    except Exception:
        page.wait_for_timeout(500)


def _extract_team_data_id(page_content: str) -> str | None:
    match = re.search(r'"sportDataTeamId"\s*:\s*"(?P<team_id>\d+)"', page_content)
    if match is not None:
        return match.group("team_id")
    return None


def _extract_season_data_id(page_content: str, season_label: str) -> str | None:
    escaped_label = re.escape(season_label)
    patterns = (
        rf'data-id="(?P<season_id>\d+)"[^>]*data-season-name="{escaped_label}"',
        rf'href="#{escaped_label}"\s+data-id="(?P<season_id>\d+)"',
    )
    for pattern in patterns:
        match = re.search(pattern, page_content)
        if match is not None:
            return match.group("season_id")
    return None


def _build_schedule_fragment_url(
    *,
    team_main_page_url: str,
    team_data_id: str,
    season_data_id: str,
) -> str:
    return urljoin(
        team_main_page_url,
        f"/sport/basketball/nba/teams/main/getschedule/schedule/{team_data_id}/{season_data_id}",
    )


def _append_season_fragments(
    *,
    page_content: str,
    fragment_payloads: dict[str, str],
) -> str:
    synthetic_blocks = "".join(
        _wrap_season_fragment(season_label=season_label, fragment_html=fragment_html)
        for season_label, fragment_html in fragment_payloads.items()
    )
    return f'{page_content}\n<div data-season-fragment-root="covers">{synthetic_blocks}</div>'


def _wrap_season_fragment(*, season_label: str, fragment_html: str) -> str:
    escaped_label = escape(season_label, quote=True)
    return (
        f'<section data-season-fragment="{escaped_label}" '
        f'data-season-name="{escaped_label}">{fragment_html}</section>'
    )
