import json
import logging
from contextlib import contextmanager

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.ingestion.providers import (
    TeamPageFetchResult,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services import initial_dataset_load as loader
from bookmaker_detector_api.services.workflow_logging import WORKFLOW_LOGGER_NAME
from tests.support.covers_fixtures import (
    DEFAULT_INDEX_URL,
    DEFAULT_TEAM_PAGE_URL,
    build_fixture_backed_covers_provider,
    load_covers_fixture,
)


def _workflow_events(caplog) -> list[dict[str, object]]:
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == WORKFLOW_LOGGER_NAME
    ]


def test_build_initial_dataset_load_targets_matches_discovered_team_pages(monkeypatch) -> None:
    monkeypatch.setattr(
        loader,
        "_load_team_scope",
        lambda connection, *, provider_name, team_codes: [
            {
                "code": "LAL",
                "name": "Los Angeles Lakers",
                "team_slug": "los-angeles-lakers",
                "provider_team_key": None,
                "provider_team_slug": None,
            },
            {
                "code": "BOS",
                "name": "Boston Celtics",
                "team_slug": "boston-celtics",
                "provider_team_key": None,
                "provider_team_slug": None,
            },
        ],
    )
    monkeypatch.setattr(
        loader,
        "_load_season_scope",
        lambda connection, *, season_labels: [
            {"label": "2021-2022", "start_date": None, "end_date": None},
            {"label": "2022-2023", "start_date": None, "end_date": None},
        ],
    )

    class FakeProvider:
        provider_name = "covers"

        def discover_team_pages(self, *, index_url):
            return [
                loader.DiscoveredTeamPage(
                    team_name="Los Angeles Lakers",
                    team_main_page_url="https://example.com/lal",
                    team_slug="los-angeles-lakers",
                    source_url=index_url,
                ),
                loader.DiscoveredTeamPage(
                    team_name="Boston Celtics",
                    team_main_page_url="https://example.com/bos",
                    team_slug="boston-celtics",
                    source_url=index_url,
                ),
            ]

        def resolve_team_main_page_url(self, *, team_page):
            return team_page.team_main_page_url

    targets = loader.build_initial_dataset_load_targets(
        object(),
        provider=FakeProvider(),
        base_url="https://example.com/index",
    )

    assert len(targets) == 2
    assert targets[0].team_code == "LAL"
    assert targets[0].team_main_page_url == "https://example.com/lal"
    assert targets[0].season_labels == ("2021-2022", "2022-2023")
    assert targets[1].team_code == "BOS"


def test_build_initial_dataset_load_targets_matches_alias_slug_variants(monkeypatch) -> None:
    monkeypatch.setattr(
        loader,
        "_load_team_scope",
        lambda connection, *, provider_name, team_codes: [
            {
                "code": "LAL",
                "name": "Los Angeles Lakers",
                "team_slug": "los-angeles-lakers",
                "provider_team_key": None,
                "provider_team_slug": None,
            }
        ],
    )
    monkeypatch.setattr(
        loader,
        "_load_season_scope",
        lambda connection, *, season_labels: [
            {"label": "2021-2022", "start_date": None, "end_date": None}
        ],
    )

    class FakeProvider:
        provider_name = "covers"

        def discover_team_pages(self, *, index_url):
            return [
                loader.DiscoveredTeamPage(
                    team_name="LA Lakers",
                    team_main_page_url="https://example.com/lal",
                    team_slug="la-lakers",
                    source_url=index_url,
                )
            ]

        def resolve_team_main_page_url(self, *, team_page):
            return team_page.team_main_page_url

    targets = loader.build_initial_dataset_load_targets(
        object(),
        provider=FakeProvider(),
        base_url="https://example.com/index",
    )

    assert len(targets) == 1
    assert targets[0].team_code == "LAL"
    assert targets[0].team_main_page_url == "https://example.com/lal"


def test_run_initial_production_dataset_load_fetches_each_team_page_once(monkeypatch) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "build_initial_dataset_load_targets",
        lambda connection, *, provider, base_url, team_codes, season_labels: [
            loader.DatasetLoadTarget(
                team_code="LAL",
                team_name="Los Angeles Lakers",
                team_slug="los-angeles-lakers",
                team_main_page_url="https://example.com/lal",
                season_labels=("2021-2022", "2022-2023"),
            )
        ],
    )

    fetch_urls: list[str] = []
    ingestion_calls: list[tuple[str, str, str]] = []

    class FakeProvider:
        provider_name = "covers"

        def fetch_team_main_page(self, *, url, requested_season_labels, browser_fallback):
            fetch_urls.append(url)
            assert tuple(requested_season_labels) == ("2021-2022", "2022-2023")
            assert browser_fallback is False
            return TeamPageFetchResult(
                fetched_page=type(
                    "FetchedPage",
                    (),
                    {
                        "content": "<html></html>",
                        "status": "SUCCESS",
                        "http_status": 200,
                    },
                )(),
                diagnostics=("season_block_selector_match:page-fallback",),
            )

    class FakeRepository:
        def __init__(self, connection) -> None:
            self.connection = connection

        def create_job_run(self, *, job_name, requested_by, payload):
            return 0

        def create_page_retrieval(self, *, job_id, record):
            return 0

        def complete_job_run(self, *, job_id, summary, status):
            return None

    monkeypatch.setattr(loader, "CoversHistoricalTeamPageProvider", FakeProvider)
    monkeypatch.setattr(loader, "PostgresIngestionRepository", FakeRepository)
    monkeypatch.setattr(loader, "store_raw_payload", lambda **kwargs: "payload.html")

    def fake_ingest_historical_team_page(*, request, provider, repository):
        ingestion_calls.append((request.team_code, request.season_label, request.source_url))
        return type(
            "PersistedIngestionRun",
            (),
            {
                "job_id": len(ingestion_calls),
                "page_retrieval_id": len(ingestion_calls),
                "raw_rows_saved": 3,
                "canonical_games_saved": 3,
                "metrics_saved": 3,
                "parser_snapshot_path": "parser-output.json",
                "diagnostics": ["season_block_selector_match:page-fallback"],
            },
        )()

    monkeypatch.setattr(loader, "ingest_historical_team_page", fake_ingest_historical_team_page)

    result = loader.run_initial_production_dataset_load(
        season_labels=["2021-2022", "2022-2023"],
        continue_on_error=False,
    )

    assert result["status"] == "COMPLETED"
    assert result["team_page_target_count"] == 1
    assert result["target_count"] == 2
    assert result["processed_target_count"] == 2
    assert fetch_urls == ["https://example.com/lal"]
    assert ingestion_calls == [
        ("LAL", "2021-2022", "https://example.com/lal"),
        ("LAL", "2022-2023", "https://example.com/lal"),
    ]
    assert [entry["season_label"] for entry in result["results"]] == [
        "2021-2022",
        "2022-2023",
    ]
    assert all(
        entry["diagnostics"] == ["season_block_selector_match:page-fallback"]
        for entry in result["results"]
    )
    assert all(entry["parser_snapshot_path"] == "parser-output.json" for entry in result["results"])


def test_run_initial_production_dataset_load_emits_structured_workflow_logs(
    monkeypatch, caplog
) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "build_initial_dataset_load_targets",
        lambda connection, *, provider, base_url, team_codes, season_labels: [
            loader.DatasetLoadTarget(
                team_code="LAL",
                team_name="Los Angeles Lakers",
                team_slug="los-angeles-lakers",
                team_main_page_url="https://example.com/lal",
                season_labels=("2021-2022",),
            )
        ],
    )

    class FakeProvider:
        provider_name = "covers"

        def fetch_team_main_page(self, *, url, requested_season_labels, browser_fallback):
            return TeamPageFetchResult(
                fetched_page=type(
                    "FetchedPage",
                    (),
                    {
                        "content": "<html></html>",
                        "status": "SUCCESS",
                        "http_status": 200,
                    },
                )(),
                diagnostics=(),
            )

    class FakeRepository:
        def __init__(self, connection) -> None:
            self.connection = connection

        def create_job_run(self, *, job_name, requested_by, payload):
            return 0

        def create_page_retrieval(self, *, job_id, record):
            return 0

        def complete_job_run(self, *, job_id, summary, status):
            return None

    monkeypatch.setattr(loader, "CoversHistoricalTeamPageProvider", FakeProvider)
    monkeypatch.setattr(loader, "PostgresIngestionRepository", FakeRepository)
    monkeypatch.setattr(loader, "store_raw_payload", lambda **kwargs: "payload.html")
    monkeypatch.setattr(
        loader,
        "ingest_historical_team_page",
        lambda **kwargs: type(
            "PersistedIngestionRun",
            (),
            {
                "job_id": 1,
                "page_retrieval_id": 1,
                "raw_rows_saved": 3,
                "canonical_games_saved": 3,
                "metrics_saved": 3,
                "parser_snapshot_path": "parser-output.json",
                "diagnostics": [],
            },
        )(),
    )

    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        result = loader.run_initial_production_dataset_load(
            season_labels=["2021-2022"],
            continue_on_error=False,
        )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_succeeded",
    ]
    assert events[0]["workflow_name"] == "ingestion.initial_dataset_load"
    assert events[1]["processed_target_count"] == result["processed_target_count"]
    assert events[1]["completed_target_count"] == result["completed_target_count"]


def test_run_initial_production_dataset_load_uses_browser_fallback_only_when_needed(
    monkeypatch,
) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "build_initial_dataset_load_targets",
        lambda connection, *, provider, base_url, team_codes, season_labels: [
            loader.DatasetLoadTarget(
                team_code="LAL",
                team_name="Los Angeles Lakers",
                team_slug="los-angeles-lakers",
                team_main_page_url="https://example.com/lal",
                season_labels=("2021-2022", "2022-2023"),
            )
        ],
    )

    fetch_calls: list[tuple[str, tuple[str, ...], bool]] = []

    class FakeProvider:
        provider_name = "covers"

        def fetch_team_main_page(self, *, url, requested_season_labels, browser_fallback):
            fetch_calls.append((url, tuple(requested_season_labels), browser_fallback))
            return TeamPageFetchResult(
                fetched_page=type(
                    "FetchedPage",
                    (),
                    {
                        "content": "<html></html>",
                        "status": "SUCCESS",
                        "http_status": 200,
                    },
                )(),
                diagnostics=("browser_fallback_requested", "browser_fallback_used"),
                used_browser_fallback=True,
                missing_season_labels=("2022-2023",),
            )

    class FakeRepository:
        def __init__(self, connection) -> None:
            self.connection = connection

        def create_job_run(self, *, job_name, requested_by, payload):
            return 0

        def create_page_retrieval(self, *, job_id, record):
            return 0

        def complete_job_run(self, *, job_id, summary, status):
            return None

    monkeypatch.setattr(loader, "CoversHistoricalTeamPageProvider", FakeProvider)
    monkeypatch.setattr(loader, "PostgresIngestionRepository", FakeRepository)
    monkeypatch.setattr(loader, "store_raw_payload", lambda **kwargs: "payload.html")

    def fake_ingest_historical_team_page(*, request, provider, repository):
        return type(
            "PersistedIngestionRun",
            (),
            {
                "job_id": 1,
                "page_retrieval_id": 1,
                "raw_rows_saved": 0,
                "canonical_games_saved": 0,
                "metrics_saved": 0,
                "parser_snapshot_path": None,
                "diagnostics": request.diagnostics,
            },
        )()

    monkeypatch.setattr(loader, "ingest_historical_team_page", fake_ingest_historical_team_page)

    result = loader.run_initial_production_dataset_load(
        season_labels=["2021-2022", "2022-2023"],
        browser_fallback=True,
    )

    assert fetch_calls == [("https://example.com/lal", ("2021-2022", "2022-2023"), True)]
    assert result["results"][0]["diagnostics"] == [
        "browser_fallback_requested",
        "browser_fallback_used",
    ]
    assert result["results"][1]["diagnostics"] == [
        "browser_fallback_requested",
        "browser_fallback_used",
        "browser_fallback_season_still_missing",
    ]


def test_run_initial_production_dataset_load_exercises_discovery_and_historical_fragment_flow(
    monkeypatch,
    tmp_path,
) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "_load_team_scope",
        lambda connection, *, provider_name, team_codes: [
            {
                "code": "LAL",
                "name": "Los Angeles Lakers",
                "team_slug": "los-angeles-lakers",
                "provider_team_key": None,
                "provider_team_slug": None,
            }
        ],
    )
    monkeypatch.setattr(
        loader,
        "_load_season_scope",
        lambda connection, *, season_labels: [
            {"label": "2023-2024", "start_date": None, "end_date": None}
        ],
    )

    repository = InMemoryIngestionRepository()
    monkeypatch.setattr(
        loader,
        "CoversHistoricalTeamPageProvider",
        build_fixture_backed_covers_provider(
            schedule_fragment_html=load_covers_fixture(
                "covers_live_schedule_fragment_2023_2024.html"
            )
        ),
    )
    monkeypatch.setattr(loader, "PostgresIngestionRepository", lambda connection: repository)

    original_payload_dir = settings.raw_payload_dir
    original_parser_snapshot_dir = settings.parser_snapshot_dir
    settings.raw_payload_dir = str(tmp_path / "raw-payloads")
    settings.parser_snapshot_dir = str(tmp_path / "parser-output")

    try:
        result = loader.run_initial_production_dataset_load(
            base_url=DEFAULT_INDEX_URL,
            season_labels=["2023-2024"],
            continue_on_error=False,
            persist_payload=True,
            browser_fallback=True,
        )
    finally:
        settings.raw_payload_dir = original_payload_dir
        settings.parser_snapshot_dir = original_parser_snapshot_dir

    assert result["status"] == "COMPLETED"
    assert result["team_page_target_count"] == 1
    assert result["target_count"] == 1
    assert result["processed_target_count"] == 1
    assert result["targets"][0]["team_code"] == "LAL"
    assert result["targets"][0]["team_main_page_url"] == DEFAULT_TEAM_PAGE_URL

    entry = result["results"][0]
    assert entry["status"] == "COMPLETED"
    assert entry["team_code"] == "LAL"
    assert entry["season_label"] == "2023-2024"
    assert entry["source_url"] == DEFAULT_TEAM_PAGE_URL
    assert entry["raw_rows_saved"] == 1
    assert entry["canonical_games_saved"] == 1
    assert entry["metrics_saved"] == 1
    assert entry["diagnostics"] == [
        "season_content_fetch_used",
        "season_block_selector_match:season-fragment",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert entry["payload_storage_path"] is not None
    assert entry["parser_snapshot_path"] is not None

    assert len(repository.raw_rows) == 1
    assert repository.raw_rows[0]["opponent_code"] == "NOP"
    assert repository.raw_rows[0]["source_page_season_label"] == "2023-2024"
    assert repository.job_runs[0]["summary"]["diagnostics"] == [
        "season_content_fetch_used",
        "season_block_selector_match:season-fragment",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert repository.job_runs[0]["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"alias_name": 1},
        "ats_parse_mode": {"full": 1},
        "ou_parse_mode": {"full": 1},
    }
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"VALID": 1}


def test_run_initial_production_dataset_load_reports_historical_season_still_missing(
    monkeypatch,
    tmp_path,
) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "_load_team_scope",
        lambda connection, *, provider_name, team_codes: [
            {
                "code": "LAL",
                "name": "Los Angeles Lakers",
                "team_slug": "los-angeles-lakers",
                "provider_team_key": None,
                "provider_team_slug": None,
            }
        ],
    )
    monkeypatch.setattr(
        loader,
        "_load_season_scope",
        lambda connection, *, season_labels: [
            {"label": "2023-2024", "start_date": None, "end_date": None}
        ],
    )

    repository = InMemoryIngestionRepository()
    monkeypatch.setattr(
        loader,
        "CoversHistoricalTeamPageProvider",
        build_fixture_backed_covers_provider(
            schedule_fragment_html=load_covers_fixture(
                "covers_live_schedule_fragment_empty_2023_2024.html"
            ),
            browser_page_html=load_covers_fixture("covers_live_team_page.html"),
        ),
    )
    monkeypatch.setattr(loader, "PostgresIngestionRepository", lambda connection: repository)

    original_payload_dir = settings.raw_payload_dir
    original_parser_snapshot_dir = settings.parser_snapshot_dir
    settings.raw_payload_dir = str(tmp_path / "raw-payloads")
    settings.parser_snapshot_dir = str(tmp_path / "parser-output")

    try:
        result = loader.run_initial_production_dataset_load(
            base_url=DEFAULT_INDEX_URL,
            season_labels=["2023-2024"],
            continue_on_error=False,
            persist_payload=True,
            browser_fallback=True,
        )
    finally:
        settings.raw_payload_dir = original_payload_dir
        settings.parser_snapshot_dir = original_parser_snapshot_dir

    assert result["status"] == "COMPLETED"
    assert result["processed_target_count"] == 1
    entry = result["results"][0]
    assert entry["status"] == "COMPLETED"
    assert entry["team_code"] == "LAL"
    assert entry["season_label"] == "2023-2024"
    assert entry["raw_rows_saved"] == 0
    assert entry["canonical_games_saved"] == 0
    assert entry["metrics_saved"] == 0
    assert entry["payload_storage_path"] is not None
    assert entry["parser_snapshot_path"] is not None
    assert entry["diagnostics"] == [
        "season_content_fetch_used",
        "browser_fallback_requested",
        "browser_fallback_used",
        "browser_fallback_incomplete",
        "browser_fallback_season_still_missing",
        "season_block_selector_match:season-content",
        "regular_season_rows_missing",
    ]

    assert repository.raw_rows == []
    assert repository.job_runs[0]["summary"]["raw_rows_saved"] == 0
    assert repository.job_runs[0]["summary"]["canonical_games_saved"] == 0
    assert repository.job_runs[0]["summary"]["metrics_saved"] == 0
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 7
    assert repository.job_runs[0]["summary"]["diagnostics"] == [
        "season_content_fetch_used",
        "browser_fallback_requested",
        "browser_fallback_used",
        "browser_fallback_incomplete",
        "browser_fallback_season_still_missing",
        "season_block_selector_match:season-content",
        "regular_season_rows_missing",
    ]
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {}


def test_parse_csv_values_handles_empty_strings() -> None:
    assert loader.parse_csv_values(None) is None
    assert loader.parse_csv_values("") is None
    assert loader.parse_csv_values("LAL, BOS ,, NYK") == ["LAL", "BOS", "NYK"]


def test_run_target_load_rolls_back_aborted_transaction_before_failure_logging(
    monkeypatch,
) -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.rollback_calls = 0

        def rollback(self) -> None:
            self.rollback_calls += 1

    class FakeRepository:
        def __init__(self) -> None:
            self.connection = FakeConnection()

    class FakeProvider:
        provider_name = "covers"

        def fetch_team_main_page(self, *, url, requested_season_labels, browser_fallback):
            return TeamPageFetchResult(
                fetched_page=type(
                    "FetchedPage",
                    (),
                    {
                        "content": "<html></html>",
                        "status": "SUCCESS",
                        "http_status": 200,
                    },
                )(),
                diagnostics=(),
            )

    target = loader.DatasetLoadTarget(
        team_code="BOS",
        team_name="Boston Celtics",
        team_slug="boston-celtics",
        team_main_page_url="https://example.com/bos",
        season_labels=("2024-2025",),
    )
    repository = FakeRepository()
    failure_calls: list[dict[str, object]] = []

    def fake_ingest_historical_team_page(*, request, provider, repository):
        raise RuntimeError("not-null violation")

    def fake_record_initial_load_failure(**kwargs):
        failure_calls.append(kwargs)
        return {"status": "FAILED", "diagnostics": []}

    monkeypatch.setattr(loader, "ingest_historical_team_page", fake_ingest_historical_team_page)
    monkeypatch.setattr(loader, "_record_initial_load_failure", fake_record_initial_load_failure)

    results, stopped_early = loader._run_target_load(
        repository=repository,
        provider=FakeProvider(),
        target=target,
        requested_by="test-suite",
        run_label="test-run",
        continue_on_error=True,
        persist_payload=False,
        browser_fallback=False,
    )

    assert repository.connection.rollback_calls == 1
    assert stopped_early is False
    assert results == [{"status": "FAILED", "diagnostics": []}]
    assert len(failure_calls) == 1
    assert failure_calls[0]["error_message"] == "not-null violation"
