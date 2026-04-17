from contextlib import contextmanager
from datetime import date

from bookmaker_detector_api.services import initial_dataset_load as loader


def test_build_initial_dataset_load_targets_expands_template(monkeypatch) -> None:
    monkeypatch.setattr(
        loader,
        "_load_team_scope",
        lambda connection, *, team_codes: [
            {"code": "LAL", "name": "Los Angeles Lakers"},
        ],
    )
    monkeypatch.setattr(
        loader,
        "_load_season_scope",
        lambda connection, *, season_labels: [
            {
                "label": "2023-2024",
                "start_date": date(2023, 10, 24),
                "end_date": date(2024, 6, 17),
            }
        ],
    )

    targets = loader.build_initial_dataset_load_targets(
        object(),
        source_url_template=(
            "https://example.com/{team_slug}"
            "?season={season_label}&start={season_start_year}&end={season_end_year}"
        ),
    )

    assert len(targets) == 1
    assert targets[0].team_code == "LAL"
    assert targets[0].team_slug == "los-angeles-lakers"
    assert (
        targets[0].source_url
        == "https://example.com/los-angeles-lakers?season=2023-2024&start=2023&end=2024"
    )


def test_run_initial_production_dataset_load_stops_on_first_error(monkeypatch) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield object()

    monkeypatch.setattr(loader, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(
        loader,
        "build_initial_dataset_load_targets",
        lambda connection, *, source_url_template, team_codes, season_labels: [
            loader.DatasetLoadTarget(
                team_code="LAL",
                team_name="Los Angeles Lakers",
                team_slug="los-angeles-lakers",
                season_label="2023-2024",
                season_start_date=date(2023, 10, 24),
                season_end_date=date(2024, 6, 17),
                source_url="https://example.com/lal",
            ),
            loader.DatasetLoadTarget(
                team_code="BOS",
                team_name="Boston Celtics",
                team_slug="boston-celtics",
                season_label="2023-2024",
                season_start_date=date(2023, 10, 24),
                season_end_date=date(2024, 6, 17),
                source_url="https://example.com/bos",
            ),
        ],
    )
    monkeypatch.setattr(
        loader,
        "run_fetch_and_ingest",
        lambda **kwargs: {
            "status": "FAILED",
            "job_id": 1,
            "page_retrieval_id": 1,
            "error_message": f"failed:{kwargs['team_code']}",
        },
    )

    result = loader.run_initial_production_dataset_load(
        source_url_template="https://example.com/{team_code_lower}",
        continue_on_error=False,
    )

    assert result["status"] == "FAILED"
    assert result["processed_target_count"] == 1
    assert result["failed_target_count"] == 1
    assert result["stopped_early"] is True
    assert result["results"][0]["team_code"] == "LAL"
    assert result["results"][0]["error_message"] == "failed:LAL"


def test_parse_csv_values_handles_empty_strings() -> None:
    assert loader.parse_csv_values(None) is None
    assert loader.parse_csv_values("") is None
    assert loader.parse_csv_values("LAL, BOS ,, NYK") == ["LAL", "BOS", "NYK"]
