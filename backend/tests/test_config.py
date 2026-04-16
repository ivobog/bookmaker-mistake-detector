from bookmaker_detector_api.config import Settings


def test_settings_ignore_empty_odds_api_env_values(monkeypatch):
    monkeypatch.setenv("THE_ODDS_API_TIMEOUT_SECONDS", "")
    monkeypatch.setenv("THE_ODDS_API_BOOKMAKERS", "")

    settings = Settings(_env_file=None)

    assert settings.the_odds_api_timeout_seconds == 10.0
    assert settings.the_odds_api_bookmakers is None
