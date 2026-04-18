from bookmaker_detector_api.config import Settings


def test_settings_ignore_empty_odds_api_env_values(monkeypatch):
    monkeypatch.setenv("THE_ODDS_API_TIMEOUT_SECONDS", "")
    monkeypatch.setenv("THE_ODDS_API_BOOKMAKERS", "")

    settings = Settings(_env_file=None)

    assert settings.the_odds_api_timeout_seconds == 10.0
    assert settings.the_odds_api_bookmakers is None


def test_settings_resolve_repository_mode_override() -> None:
    assert Settings(_env_file=None, api_env="development").use_postgres_stable_read_mode is False
    assert (
        Settings(_env_file=None, api_env="production").use_postgres_stable_read_mode is True
    )
    assert (
        Settings(
            _env_file=None,
            api_env="development",
            api_repository_mode="postgres",
        ).use_postgres_stable_read_mode
        is True
    )
    assert (
        Settings(
            _env_file=None,
            api_env="production",
            api_repository_mode="in_memory",
        ).use_postgres_stable_read_mode
        is False
    )
