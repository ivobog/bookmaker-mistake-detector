from bookmaker_detector_api.config import Settings


def test_settings_ignore_empty_odds_api_env_values(monkeypatch):
    monkeypatch.setenv("THE_ODDS_API_TIMEOUT_SECONDS", "")
    monkeypatch.setenv("THE_ODDS_API_BOOKMAKERS", "")

    settings = Settings(_env_file=None)

    assert settings.the_odds_api_timeout_seconds == 10.0
    assert settings.the_odds_api_bookmakers is None


def test_settings_allow_test_helpers_only_outside_production() -> None:
    assert (
        Settings(
            _env_file=None,
            api_env="development",
            api_enable_test_helpers=True,
        ).allow_test_helpers
        is True
    )
    assert (
        Settings(
            _env_file=None,
            api_env="production",
            api_enable_test_helpers=True,
        ).allow_test_helpers
        is False
    )


def test_settings_default_runtime_schema_mutation_to_disabled() -> None:
    assert Settings(_env_file=None).resolved_postgres_allow_runtime_schema_mutation is False
    assert (
        Settings(
            _env_file=None,
            postgres_allow_runtime_schema_mutation=True,
        ).resolved_postgres_allow_runtime_schema_mutation
        is True
    )
