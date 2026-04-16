from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_env: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = "postgresql://bookmaker:bookmaker@localhost:5432/bookmaker_detector"
    raw_payload_dir: str = "artifacts/raw-pages"
    the_odds_api_key: str | None = None
    the_odds_api_base_url: str = "https://api.the-odds-api.com/v4"
    the_odds_api_sport_key: str = "basketball_nba"
    the_odds_api_regions: str = "us"
    the_odds_api_markets: str = "spreads,totals"
    the_odds_api_odds_format: str = "american"
    the_odds_api_bookmakers: str | None = None
    the_odds_api_timeout_seconds: float = 10.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )

    @property
    def raw_payload_path(self) -> Path:
        return Path(self.raw_payload_dir)


settings = Settings()
