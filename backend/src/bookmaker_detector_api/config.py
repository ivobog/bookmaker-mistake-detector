from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_env: str = "development"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    database_url: str = "postgresql://bookmaker:bookmaker@localhost:5432/bookmaker_detector"
    raw_payload_dir: str = "artifacts/raw-pages"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore",
    )

    @property
    def raw_payload_path(self) -> Path:
        return Path(self.raw_payload_dir)


settings = Settings()
