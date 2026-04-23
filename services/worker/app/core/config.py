from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[4]
SERVICE_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    env: Literal["development", "staging", "production"] = "development"
    api_base_url: str = "http://localhost:8000"
    gemini_api_key: str = ""
    gemini_model: str = "google-gla:gemini-2.5-flash"
    inngest_app_id: str = "pulse-worker"
    ticketmaster_api_key: str = ""
    internal_ingest_secret: str = ""

    model_config = SettingsConfigDict(
        env_file=(str(REPO_ROOT / ".env"), str(SERVICE_ROOT / ".env")),
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
