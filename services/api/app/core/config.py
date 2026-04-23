from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[4]
SERVICE_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "Pulse API"
    env: Literal["development", "staging", "production"] = "development"
    database_url: str = "sqlite+aiosqlite:///./pulse.db"
    default_user_email: str = "beta@pulse.local"
    api_base_url: str = "http://localhost:8000"
    web_app_url: str = "http://localhost:3000"

    supabase_url: str = ""
    supabase_anon_key: str = ""
    oauth_state_secret: str = ""
    internal_ingest_secret: str = ""

    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_redirect_uri: str = "http://localhost:8000/v1/reddit/connect/callback"

    apple_maps_team_id: str = ""
    apple_maps_key_id: str = ""
    apple_maps_private_key: str = ""
    apple_maps_maps_id: str = ""
    apple_maps_origin: str = "http://localhost:3000"
    apple_maps_web_token_ttl_seconds: int = 3600

    model_config = SettingsConfigDict(
        env_file=(str(REPO_ROOT / ".env"), str(SERVICE_ROOT / ".env")),
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
