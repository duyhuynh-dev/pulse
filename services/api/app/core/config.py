from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import SplitResult, urlsplit, urlunsplit

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[4]
SERVICE_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    app_name: str = "Pulse API"
    env: Literal["development", "staging", "production"] = "development"
    database_url: str = "sqlite+aiosqlite:///./pulse.db"
    default_user_email: str = "beta@pulse.local"
    api_base_url: str = "http://localhost:8000"
    worker_base_url: str = "http://localhost:8001"
    web_app_url: str = "http://localhost:3000"
    web_allowed_origins: str = ""

    supabase_url: str = ""
    supabase_anon_key: str = ""
    oauth_state_secret: str = ""
    internal_ingest_secret: str = ""
    resend_api_key: str = ""
    digest_from_email: str = "Pulse <onboarding@resend.dev>"

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

    @property
    def allowed_web_origins(self) -> list[str]:
        origins = {_normalize_origin(self.web_app_url)}

        if self.web_allowed_origins:
            origins.update(
                _normalize_origin(origin)
                for origin in self.web_allowed_origins.split(",")
                if origin.strip()
            )

        parsed = urlsplit(self.web_app_url)
        if parsed.hostname in {"localhost", "127.0.0.1"}:
            for host in ("localhost", "127.0.0.1"):
                origins.add(_replace_hostname(parsed, host))

        return sorted(origins)


def _normalize_origin(origin: str) -> str:
    return origin.strip().rstrip("/")


def _replace_hostname(parsed: SplitResult, host: str) -> str:
    if parsed.port:
        netloc = f"{host}:{parsed.port}"
    else:
        netloc = host
    return urlunsplit((parsed.scheme, netloc, "", "", ""))


@lru_cache
def get_settings() -> Settings:
    return Settings()
