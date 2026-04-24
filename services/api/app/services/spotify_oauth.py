from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

import httpx

from app.core.config import Settings, get_settings

SPOTIFY_ACCOUNTS_BASE = "https://accounts.spotify.com"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"


def build_spotify_authorize_url(state: str, settings: Settings | None = None) -> str:
    resolved = settings or get_settings()
    if not resolved.spotify_client_id:
        raise ValueError("Spotify client ID is not configured.")

    query = urlencode(
        {
            "client_id": resolved.spotify_client_id,
            "response_type": "code",
            "redirect_uri": resolved.spotify_redirect_uri,
            "scope": resolved.spotify_scopes,
            "state": state,
        }
    )
    return f"{SPOTIFY_ACCOUNTS_BASE}/authorize?{query}"


async def exchange_spotify_code(
    code: str,
    *,
    settings: Settings | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    resolved = settings or get_settings()
    _require_spotify_credentials(resolved)
    return await _token_request(
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": resolved.spotify_redirect_uri,
        },
        settings=resolved,
        client=client,
    )


async def refresh_spotify_access_token(
    refresh_token: str,
    *,
    settings: Settings | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    resolved = settings or get_settings()
    _require_spotify_credentials(resolved)
    return await _token_request(
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        settings=resolved,
        client=client,
    )


async def fetch_spotify_client_credentials_token(
    *,
    settings: Settings | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    resolved = settings or get_settings()
    _require_spotify_credentials(resolved)
    return await _token_request(
        data={"grant_type": "client_credentials"},
        settings=resolved,
        client=client,
    )


async def fetch_spotify_profile(
    access_token: str,
    *,
    settings: Settings | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    resolved = settings or get_settings()
    async with _spotify_client(client, resolved) as active_client:
        response = await active_client.get(
            f"{SPOTIFY_API_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    response.raise_for_status()
    return response.json()


async def _token_request(
    *,
    data: dict[str, str],
    settings: Settings,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    async with _spotify_client(client, settings) as active_client:
        response = await active_client.post(
            f"{SPOTIFY_ACCOUNTS_BASE}/api/token",
            auth=(settings.spotify_client_id, settings.spotify_client_secret),
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    response.raise_for_status()
    return response.json()


def _require_spotify_credentials(settings: Settings) -> None:
    if not settings.spotify_client_id or not settings.spotify_client_secret:
        raise ValueError("Spotify OAuth credentials are not configured.")


def _spotify_client(
    client: httpx.AsyncClient | None,
    settings: Settings,
) -> httpx.AsyncClient | _ManagedAsyncClient:
    if client is not None:
        return _ManagedAsyncClient(client)
    return httpx.AsyncClient(timeout=settings.spotify_timeout_seconds)


class _ManagedAsyncClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def __aenter__(self) -> httpx.AsyncClient:
        return self.client

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None
