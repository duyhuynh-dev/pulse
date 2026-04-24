from __future__ import annotations

import httpx
import pytest

from app.core.config import Settings
from app.models.user import OAuthConnection
from app.taste.errors import InsufficientSignalError, ProviderUnavailableError
from app.taste.providers.spotify import SpotifyProvider


class FakeSession:
    def __init__(self) -> None:
        self.committed = False

    async def commit(self) -> None:
        self.committed = True


@pytest.mark.asyncio
async def test_spotify_provider_builds_theme_profile_from_top_artists_and_tracks() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"].startswith("Bearer ")
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-1", "display_name": "Duy"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"name": "Warehouse Hero", "genres": ["techno", "electronic"], "popularity": 55},
                        {"name": "Indie Star", "genres": ["indie rock", "dream pop"], "popularity": 60},
                        {"name": "Blue Note Trio", "genres": ["jazz", "neo soul"], "popularity": 49},
                    ]
                },
            )
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "name": "After Hours",
                            "artists": [{"name": "Warehouse Hero"}],
                        },
                        {
                            "name": "Tour Diary",
                            "artists": [{"name": "Indie Star"}],
                        },
                    ]
                },
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "track": {
                                "name": "Late Set",
                                "artists": [{"name": "Warehouse Hero"}],
                            }
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(spotify_timeout_seconds=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(
        provider="spotify",
        access_token_encrypted="spotify-access-token",
        refresh_token_encrypted="spotify-refresh-token",
    )

    profile = await provider.build_profile(FakeSession(), connection)

    assert profile.source == "spotify"
    assert profile.source_key == "spotify-user-1"
    assert profile.username == "Duy"
    assert [theme.id for theme in profile.themes[:3]] == [
        "underground_dance",
        "indie_live_music",
        "jazz_intimate_shows",
    ]
    assert profile.themes[0].evidence.top_examples[0].type == "spotify_artist"
    assert profile.themes[0].evidence.provider_notes
    assert "topGenres" in profile.unmatched_activity


@pytest.mark.asyncio
async def test_spotify_provider_accepts_broader_mainstream_genres() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-4", "display_name": "Jordan"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "name": "Skyline Pop",
                            "genres": ["dance pop", "electropop", "metropopolis"],
                            "popularity": 77,
                        },
                        {
                            "name": "Room Show Kids",
                            "genres": ["modern alternative pop", "indie pop"],
                            "popularity": 69,
                        },
                        {
                            "name": "Night Cipher",
                            "genres": ["pop rap", "hip hop"],
                            "popularity": 72,
                        },
                    ]
                },
            )
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {"name": "Night Drive", "artists": [{"name": "Skyline Pop"}]},
                        {"name": "Live Session", "artists": [{"name": "Room Show Kids"}]},
                    ]
                },
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(
                200,
                json={"items": [{"track": {"name": "Summer Remix", "artists": [{"name": "Skyline Pop"}]}}]},
            )
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(spotify_timeout_seconds=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="token")

    profile = await provider.build_profile(FakeSession(), connection)

    theme_ids = {theme.id for theme in profile.themes}
    assert "rooftop_lounges" in theme_ids
    assert "indie_live_music" in theme_ids
    assert "hiphop_rap_shows" in theme_ids


@pytest.mark.asyncio
async def test_spotify_provider_enriches_sparse_tracks_with_artist_metadata() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-5", "display_name": "Sparse"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "name": "beside you",
                            "artists": [{"id": "artist-keshi", "name": "keshi"}],
                        }
                    ]
                },
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/artists":
            assert request.url.params["ids"] == "artist-keshi"
            return httpx.Response(
                200,
                json={
                    "artists": [
                        {
                            "id": "artist-keshi",
                            "name": "keshi",
                            "genres": ["chill r&b", "bedroom pop"],
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(spotify_timeout_seconds=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="token")

    profile = await provider.build_profile(FakeSession(), connection)

    theme_ids = {theme.id for theme in profile.themes}
    assert "rooftop_lounges" in theme_ids or "indie_live_music" in theme_ids


@pytest.mark.asyncio
async def test_spotify_provider_falls_back_to_app_token_for_artist_metadata() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-6", "display_name": "Fallback"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "name": "beside you",
                            "artists": [{"id": "artist-keshi", "name": "keshi"}],
                        }
                    ]
                },
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/artists":
            auth_header = request.headers["Authorization"]
            if auth_header == "Bearer user-token":
                return httpx.Response(403, json={"error": {"status": 403}})
            assert auth_header == "Bearer app-token"
            return httpx.Response(
                200,
                json={
                    "artists": [
                        {
                            "id": "artist-keshi",
                            "name": "keshi",
                            "genres": ["chill r&b", "bedroom pop"],
                        }
                    ]
                },
            )
        if request.url.path == "/api/token":
            return httpx.Response(200, json={"access_token": "app-token", "token_type": "Bearer"})
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(
            spotify_client_id="client-id",
            spotify_client_secret="client-secret",
            spotify_timeout_seconds=5.0,
        ),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="user-token")

    profile = await provider.build_profile(FakeSession(), connection)

    theme_ids = {theme.id for theme in profile.themes}
    assert "rooftop_lounges" in theme_ids or "indie_live_music" in theme_ids


@pytest.mark.asyncio
async def test_spotify_provider_ignores_artist_enrichment_when_spotify_blocks_it() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-7", "display_name": "Graceful"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "name": "beside you",
                            "artists": [{"id": "artist-keshi", "name": "keshi"}],
                        }
                    ]
                },
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/artists":
            return httpx.Response(403, json={"error": {"status": 403}})
        if request.url.path == "/api/token":
            return httpx.Response(200, json={"access_token": "app-token", "token_type": "Bearer"})
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(
            spotify_client_id="client-id",
            spotify_client_secret="client-secret",
            spotify_timeout_seconds=5.0,
        ),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="user-token")

    with pytest.raises(InsufficientSignalError):
        await provider.build_profile(FakeSession(), connection)


@pytest.mark.asyncio
async def test_spotify_provider_raises_when_no_supported_signal_exists() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-2", "display_name": "Casey"})
        if request.url.path == "/v1/me/top/artists":
            return httpx.Response(
                200,
                json={"items": [{"name": "Podcast Core", "genres": ["talk"], "popularity": 30}]},
            )
        if request.url.path == "/v1/me/top/tracks":
            return httpx.Response(
                200,
                json={"items": [{"name": "No Signal", "artists": [{"id": "artist-talk", "name": "Podcast Core"}]}]},
            )
        if request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(200, json={"items": []})
        if request.url.path == "/v1/artists":
            return httpx.Response(
                200,
                json={"artists": [{"id": "artist-talk", "name": "Podcast Core", "genres": ["talk"]}]},
            )
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(spotify_timeout_seconds=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="token")

    with pytest.raises(InsufficientSignalError):
        await provider.build_profile(FakeSession(), connection)


@pytest.mark.asyncio
async def test_spotify_provider_refreshes_expired_access_token() -> None:
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if request.url.host == "api.spotify.com" and request.url.path == "/v1/me/top/artists":
            if request.headers["Authorization"] == "Bearer expired-token":
                return httpx.Response(401, json={"error": {"status": 401}})
            return httpx.Response(
                200,
                json={"items": [{"name": "Warehouse Hero", "genres": ["techno"], "popularity": 60}]},
            )
        if request.url.host == "api.spotify.com" and request.url.path == "/v1/me/top/tracks":
            return httpx.Response(200, json={"items": []})
        if request.url.host == "api.spotify.com" and request.url.path == "/v1/me/player/recently-played":
            return httpx.Response(200, json={"items": []})
        if request.url.host == "api.spotify.com" and request.url.path == "/v1/me":
            return httpx.Response(200, json={"id": "spotify-user-3", "display_name": "Refresh Case"})
        if request.url.host == "accounts.spotify.com" and request.url.path == "/api/token":
            return httpx.Response(
                200,
                json={"access_token": "fresh-token", "token_type": "Bearer", "scope": "user-top-read"},
            )
        raise AssertionError(f"Unexpected URL {request.url}")

    settings = Settings(
        spotify_client_id="client-id",
        spotify_client_secret="client-secret",
        spotify_timeout_seconds=5.0,
    )
    provider = SpotifyProvider(
        settings=settings,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(
        provider="spotify",
        access_token_encrypted="expired-token",
        refresh_token_encrypted="refresh-token",
    )
    session = FakeSession()

    profile = await provider.build_profile(session, connection)

    assert profile.source_key == "spotify-user-3"
    assert connection.access_token_encrypted == "fresh-token"
    assert session.committed is True
    assert any("/api/token" in call for call in calls)


@pytest.mark.asyncio
async def test_spotify_provider_requires_refresh_when_access_token_is_expired() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.spotify.com":
            return httpx.Response(401, json={"error": {"status": 401}})
        raise AssertionError(f"Unexpected URL {request.url}")

    provider = SpotifyProvider(
        settings=Settings(spotify_timeout_seconds=5.0),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    connection = OAuthConnection(provider="spotify", access_token_encrypted="expired-token")

    with pytest.raises(ProviderUnavailableError):
        await provider.build_profile(FakeSession(), connection)
