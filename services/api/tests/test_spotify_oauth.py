from urllib.parse import parse_qs, urlparse

import pytest

from app.core.config import Settings
from app.services.spotify_oauth import build_spotify_authorize_url


def test_build_spotify_authorize_url_includes_expected_query() -> None:
    settings = Settings(
        spotify_client_id="spotify-client-id",
        spotify_client_secret="spotify-secret",
        spotify_redirect_uri="http://localhost:8000/v1/spotify/connect/callback",
        spotify_scopes="user-read-email user-top-read user-read-recently-played",
    )

    authorize_url = build_spotify_authorize_url("state-token", settings=settings)
    parsed = urlparse(authorize_url)
    query = parse_qs(parsed.query)

    assert parsed.netloc == "accounts.spotify.com"
    assert parsed.path == "/authorize"
    assert query["client_id"] == ["spotify-client-id"]
    assert query["state"] == ["state-token"]
    assert query["scope"] == ["user-read-email user-top-read user-read-recently-played"]
    assert query["redirect_uri"] == ["http://localhost:8000/v1/spotify/connect/callback"]


def test_build_spotify_authorize_url_requires_client_id() -> None:
    settings = Settings(spotify_client_id="", spotify_client_secret="")

    with pytest.raises(ValueError):
        build_spotify_authorize_url("state-token", settings=settings)
