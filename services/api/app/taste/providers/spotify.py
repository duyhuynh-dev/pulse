from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.models.user import OAuthConnection
from app.services.spotify_oauth import SPOTIFY_API_BASE, fetch_spotify_profile, refresh_spotify_access_token
from app.taste.errors import InsufficientSignalError, ProviderUnavailableError
from app.taste.profile_contracts import (
    TasteProfile,
    TasteTheme,
    ThemeEvidence,
    ThemeEvidenceCount,
    ThemeEvidenceSnippet,
)
from app.taste.theme_catalog import THEME_CATALOG_BY_ID


@dataclass(frozen=True)
class SpotifyThemeRule:
    theme_id: str
    genre_keywords: dict[str, int]
    track_keywords: dict[str, int]
    provider_note: str


SPOTIFY_THEME_RULES: tuple[SpotifyThemeRule, ...] = (
    SpotifyThemeRule(
        theme_id="underground_dance",
        genre_keywords={
            "techno": 3,
            "house": 2,
            "electronic": 1,
            "electronica": 2,
            "dance": 1,
            "edm": 2,
            "deep house": 3,
            "progressive house": 2,
            "melodic house": 3,
            "tech house": 3,
            "minimal techno": 3,
            "uk garage": 3,
            "garage": 2,
            "future garage": 3,
            "drum and bass": 3,
            "dnb": 3,
            "breakbeat": 2,
            "trance": 2,
            "dubstep": 2,
            "future bass": 2,
            "bass": 1,
        },
        track_keywords={"dj": 1, "mix": 1, "club": 1, "warehouse": 2, "after": 1},
        provider_note="Built from your top Spotify artists and recent club-leaning listening.",
    ),
    SpotifyThemeRule(
        theme_id="indie_live_music",
        genre_keywords={
            "indie": 3,
            "alternative": 2,
            "alt": 1,
            "rock": 1,
            "alternative rock": 2,
            "alternative pop": 2,
            "modern alternative pop": 3,
            "modern rock": 2,
            "alt z": 2,
            "indie pop": 3,
            "indie poptimism": 2,
            "pop rock": 2,
            "folk": 1,
            "folk-pop": 2,
            "shoegaze": 2,
            "dream pop": 2,
            "singer-songwriter": 2,
            "indie rock": 3,
            "bedroom pop": 2,
            "chamber pop": 2,
            "stomp and holler": 2,
            "neo mellow": 2,
            "art rock": 2,
        },
        track_keywords={"live": 1, "tour": 1, "band": 1, "session": 1},
        provider_note="Built from the indie and touring-band artists that dominate your Spotify rotation.",
    ),
    SpotifyThemeRule(
        theme_id="gallery_nights",
        genre_keywords={
            "ambient": 2,
            "experimental": 2,
            "art pop": 3,
            "modern classical": 2,
            "neo-classical": 2,
            "avant-garde": 3,
            "trip hop": 2,
            "downtempo": 2,
            "indietronica": 2,
            "chillwave": 2,
            "lo-fi": 1,
            "lo-fi beats": 2,
        },
        track_keywords={"installation": 2, "gallery": 2, "exhibit": 2},
        provider_note="Built from experimental and art-leaning artists in your recent listening.",
    ),
    SpotifyThemeRule(
        theme_id="jazz_intimate_shows",
        genre_keywords={
            "jazz": 3,
            "bebop": 3,
            "neo soul": 2,
            "neo-soul": 2,
            "soul jazz": 3,
            "vocal jazz": 3,
            "instrumental": 1,
            "acoustic": 1,
            "piano": 1,
            "soul": 1,
            "coffeehouse": 1,
        },
        track_keywords={"quartet": 2, "trio": 2, "live at": 1},
        provider_note="Built from jazz and intimate-session signals across your top artists and tracks.",
    ),
    SpotifyThemeRule(
        theme_id="hiphop_rap_shows",
        genre_keywords={
            "hip hop": 3,
            "rap": 3,
            "pop rap": 2,
            "trap": 2,
            "drill": 3,
            "grime": 2,
            "boom bap": 2,
        },
        track_keywords={"freestyle": 2, "remix": 1, "cypher": 2},
        provider_note="Built from the rap and hip-hop artists showing up repeatedly in your Spotify taste.",
    ),
    SpotifyThemeRule(
        theme_id="dive_bar_scene",
        genre_keywords={
            "garage rock": 3,
            "punk": 2,
            "americana": 2,
            "blues rock": 2,
            "indie folk": 2,
            "alt-country": 3,
            "alternative country": 3,
            "post-punk": 3,
            "folk rock": 2,
            "emo": 2,
            "hardcore": 2,
            "folk punk": 3,
        },
        track_keywords={"bar": 1, "whiskey": 1, "honky": 2},
        provider_note="Built from local-scene guitar music that usually maps to room-first nights out.",
    ),
    SpotifyThemeRule(
        theme_id="rooftop_lounges",
        genre_keywords={
            "dance pop": 2,
            "pop": 1,
            "electropop": 2,
            "alt z": 1,
            "latin pop": 2,
            "latin": 1,
            "disco": 2,
            "funk": 2,
            "r&b": 2,
            "rnb": 2,
            "chill r&b": 2,
            "urban contemporary": 2,
            "afrobeats": 2,
            "afropop": 2,
            "afroswing": 2,
            "tropical house": 2,
            "nu-disco": 3,
            "metropopolis": 2,
            "escape room": 2,
        },
        track_keywords={"sunset": 1, "summer": 1, "night drive": 1, "remix": 1},
        provider_note="Built from polished dance-pop, disco, and lounge-adjacent listening patterns.",
    ),
)


class SpotifyProvider:
    source_name = "spotify"

    def __init__(
        self,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._external_client = client

    async def build_profile(
        self,
        session: AsyncSession,
        connection: OAuthConnection,
    ) -> TasteProfile:
        if not connection.access_token_encrypted:
            raise ProviderUnavailableError("Spotify is connected, but no access token is stored.")

        bundle = await self._fetch_bundle(session, connection)
        themes = self._score_themes(bundle)
        if not themes:
            raise InsufficientSignalError(
                "Spotify listening history did not surface enough nightlife signal yet."
            )

        account = bundle["profile"]
        username = account.get("display_name") or account.get("id")
        return TasteProfile(
            source="spotify",
            source_key=account.get("id") or connection.provider_user_id or "spotify-user",
            username=username,
            themes=themes,
            unmatched_activity={"topGenres": _top_unmatched_genres(bundle)},
        )

    async def _fetch_bundle(
        self,
        session: AsyncSession,
        connection: OAuthConnection,
    ) -> dict[str, Any]:
        token = connection.access_token_encrypted
        try:
            return await self._request_bundle(token)
        except _SpotifyUnauthorizedError:
            if not connection.refresh_token_encrypted:
                raise ProviderUnavailableError(
                    "Spotify connection expired. Reconnect Spotify and try again."
                ) from None

            refreshed = await refresh_spotify_access_token(
                connection.refresh_token_encrypted,
                settings=self.settings,
                client=self._external_client,
            )
            token = refreshed.get("access_token")
            if not token:
                raise ProviderUnavailableError(
                    "Spotify refresh succeeded without returning an access token."
                )

            connection.access_token_encrypted = token
            if refreshed.get("refresh_token"):
                connection.refresh_token_encrypted = refreshed["refresh_token"]
            if refreshed.get("scope"):
                connection.scope_csv = refreshed["scope"]
            await session.commit()
            return await self._request_bundle(token)

    async def _request_bundle(self, access_token: str) -> dict[str, Any]:
        async with self._client_context() as client:
            top_artists, top_tracks, recent_tracks, profile = await asyncio.gather(
                self._spotify_get(
                    client,
                    access_token,
                    "/me/top/artists",
                    {"limit": "20", "time_range": "medium_term"},
                ),
                self._spotify_get(
                    client,
                    access_token,
                    "/me/top/tracks",
                    {"limit": "20", "time_range": "medium_term"},
                ),
                self._spotify_get(
                    client,
                    access_token,
                    "/me/player/recently-played",
                    {"limit": "20"},
                ),
                fetch_spotify_profile(
                    access_token,
                    settings=self.settings,
                    client=client,
                ),
            )

        return {
            "profile": profile,
            "top_artists": top_artists.get("items", []),
            "top_tracks": top_tracks.get("items", []),
            "recent_tracks": recent_tracks.get("items", []),
        }

    async def _spotify_get(
        self,
        client: httpx.AsyncClient,
        access_token: str,
        path: str,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        try:
            response = await client.get(
                f"{SPOTIFY_API_BASE}{path}",
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
            )
        except httpx.TimeoutException as error:
            raise ProviderUnavailableError("Spotify timed out while fetching listening data.") from error
        except httpx.HTTPError as error:
            raise ProviderUnavailableError("Unable to reach Spotify right now.") from error

        if response.status_code == 401:
            raise _SpotifyUnauthorizedError()
        if response.status_code == 403:
            raise ProviderUnavailableError("Spotify rejected the request scope for this account.")
        if response.status_code >= 400:
            raise ProviderUnavailableError(
                f"Spotify returned an unexpected status code ({response.status_code})."
            )
        return response.json()

    def _score_themes(self, bundle: dict[str, Any]) -> list[TasteTheme]:
        top_artists = bundle["top_artists"]
        top_tracks = bundle["top_tracks"]
        recent_tracks = bundle["recent_tracks"]

        artist_signal_index: dict[str, set[str]] = defaultdict(set)
        keyword_counts: dict[str, Counter[str]] = defaultdict(Counter)
        points: Counter[str] = Counter()
        artist_examples: dict[str, list[ThemeEvidenceSnippet]] = defaultdict(list)
        track_examples: dict[str, list[ThemeEvidenceSnippet]] = defaultdict(list)

        for index, artist in enumerate(top_artists):
            genres = [genre.lower() for genre in artist.get("genres", [])]
            if not genres:
                continue

            weight = 1.0 if index < 5 else 0.7 if index < 10 else 0.45
            artist_name = artist.get("name") or "Unknown artist"
            flattened_genres = " · ".join(artist.get("genres", [])[:3])

            for rule in SPOTIFY_THEME_RULES:
                matched_keywords = {
                    keyword
                    for genre in genres
                    for keyword in rule.genre_keywords
                    if keyword in genre
                }
                if not matched_keywords:
                    continue

                keyword_score = sum(rule.genre_keywords[keyword] for keyword in matched_keywords)
                points[rule.theme_id] += keyword_score * weight
                artist_signal_index[rule.theme_id].add(artist_name.lower())
                for keyword in matched_keywords:
                    keyword_counts[rule.theme_id][keyword] += 1

                if len(artist_examples[rule.theme_id]) < 2:
                    artist_examples[rule.theme_id].append(
                        ThemeEvidenceSnippet(
                            type="spotify_artist",
                            snippet=f"Top artist: {artist_name} ({flattened_genres})",
                        )
                    )

        for index, track in enumerate(top_tracks):
            self._score_track(
                theme_points=points,
                keyword_counts=keyword_counts,
                track_examples=track_examples,
                artist_signal_index=artist_signal_index,
                track=track,
                weight=0.8 if index < 10 else 0.45,
                recent=False,
            )

        for item in recent_tracks:
            track = item.get("track") or {}
            self._score_track(
                theme_points=points,
                keyword_counts=keyword_counts,
                track_examples=track_examples,
                artist_signal_index=artist_signal_index,
                track=track,
                weight=1.1,
                recent=True,
            )

        themes: list[TasteTheme] = []
        for rule in SPOTIFY_THEME_RULES:
            total_points = points[rule.theme_id]
            if total_points < 1.4:
                continue

            label = THEME_CATALOG_BY_ID[rule.theme_id].label
            examples = artist_examples[rule.theme_id] + track_examples[rule.theme_id]
            if not examples:
                continue

            confidence = min(89, max(24, round(total_points * 4.5)))
            themes.append(
                TasteTheme(
                    id=rule.theme_id,
                    label=label,
                    confidence=confidence,
                    confidence_label=_confidence_label(confidence),
                    evidence=ThemeEvidence(
                        matched_keywords=[
                            ThemeEvidenceCount(key=keyword, count=count)
                            for keyword, count in keyword_counts[rule.theme_id].most_common(4)
                        ],
                        top_examples=examples[:3],
                        provider_notes=[rule.provider_note],
                    ),
                )
            )

        return sorted(themes, key=lambda theme: (-theme.confidence, theme.label.lower()))

    def _score_track(
        self,
        *,
        theme_points: Counter[str],
        keyword_counts: dict[str, Counter[str]],
        track_examples: dict[str, list[ThemeEvidenceSnippet]],
        artist_signal_index: dict[str, set[str]],
        track: dict[str, Any],
        weight: float,
        recent: bool,
    ) -> None:
        track_name = (track.get("name") or "").strip()
        if not track_name:
            return

        artist_names = [artist.get("name", "").strip() for artist in track.get("artists", []) if artist.get("name")]
        track_text = f"{track_name} {' '.join(artist_names)}".lower()

        for rule in SPOTIFY_THEME_RULES:
            matched = False
            if any(artist.lower() in artist_signal_index[rule.theme_id] for artist in artist_names):
                theme_points[rule.theme_id] += 1.6 * weight
                matched = True

            matched_track_keywords = [
                keyword for keyword in rule.track_keywords if keyword in track_text
            ]
            if matched_track_keywords:
                theme_points[rule.theme_id] += sum(rule.track_keywords[keyword] for keyword in matched_track_keywords) * weight
                matched = True
                for keyword in matched_track_keywords:
                    keyword_counts[rule.theme_id][keyword] += 1

            if matched and len(track_examples[rule.theme_id]) < 1:
                descriptor = "Recently played" if recent else "Top track"
                track_examples[rule.theme_id].append(
                    ThemeEvidenceSnippet(
                        type="spotify_track",
                        snippet=f"{descriptor}: {track_name} — {', '.join(artist_names[:2])}",
                    )
                )

    def _client_context(self) -> httpx.AsyncClient | _ManagedAsyncClient:
        if self._external_client is not None:
            return _ManagedAsyncClient(self._external_client)
        return httpx.AsyncClient(timeout=self.settings.spotify_timeout_seconds)


class _ManagedAsyncClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def __aenter__(self) -> httpx.AsyncClient:
        return self.client

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _SpotifyUnauthorizedError(Exception):
    pass


def _confidence_label(confidence: int) -> str:
    if confidence >= 80:
        return "Strong"
    if confidence >= 60:
        return "Clear"
    if confidence >= 40:
        return "Emerging"
    return "Weak"


def _top_unmatched_genres(bundle: dict[str, Any]) -> list[dict[str, int]]:
    matched_keywords = {
        keyword
        for rule in SPOTIFY_THEME_RULES
        for keyword in rule.genre_keywords
    }
    unmatched: Counter[str] = Counter()
    for artist in bundle["top_artists"]:
        for genre in artist.get("genres", []):
            lowered = genre.lower()
            if any(keyword in lowered for keyword in matched_keywords):
                continue
            unmatched[genre] += 1

    return [{"genre": genre, "count": count} for genre, count in unmatched.most_common(6)]
