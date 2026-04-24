from datetime import datetime
from typing import Annotated
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.session import get_db
from app.models.recommendation import FeedbackEvent
from app.models.user import OAuthConnection, UserAnchorLocation, UserConstraint
from app.schemas.auth import AuthViewerResponse, RedditConnectStartResponse, SpotifyConnectStartResponse
from app.schemas.common import OkResponse, SupplySyncResponse
from app.schemas.digest import DigestBatchResponse, DigestPreviewResponse, DigestSendResponse
from app.schemas.ingestion import CandidateIngestPayload, CandidateIngestResponse
from app.schemas.maps import MapTokenResponse
from app.schemas.profile import (
    AnchorPayload,
    EmailPreferencePayload,
    EmailPreferenceResponse,
    InterestListResponse,
    InterestListUpdate,
    UserConstraintPayload,
)
from app.schemas.recommendations import ArchiveResponse, FeedbackPayload, RecommendationsMapResponse
from app.schemas.taste import (
    ManualTastePayload,
    TasteProfileResponse,
    ThemeCatalogItemResponse,
    ThemeCatalogResponse,
)
from app.services.apple_maps import build_mapkit_token
from app.services.auth import (
    build_oauth_state,
    clear_pulse_session_cookie,
    get_or_create_user,
    parse_oauth_state,
    require_authenticated_user,
    resolve_user,
    set_pulse_session_cookie,
)
from app.services.digest import build_digest_preview, send_digest_preview, send_due_weekly_digests
from app.services.ingestion import upsert_ingested_candidates
from app.services.profile import get_email_preferences, list_interests, update_email_preferences, update_interests
from app.services.recommendations import get_archive, get_map_recommendations, refresh_recommendations_for_user
from app.services.reddit_oauth import build_reddit_authorize_url
from app.services.seed import bootstrap_user_with_mock_reddit
from app.services.spotify_oauth import build_spotify_authorize_url, exchange_spotify_code, fetch_spotify_profile
from app.services.worker_sync import trigger_worker_supply_sync
from app.taste.errors import InsufficientSignalError, TasteProviderError
from app.taste.profile_contracts import TasteProfile
from app.taste.profile_service import apply_taste_profile
from app.taste.providers.manual import ManualThemeProvider
from app.taste.providers.spotify import SpotifyProvider

router = APIRouter(prefix="/v1")


def _resolve_web_app_url(settings, request: Request) -> str:
    configured = urlsplit(settings.web_app_url)
    request_host = request.url.hostname

    if request_host in {"127.0.0.1", "localhost"} and configured.hostname in {"127.0.0.1", "localhost"}:
        port = configured.port or 3000
        return urlunsplit((configured.scheme or request.url.scheme, f"{request_host}:{port}", "", "", ""))

    return settings.web_app_url


async def current_identity(
    request: Request,
    session: AsyncSession = Depends(get_db),
    authorization: Annotated[str | None, Header()] = None,
    x_pulse_user_email: Annotated[str | None, Header()] = None,
):
    settings = get_settings()
    pulse_session_token = request.cookies.get(settings.pulse_session_cookie_name)
    return await resolve_user(session, authorization, x_pulse_user_email, pulse_session_token)


async def authenticated_identity(
    request: Request,
    session: AsyncSession = Depends(get_db),
    authorization: Annotated[str | None, Header()] = None,
    x_pulse_user_email: Annotated[str | None, Header()] = None,
):
    settings = get_settings()
    pulse_session_token = request.cookies.get(settings.pulse_session_cookie_name)
    return await require_authenticated_user(session, authorization, x_pulse_user_email, pulse_session_token)


async def current_user(identity=Depends(current_identity)):
    return identity.user


async def verify_internal_ingest(
    x_pulse_ingest_secret: Annotated[str | None, Header()] = None,
) -> None:
    settings = get_settings()
    if settings.internal_ingest_secret and x_pulse_ingest_secret != settings.internal_ingest_secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid ingest secret.")


@router.get("/auth/me", response_model=AuthViewerResponse)
async def auth_me(
    session: AsyncSession = Depends(get_db),
    identity=Depends(current_identity),
) -> AuthViewerResponse:
    live_connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == identity.user.id,
            OAuthConnection.provider == "reddit",
        )
    )
    sample_connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == identity.user.id,
            OAuthConnection.provider == "reddit_mock",
        )
    )
    spotify_connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == identity.user.id,
            OAuthConnection.provider == "spotify",
        )
    )
    connection_mode = "live" if live_connection else "sample" if sample_connection else "none"
    return AuthViewerResponse(
        userId=identity.user.id,
        email=identity.user.email,
        displayName=identity.user.display_name,
        isAuthenticated=identity.is_authenticated,
        isDemo=identity.is_demo,
        authMethod=identity.auth_method,
        redditConnected=connection_mode != "none",
        redditConnectionMode=connection_mode,
        spotifyConnected=spotify_connection is not None,
    )


@router.post("/auth/sign-out", response_model=OkResponse)
async def auth_sign_out() -> JSONResponse:
    response = JSONResponse(OkResponse().model_dump())
    clear_pulse_session_cookie(response)
    return response


@router.post("/reddit/connect/start", response_model=RedditConnectStartResponse)
async def reddit_connect_start(
    identity=Depends(authenticated_identity),
) -> RedditConnectStartResponse:
    settings = get_settings()
    if not settings.oauth_state_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OAuth state secret is not configured.",
        )

    state = build_oauth_state(identity.user.email, settings.oauth_state_secret, purpose="reddit-connect")
    try:
        authorize_url = build_reddit_authorize_url(state)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
    return RedditConnectStartResponse(authorizeUrl=authorize_url)


@router.get("/reddit/connect/callback")
async def reddit_connect_callback(
    request: Request,
    code: str | None = None,
    error: str | None = None,
    state: str | None = None,
    session: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    if error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error)
    if not code:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing Reddit authorization code.")
    if not state:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing OAuth state.")
    if not settings.oauth_state_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OAuth state secret is not configured.",
        )

    email = parse_oauth_state(state, settings.oauth_state_secret, purpose="reddit-connect")
    user = await get_or_create_user(session, email=email)

    token_payload = {
        "access_token": code,
        "refresh_token": code,
        "scope": "identity history mysubreddits",
        "name": "reddit-demo-user",
    }

    if settings.reddit_client_id and settings.reddit_client_secret:
        async with httpx.AsyncClient(timeout=20.0) as client:
            token_response = await client.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(settings.reddit_client_id, settings.reddit_client_secret),
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.reddit_redirect_uri,
                },
                headers={"User-Agent": "PulseMVP/0.1"},
            )
            token_response.raise_for_status()
            token_payload = token_response.json()

            me_response = await client.get(
                "https://oauth.reddit.com/api/v1/me",
                headers={
                    "Authorization": f"bearer {token_payload['access_token']}",
                    "User-Agent": "PulseMVP/0.1",
                },
            )
            me_response.raise_for_status()
            token_payload["name"] = me_response.json().get("name", "reddit-user")

    connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == user.id,
            OAuthConnection.provider == "reddit",
        )
    )
    if connection is None:
        connection = OAuthConnection(user_id=user.id, provider="reddit")
        session.add(connection)

    connection.provider_user_id = token_payload.get("name")
    connection.access_token_encrypted = token_payload.get("access_token")
    connection.refresh_token_encrypted = token_payload.get("refresh_token")
    connection.scope_csv = token_payload.get("scope")
    await session.commit()
    web_app_url = _resolve_web_app_url(settings, request)
    return RedirectResponse(f"{web_app_url}/?reddit=connected", status_code=status.HTTP_302_FOUND)


@router.post("/spotify/connect/start", response_model=SpotifyConnectStartResponse)
async def spotify_connect_start(
    identity=Depends(current_identity),
) -> SpotifyConnectStartResponse:
    settings = get_settings()
    if not settings.oauth_state_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OAuth state secret is not configured.",
        )

    state = build_oauth_state(
        identity.user.email if identity.is_authenticated else None,
        settings.oauth_state_secret,
        purpose="spotify-connect",
    )
    try:
        authorize_url = build_spotify_authorize_url(state)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
    return SpotifyConnectStartResponse(authorizeUrl=authorize_url)


@router.get("/spotify/connect/callback")
async def spotify_connect_callback(
    request: Request,
    code: str | None = None,
    error: str | None = None,
    state: str | None = None,
    session: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    if error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error)
    if not code:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing Spotify authorization code.")
    if not state:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing OAuth state.")
    if not settings.oauth_state_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OAuth state secret is not configured.",
        )

    state_email = parse_oauth_state(
        state,
        settings.oauth_state_secret,
        purpose="spotify-connect",
        required_sub=False,
    )

    try:
        token_payload = await exchange_spotify_code(code)
        me_payload = await fetch_spotify_profile(token_payload["access_token"])
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
    except httpx.HTTPError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Spotify OAuth exchange failed.",
        ) from error

    resolved_email = state_email or me_payload.get("email")
    if not resolved_email:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Spotify did not return an email address for Pulse sign-in.",
        )

    user = await get_or_create_user(
        session,
        email=resolved_email,
        display_name=me_payload.get("display_name") or resolved_email,
    )

    connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == user.id,
            OAuthConnection.provider == "spotify",
        )
    )
    if connection is None:
        connection = OAuthConnection(user_id=user.id, provider="spotify")
        session.add(connection)

    connection.provider_user_id = me_payload.get("id")
    connection.access_token_encrypted = token_payload.get("access_token")
    connection.refresh_token_encrypted = token_payload.get("refresh_token") or connection.refresh_token_encrypted
    connection.scope_csv = token_payload.get("scope")
    await session.commit()
    web_app_url = _resolve_web_app_url(settings, request)
    response = RedirectResponse(f"{web_app_url}/?spotify=connected", status_code=status.HTTP_302_FOUND)
    set_pulse_session_cookie(response, user.id)
    return response


@router.post("/reddit/mock-connect", response_model=OkResponse)
async def reddit_mock_connect(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> OkResponse:
    await bootstrap_user_with_mock_reddit(session, identity.user, create_connection=True)
    return OkResponse()


@router.post("/internal/ingest/candidates", response_model=CandidateIngestResponse)
async def internal_ingest_candidates(
    payload: CandidateIngestPayload,
    _: None = Depends(verify_internal_ingest),
    session: AsyncSession = Depends(get_db),
) -> CandidateIngestResponse:
    return await upsert_ingested_candidates(session, payload)


@router.get("/profile/interests", response_model=InterestListResponse)
async def profile_interests(
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> InterestListResponse:
    return InterestListResponse(topics=await list_interests(session, user))


@router.patch("/profile/interests", response_model=InterestListResponse)
async def profile_interests_update(
    payload: InterestListUpdate,
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> InterestListResponse:
    topics = await update_interests(session, user, payload.topics)
    return InterestListResponse(topics=topics)


@router.get("/profile/email-preferences", response_model=EmailPreferenceResponse)
async def profile_email_preferences(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> EmailPreferenceResponse:
    return await get_email_preferences(session, identity.user)


@router.patch("/profile/email-preferences", response_model=EmailPreferenceResponse)
async def profile_email_preferences_update(
    payload: EmailPreferencePayload,
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> EmailPreferenceResponse:
    return await update_email_preferences(session, identity.user, payload)


@router.post("/profile/anchor", response_model=OkResponse)
async def profile_anchor(
    payload: AnchorPayload,
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> OkResponse:
    session.add(
        UserAnchorLocation(
            user_id=user.id,
            source=payload.source,
            neighborhood=payload.neighborhood,
            zip_code=payload.zipCode,
            latitude=payload.latitude,
            longitude=payload.longitude,
            is_session_only=payload.source == "live",
        )
    )
    await session.flush()
    await refresh_recommendations_for_user(session, user, force=True)
    return OkResponse()


@router.post("/profile/constraints", response_model=OkResponse)
async def profile_constraints(
    payload: UserConstraintPayload,
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> OkResponse:
    constraint = await session.scalar(select(UserConstraint).where(UserConstraint.user_id == user.id))
    if constraint is None:
        constraint = UserConstraint(user_id=user.id)
        session.add(constraint)

    constraint.city = payload.city
    constraint.neighborhood = payload.neighborhood
    constraint.zip_code = payload.zipCode
    constraint.radius_miles = payload.radiusMiles
    constraint.budget_level = payload.budgetLevel
    constraint.preferred_days_csv = ",".join(payload.preferredDays)
    constraint.social_mode = payload.socialMode
    await session.flush()
    await refresh_recommendations_for_user(session, user, force=True)
    return OkResponse()


@router.get("/recommendations/map", response_model=RecommendationsMapResponse)
async def recommendations_map(
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> RecommendationsMapResponse:
    return await get_map_recommendations(session, user)


@router.post("/recommendations/refresh", response_model=OkResponse)
async def recommendations_refresh(
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> OkResponse:
    await refresh_recommendations_for_user(session, user, force=True)
    return OkResponse()


@router.post("/supply/sync", response_model=SupplySyncResponse)
async def supply_sync(
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> SupplySyncResponse:
    try:
        payload = await trigger_worker_supply_sync()
    except RuntimeError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(error),
        ) from error
    await refresh_recommendations_for_user(session, user, force=True)
    return payload


@router.get("/recommendations/archive", response_model=ArchiveResponse)
async def recommendations_archive(
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> ArchiveResponse:
    return await get_archive(session, user)


@router.get("/digest/preview", response_model=DigestPreviewResponse)
async def digest_preview(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> DigestPreviewResponse:
    try:
        preview = await build_digest_preview(session, identity.user)
    except RuntimeError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
    return preview.response


@router.post("/digest/send-preview", response_model=DigestSendResponse)
async def digest_send_preview(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> DigestSendResponse:
    try:
        return await send_digest_preview(session, identity.user)
    except RuntimeError as error:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        if "No recommendations are available yet" in str(error):
            status_code = status.HTTP_409_CONFLICT
        raise HTTPException(status_code=status_code, detail=str(error)) from error


@router.post("/internal/digests/send-weekly", response_model=DigestBatchResponse)
async def digest_send_weekly_internal(
    dry_run: bool = False,
    now_override: datetime | None = None,
    _: None = Depends(verify_internal_ingest),
    session: AsyncSession = Depends(get_db),
) -> DigestBatchResponse:
    try:
        return await send_due_weekly_digests(session, now_utc=now_override, dry_run=dry_run)
    except RuntimeError as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error


@router.post("/recommendations/{recommendation_id}/feedback", response_model=OkResponse)
async def recommendation_feedback(
    recommendation_id: str,
    payload: FeedbackPayload,
    session: AsyncSession = Depends(get_db),
    user=Depends(current_user),
) -> OkResponse:
    session.add(
        FeedbackEvent(
            user_id=user.id,
            recommendation_id=recommendation_id,
            action=payload.action,
            reasons_json=[reason.model_dump() for reason in payload.reasons],
        )
    )
    await session.flush()
    await refresh_recommendations_for_user(session, user, force=True)
    return OkResponse()


@router.get("/maps/token", response_model=MapTokenResponse)
async def maps_token() -> MapTokenResponse:
    try:
        return MapTokenResponse(enabled=True, token=build_mapkit_token())
    except ValueError:
        return MapTokenResponse(enabled=False, token=None)


def _serialize_taste_profile(profile: TasteProfile) -> TasteProfileResponse:
    return TasteProfileResponse(
        source=profile.source,
        sourceKey=profile.source_key,
        username=profile.username,
        generatedAt=profile.generated_at.isoformat(),
        themes=[
            {
                "id": theme.id,
                "label": theme.label,
                "confidence": theme.confidence,
                "confidenceLabel": theme.confidence_label,
                "evidence": {
                    "matchedSubreddits": [item.model_dump() for item in theme.evidence.matched_subreddits],
                    "matchedKeywords": [item.model_dump() for item in theme.evidence.matched_keywords],
                    "topExamples": [item.model_dump() for item in theme.evidence.top_examples],
                    "providerNotes": theme.evidence.provider_notes,
                },
            }
            for theme in profile.themes
        ],
        unmatchedActivity=profile.unmatched_activity,
    )


def _raise_taste_provider_http_error(error: TasteProviderError) -> None:
    status_code = status.HTTP_400_BAD_REQUEST
    if error.code in {"insufficient_signal", "no_public_activity"}:
        status_code = status.HTTP_409_CONFLICT
    if error.retryable or error.code == "provider_unavailable":
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    raise HTTPException(status_code=status_code, detail=error.message) from error


async def _require_oauth_connection(
    session: AsyncSession,
    user_id: str,
    provider_name: str,
) -> OAuthConnection:
    connection = await session.scalar(
        select(OAuthConnection).where(
            OAuthConnection.user_id == user_id,
            OAuthConnection.provider == provider_name,
        )
    )
    if connection is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Connect {provider_name.title()} before previewing this provider.",
        )
    return connection


@router.get("/taste/themes", response_model=ThemeCatalogResponse)
async def taste_themes() -> ThemeCatalogResponse:
    provider = ManualThemeProvider()
    return ThemeCatalogResponse(
        items=[ThemeCatalogItemResponse(**item.model_dump()) for item in provider.available_themes()]
    )


@router.post("/taste/manual/preview", response_model=TasteProfileResponse)
async def taste_manual_preview(payload: ManualTastePayload) -> TasteProfileResponse:
    provider = ManualThemeProvider()
    try:
        profile = await provider.build_profile(payload.selectedThemeIds)
    except TasteProviderError as error:
        _raise_taste_provider_http_error(error)
    return _serialize_taste_profile(profile)


@router.post("/taste/manual/apply", response_model=TasteProfileResponse)
async def taste_manual_apply(
    payload: ManualTastePayload,
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> TasteProfileResponse:
    provider = ManualThemeProvider()
    try:
        profile = await provider.build_profile(payload.selectedThemeIds)
        applied = await apply_taste_profile(session, identity.user, profile)
    except TasteProviderError as error:
        _raise_taste_provider_http_error(error)
    return _serialize_taste_profile(applied)


@router.get("/taste/spotify/preview", response_model=TasteProfileResponse)
async def taste_spotify_preview(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> TasteProfileResponse:
    connection = await _require_oauth_connection(session, identity.user.id, "spotify")
    provider = SpotifyProvider()
    try:
        profile = await provider.build_profile(session, connection)
    except InsufficientSignalError as error:
        empty_profile = TasteProfile(
            source="spotify",
            source_key=connection.provider_user_id or connection.id,
            username=identity.user.display_name or identity.user.email,
            themes=[],
            unmatched_activity={
                "reason": error.message,
                "providerUserId": connection.provider_user_id,
            },
        )
        return _serialize_taste_profile(empty_profile)
    except TasteProviderError as error:
        _raise_taste_provider_http_error(error)
    return _serialize_taste_profile(profile)


@router.post("/taste/spotify/apply", response_model=TasteProfileResponse)
async def taste_spotify_apply(
    session: AsyncSession = Depends(get_db),
    identity=Depends(authenticated_identity),
) -> TasteProfileResponse:
    connection = await _require_oauth_connection(session, identity.user.id, "spotify")
    provider = SpotifyProvider()
    try:
        profile = await provider.build_profile(session, connection)
        applied = await apply_taste_profile(session, identity.user, profile)
    except TasteProviderError as error:
        _raise_taste_provider_http_error(error)
    return _serialize_taste_profile(applied)
