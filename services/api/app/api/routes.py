from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.session import get_db
from app.models.recommendation import FeedbackEvent
from app.models.user import OAuthConnection, UserAnchorLocation, UserConstraint
from app.schemas.auth import AuthViewerResponse, RedditConnectStartResponse
from app.schemas.common import OkResponse, SupplySyncResponse
from app.schemas.ingestion import CandidateIngestPayload, CandidateIngestResponse
from app.schemas.maps import MapTokenResponse
from app.schemas.profile import AnchorPayload, InterestListResponse, InterestListUpdate, UserConstraintPayload
from app.schemas.recommendations import ArchiveResponse, FeedbackPayload, RecommendationsMapResponse
from app.services.apple_maps import build_mapkit_token
from app.services.auth import (
    get_or_create_user,
    build_oauth_state,
    parse_oauth_state,
    require_authenticated_user,
    resolve_user,
)
from app.services.ingestion import upsert_ingested_candidates
from app.services.profile import list_interests, update_interests
from app.services.recommendations import get_archive, get_map_recommendations, refresh_recommendations_for_user
from app.services.reddit_oauth import build_reddit_authorize_url
from app.services.seed import bootstrap_user_with_mock_reddit
from app.services.worker_sync import trigger_worker_supply_sync

router = APIRouter(prefix="/v1")


async def current_identity(
    session: AsyncSession = Depends(get_db),
    authorization: Annotated[str | None, Header()] = None,
    x_pulse_user_email: Annotated[str | None, Header()] = None,
):
    return await resolve_user(session, authorization, x_pulse_user_email)


async def authenticated_identity(
    session: AsyncSession = Depends(get_db),
    authorization: Annotated[str | None, Header()] = None,
    x_pulse_user_email: Annotated[str | None, Header()] = None,
):
    return await require_authenticated_user(session, authorization, x_pulse_user_email)


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
    connection_mode = "live" if live_connection else "sample" if sample_connection else "none"
    return AuthViewerResponse(
        email=identity.user.email,
        displayName=identity.user.display_name,
        isAuthenticated=identity.is_authenticated,
        isDemo=identity.is_demo,
        redditConnected=connection_mode != "none",
        redditConnectionMode=connection_mode,
    )


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

    state = build_oauth_state(identity.user.email, settings.oauth_state_secret)
    try:
        authorize_url = build_reddit_authorize_url(state)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error
    return RedditConnectStartResponse(authorizeUrl=authorize_url)


@router.get("/reddit/connect/callback")
async def reddit_connect_callback(
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

    email = parse_oauth_state(state, settings.oauth_state_secret)
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
    return RedirectResponse(f"{settings.web_app_url}/?reddit=connected", status_code=status.HTTP_302_FOUND)


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
