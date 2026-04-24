from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx
import jwt
from fastapi import HTTPException, status
from fastapi.responses import Response
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.user import EmailPreference, User


@dataclass
class ResolvedUser:
    user: User
    is_authenticated: bool
    is_demo: bool
    auth_method: Literal["demo", "supabase", "pulse_session", "email_header"]


async def get_or_create_user(
    session: AsyncSession,
    email: str | None = None,
    display_name: str | None = None,
) -> User:
    settings = get_settings()
    email = email or settings.default_user_email
    user = await session.scalar(select(User).where(User.email == email))
    if user:
        return user

    user = User(email=email, display_name=display_name or "Pulse Beta User")
    session.add(user)
    await session.flush()
    session.add(
        EmailPreference(
            user_id=user.id,
            weekly_digest_enabled=True,
            digest_day="Tuesday",
            digest_time_local="09:00",
        )
    )
    await session.commit()
    await session.refresh(user)
    return user


def pulse_session_secret(settings=None) -> str:
    settings = settings or get_settings()
    secret = settings.pulse_session_secret or settings.oauth_state_secret
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Pulse session secret is not configured.",
        )
    return secret


def extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None

    scheme, _, value = authorization.partition(" ")
    if scheme.lower() != "bearer" or not value:
        return None
    return value


async def fetch_supabase_user(access_token: str) -> dict[str, Any]:
    settings = get_settings()
    if not settings.supabase_url or not settings.supabase_anon_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase server-side auth is not configured.",
        )

    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(
            f"{settings.supabase_url.rstrip('/')}/auth/v1/user",
            headers={
                "apikey": settings.supabase_anon_key,
                "Authorization": f"Bearer {access_token}",
            },
        )

    if response.status_code != status.HTTP_200_OK:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Supabase session is invalid or expired.",
        )

    return response.json()


def build_pulse_session_token(
    user_id: str,
    secret: str,
    *,
    expires_in_seconds: int = 60 * 60 * 24 * 30,
) -> str:
    now = datetime.now(tz=UTC)
    return jwt.encode(
        {
            "sub": user_id,
            "purpose": "pulse-session",
            "iat": int(now.timestamp()),
            "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
        },
        secret,
        algorithm="HS256",
    )


def parse_pulse_session_token(session_token: str, secret: str) -> str:
    try:
        payload = jwt.decode(session_token, secret, algorithms=["HS256"])
    except ExpiredSignatureError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Pulse session has expired.",
        ) from error
    except InvalidTokenError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Pulse session is invalid.",
        ) from error

    if payload.get("purpose") != "pulse-session" or not payload.get("sub"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Pulse session is invalid.",
        )

    return str(payload["sub"])


def set_pulse_session_cookie(response: Response, user_id: str, settings=None) -> None:
    settings = settings or get_settings()
    token = build_pulse_session_token(
        user_id,
        pulse_session_secret(settings),
        expires_in_seconds=settings.pulse_session_ttl_seconds,
    )
    response.set_cookie(
        key=settings.pulse_session_cookie_name,
        value=token,
        max_age=settings.pulse_session_ttl_seconds,
        httponly=True,
        samesite=_pulse_session_cookie_samesite(settings),
        secure=settings.web_app_url.startswith("https://"),
        path="/",
    )


def clear_pulse_session_cookie(response: Response, settings=None) -> None:
    settings = settings or get_settings()
    response.delete_cookie(
        key=settings.pulse_session_cookie_name,
        httponly=True,
        samesite=_pulse_session_cookie_samesite(settings),
        secure=settings.web_app_url.startswith("https://"),
        path="/",
    )


def _pulse_session_cookie_samesite(settings=None) -> Literal["lax", "none"]:
    settings = settings or get_settings()
    if not settings.web_app_url.startswith("https://"):
        return "lax"

    web_origin = _origin_tuple(settings.web_app_url)
    api_origin = _origin_tuple(settings.api_base_url)
    return "none" if web_origin != api_origin else "lax"


def _origin_tuple(url: str) -> tuple[str, str, int | None]:
    parsed = urlsplit(url)
    return parsed.scheme, parsed.hostname or "", parsed.port


async def resolve_user(
    session: AsyncSession,
    authorization: str | None = None,
    x_pulse_user_email: str | None = None,
    pulse_session_token: str | None = None,
) -> ResolvedUser:
    access_token = extract_bearer_token(authorization)
    if access_token:
        auth_user = await fetch_supabase_user(access_token)
        email = auth_user.get("email")
        if not email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Supabase session is missing an email claim.",
            )

        user = await get_or_create_user(
            session,
            email=email,
            display_name=auth_user.get("email") or auth_user.get("phone"),
        )
        return ResolvedUser(user=user, is_authenticated=True, is_demo=False, auth_method="supabase")

    if pulse_session_token:
        try:
            user_id = parse_pulse_session_token(pulse_session_token, pulse_session_secret())
        except HTTPException:
            user_id = None

        if user_id:
            user = await session.scalar(select(User).where(User.id == user_id))
            if user:
                return ResolvedUser(
                    user=user,
                    is_authenticated=True,
                    is_demo=False,
                    auth_method="pulse_session",
                )

    if x_pulse_user_email:
        user = await get_or_create_user(session, email=x_pulse_user_email)
        return ResolvedUser(user=user, is_authenticated=True, is_demo=False, auth_method="email_header")

    user = await get_or_create_user(session)
    return ResolvedUser(user=user, is_authenticated=False, is_demo=True, auth_method="demo")


async def require_authenticated_user(
    session: AsyncSession,
    authorization: str | None = None,
    x_pulse_user_email: str | None = None,
    pulse_session_token: str | None = None,
) -> ResolvedUser:
    resolved = await resolve_user(session, authorization, x_pulse_user_email, pulse_session_token)
    if resolved.is_authenticated:
        return resolved

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required for this action.",
    )


def build_oauth_state(
    email: str | None,
    secret: str,
    *,
    purpose: str = "reddit-connect",
    expires_in_seconds: int = 600,
) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "purpose": purpose,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
    }
    if email:
        payload["sub"] = email
    return jwt.encode(
        payload,
        secret,
        algorithm="HS256",
    )


def parse_oauth_state(
    state_token: str,
    secret: str,
    *,
    purpose: str = "reddit-connect",
    required_sub: bool = True,
) -> str | None:
    try:
        payload = jwt.decode(state_token, secret, algorithms=["HS256"])
    except ExpiredSignatureError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth state has expired.",
        ) from error
    except InvalidTokenError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth state is invalid.",
        ) from error

    if payload.get("purpose") != purpose:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth state is invalid.",
        )

    if required_sub and not payload.get("sub"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OAuth state is invalid.",
        )

    return str(payload["sub"]) if payload.get("sub") else None
