from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from html import escape
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.recommendation import DigestDelivery, RecommendationRun
from app.models.user import EmailPreference, User
from app.schemas.digest import DigestBatchResponse, DigestPreviewResponse, DigestSendResponse
from app.schemas.recommendations import VenueRecommendationCard
from app.services.recommendations import get_archive, refresh_recommendations_for_user


@dataclass
class DigestPreviewPayload:
    run: RecommendationRun
    response: DigestPreviewResponse


SCHEDULED_DIGEST_PROVIDER = "resend-scheduled"
PREVIEW_DIGEST_PROVIDER = "resend-preview"


async def build_digest_preview(session: AsyncSession, user: User) -> DigestPreviewPayload:
    run = await refresh_recommendations_for_user(session, user)
    archive = await get_archive(session, user)
    items = archive.items[:5]
    if not items:
        raise RuntimeError("No recommendations are available yet, so Pulse cannot build a digest preview.")

    subject = _digest_subject(items)
    preheader = _digest_preheader(items)
    timezone = _user_timezone(user)
    html = _render_digest_html(user, items, subject, preheader, timezone)
    text = _render_digest_text(items, subject, preheader, timezone)

    return DigestPreviewPayload(
        run=run,
        response=DigestPreviewResponse(
            recipientEmail=user.email,
            subject=subject,
            preheader=preheader,
            html=html,
            text=text,
            generatedAt=run.created_at.isoformat(),
            items=items,
        ),
    )


async def send_digest_preview(session: AsyncSession, user: User) -> DigestSendResponse:
    preview = await build_digest_preview(session, user)
    return await _send_digest_email(
        session=session,
        user=user,
        preview=preview,
        provider=PREVIEW_DIGEST_PROVIDER,
    )


async def send_due_weekly_digests(
    session: AsyncSession,
    now_utc: datetime | None = None,
    *,
    dry_run: bool = False,
) -> DigestBatchResponse:
    settings = get_settings()
    if not settings.resend_api_key:
        raise RuntimeError("Resend is not configured. Add RESEND_API_KEY to send scheduled digests.")

    current_time = now_utc or datetime.now(tz=UTC)
    users = list((await session.scalars(select(User).order_by(User.created_at.asc()))).all())

    response = DigestBatchResponse(dryRun=dry_run)

    for user in users:
        preference = await session.scalar(select(EmailPreference).where(EmailPreference.user_id == user.id))
        if preference is None or not preference.weekly_digest_enabled:
            response.skipped += 1
            continue

        if not _digest_due_now(user, preference, current_time):
            response.skipped += 1
            continue

        response.processedUsers += 1

        if await _digest_already_sent_today(session, user, provider=SCHEDULED_DIGEST_PROVIDER, now_utc=current_time):
            response.skipped += 1
            continue

        response.recipients.append(user.email)
        if dry_run:
            response.wouldSend += 1
            continue

        try:
            preview = await build_digest_preview(session, user)
            await _send_digest_email(
                session=session,
                user=user,
                preview=preview,
                provider=SCHEDULED_DIGEST_PROVIDER,
            )
            response.sent += 1
        except RuntimeError:
            response.failed += 1

    return response


async def _send_digest_email(
    session: AsyncSession,
    user: User,
    preview: DigestPreviewPayload,
    *,
    provider: str,
) -> DigestSendResponse:
    settings = get_settings()
    if not settings.resend_api_key:
        raise RuntimeError("Resend is not configured. Add RESEND_API_KEY to send digests.")

    delivery = DigestDelivery(
        user_id=user.id,
        recommendation_run_id=preview.run.id,
        provider=provider,
        status="queued",
    )
    session.add(delivery)
    await session.flush()

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {settings.resend_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": settings.digest_from_email,
                    "to": [user.email],
                    "subject": preview.response.subject,
                    "html": preview.response.html,
                    "text": preview.response.text,
                },
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as error:
        delivery.status = "failed"
        await session.commit()
        provider_detail = _provider_error_detail(error.response)
        raise RuntimeError(
            f"Pulse could not send the digest preview. {provider_detail}"
        ) from error
    except httpx.HTTPError as error:
        delivery.status = "failed"
        await session.commit()
        raise RuntimeError(
            "Pulse could not send the digest preview because Resend was unreachable. Check your network and try again."
        ) from error

    delivery.status = "sent"
    await session.commit()
    return DigestSendResponse(recipientEmail=user.email, provider=provider, status="sent")


def _digest_subject(items: list[VenueRecommendationCard]) -> str:
    return f"Pulse Weekly: {len(items)} NYC picks for this week"


def _digest_preheader(items: list[VenueRecommendationCard]) -> str:
    lead_names = [item.venueName for item in items[:2]]
    if len(items) == 1:
        return f"{lead_names[0]} is leading your latest Pulse shortlist."
    if len(items) == 2:
        return f"{lead_names[0]} and {lead_names[1]} are leading your latest Pulse shortlist."
    return f"{lead_names[0]}, {lead_names[1]}, and {len(items) - 2} more picks are leading your latest Pulse shortlist."


def _render_digest_html(
    user: User,
    items: list[VenueRecommendationCard],
    subject: str,
    preheader: str,
    timezone: ZoneInfo,
) -> str:
    settings = get_settings()
    intro_name = user.display_name or user.email.split("@")[0]
    cards_html = "".join(
        f'<div style="margin-top:{0 if index == 0 else 16}px;">{_render_card_html(item, timezone)}</div>'
        for index, item in enumerate(items)
    )
    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{escape(subject)}</title>
  </head>
  <body style="margin:0;padding:0;background:#f6f0e6;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#14213d;">
    <div style="display:none;max-height:0;overflow:hidden;opacity:0;">{escape(preheader)}</div>
    <div style="max-width:680px;margin:0 auto;padding:32px 20px 48px;">
      <div style="background:#fffdf9;border:1px solid #ddcfbf;border-radius:28px;padding:32px;box-shadow:0 24px 60px rgba(20,33,61,0.08);">
        <div style="display:inline-block;padding:8px 14px;border-radius:999px;background:#d7f0eb;color:#167d73;font-size:11px;font-weight:700;letter-spacing:0.18em;text-transform:uppercase;">Pulse Weekly</div>
        <h1 style="margin:20px 0 10px;font-size:36px;line-height:1.05;">{escape(intro_name)}, your city picks are ready.</h1>
        <p style="margin:0 0 24px;font-size:16px;line-height:1.7;color:#4a6078;">
          Pulse translated your latest signals into a sharper shortlist for this week. Here are the venues currently leading your map.
        </p>
        <div>{cards_html}</div>
        <div style="margin-top:28px;padding-top:22px;border-top:1px solid #e5d9cb;">
          <a href="{escape(settings.web_app_url)}" style="display:inline-block;padding:14px 20px;border-radius:999px;background:#14213d;color:#ffffff;text-decoration:none;font-weight:600;">
            Open the live map
          </a>
          <p style="margin:16px 0 0;font-size:13px;line-height:1.7;color:#6a7c8e;">
            This preview was sent from the current recommendation run so you can check tone, quality, and ordering before the full digest automation goes live.
          </p>
        </div>
      </div>
    </div>
  </body>
</html>"""


def _render_card_html(item: VenueRecommendationCard, timezone: ZoneInfo) -> str:
    reasons = "".join(
        f"<li style=\"margin:0 0 6px;\">"
        f"<span style=\"font-weight:600;color:#14213d;\">{escape(reason.title)}:</span> {escape(reason.detail)}</li>"
        for reason in item.reasons[:3]
    )
    travel = " · ".join(escape(travel_item.label) for travel_item in item.travel)
    return f"""
      <div style="border:1px solid #e5d9cb;border-radius:24px;padding:20px;background:#ffffff;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
          <tr>
            <td valign="top" style="padding:0;">
              <div style="font-size:11px;letter-spacing:0.2em;text-transform:uppercase;color:#6a7c8e;">{escape(item.neighborhood)}</div>
              <h2 style="margin:10px 0 4px;font-size:24px;line-height:1.1;">{escape(item.venueName)}</h2>
              <p style="margin:0;color:#4a6078;font-size:15px;line-height:1.6;">{escape(item.eventTitle)}</p>
            </td>
            <td align="right" valign="top" style="padding:0 0 0 12px;white-space:nowrap;">
              <span style="display:inline-block;padding:8px 12px;border-radius:999px;background:#f4faf8;color:#167d73;font-size:11px;font-weight:700;letter-spacing:0.18em;text-transform:uppercase;line-height:1;white-space:nowrap;mso-line-height-rule:exactly;">
                {escape(item.scoreBand)}
              </span>
            </td>
          </tr>
        </table>
        <p style="margin:16px 0 0;color:#4a6078;font-size:14px;line-height:1.7;">
          {escape(_format_event_time(item.startsAt, timezone))} · {escape(item.priceLabel)} · {escape(item.address)}
        </p>
        <p style="margin:8px 0 0;color:#4a6078;font-size:14px;line-height:1.7;">{travel}</p>
        <ul style="margin:16px 0 0;padding-left:18px;color:#4a6078;font-size:14px;line-height:1.7;">{reasons}</ul>
      </div>
    """


def _render_digest_text(
    items: list[VenueRecommendationCard],
    subject: str,
    preheader: str,
    timezone: ZoneInfo,
) -> str:
    lines = [subject, preheader, ""]
    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"{index}. {item.venueName} — {item.eventTitle}",
                f"   {item.neighborhood} · {_format_event_time(item.startsAt, timezone)} · {item.priceLabel}",
                f"   {item.address}",
            ]
        )
        for reason in item.reasons[:3]:
            lines.append(f"   - {reason.title}: {reason.detail}")
        if item.travel:
            lines.append(f"   - Travel: {', '.join(travel.label for travel in item.travel)}")
        lines.append("")
    lines.append("Open the live map in Pulse to keep exploring this week’s shortlist.")
    return "\n".join(lines).strip()


def _format_event_time(value: str, timezone: ZoneInfo) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return parsed.astimezone(timezone).strftime("%a, %b %-d · %-I:%M %p")


def _user_timezone(user: User) -> ZoneInfo:
    try:
        return ZoneInfo(user.timezone or "America/New_York")
    except ZoneInfoNotFoundError:
        return ZoneInfo("America/New_York")


def _digest_due_now(user: User, preference: EmailPreference, now_utc: datetime) -> bool:
    now_local = now_utc.astimezone(_user_timezone(user))
    if preference.digest_day != now_local.strftime("%A"):
        return False

    try:
        hour_text, minute_text = preference.digest_time_local.split(":", maxsplit=1)
        target_hour = int(hour_text)
        target_minute = int(minute_text)
    except (ValueError, AttributeError):
        target_hour = 9
        target_minute = 0

    target_local = now_local.replace(
        hour=target_hour,
        minute=target_minute,
        second=0,
        microsecond=0,
    )
    window_end = target_local + timedelta(minutes=15)
    return target_local <= now_local < window_end


async def _digest_already_sent_today(
    session: AsyncSession,
    user: User,
    *,
    provider: str,
    now_utc: datetime,
) -> bool:
    timezone = _user_timezone(user)
    now_local = now_utc.astimezone(timezone)
    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(UTC)
    day_end_utc = day_end_local.astimezone(UTC)

    existing = await session.scalar(
        select(DigestDelivery).where(
            and_(
                DigestDelivery.user_id == user.id,
                DigestDelivery.provider == provider,
                DigestDelivery.status == "sent",
                DigestDelivery.created_at >= day_start_utc,
                DigestDelivery.created_at < day_end_utc,
            )
        ).limit(1)
    )
    return existing is not None


def _provider_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        for key in ("message", "error", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    text = response.text.strip()
    if text:
        return text

    return f"Resend returned status {response.status_code}. Check your sender/domain configuration and try again."
