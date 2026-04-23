from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.recommendation import DigestDelivery, RecommendationRun
from app.models.user import User
from app.schemas.digest import DigestPreviewResponse, DigestSendResponse
from app.schemas.recommendations import VenueRecommendationCard
from app.services.recommendations import get_archive, refresh_recommendations_for_user


@dataclass
class DigestPreviewPayload:
    run: RecommendationRun
    response: DigestPreviewResponse


async def build_digest_preview(session: AsyncSession, user: User) -> DigestPreviewPayload:
    run = await refresh_recommendations_for_user(session, user)
    archive = await get_archive(session, user)
    items = archive.items[:5]
    if not items:
        raise RuntimeError("No recommendations are available yet, so Pulse cannot build a digest preview.")

    subject = _digest_subject(items)
    preheader = _digest_preheader(items)
    html = _render_digest_html(user, items, subject, preheader)
    text = _render_digest_text(items, subject, preheader)

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
    settings = get_settings()
    if not settings.resend_api_key:
        raise RuntimeError("Resend is not configured. Add RESEND_API_KEY to send digest previews.")

    preview = await build_digest_preview(session, user)
    delivery = DigestDelivery(
        user_id=user.id,
        recommendation_run_id=preview.run.id,
        provider="resend-preview",
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
    except httpx.HTTPError as error:
        delivery.status = "failed"
        await session.commit()
        raise RuntimeError("Pulse could not send the digest preview. Check your Resend configuration and try again.") from error

    delivery.status = "sent"
    await session.commit()
    return DigestSendResponse(recipientEmail=user.email, status="sent")


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
) -> str:
    settings = get_settings()
    intro_name = user.display_name or user.email.split("@")[0]
    cards_html = "".join(_render_card_html(item) for item in items)
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
        <div style="display:grid;gap:16px;">{cards_html}</div>
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


def _render_card_html(item: VenueRecommendationCard) -> str:
    reasons = "".join(
        f"<li style=\"margin:0 0 6px;\">"
        f"<span style=\"font-weight:600;color:#14213d;\">{escape(reason.title)}:</span> {escape(reason.detail)}</li>"
        for reason in item.reasons[:3]
    )
    travel = " · ".join(escape(travel_item.label) for travel_item in item.travel)
    return f"""
      <div style="border:1px solid #e5d9cb;border-radius:24px;padding:20px;background:#ffffff;">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">
          <div>
            <div style="font-size:11px;letter-spacing:0.2em;text-transform:uppercase;color:#6a7c8e;">{escape(item.neighborhood)}</div>
            <h2 style="margin:10px 0 4px;font-size:24px;line-height:1.1;">{escape(item.venueName)}</h2>
            <p style="margin:0;color:#4a6078;font-size:15px;line-height:1.6;">{escape(item.eventTitle)}</p>
          </div>
          <div style="padding:8px 12px;border-radius:999px;background:#f4faf8;color:#167d73;font-size:11px;font-weight:700;letter-spacing:0.18em;text-transform:uppercase;">{escape(item.scoreBand)}</div>
        </div>
        <p style="margin:16px 0 0;color:#4a6078;font-size:14px;line-height:1.7;">
          {escape(_format_event_time(item.startsAt))} · {escape(item.priceLabel)} · {escape(item.address)}
        </p>
        <p style="margin:8px 0 0;color:#4a6078;font-size:14px;line-height:1.7;">{travel}</p>
        <ul style="margin:16px 0 0;padding-left:18px;color:#4a6078;font-size:14px;line-height:1.7;">{reasons}</ul>
      </div>
    """


def _render_digest_text(
    items: list[VenueRecommendationCard],
    subject: str,
    preheader: str,
) -> str:
    lines = [subject, preheader, ""]
    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"{index}. {item.venueName} — {item.eventTitle}",
                f"   {item.neighborhood} · {_format_event_time(item.startsAt)} · {item.priceLabel}",
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


def _format_event_time(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return parsed.strftime("%a, %b %-d · %-I:%M %p")
