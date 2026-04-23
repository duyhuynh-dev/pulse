import httpx
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from app.schemas.recommendations import RecommendationReason, TravelEstimate, VenueRecommendationCard
from app.services.digest import (
    _digest_due_now,
    _digest_preheader,
    _digest_subject,
    _format_event_time,
    _provider_error_detail,
    _render_digest_html,
    _render_digest_text,
)
from app.models.user import EmailPreference, User


def _sample_card(venue_name: str, neighborhood: str, event_title: str) -> VenueRecommendationCard:
    return VenueRecommendationCard(
        venueId=f"{venue_name}-id",
        venueName=venue_name,
        neighborhood=neighborhood,
        address="123 Example St, New York, NY",
        eventTitle=event_title,
        eventId=f"{venue_name}-event",
        startsAt="2026-04-25T23:30:00+00:00",
        priceLabel="$25-$35",
        scoreBand="high",
        score=0.88,
        travel=[
            TravelEstimate(mode="walk", label="18 min walk", minutes=18),
            TravelEstimate(mode="transit", label="12 min transit", minutes=12),
        ],
        reasons=[
            RecommendationReason(title="Profile match", detail="This venue lines up with your indie live music signals."),
            RecommendationReason(title="Travel fit", detail="About 12 min by transit from your current NYC anchor."),
        ],
        secondaryEvents=[],
    )


def test_digest_subject_and_preheader_reflect_shortlist() -> None:
    items = [
        _sample_card("Elsewhere", "Bushwick", "After-Hours Techno Showcase"),
        _sample_card("Mercury Lounge", "Lower East Side", "Intimate Alt-Pop Performance"),
        _sample_card("Le Poisson Rouge", "Greenwich Village", "Late Night Listening Room"),
    ]

    assert _digest_subject(items) == "Pulse Weekly: 3 NYC picks for this week"
    assert _digest_preheader(items) == "Elsewhere, Mercury Lounge, and 1 more picks are leading your latest Pulse shortlist."


def test_digest_renderers_include_key_event_details() -> None:
    user = User(email="duy@example.com", display_name="Duy", timezone="America/New_York")
    items = [_sample_card("Elsewhere", "Bushwick", "After-Hours Techno Showcase")]

    html = _render_digest_html(
        user,
        items,
        "Pulse Weekly: 1 NYC picks for this week",
        "Elsewhere is leading your latest Pulse shortlist.",
        ZoneInfo("America/New_York"),
    )
    text = _render_digest_text(
        items,
        "Pulse Weekly: 1 NYC picks for this week",
        "Elsewhere is leading your latest Pulse shortlist.",
        ZoneInfo("America/New_York"),
    )

    assert "Duy, your city picks are ready." in html
    assert "After-Hours Techno Showcase" in html
    assert "Open the live map" in html
    assert 'table role="presentation"' in html
    assert "display:inline-block;padding:8px 12px" in html
    assert "Pulse Weekly: 1 NYC picks for this week" in text
    assert "Travel: 18 min walk, 12 min transit" in text
    assert "Sat, Apr 25 · 7:30 PM" in html
    assert "Sat, Apr 25 · 7:30 PM" in text


def test_provider_error_detail_prefers_json_message() -> None:
    request = httpx.Request("POST", "https://api.resend.com/emails")
    response = httpx.Response(
        status_code=422,
        request=request,
        json={"message": "The sender domain is not verified."},
    )

    assert _provider_error_detail(response) == "The sender domain is not verified."


def test_digest_due_now_respects_user_day_time_and_timezone() -> None:
    user = User(email="duy@example.com", timezone="America/New_York")
    preference = EmailPreference(
        user_id="user-1",
        weekly_digest_enabled=True,
        digest_day="Tuesday",
        digest_time_local="09:00",
    )

    due_now = datetime(2026, 4, 21, 13, 5, tzinfo=UTC)
    too_late = datetime(2026, 4, 21, 13, 20, tzinfo=UTC)
    wrong_day = datetime(2026, 4, 22, 13, 5, tzinfo=UTC)

    assert _digest_due_now(user, preference, due_now) is True
    assert _digest_due_now(user, preference, too_late) is False
    assert _digest_due_now(user, preference, wrong_day) is False


def test_format_event_time_converts_to_user_timezone() -> None:
    assert _format_event_time("2026-04-28T00:30:00+00:00", ZoneInfo("America/New_York")) == "Mon, Apr 27 · 8:30 PM"
