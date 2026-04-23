from app.schemas.recommendations import RecommendationReason, TravelEstimate, VenueRecommendationCard
from app.services.digest import _digest_preheader, _digest_subject, _render_digest_html, _render_digest_text
from app.models.user import User


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
    user = User(email="duy@example.com", display_name="Duy")
    items = [_sample_card("Elsewhere", "Bushwick", "After-Hours Techno Showcase")]

    html = _render_digest_html(
        user,
        items,
        "Pulse Weekly: 1 NYC picks for this week",
        "Elsewhere is leading your latest Pulse shortlist.",
    )
    text = _render_digest_text(
        items,
        "Pulse Weekly: 1 NYC picks for this week",
        "Elsewhere is leading your latest Pulse shortlist.",
    )

    assert "Duy, your city picks are ready." in html
    assert "After-Hours Techno Showcase" in html
    assert "Open the live map" in html
    assert "Pulse Weekly: 1 NYC picks for this week" in text
    assert "Travel: 18 min walk, 12 min transit" in text
