from app.models.events import CanonicalEvent
from app.models.events import EventOccurrence
from app.models.profile import UserInterestProfile
from app.models.events import Venue
from app.services.recommendations import (
    FeedbackSignals,
    _archive_kind,
    _archive_title,
    _candidate_score,
    _deletable_run_ids,
    _derive_topic_keys,
    _feedback_adjustment,
    _occurrence_is_rankable,
    _parse_occurrence_start,
    _score_band,
)
from datetime import UTC, datetime, timedelta


def test_muted_topic_scores_lower_than_matching_active_topic() -> None:
    profiles_by_key = {
        "underground_dance": UserInterestProfile(
            user_id="user-1",
            topic_key="underground_dance",
            label="Underground dance",
            confidence=0.94,
            boosted=False,
            muted=True,
        ),
        "gallery_nights": UserInterestProfile(
            user_id="user-1",
            topic_key="gallery_nights",
            label="Gallery nights",
            confidence=0.72,
            boosted=False,
            muted=False,
        ),
        "indie_live_music": UserInterestProfile(
            user_id="user-1",
            topic_key="indie_live_music",
            label="Indie live music",
            confidence=0.88,
            boosted=False,
            muted=False,
        ),
    }

    underground_score, _, underground_muted = _candidate_score(
        ["underground_dance"],
        profiles_by_key,
        source_confidence=0.93,
        transit_minutes=28,
        budget_fit=0.92,
    )
    mixed_score, _, mixed_muted = _candidate_score(
        ["underground_dance", "gallery_nights"],
        profiles_by_key,
        source_confidence=0.88,
        transit_minutes=30,
        budget_fit=0.72,
    )
    indie_score, indie_matched, _ = _candidate_score(
        ["indie_live_music"],
        profiles_by_key,
        source_confidence=0.83,
        transit_minutes=22,
        budget_fit=0.92,
    )

    assert underground_score < mixed_score < indie_score
    assert [topic.label for topic in underground_muted] == ["Underground dance"]
    assert [topic.label for topic in mixed_muted] == ["Underground dance"]
    assert [topic.label for topic in indie_matched] == ["Indie live music"]


def test_score_band_thresholds_match_ranking_copy() -> None:
    assert _score_band(0.84) == "high"
    assert _score_band(0.64) == "medium"
    assert _score_band(0.41) == "low"


def test_topic_keys_can_be_derived_from_event_text() -> None:
    event = CanonicalEvent(
        source_id="source-1",
        source_event_key="event-1",
        title="Warehouse techno installation night",
        category="culture",
        summary="A visual-heavy art opening with late-night DJs.",
    )

    topic_keys = _derive_topic_keys(event, ["brooklyn", "gallery opening"])
    assert "underground_dance" in topic_keys
    assert "gallery_nights" in topic_keys


def test_topic_keys_cover_broader_cultural_themes() -> None:
    event = CanonicalEvent(
        source_id="source-1",
        source_event_key="event-2",
        title="Brooklyn vintage market and design pop-up",
        category="shopping",
        summary="A boutique-heavy collector fair with menswear racks and local design labels.",
    )

    topic_keys = _derive_topic_keys(event, ["popup market", "vintage", "collector"])
    assert "collector_marketplaces" in topic_keys
    assert "style_design_shopping" in topic_keys


def test_feedback_adjustment_boosts_saved_venue_and_topics() -> None:
    profiles_by_key = {
        "indie_live_music": UserInterestProfile(
            user_id="user-1",
            topic_key="indie_live_music",
            label="Indie live music",
            confidence=0.88,
            boosted=False,
            muted=False,
        )
    }
    venue = Venue(
        name="Mercury Lounge",
        neighborhood="Lower East Side",
        address="217 E Houston St, New York, NY",
        city="New York City",
        state="NY",
        latitude=40.7222,
        longitude=-73.9864,
    )
    venue.id = "venue-1"
    signals = FeedbackSignals(
        saved_venues={"venue-1": 1.0},
        saved_topics={"indie_live_music": 0.8},
    )

    adjustment, feedback_reason = _feedback_adjustment(
        ["indie_live_music"],
        profiles_by_key,
        venue,
        signals,
    )

    assert adjustment > 0
    assert feedback_reason is not None
    assert feedback_reason["title"] == "Saved before"


def test_candidate_score_strong_match_beats_generic_event() -> None:
    profiles_by_key = {
        "collector_marketplaces": UserInterestProfile(
            user_id="user-1",
            topic_key="collector_marketplaces",
            label="Collector marketplaces",
            confidence=0.95,
            boosted=False,
            muted=False,
        ),
        "style_design_shopping": UserInterestProfile(
            user_id="user-1",
            topic_key="style_design_shopping",
            label="Style / design shopping",
            confidence=0.82,
            boosted=False,
            muted=False,
        ),
    }

    matched_score, matched_topics, _ = _candidate_score(
        ["collector_marketplaces", "style_design_shopping"],
        profiles_by_key,
        source_confidence=0.76,
        transit_minutes=30,
        budget_fit=0.9,
    )
    generic_score, generic_topics, _ = _candidate_score(
        [],
        profiles_by_key,
        source_confidence=0.92,
        transit_minutes=20,
        budget_fit=0.92,
    )

    assert matched_score > generic_score
    assert [topic.label for topic in matched_topics] == ["Collector marketplaces", "Style / design shopping"]
    assert generic_topics == []


def test_feedback_adjustment_penalizes_dismissed_patterns() -> None:
    profiles_by_key = {
        "underground_dance": UserInterestProfile(
            user_id="user-1",
            topic_key="underground_dance",
            label="Underground dance",
            confidence=0.94,
            boosted=False,
            muted=False,
        )
    }
    venue = Venue(
        name="Elsewhere",
        neighborhood="Bushwick",
        address="599 Johnson Ave, Brooklyn, NY",
        city="New York City",
        state="NY",
        latitude=40.7063,
        longitude=-73.9232,
    )
    venue.id = "venue-2"
    signals = FeedbackSignals(
        dismissed_topics={"underground_dance": 1.1},
        dismissed_neighborhoods={"bushwick": 0.9},
    )

    adjustment, feedback_reason = _feedback_adjustment(
        ["underground_dance"],
        profiles_by_key,
        venue,
        signals,
    )

    assert adjustment < 0
    assert feedback_reason is not None
    assert feedback_reason["title"] == "Dismiss pattern"


def test_deletable_run_ids_preserve_digest_backed_history() -> None:
    run_ids = ["run-live", "run-preview", "run-scheduled"]
    protected_run_ids = {"run-preview", "run-scheduled"}

    assert _deletable_run_ids(run_ids, protected_run_ids) == ["run-live"]


def test_archive_kind_and_title_match_delivery_provider() -> None:
    assert _archive_kind(None) == "live"
    assert _archive_title("live") == "Current shortlist"
    assert _archive_kind("resend-preview") == "preview"
    assert _archive_title("preview") == "Preview send"
    assert _archive_kind("resend-scheduled") == "scheduled"
    assert _archive_title("scheduled") == "Weekly digest"


def test_parse_occurrence_start_handles_iso_datetimes() -> None:
    parsed = _parse_occurrence_start("2026-04-28T00:30:00+00:00")

    assert parsed == datetime(2026, 4, 28, 0, 30, tzinfo=UTC)


def test_occurrence_is_rankable_filters_past_and_far_future_events() -> None:
    now = datetime(2026, 4, 23, 16, 0, tzinfo=UTC)

    current_occurrence = EventOccurrence(
        event_id="event-1",
        venue_id="venue-1",
        starts_at="2026-04-23T20:00:00+00:00",
    )
    stale_occurrence = EventOccurrence(
        event_id="event-2",
        venue_id="venue-1",
        starts_at=(now - timedelta(hours=3)).isoformat(),
    )
    far_future_occurrence = EventOccurrence(
        event_id="event-3",
        venue_id="venue-1",
        starts_at=(now + timedelta(days=75)).isoformat(),
    )

    assert _occurrence_is_rankable(current_occurrence, now=now) is True
    assert _occurrence_is_rankable(stale_occurrence, now=now) is False
    assert _occurrence_is_rankable(far_future_occurrence, now=now) is False
