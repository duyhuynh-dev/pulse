import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.events import CanonicalEvent, EventOccurrence, EventSource, Venue
from app.models.profile import UserInterestProfile
from app.models.recommendation import DigestDelivery, FeedbackEvent, RecommendationRun, VenueRecommendation
from app.models.user import User, UserAnchorLocation, UserConstraint
from app.schemas.recommendations import (
    ArchiveResponse,
    ArchiveSnapshot,
    RecommendationDebugSummary,
    RecommendationDebugVenue,
    RecommendationDriverSummary,
    MapVenuePin,
    MapContext,
    RecommendationsMapResponse,
    RecommendationFreshness,
    RecommendationProvenance,
    RecommendationReason,
    RecommendationScoreBreakdownItem,
    TravelEstimate,
    VenueRecommendationCard,
)
from app.services.travel import estimate_travel_bands

DEFAULT_VIEWPORT = {
    "latitude": 40.73061,
    "longitude": -73.935242,
    "latitudeDelta": 0.22,
    "longitudeDelta": 0.22,
}
SERVICE_AREA_NAME = "New York City"
NYC_SERVICE_AREA = {
    "min_latitude": 40.45,
    "max_latitude": 40.95,
    "min_longitude": -74.35,
    "max_longitude": -73.65,
}
RECOMMENDATION_MAX_AGE = timedelta(minutes=30)
FEEDBACK_LOOKBACK_WINDOW = timedelta(days=28)
OCCURRENCE_LOOKBACK_WINDOW = timedelta(hours=2)
OCCURRENCE_LOOKAHEAD_WINDOW = timedelta(days=60)
TOPIC_KEYWORD_MAP = {
    "underground_dance": ["techno", "warehouse", "club", "dance", "rave", "dj"],
    "indie_live_music": ["indie", "band", "concert", "live music", "show", "songwriter", "alt-pop"],
    "gallery_nights": ["gallery", "art", "opening", "installation", "visual"],
    "creative_meetups": ["meetup", "creative", "community", "networking"],
    "collector_marketplaces": ["market", "popup", "fair", "vintage", "swap", "collector"],
    "student_intellectual_scene": ["reading", "book", "lecture", "talk", "screening", "community"],
    "ambitious_professional_scene": ["networking", "panel", "speaker", "founder", "industry", "cocktail"],
    "style_design_shopping": ["design", "fashion", "vintage", "market", "popup", "boutique"],
}
TOPIC_CATEGORY_HINTS = {
    "underground_dance": ["club", "dance", "dj", "electronic", "live music", "nightlife"],
    "indie_live_music": ["concert", "live music", "performance", "show"],
    "gallery_nights": ["art", "culture", "exhibition", "gallery", "screening"],
    "creative_meetups": ["community", "conversation", "meetup", "networking", "talk", "workshop"],
    "collector_marketplaces": ["bazaar", "fair", "market", "popup", "shopping", "swap", "vintage"],
    "student_intellectual_scene": ["book", "campus", "discussion", "lecture", "reading", "screening", "seminar", "talk"],
    "ambitious_professional_scene": ["career", "founder", "industry", "networking", "panel", "professional", "speaker", "talk"],
    "style_design_shopping": ["boutique", "design", "fashion", "market", "popup", "shopping", "thrift", "vintage"],
}
BROAD_CULTURAL_THEME_KEYS = {
    "collector_marketplaces",
    "student_intellectual_scene",
    "ambitious_professional_scene",
    "style_design_shopping",
    "creative_meetups",
}
REASON_META_KEY = "_pulseMeta"
REASON_META_SCORE_SUMMARY = "score_summary"
REASON_META_SCORE_BREAKDOWN = "score_breakdown"


@dataclass
class FeedbackSignals:
    saved_venues: dict[str, float] = field(default_factory=dict)
    dismissed_venues: dict[str, float] = field(default_factory=dict)
    saved_topics: dict[str, float] = field(default_factory=dict)
    dismissed_topics: dict[str, float] = field(default_factory=dict)
    saved_neighborhoods: dict[str, float] = field(default_factory=dict)
    dismissed_neighborhoods: dict[str, float] = field(default_factory=dict)


@dataclass
class AnchorResolution:
    requested_anchor: UserAnchorLocation | None
    active_anchor: UserAnchorLocation | None
    requested_within_service_area: bool = True
    used_fallback_anchor: bool = False
    fallback_reason: str | None = None


@dataclass
class CandidateScoreComponents:
    interest_fit: float
    category_fit: float
    distance_fit: float
    budget_fit: float
    source_confidence: float
    transit_minutes: int
    weighted_interest: float
    weighted_category: float
    weighted_distance: float
    weighted_budget: float
    weighted_source: float


async def _latest_run(session: AsyncSession, user_id: str) -> RecommendationRun | None:
    return await session.scalar(
        select(RecommendationRun)
        .where(RecommendationRun.user_id == user_id)
        .order_by(desc(RecommendationRun.created_at))
        .limit(1)
    )


async def _user_anchor(session: AsyncSession, user_id: str) -> UserAnchorLocation | None:
    anchors = list(
        (
            await session.scalars(
                select(UserAnchorLocation)
                .where(UserAnchorLocation.user_id == user_id)
                .order_by(desc(UserAnchorLocation.created_at))
            )
        ).all()
    )
    return _resolve_anchor(anchors).active_anchor


async def _user_anchor_resolution(session: AsyncSession, user_id: str) -> AnchorResolution:
    anchors = list(
        (
            await session.scalars(
                select(UserAnchorLocation)
                .where(UserAnchorLocation.user_id == user_id)
                .order_by(desc(UserAnchorLocation.created_at))
            )
        ).all()
    )
    return _resolve_anchor(anchors)


async def _user_constraints(session: AsyncSession, user_id: str) -> UserConstraint | None:
    return await session.scalar(select(UserConstraint).where(UserConstraint.user_id == user_id).limit(1))


async def _feedback_signals(session: AsyncSession, user_id: str) -> FeedbackSignals:
    since = datetime.now(tz=UTC) - FEEDBACK_LOOKBACK_WINDOW
    feedback_rows = list(
        (
            await session.scalars(
                select(FeedbackEvent)
                .where(FeedbackEvent.user_id == user_id, FeedbackEvent.created_at >= since)
                .order_by(desc(FeedbackEvent.created_at))
            )
        ).all()
    )
    if not feedback_rows:
        return FeedbackSignals()

    occurrence_ids = {row.recommendation_id for row in feedback_rows}
    occurrences = list(
        (
            await session.scalars(
                select(EventOccurrence).where(EventOccurrence.id.in_(occurrence_ids))
            )
        ).all()
    )
    occurrences_by_id = {occurrence.id: occurrence for occurrence in occurrences}

    venue_ids = {occurrence.venue_id for occurrence in occurrences}
    event_ids = {occurrence.event_id for occurrence in occurrences}
    venues = (
        list((await session.scalars(select(Venue).where(Venue.id.in_(venue_ids)))).all())
        if venue_ids
        else []
    )
    events = (
        list((await session.scalars(select(CanonicalEvent).where(CanonicalEvent.id.in_(event_ids)))).all())
        if event_ids
        else []
    )
    venues_by_id = {venue.id: venue for venue in venues}
    events_by_id = {event.id: event for event in events}

    signals = FeedbackSignals()

    for row in feedback_rows:
        occurrence = occurrences_by_id.get(row.recommendation_id)
        if occurrence is None:
            continue

        weight = _feedback_recency_weight(row.created_at)
        venue = venues_by_id.get(occurrence.venue_id)
        event = events_by_id.get(occurrence.event_id)
        metadata = occurrence.metadata_json or {}
        topic_keys = metadata.get("topicKeys") or (
            _derive_topic_keys(event, metadata.get("tags", [])) if event is not None else []
        )

        venue_store = signals.saved_venues if row.action == "save" else signals.dismissed_venues
        topic_store = signals.saved_topics if row.action == "save" else signals.dismissed_topics
        neighborhood_store = signals.saved_neighborhoods if row.action == "save" else signals.dismissed_neighborhoods

        if venue is not None:
            _add_feedback_weight(venue_store, venue.id, weight)
            _add_feedback_weight(neighborhood_store, venue.neighborhood, weight)

        for topic_key in topic_keys:
            _add_feedback_weight(topic_store, topic_key, weight)

    return signals


def _timestamp_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _within_nyc_service_area(latitude: float, longitude: float) -> bool:
    return (
        NYC_SERVICE_AREA["min_latitude"] <= latitude <= NYC_SERVICE_AREA["max_latitude"]
        and NYC_SERVICE_AREA["min_longitude"] <= longitude <= NYC_SERVICE_AREA["max_longitude"]
    )


def _select_active_anchor(anchors: list[UserAnchorLocation]) -> UserAnchorLocation | None:
    return _resolve_anchor(anchors).active_anchor


def _resolve_anchor(anchors: list[UserAnchorLocation]) -> AnchorResolution:
    if not anchors:
        return AnchorResolution(requested_anchor=None, active_anchor=None)

    requested_anchor = anchors[0]
    requested_within_service_area = True
    if requested_anchor.latitude is not None and requested_anchor.longitude is not None:
        requested_within_service_area = _within_nyc_service_area(
            requested_anchor.latitude,
            requested_anchor.longitude,
        )

    for anchor in anchors:
        if anchor.latitude is not None and anchor.longitude is not None:
            if _within_nyc_service_area(anchor.latitude, anchor.longitude):
                return AnchorResolution(
                    requested_anchor=requested_anchor,
                    active_anchor=anchor,
                    requested_within_service_area=requested_within_service_area,
                    used_fallback_anchor=anchor is not requested_anchor,
                    fallback_reason=_fallback_reason(requested_anchor, anchor)
                    if anchor is not requested_anchor
                    else None,
                )
            continue

        if anchor.zip_code or anchor.neighborhood:
            return AnchorResolution(
                requested_anchor=requested_anchor,
                active_anchor=anchor,
                requested_within_service_area=requested_within_service_area,
                used_fallback_anchor=anchor is not requested_anchor,
                fallback_reason=_fallback_reason(requested_anchor, anchor)
                if anchor is not requested_anchor
                else None,
            )

    fallback_reason = None
    if (
        requested_anchor.latitude is not None
        and requested_anchor.longitude is not None
        and not requested_within_service_area
    ):
        fallback_reason = (
            f"Pulse is currently scoped to {SERVICE_AREA_NAME}, so live locations outside the city are not used yet."
        )

    return AnchorResolution(
        requested_anchor=requested_anchor,
        active_anchor=requested_anchor,
        requested_within_service_area=requested_within_service_area,
        used_fallback_anchor=False,
        fallback_reason=fallback_reason,
    )


def _fallback_reason(
    requested_anchor: UserAnchorLocation | None,
    active_anchor: UserAnchorLocation | None,
) -> str | None:
    if requested_anchor is None or active_anchor is None:
        return None

    if (
        requested_anchor.source == "live"
        and requested_anchor.latitude is not None
        and requested_anchor.longitude is not None
        and not _within_nyc_service_area(requested_anchor.latitude, requested_anchor.longitude)
    ):
        return (
            f"Pulse is currently scoped to {SERVICE_AREA_NAME}, so the map stayed anchored to "
            f"{_anchor_label(active_anchor)}."
        )

    return None


def _anchor_label(anchor: UserAnchorLocation | None) -> str:
    if anchor is None:
        return "NYC"
    if anchor.neighborhood:
        return anchor.neighborhood
    if anchor.zip_code:
        return anchor.zip_code
    if anchor.source == "live":
        return "Current location"
    return "NYC"


def _build_map_context(resolution: AnchorResolution) -> MapContext:
    active_anchor = resolution.active_anchor
    requested_anchor = resolution.requested_anchor
    return MapContext(
        serviceArea=SERVICE_AREA_NAME,
        activeAnchorLabel=_anchor_label(active_anchor),
        activeAnchorSource=active_anchor.source if active_anchor else "default",
        requestedAnchorLabel=_anchor_label(requested_anchor) if requested_anchor else None,
        requestedAnchorSource=requested_anchor.source if requested_anchor else None,
        requestedAnchorWithinServiceArea=resolution.requested_within_service_area,
        usedFallbackAnchor=resolution.used_fallback_anchor,
        fallbackReason=resolution.fallback_reason,
    )


def _anchor_coordinates(anchor: UserAnchorLocation | None) -> tuple[float, float]:
    if anchor and anchor.latitude is not None and anchor.longitude is not None:
        return anchor.latitude, anchor.longitude

    zip_to_coordinate = {
        "10003": (40.7315, -73.9897),
        "11211": (40.7176, -73.9533),
        "10014": (40.7347, -74.0060),
    }
    if anchor and anchor.zip_code and anchor.zip_code in zip_to_coordinate:
        return zip_to_coordinate[anchor.zip_code]

    return (DEFAULT_VIEWPORT["latitude"], DEFAULT_VIEWPORT["longitude"])


def _viewport_for_anchor(anchor: UserAnchorLocation | None) -> dict[str, float]:
    latitude, longitude = _anchor_coordinates(anchor)
    return {
        "latitude": latitude,
        "longitude": longitude,
        "latitudeDelta": DEFAULT_VIEWPORT["latitudeDelta"],
        "longitudeDelta": DEFAULT_VIEWPORT["longitudeDelta"],
    }


def _clamp_score(value: float) -> float:
    return max(0.05, min(0.99, round(value, 3)))


def _feedback_recency_weight(created_at: datetime) -> float:
    age = datetime.now(tz=UTC) - _timestamp_utc(created_at)
    if age <= timedelta(days=7):
        return 1.0
    if age <= timedelta(days=14):
        return 0.7
    return 0.45


def _add_feedback_weight(store: dict[str, float], key: str | None, weight: float) -> None:
    normalized_key = _normalize_text(key)
    if not normalized_key:
        return
    store[normalized_key] = store.get(normalized_key, 0.0) + weight


def _average_feedback_weight(keys: list[str], store: dict[str, float]) -> float:
    normalized_keys = [_normalize_text(key) for key in keys if _normalize_text(key)]
    if not normalized_keys:
        return 0.0
    weights = [store.get(key, 0.0) for key in normalized_keys]
    return sum(weights) / len(weights)


def _topic_weight(topic: UserInterestProfile) -> float:
    if topic.muted:
        return 0.05
    base = 0.24 + (topic.confidence * 0.64)
    if topic.boosted:
        base += 0.12
    return min(0.99, base)


def _interest_fit(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
) -> tuple[float, list[UserInterestProfile], list[UserInterestProfile]]:
    if not topic_keys:
        return (0.34, [], [])

    matched_topics: list[UserInterestProfile] = []
    muted_topics: list[UserInterestProfile] = []
    weights: list[float] = []

    for key in topic_keys:
        topic = profiles_by_key.get(key)
        if topic is None:
            continue

        if topic.muted:
            muted_topics.append(topic)
        else:
            matched_topics.append(topic)
        weights.append(_topic_weight(topic))

    if not weights:
        return (0.34, [], [])

    average_weight = sum(weights) / len(weights)
    strongest_weight = max(weights)
    diversity_bonus = min(0.10, max(0, len(matched_topics) - 1) * 0.04)
    score = (average_weight * 0.62) + (strongest_weight * 0.38) + diversity_bonus
    if muted_topics and not matched_topics:
        score *= 0.35
    elif muted_topics:
        score -= 0.12 * len(muted_topics)

    return (_clamp_score(score), matched_topics, muted_topics)


def _category_affinity(
    category: str,
    tags: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
) -> float:
    blob = " ".join(
        filter(
            None,
            [
                _normalize_text(category),
                " ".join(_normalize_text(tag) for tag in tags),
            ],
        )
    )
    if not blob:
        return 0.0

    matching_weights: list[float] = []
    for topic_key, topic in profiles_by_key.items():
        if topic.muted:
            continue
        hints = TOPIC_CATEGORY_HINTS.get(topic_key, [])
        if any(hint in blob for hint in hints):
            matching_weights.append(_topic_weight(topic))

    if not matching_weights:
        return 0.0

    strongest = max(matching_weights)
    breadth_bonus = min(0.05, max(0, len(matching_weights) - 1) * 0.025)
    return min(0.18, (strongest * 0.16) + breadth_bonus)


def _transit_minutes(travel: list[dict]) -> int:
    for band in travel:
        if band["mode"] == "transit":
            return band["minutes"]
    return 45


def _distance_fit(transit_minutes: int) -> float:
    if transit_minutes <= 25:
        return 0.95
    if transit_minutes <= 40:
        return 0.82
    if transit_minutes <= 55:
        return 0.68
    return 0.52


def _budget_fit(constraints: UserConstraint | None, occurrence: EventOccurrence) -> float:
    max_price = occurrence.max_price if occurrence.max_price is not None else occurrence.min_price
    budget_level = constraints.budget_level if constraints and constraints.budget_level else "under_75"

    if max_price is None:
        return 0.78
    if budget_level == "flexible":
        return 0.9
    if budget_level == "free":
        return 1.0 if max_price <= 0 else 0.25

    budget_threshold = 30 if budget_level == "under_30" else 75
    if max_price <= budget_threshold:
        return 0.92
    if max_price <= budget_threshold + 15:
        return 0.72
    return 0.45


def _score_band(score: float) -> str:
    if score >= 0.78:
        return "high"
    if score >= 0.58:
        return "medium"
    return "low"


def _join_labels(labels: list[str]) -> str:
    if not labels:
        return ""
    if len(labels) == 1:
        return labels[0]
    if len(labels) == 2:
        return f"{labels[0]} and {labels[1]}"
    return f"{', '.join(labels[:-1])}, and {labels[-1]}"


def _feedback_topic_labels(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
) -> list[str]:
    labels: list[str] = []
    for key in topic_keys:
        topic = profiles_by_key.get(key)
        if topic is None or topic.label in labels:
            continue
        labels.append(topic.label)
    return labels


def _feedback_adjustment(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
    venue: Venue,
    feedback_signals: FeedbackSignals,
) -> tuple[float, dict | None]:
    adjustment = 0.0

    saved_venue_weight = feedback_signals.saved_venues.get(_normalize_text(venue.id), 0.0)
    dismissed_venue_weight = feedback_signals.dismissed_venues.get(_normalize_text(venue.id), 0.0)
    saved_topic_weight = _average_feedback_weight(topic_keys, feedback_signals.saved_topics)
    dismissed_topic_weight = _average_feedback_weight(topic_keys, feedback_signals.dismissed_topics)
    neighborhood_key = _normalize_text(venue.neighborhood)
    neighborhood_delta = (
        feedback_signals.saved_neighborhoods.get(neighborhood_key, 0.0)
        - feedback_signals.dismissed_neighborhoods.get(neighborhood_key, 0.0)
    )

    if saved_venue_weight:
        adjustment += min(0.18, 0.10 + (saved_venue_weight * 0.05))
    if dismissed_venue_weight:
        adjustment -= min(0.34, 0.18 + (dismissed_venue_weight * 0.08))

    topic_delta = saved_topic_weight - dismissed_topic_weight
    adjustment += max(-0.12, min(0.12, topic_delta * 0.10))
    adjustment += max(-0.06, min(0.06, neighborhood_delta * 0.04))

    feedback_reason: dict | None = None
    topic_labels = _feedback_topic_labels(topic_keys, profiles_by_key)

    if dismissed_venue_weight >= 0.45:
        feedback_reason = {
            "title": "Dismissed before",
            "detail": f"You recently dismissed {venue.name}, so Pulse now holds it back.",
        }
    elif saved_venue_weight >= 0.45:
        feedback_reason = {
            "title": "Saved before",
            "detail": f"You have saved {venue.name} before, so similar runs get a lift.",
        }
    elif topic_delta <= -0.35 and topic_labels:
        feedback_reason = {
            "title": "Dismiss pattern",
            "detail": f"You often dismiss {_join_labels(topic_labels)} picks, so this is ranked more cautiously.",
        }
    elif topic_delta >= 0.35 and topic_labels:
        feedback_reason = {
            "title": "Save pattern",
            "detail": f"You tend to save {_join_labels(topic_labels)} picks, so this gets a small lift.",
        }
    elif neighborhood_delta <= -0.6 and venue.neighborhood:
        feedback_reason = {
            "title": "Area pattern",
            "detail": f"Pulse has seen more dismisses around {venue.neighborhood}, so this area is weighted down a bit.",
        }
    elif neighborhood_delta >= 0.6 and venue.neighborhood:
        feedback_reason = {
            "title": "Area pattern",
            "detail": f"You have saved a few spots around {venue.neighborhood}, so this area gets a gentle boost.",
        }

    return max(-0.38, min(0.22, adjustment)), feedback_reason


def _reason_items(
    matched_topics: list[UserInterestProfile],
    muted_topics: list[UserInterestProfile],
    travel: list[dict],
    budget_fit: float,
    venue: Venue,
    feedback_reason: dict | None = None,
) -> list[dict]:
    reasons: list[dict] = []
    boosted_labels = [topic.label for topic in matched_topics if topic.boosted]
    matched_labels = [topic.label for topic in matched_topics if not topic.boosted]

    if boosted_labels:
        reasons.append(
            {
                "title": "Boosted fit",
                "detail": f"You boosted {_join_labels(boosted_labels)}, so {venue.name} moved up in this run.",
            }
        )
    elif matched_labels:
        reasons.append(
            {
                "title": "Profile match",
                "detail": f"This venue lines up with your {_join_labels(matched_labels)} signals.",
            }
        )

    if feedback_reason is not None:
        reasons.append(feedback_reason)

    if muted_topics:
        reasons.append(
            {
                "title": "Muted signal",
                "detail": f"Muted topics like {_join_labels([topic.label for topic in muted_topics])} now reduce this score.",
            }
        )

    transit_minutes = _transit_minutes(travel)
    reasons.append(
        {
            "title": "Travel fit",
            "detail": f"About {transit_minutes} min by transit from your current NYC anchor.",
        }
    )

    reasons.append(
        {
            "title": "Budget fit",
            "detail": "Comfortably inside budget." if budget_fit >= 0.85 else "A little pricier, but still workable.",
        }
    )
    return reasons[:3]


def _candidate_score(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
    source_confidence: float,
    transit_minutes: int,
    budget_fit: float,
    *,
    category: str = "",
    tags: list[str] | None = None,
) -> tuple[float, list[UserInterestProfile], list[UserInterestProfile]]:
    score, matched_topics, muted_topics, _ = _candidate_score_with_components(
        topic_keys,
        profiles_by_key,
        source_confidence,
        transit_minutes,
        budget_fit,
        category=category,
        tags=tags,
    )
    return score, matched_topics, muted_topics


def _candidate_score_with_components(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
    source_confidence: float,
    transit_minutes: int,
    budget_fit: float,
    *,
    category: str = "",
    tags: list[str] | None = None,
) -> tuple[float, list[UserInterestProfile], list[UserInterestProfile], CandidateScoreComponents]:
    interest_fit, matched_topics, muted_topics = _interest_fit(topic_keys, profiles_by_key)
    category_fit = _category_affinity(category, tags or [], profiles_by_key)
    distance_fit = _distance_fit(transit_minutes)
    weighted_interest = interest_fit * 0.64
    weighted_category = category_fit * 0.15
    weighted_distance = distance_fit * 0.11
    weighted_budget = budget_fit * 0.10
    weighted_source = source_confidence * 0.05
    total_score = _clamp_score(
        weighted_interest
        + weighted_category
        + weighted_distance
        + weighted_budget
        + weighted_source
    )
    return (
        total_score,
        matched_topics,
        muted_topics,
        CandidateScoreComponents(
            interest_fit=interest_fit,
            category_fit=category_fit,
            distance_fit=distance_fit,
            budget_fit=budget_fit,
            source_confidence=source_confidence,
            transit_minutes=transit_minutes,
            weighted_interest=weighted_interest,
            weighted_category=weighted_category,
            weighted_distance=weighted_distance,
            weighted_budget=weighted_budget,
            weighted_source=weighted_source,
        ),
    )


def _impact_label(contribution: float) -> str:
    magnitude = abs(contribution)
    if contribution < 0:
        if magnitude >= 0.12:
            return "holding it back"
        if magnitude >= 0.05:
            return "soft drag"
        return "small drag"

    if magnitude >= 0.35:
        return "driving this pick"
    if magnitude >= 0.12:
        return "strong support"
    if magnitude >= 0.05:
        return "helping"
    return "small lift"


def _score_breakdown_items(
    *,
    components: CandidateScoreComponents,
    matched_labels: list[str],
    muted_labels: list[str],
    feedback_adjustment: float,
    feedback_reason: dict | None,
) -> list[dict]:
    items: list[dict] = [
        {
            "key": "profile_fit",
            "label": "Profile fit",
            "impactLabel": _impact_label(components.weighted_interest),
            "detail": (
                f"Matched {_join_labels(matched_labels)}."
                if matched_labels
                else "No direct theme match, so this leaned on weaker defaults."
            ),
            "contribution": round(components.weighted_interest, 3),
            "direction": "positive",
            "summaryLabel": "profile fit",
        },
        {
            "key": "distance_fit",
            "label": "Travel fit",
            "impactLabel": _impact_label(components.weighted_distance),
            "detail": f"About {components.transit_minutes} min by transit from your current NYC anchor.",
            "contribution": round(components.weighted_distance, 3),
            "direction": "positive",
            "summaryLabel": "travel convenience",
        },
        {
            "key": "budget_fit",
            "label": "Budget fit",
            "impactLabel": _impact_label(components.weighted_budget),
            "detail": (
                "Comfortably inside budget."
                if components.budget_fit >= 0.85
                else "A little pricier, but still workable."
            ),
            "contribution": round(components.weighted_budget, 3),
            "direction": "positive",
            "summaryLabel": "budget fit",
        },
        {
            "key": "source_trust",
            "label": "Source trust",
            "impactLabel": _impact_label(components.weighted_source),
            "detail": (
                "Backed by a highly trusted source."
                if components.source_confidence >= 0.88
                else "Supported by a solid source signal."
                if components.source_confidence >= 0.78
                else "Still useful, but from a lighter-confidence source."
            ),
            "contribution": round(components.weighted_source, 3),
            "direction": "positive",
            "summaryLabel": "source trust",
        },
    ]

    if components.weighted_category >= 0.015:
        items.append(
            {
                "key": "category_fit",
                "label": "Category overlap",
                "impactLabel": _impact_label(components.weighted_category),
                "detail": "Event tags and category echoed your active themes.",
                "contribution": round(components.weighted_category, 3),
                "direction": "positive",
                "summaryLabel": "category overlap",
            }
        )

    if muted_labels and feedback_reason is None:
        items.append(
            {
                "key": "muted_topics",
                "label": "Muted topics",
                "impactLabel": "holding it back",
                "detail": f"Muted topics like {_join_labels(muted_labels)} reduced this pick's ceiling.",
                "contribution": round(-0.06, 3),
                "direction": "negative",
                "summaryLabel": "muted topics",
            }
        )

    if abs(feedback_adjustment) >= 0.015:
        items.append(
            {
                "key": "feedback",
                "label": "Recent feedback",
                "impactLabel": _impact_label(feedback_adjustment),
                "detail": (
                    feedback_reason["detail"]
                    if feedback_reason is not None
                    else "Recent saves and dismisses nudged this venue's rank."
                ),
                "contribution": round(feedback_adjustment, 3),
                "direction": "positive" if feedback_adjustment >= 0 else "negative",
                "summaryLabel": "recent feedback" if feedback_adjustment >= 0 else "recent dismiss patterns",
            }
        )

    return sorted(items, key=lambda item: abs(item["contribution"]), reverse=True)


def _score_summary(score_breakdown: list[dict]) -> str | None:
    positive_items = [item for item in score_breakdown if item["contribution"] > 0.025]
    negative_items = [item for item in score_breakdown if item["contribution"] < -0.025]

    if positive_items[:2]:
        lead_labels = [item["summaryLabel"] for item in positive_items[:2]]
        if len(lead_labels) == 1:
            summary = f"Mostly driven by {lead_labels[0]}."
        else:
            summary = f"Led by {lead_labels[0]} and {lead_labels[1]}."
    elif positive_items:
        summary = f"Mostly driven by {positive_items[0]['summaryLabel']}."
    else:
        summary = "This pick is hanging together on smaller supporting signals."

    if negative_items:
        summary = f"{summary[:-1]}, with {negative_items[0]['summaryLabel']} holding it back."

    return summary


def _pack_reason_payload(
    reasons: list[dict],
    *,
    score_summary: str | None,
    score_breakdown: list[dict],
) -> list[dict]:
    payload = [dict(reason) for reason in reasons]
    if score_summary:
        payload.append({REASON_META_KEY: REASON_META_SCORE_SUMMARY, "summary": score_summary})
    if score_breakdown:
        payload.append({REASON_META_KEY: REASON_META_SCORE_BREAKDOWN, "items": score_breakdown})
    return payload


def _unpack_reason_payload(
    payload: list[dict] | None,
) -> tuple[list[RecommendationReason], str | None, list[RecommendationScoreBreakdownItem]]:
    reasons: list[RecommendationReason] = []
    score_summary: str | None = None
    score_breakdown: list[RecommendationScoreBreakdownItem] = []

    for item in payload or []:
        meta_key = item.get(REASON_META_KEY)
        if meta_key == REASON_META_SCORE_SUMMARY:
            candidate_summary = item.get("summary")
            if isinstance(candidate_summary, str):
                score_summary = candidate_summary
            continue
        if meta_key == REASON_META_SCORE_BREAKDOWN:
            candidate_items = item.get("items") or []
            score_breakdown = [RecommendationScoreBreakdownItem(**candidate) for candidate in candidate_items]
            continue
        if "title" in item and "detail" in item:
            reasons.append(RecommendationReason(title=item["title"], detail=item["detail"]))

    return reasons, score_summary, score_breakdown


def _constraints_snapshot(constraints: UserConstraint | None) -> dict:
    if constraints is None:
        return {
            "city": SERVICE_AREA_NAME,
            "neighborhood": None,
            "zipCode": None,
            "radiusMiles": 8,
            "budgetLevel": "under_75",
            "preferredDays": ["Thursday", "Friday", "Saturday"],
            "socialMode": "either",
        }

    return {
        "city": constraints.city,
        "neighborhood": constraints.neighborhood,
        "zipCode": constraints.zip_code,
        "radiusMiles": constraints.radius_miles,
        "budgetLevel": constraints.budget_level,
        "preferredDays": constraints.preferred_days_csv.split(",") if constraints.preferred_days_csv else [],
        "socialMode": constraints.social_mode,
    }


def _topic_labels(rows: list[UserInterestProfile], *, muted: bool) -> list[str]:
    return [row.label for row in rows if row.muted is muted]


def _topic_snapshot(rows: list[UserInterestProfile]) -> list[dict]:
    return sorted(
        [
            {
                "topicKey": row.topic_key,
                "confidence": round(row.confidence, 3),
                "boosted": row.boosted,
                "muted": row.muted,
            }
            for row in rows
        ],
        key=lambda item: item["topicKey"],
    )


def _context_hash(
    *,
    run: RecommendationRun,
    resolution: AnchorResolution,
    constraints: UserConstraint | None,
    topics: list[UserInterestProfile],
    items: list[VenueRecommendationCard],
) -> str:
    active_anchor = resolution.active_anchor
    requested_anchor = resolution.requested_anchor
    payload = {
        "runId": run.id,
        "generatedAt": _timestamp_utc(run.created_at).isoformat(),
        "serviceArea": SERVICE_AREA_NAME,
        "activeAnchor": {
            "label": _anchor_label(active_anchor),
            "source": active_anchor.source if active_anchor else "default",
            "latitude": active_anchor.latitude if active_anchor else None,
            "longitude": active_anchor.longitude if active_anchor else None,
            "zipCode": active_anchor.zip_code if active_anchor else None,
            "neighborhood": active_anchor.neighborhood if active_anchor else None,
        },
        "requestedAnchor": {
            "label": _anchor_label(requested_anchor) if requested_anchor else None,
            "source": requested_anchor.source if requested_anchor else None,
            "withinServiceArea": resolution.requested_within_service_area,
            "usedFallback": resolution.used_fallback_anchor,
        },
        "constraints": _constraints_snapshot(constraints),
        "topics": _topic_snapshot(topics),
        "shortlist": [
            {
                "venueId": item.venueId,
                "eventId": item.eventId,
                "score": round(item.score, 3),
                "scoreBand": item.scoreBand,
            }
            for item in items
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]


def _driver_summaries(
    items: list[VenueRecommendationCard],
) -> tuple[list[RecommendationDriverSummary], list[RecommendationDriverSummary]]:
    buckets: dict[str, dict] = {}
    for item in items:
        for factor in item.scoreBreakdown:
            bucket = buckets.setdefault(
                factor.key,
                {
                    "key": factor.key,
                    "label": factor.label,
                    "contributionSum": 0.0,
                    "count": 0,
                    "venues": [],
                },
            )
            bucket["contributionSum"] += factor.contribution
            bucket["count"] += 1
            bucket["venues"].append((abs(factor.contribution), item.venueName))

    summaries: list[RecommendationDriverSummary] = []
    for bucket in buckets.values():
        average_contribution = round(bucket["contributionSum"] / max(1, bucket["count"]), 3)
        unique_top_venues: list[str] = []
        for _, venue_name in sorted(bucket["venues"], key=lambda item: item[0], reverse=True):
            if venue_name in unique_top_venues:
                continue
            unique_top_venues.append(venue_name)
            if len(unique_top_venues) == 3:
                break

        summaries.append(
            RecommendationDriverSummary(
                key=bucket["key"],
                label=bucket["label"],
                impactLabel=_impact_label(average_contribution),
                averageContribution=average_contribution,
                venueCount=bucket["count"],
                topVenues=unique_top_venues,
            )
        )

    positive = sorted(
        [summary for summary in summaries if summary.averageContribution > 0.02],
        key=lambda summary: summary.averageContribution,
        reverse=True,
    )
    negative = sorted(
        [summary for summary in summaries if summary.averageContribution < -0.02],
        key=lambda summary: summary.averageContribution,
    )
    return positive[:4], negative[:4]


def _debug_summary_sentence(
    positive: list[RecommendationDriverSummary],
    negative: list[RecommendationDriverSummary],
) -> str | None:
    if not positive and not negative:
        return None
    if positive and negative:
        secondary_label = positive[1].label.lower() if len(positive) > 1 else positive[0].label.lower()
        return (
            f"This run is mostly driven by {positive[0].label.lower()} and {secondary_label}, "
            f"with {negative[0].label.lower()} creating the main drag."
        )
    if positive:
        lead = positive[0]
        if len(positive) > 1:
            return f"This run is mostly driven by {lead.label.lower()} and {positive[1].label.lower()}."
        return f"This run is mostly driven by {lead.label.lower()}."
    lead_drag = negative[0]
    return f"This run is mostly being held back by {lead_drag.label.lower()}."


def _normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def _derive_topic_keys(event: CanonicalEvent, tags: list[str]) -> list[str]:
    text = " ".join(
        filter(
            None,
            [
                _normalize_text(event.title),
                _normalize_text(event.summary),
                _normalize_text(event.category),
                " ".join(_normalize_text(tag) for tag in tags),
            ],
        )
    )
    derived = [
        key
        for key, keywords in TOPIC_KEYWORD_MAP.items()
        if any(keyword in text for keyword in keywords)
    ]
    return derived


def _secondary_events_payload(entries: list[dict]) -> list[dict]:
    payload: list[dict] = []
    for entry in entries[1:3]:
        payload.append(
            {
                "eventId": entry["occurrence"].id,
                "title": entry["event"].title,
                "startsAt": entry["occurrence"].starts_at,
            }
        )
    return payload


def _dominant_topic_key(topic_keys: list[str], profiles_by_key: dict[str, UserInterestProfile]) -> str | None:
    best_key: str | None = None
    best_weight = -1.0
    for key in topic_keys:
        topic = profiles_by_key.get(key)
        if topic is None:
            continue
        weight = _topic_weight(topic)
        if weight > best_weight:
            best_weight = weight
            best_key = key
    return best_key or (topic_keys[0] if topic_keys else None)


def _active_theme_keys(profiles_by_key: dict[str, UserInterestProfile]) -> set[str]:
    return {
        key
        for key, topic in profiles_by_key.items()
        if not topic.muted and topic.confidence >= 0.6
    }


def _selection_mix_score(
    primary: dict,
    *,
    chosen_entries: list[dict],
    preferred_theme_keys: set[str],
) -> float:
    score = primary["score"]
    category = _normalize_text(primary.get("category"))
    topic_keys = set(primary.get("topic_keys", []))
    dominant_topic_key = primary.get("dominant_topic_key")

    chosen_categories = [_normalize_text(entry.get("category")) for entry in chosen_entries]
    chosen_dominant_topics = [entry.get("dominant_topic_key") for entry in chosen_entries]
    covered_theme_keys = {
        key
        for entry in chosen_entries
        for key in entry.get("topic_keys", [])
    }

    if dominant_topic_key and dominant_topic_key in preferred_theme_keys and dominant_topic_key not in covered_theme_keys:
        score += 0.06

    uncovered_broad_topics = (topic_keys & preferred_theme_keys & BROAD_CULTURAL_THEME_KEYS) - covered_theme_keys
    if uncovered_broad_topics:
        score += 0.04

    duplicate_category_count = sum(1 for chosen_category in chosen_categories if category and chosen_category == category)
    duplicate_topic_count = sum(
        1 for chosen_topic in chosen_dominant_topics if dominant_topic_key and chosen_topic == dominant_topic_key
    )

    score -= duplicate_category_count * 0.03
    score -= duplicate_topic_count * 0.05
    return score


def _select_ranked_venues(
    ranked_venues: list[list[dict]],
    profiles_by_key: dict[str, UserInterestProfile],
    *,
    limit: int = 8,
) -> list[list[dict]]:
    if len(ranked_venues) <= limit:
        return ranked_venues

    preferred_theme_keys = _active_theme_keys(profiles_by_key)
    remaining = [entries for entries in ranked_venues if entries]
    chosen: list[list[dict]] = []
    chosen_entries: list[dict] = []

    while remaining and len(chosen) < limit:
        best_index = 0
        best_score = float("-inf")
        for index, entries in enumerate(remaining):
            selection_score = _selection_mix_score(
                entries[0],
                chosen_entries=chosen_entries,
                preferred_theme_keys=preferred_theme_keys,
            )
            if selection_score > best_score:
                best_score = selection_score
                best_index = index

        selected_entries = remaining.pop(best_index)
        chosen.append(selected_entries)
        chosen_entries.append(selected_entries[0])

    return chosen


def _archive_kind(provider: str | None) -> str:
    if not provider:
        return "live"
    if "scheduled" in provider:
        return "scheduled"
    if "preview" in provider:
        return "preview"
    return "snapshot"


def _archive_title(kind: str) -> str:
    if kind == "scheduled":
        return "Weekly digest"
    if kind == "preview":
        return "Preview send"
    if kind == "snapshot":
        return "Saved snapshot"
    return "Current shortlist"


def _display_timezone(user: User) -> str:
    return user.timezone or "America/New_York"


def _deletable_run_ids(run_ids: list[str], protected_run_ids: set[str]) -> list[str]:
    return [run_id for run_id in run_ids if run_id not in protected_run_ids]


def _run_is_stale(run: RecommendationRun) -> bool:
    return datetime.now(tz=UTC) - _timestamp_utc(run.created_at) >= RECOMMENDATION_MAX_AGE


def _parse_occurrence_start(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _occurrence_is_rankable(occurrence: EventOccurrence, *, now: datetime | None = None) -> bool:
    starts_at = _parse_occurrence_start(occurrence.starts_at)
    if starts_at is None:
        return False
    current_time = now or datetime.now(tz=UTC)
    if starts_at < current_time - OCCURRENCE_LOOKBACK_WINDOW:
        return False
    if starts_at > current_time + OCCURRENCE_LOOKAHEAD_WINDOW:
        return False
    return True


def _run_context_changed(
    run: RecommendationRun,
    anchor: UserAnchorLocation | None,
    constraints: UserConstraint | None,
) -> bool:
    run_created_at = _timestamp_utc(run.created_at)

    if anchor is not None and _timestamp_utc(anchor.created_at) > run_created_at:
        return True

    if constraints is None:
        return False

    constraint_updated_at = constraints.updated_at or constraints.created_at
    return _timestamp_utc(constraint_updated_at) > run_created_at


async def _catalog_changed_since(
    session: AsyncSession,
    run: RecommendationRun,
) -> bool:
    latest_occurrence_update = await session.scalar(
        select(EventOccurrence.updated_at)
        .where(EventOccurrence.is_active.is_(True))
        .order_by(desc(EventOccurrence.updated_at))
        .limit(1)
    )
    if latest_occurrence_update is None:
        return False
    return _timestamp_utc(latest_occurrence_update) > _timestamp_utc(run.created_at)


async def _replace_user_runs(session: AsyncSession, user_id: str) -> None:
    run_ids = list((await session.scalars(select(RecommendationRun.id).where(RecommendationRun.user_id == user_id))).all())
    if not run_ids:
        return

    protected_run_ids = set(
        (
            await session.scalars(
                select(DigestDelivery.recommendation_run_id).where(
                    DigestDelivery.recommendation_run_id.in_(run_ids)
                )
            )
        ).all()
    )
    deletable_run_ids = _deletable_run_ids(run_ids, protected_run_ids)
    if not deletable_run_ids:
        return

    await session.execute(delete(VenueRecommendation).where(VenueRecommendation.run_id.in_(deletable_run_ids)))
    await session.execute(delete(DigestDelivery).where(DigestDelivery.recommendation_run_id.in_(deletable_run_ids)))
    await session.execute(delete(RecommendationRun).where(RecommendationRun.id.in_(deletable_run_ids)))
    await session.flush()


async def refresh_recommendations_for_user(
    session: AsyncSession,
    user: User,
    *,
    force: bool = False,
    provider: str = "catalog",
    model_name: str = "pulse-deterministic-v1",
) -> RecommendationRun:
    anchor = await _user_anchor(session, user.id)
    constraints = await _user_constraints(session, user.id)
    existing_run = await _latest_run(session, user.id)
    if (
        existing_run is not None
        and not force
        and not _run_is_stale(existing_run)
        and not _run_context_changed(existing_run, anchor, constraints)
        and not await _catalog_changed_since(session, existing_run)
    ):
        return existing_run

    origin_latitude, origin_longitude = _anchor_coordinates(anchor)
    viewport = _viewport_for_anchor(anchor)
    topic_rows = (
        await session.scalars(select(UserInterestProfile).where(UserInterestProfile.user_id == user.id))
    ).all()
    profiles_by_key = {row.topic_key: row for row in topic_rows}
    feedback_signals = await _feedback_signals(session, user.id)

    occurrence_rows = (
        await session.scalars(
            select(EventOccurrence)
            .where(EventOccurrence.is_active.is_(True))
            .order_by(EventOccurrence.starts_at.asc())
        )
    ).all()

    venue_entries: dict[str, list[dict]] = {}
    for occurrence in occurrence_rows:
        if not _occurrence_is_rankable(occurrence):
            continue
        venue = await session.get(Venue, occurrence.venue_id)
        event = await session.get(CanonicalEvent, occurrence.event_id)
        if not venue or not event:
            continue
        if venue.city not in {"New York City", "New York", "Brooklyn", "Queens", "Bronx", "Staten Island"}:
            continue

        travel = estimate_travel_bands(origin_latitude, origin_longitude, venue.latitude, venue.longitude)
        metadata = occurrence.metadata_json or {}
        topic_keys = metadata.get("topicKeys") or _derive_topic_keys(event, metadata.get("tags", []))
        source_confidence = metadata.get("sourceConfidence", 0.75)
        budget_fit = _budget_fit(constraints, occurrence)
        transit_minutes = _transit_minutes(travel)
        score, matched_topics, muted_topics, score_components = _candidate_score_with_components(
            topic_keys,
            profiles_by_key,
            source_confidence,
            transit_minutes,
            budget_fit,
            category=event.category,
            tags=metadata.get("tags", []),
        )
        feedback_adjustment, feedback_reason = _feedback_adjustment(
            topic_keys=topic_keys,
            profiles_by_key=profiles_by_key,
            venue=venue,
            feedback_signals=feedback_signals,
        )
        adjusted_score = _clamp_score(score + feedback_adjustment)
        matched_labels = [topic.label for topic in matched_topics]
        muted_labels = [topic.label for topic in muted_topics]
        score_breakdown = _score_breakdown_items(
            components=score_components,
            matched_labels=matched_labels,
            muted_labels=muted_labels,
            feedback_adjustment=feedback_adjustment,
            feedback_reason=feedback_reason,
        )
        score_summary = _score_summary(score_breakdown)
        entry = {
            "venue": venue,
            "event": event,
            "occurrence": occurrence,
            "travel": travel,
            "score": adjusted_score,
            "score_band": _score_band(adjusted_score),
            "category": event.category,
            "topic_keys": topic_keys,
            "dominant_topic_key": _dominant_topic_key(topic_keys, profiles_by_key),
            "reasons": _reason_items(
                matched_topics=matched_topics,
                muted_topics=muted_topics,
                travel=travel,
                budget_fit=budget_fit,
                venue=venue,
                feedback_reason=feedback_reason,
            ),
            "score_summary": score_summary,
            "score_breakdown": score_breakdown,
        }
        venue_entries.setdefault(venue.id, []).append(entry)

    await _replace_user_runs(session, user.id)
    run = RecommendationRun(
        user_id=user.id,
        provider=provider,
        model_name=model_name,
        viewport_json=viewport,
    )
    session.add(run)
    await session.flush()

    ranked_venues = sorted(
        (
            sorted(entries, key=lambda item: item["score"], reverse=True)
            for entries in venue_entries.values()
        ),
        key=lambda entries: entries[0]["score"] if entries else 0.0,
        reverse=True,
    )

    selected_ranked_venues = _select_ranked_venues(ranked_venues, profiles_by_key, limit=8)

    for rank, entries in enumerate(selected_ranked_venues, start=1):
        primary = entries[0]
        session.add(
            VenueRecommendation(
                run_id=run.id,
                venue_id=primary["venue"].id,
                event_occurrence_id=primary["occurrence"].id,
                rank=rank,
                score=primary["score"],
                score_band=primary["score_band"],
                reasons_json=_pack_reason_payload(
                    primary["reasons"],
                    score_summary=primary["score_summary"],
                    score_breakdown=primary["score_breakdown"],
                ),
                travel_json=primary["travel"],
                secondary_events_json=_secondary_events_payload(entries),
            )
        )

    await session.commit()
    await session.refresh(run)
    return run


def _empty_response(display_timezone: str = "America/New_York") -> RecommendationsMapResponse:
    return RecommendationsMapResponse(
        viewport=DEFAULT_VIEWPORT,
        pins=[],
        cards={},
        generatedAt="",
        displayTimezone=display_timezone,
        userConstraint={},
        mapContext=MapContext(serviceArea=SERVICE_AREA_NAME),
    )


async def _cards_for_run(
    session: AsyncSession,
    run: RecommendationRun,
) -> tuple[list[MapVenuePin], list[VenueRecommendationCard], dict[str, VenueRecommendationCard]]:
    recommendation_rows = (
        await session.scalars(
            select(VenueRecommendation)
            .where(VenueRecommendation.run_id == run.id)
            .order_by(VenueRecommendation.rank.asc())
        )
    ).all()
    if not recommendation_rows:
        return [], [], {}

    pins: list[MapVenuePin] = []
    items: list[VenueRecommendationCard] = []
    cards: dict[str, VenueRecommendationCard] = {}

    for index, recommendation in enumerate(recommendation_rows):
        venue = await session.get(Venue, recommendation.venue_id)
        occurrence = await session.get(EventOccurrence, recommendation.event_occurrence_id)
        event = await session.get(CanonicalEvent, occurrence.event_id if occurrence else None)
        source = await session.get(EventSource, event.source_id if event else None)
        if not venue or not occurrence or not event or not source:
            continue

        travel = recommendation.travel_json or estimate_travel_bands(
            DEFAULT_VIEWPORT["latitude"],
            DEFAULT_VIEWPORT["longitude"],
            venue.latitude,
            venue.longitude,
        )
        reasons, score_summary, score_breakdown = _unpack_reason_payload(recommendation.reasons_json)

        card = VenueRecommendationCard(
            venueId=venue.id,
            venueName=venue.name,
            neighborhood=venue.neighborhood or "NYC",
            address=venue.address,
            eventTitle=event.title,
            eventId=occurrence.id,
            startsAt=occurrence.starts_at,
            priceLabel=_price_label(occurrence.min_price, occurrence.max_price),
            scoreBand=recommendation.score_band,
            score=recommendation.score,
            travel=[TravelEstimate(**item) for item in travel],
            reasons=reasons,
            freshness=_build_freshness(occurrence),
            provenance=_build_provenance(source, occurrence),
            scoreSummary=score_summary,
            scoreBreakdown=score_breakdown,
            secondaryEvents=recommendation.secondary_events_json or [],
        )
        items.append(card)
        cards[venue.id] = card
        pins.append(
            MapVenuePin(
                venueId=venue.id,
                venueName=venue.name,
                latitude=venue.latitude,
                longitude=venue.longitude,
                scoreBand=recommendation.score_band,
                selected=index == 0,
            )
        )

    return pins, items, cards


def _build_freshness(occurrence: EventOccurrence) -> RecommendationFreshness:
    discovered_at = _iso_or_none(occurrence.created_at)
    last_verified_at = _iso_or_none(occurrence.updated_at)
    freshness_label = _freshness_label(occurrence.updated_at)
    return RecommendationFreshness(
        discoveredAt=discovered_at,
        lastVerifiedAt=last_verified_at,
        freshnessLabel=freshness_label,
    )


def _build_provenance(source: EventSource, occurrence: EventOccurrence) -> RecommendationProvenance:
    metadata = occurrence.metadata_json or {}
    confidence = float(metadata.get("sourceConfidence", 0.75) or 0.75)
    return RecommendationProvenance(
        sourceName=_present_source_name(source.name),
        sourceKind=source.kind,
        sourceConfidence=round(confidence, 2),
        sourceConfidenceLabel=_source_confidence_label(confidence, source.kind),
        sourceBaseUrl=source.base_url,
        hasTicketUrl=bool(occurrence.ticket_url),
    )


def _present_source_name(name: str) -> str:
    if name == "ticketmaster":
        return "Ticketmaster"
    if name == "curated_venues":
        return "Curated venue calendars"
    return name.replace("_", " ").title()


def _source_confidence_label(confidence: float, source_kind: str) -> str:
    if source_kind == "curated_calendar" and confidence >= 0.8:
        return "High trust"
    if confidence >= 0.9:
        return "High trust"
    if confidence >= 0.78:
        return "Solid signal"
    return "Emerging signal"


def _freshness_label(updated_at: datetime) -> str:
    age = datetime.now(tz=UTC) - _timestamp_utc(updated_at)
    if age <= timedelta(hours=6):
        return "Verified recently"
    if age <= timedelta(days=1):
        return "Checked today"
    if age <= timedelta(days=3):
        return "Checked this week"
    return "Needs refresh soon"


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _timestamp_utc(value).isoformat()


async def get_map_recommendations(
    session: AsyncSession,
    user: User,
) -> RecommendationsMapResponse:
    display_timezone = _display_timezone(user)
    anchor_resolution = await _user_anchor_resolution(session, user.id)
    run = await refresh_recommendations_for_user(session, user)
    if run is None:
        return _empty_response(display_timezone)

    constraints = await _user_constraints(session, user.id)
    pins, items, cards = await _cards_for_run(session, run)
    if not items:
        return _empty_response(display_timezone)

    return RecommendationsMapResponse(
        viewport=run.viewport_json,
        pins=pins,
        cards=cards,
        generatedAt=run.created_at.isoformat(),
        displayTimezone=display_timezone,
        userConstraint={
            "city": constraints.city if constraints else SERVICE_AREA_NAME,
            "neighborhood": constraints.neighborhood if constraints else None,
            "zipCode": constraints.zip_code if constraints else None,
            "radiusMiles": constraints.radius_miles if constraints else 8,
            "budgetLevel": constraints.budget_level if constraints else "under_75",
            "preferredDays": constraints.preferred_days_csv.split(",") if constraints else ["Thursday", "Friday", "Saturday"],
            "socialMode": constraints.social_mode if constraints else "either",
        },
        mapContext=_build_map_context(anchor_resolution),
    )


async def get_recommendation_debug_summary(
    session: AsyncSession,
    user: User,
) -> RecommendationDebugSummary:
    run = await refresh_recommendations_for_user(session, user)
    if run is None:
        return RecommendationDebugSummary()

    anchor_resolution = await _user_anchor_resolution(session, user.id)
    constraints = await _user_constraints(session, user.id)
    topic_rows = list(
        (
            await session.scalars(
                select(UserInterestProfile)
                .where(UserInterestProfile.user_id == user.id)
                .order_by(UserInterestProfile.confidence.desc(), UserInterestProfile.topic_key.asc())
            )
        ).all()
    )
    _, items, _ = await _cards_for_run(session, run)
    positive_drivers, negative_drivers = _driver_summaries(items)

    return RecommendationDebugSummary(
        runId=run.id,
        generatedAt=_timestamp_utc(run.created_at).isoformat(),
        rankingModel=run.model_name,
        contextHash=_context_hash(
            run=run,
            resolution=anchor_resolution,
            constraints=constraints,
            topics=topic_rows,
            items=items,
        ),
        shortlistSize=len(items),
        summary=_debug_summary_sentence(positive_drivers, negative_drivers),
        mapContext=_build_map_context(anchor_resolution),
        activeTopics=_topic_labels(topic_rows, muted=False),
        mutedTopics=_topic_labels(topic_rows, muted=True),
        topPositiveDrivers=positive_drivers,
        topNegativeDrivers=negative_drivers,
        venues=[
            RecommendationDebugVenue(
                rank=index + 1,
                venueId=item.venueId,
                venueName=item.venueName,
                score=item.score,
                scoreBand=item.scoreBand,
                scoreSummary=item.scoreSummary,
                topDrivers=item.scoreBreakdown[:3],
            )
            for index, item in enumerate(items)
        ],
    )


async def get_archive(session: AsyncSession, user: User) -> ArchiveResponse:
    display_timezone = _display_timezone(user)
    latest_run = await refresh_recommendations_for_user(session, user)
    if latest_run is None:
        return ArchiveResponse(items=[], history=[], displayTimezone=display_timezone)

    _, items, _ = await _cards_for_run(session, latest_run)

    delivery_rows = (
        await session.scalars(
            select(DigestDelivery)
            .where(
                DigestDelivery.user_id == user.id,
                DigestDelivery.status == "sent",
            )
            .order_by(desc(DigestDelivery.created_at))
        )
    ).all()

    snapshots: list[ArchiveSnapshot] = []
    seen_run_ids: set[str] = set()

    for delivery in delivery_rows:
        if delivery.recommendation_run_id == latest_run.id:
            continue
        if delivery.recommendation_run_id in seen_run_ids:
            continue

        run = await session.get(RecommendationRun, delivery.recommendation_run_id)
        if run is None:
            continue

        _, historical_items, _ = await _cards_for_run(session, run)
        if not historical_items:
            continue

        kind = _archive_kind(delivery.provider)
        snapshots.append(
            ArchiveSnapshot(
                runId=run.id,
                kind=kind,
                title=_archive_title(kind),
                generatedAt=run.created_at.isoformat(),
                deliveredAt=delivery.created_at.isoformat(),
                items=historical_items,
            )
        )
        seen_run_ids.add(run.id)

    return ArchiveResponse(items=items, history=snapshots, displayTimezone=display_timezone)


def _price_label(min_price: float | None, max_price: float | None) -> str:
    if min_price is None and max_price is None:
        return "Price varies"
    if min_price is not None and max_price is not None and min_price == max_price:
        return f"${min_price:.0f}"
    if min_price is None:
        return f"Up to ${max_price:.0f}"
    if max_price is None:
        return f"From ${min_price:.0f}"
    return f"${min_price:.0f}-${max_price:.0f}"
