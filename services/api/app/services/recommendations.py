from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.events import CanonicalEvent, EventOccurrence, Venue
from app.models.profile import UserInterestProfile
from app.models.recommendation import DigestDelivery, RecommendationRun, VenueRecommendation
from app.models.user import User, UserAnchorLocation, UserConstraint
from app.schemas.recommendations import (
    ArchiveResponse,
    MapVenuePin,
    RecommendationsMapResponse,
    RecommendationReason,
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
NYC_SERVICE_AREA = {
    "min_latitude": 40.45,
    "max_latitude": 40.95,
    "min_longitude": -74.35,
    "max_longitude": -73.65,
}
RECOMMENDATION_MAX_AGE = timedelta(minutes=30)
TOPIC_KEYWORD_MAP = {
    "underground_dance": ["techno", "warehouse", "club", "dance", "rave", "dj"],
    "indie_live_music": ["indie", "band", "concert", "live music", "show", "songwriter", "alt-pop"],
    "gallery_nights": ["gallery", "art", "opening", "installation", "visual"],
    "creative_meetups": ["meetup", "creative", "community", "networking"],
}


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
    return _select_active_anchor(anchors)


async def _user_constraints(session: AsyncSession, user_id: str) -> UserConstraint | None:
    return await session.scalar(select(UserConstraint).where(UserConstraint.user_id == user_id).limit(1))


def _timestamp_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _within_nyc_service_area(latitude: float, longitude: float) -> bool:
    return (
        NYC_SERVICE_AREA["min_latitude"] <= latitude <= NYC_SERVICE_AREA["max_latitude"]
        and NYC_SERVICE_AREA["min_longitude"] <= longitude <= NYC_SERVICE_AREA["max_longitude"]
    )


def _select_active_anchor(anchors: list[UserAnchorLocation]) -> UserAnchorLocation | None:
    if not anchors:
        return None

    for anchor in anchors:
        if anchor.latitude is not None and anchor.longitude is not None:
            if _within_nyc_service_area(anchor.latitude, anchor.longitude):
                return anchor
            continue

        if anchor.zip_code or anchor.neighborhood:
            return anchor

    return anchors[0]


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


def _topic_weight(topic: UserInterestProfile) -> float:
    if topic.muted:
        return 0.05
    return min(0.98, 0.2 + (topic.confidence * 0.55) + (0.15 if topic.boosted else 0.0))


def _interest_fit(
    topic_keys: list[str],
    profiles_by_key: dict[str, UserInterestProfile],
) -> tuple[float, list[UserInterestProfile], list[UserInterestProfile]]:
    if not topic_keys:
        return (0.58, [], [])

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
        return (0.58, [], [])

    score = sum(weights) / len(weights)
    if muted_topics and not matched_topics:
        score *= 0.35
    elif muted_topics:
        score -= 0.12 * len(muted_topics)

    return (_clamp_score(score), matched_topics, muted_topics)


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


def _reason_items(
    matched_topics: list[UserInterestProfile],
    muted_topics: list[UserInterestProfile],
    travel: list[dict],
    budget_fit: float,
    venue: Venue,
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
) -> tuple[float, list[UserInterestProfile], list[UserInterestProfile]]:
    interest_fit, matched_topics, muted_topics = _interest_fit(topic_keys, profiles_by_key)
    total_score = _clamp_score(
        (interest_fit * 0.58)
        + (_distance_fit(transit_minutes) * 0.17)
        + (budget_fit * 0.15)
        + (source_confidence * 0.10)
    )
    return total_score, matched_topics, muted_topics


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


def _run_is_stale(run: RecommendationRun) -> bool:
    return datetime.now(tz=UTC) - _timestamp_utc(run.created_at) >= RECOMMENDATION_MAX_AGE


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


async def _replace_user_runs(session: AsyncSession, user_id: str) -> None:
    run_ids = list((await session.scalars(select(RecommendationRun.id).where(RecommendationRun.user_id == user_id))).all())
    if not run_ids:
        return

    await session.execute(delete(VenueRecommendation).where(VenueRecommendation.run_id.in_(run_ids)))
    await session.execute(delete(DigestDelivery).where(DigestDelivery.recommendation_run_id.in_(run_ids)))
    await session.execute(delete(RecommendationRun).where(RecommendationRun.id.in_(run_ids)))
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
    ):
        return existing_run

    origin_latitude, origin_longitude = _anchor_coordinates(anchor)
    viewport = _viewport_for_anchor(anchor)
    topic_rows = (
        await session.scalars(select(UserInterestProfile).where(UserInterestProfile.user_id == user.id))
    ).all()
    profiles_by_key = {row.topic_key: row for row in topic_rows}

    occurrence_rows = (
        await session.scalars(
            select(EventOccurrence)
            .where(EventOccurrence.is_active.is_(True))
            .order_by(EventOccurrence.starts_at.asc())
        )
    ).all()

    venue_entries: dict[str, list[dict]] = {}
    for occurrence in occurrence_rows:
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
        score, matched_topics, muted_topics = _candidate_score(
            topic_keys,
            profiles_by_key,
            source_confidence,
            _transit_minutes(travel),
            budget_fit,
        )
        entry = {
            "venue": venue,
            "event": event,
            "occurrence": occurrence,
            "travel": travel,
            "score": score,
            "score_band": _score_band(score),
            "reasons": _reason_items(
                matched_topics=matched_topics,
                muted_topics=muted_topics,
                travel=travel,
                budget_fit=budget_fit,
                venue=venue,
            ),
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

    for rank, entries in enumerate(ranked_venues[:8], start=1):
        primary = entries[0]
        session.add(
            VenueRecommendation(
                run_id=run.id,
                venue_id=primary["venue"].id,
                event_occurrence_id=primary["occurrence"].id,
                rank=rank,
                score=primary["score"],
                score_band=primary["score_band"],
                reasons_json=primary["reasons"],
                travel_json=primary["travel"],
                secondary_events_json=_secondary_events_payload(entries),
            )
        )

    await session.commit()
    await session.refresh(run)
    return run


def _empty_response() -> RecommendationsMapResponse:
    return RecommendationsMapResponse(
        viewport=DEFAULT_VIEWPORT,
        pins=[],
        cards={},
        generatedAt="",
        userConstraint={},
    )


async def get_map_recommendations(
    session: AsyncSession,
    user: User,
) -> RecommendationsMapResponse:
    run = await refresh_recommendations_for_user(session, user)
    if run is None:
        return _empty_response()

    anchor = await _user_anchor(session, user.id)
    constraints = await _user_constraints(session, user.id)

    recommendation_rows = (
        await session.scalars(
            select(VenueRecommendation)
            .where(VenueRecommendation.run_id == run.id)
            .order_by(VenueRecommendation.rank.asc())
        )
    ).all()
    if not recommendation_rows:
        return _empty_response()

    pins: list[MapVenuePin] = []
    cards: dict[str, VenueRecommendationCard] = {}

    for index, recommendation in enumerate(recommendation_rows):
        venue = await session.get(Venue, recommendation.venue_id)
        occurrence = await session.get(EventOccurrence, recommendation.event_occurrence_id)
        event = await session.get(CanonicalEvent, occurrence.event_id if occurrence else None)
        if not venue or not occurrence or not event:
            continue

        travel = recommendation.travel_json or estimate_travel_bands(
            DEFAULT_VIEWPORT["latitude"],
            DEFAULT_VIEWPORT["longitude"],
            venue.latitude,
            venue.longitude,
        )
        reasons = [
            RecommendationReason(title=item["title"], detail=item["detail"])
            for item in recommendation.reasons_json
        ]

        cards[venue.id] = VenueRecommendationCard(
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
            secondaryEvents=recommendation.secondary_events_json or [],
        )
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

    return RecommendationsMapResponse(
        viewport=run.viewport_json,
        pins=pins,
        cards=cards,
        generatedAt=run.created_at.isoformat(),
        userConstraint={
            "city": constraints.city if constraints else "New York City",
            "neighborhood": constraints.neighborhood if constraints else None,
            "zipCode": constraints.zip_code if constraints else None,
            "radiusMiles": constraints.radius_miles if constraints else 8,
            "budgetLevel": constraints.budget_level if constraints else "under_75",
            "preferredDays": constraints.preferred_days_csv.split(",") if constraints else ["Thursday", "Friday", "Saturday"],
            "socialMode": constraints.social_mode if constraints else "either",
        },
    )


async def get_archive(session: AsyncSession, user: User) -> ArchiveResponse:
    map_response = await get_map_recommendations(session, user)
    items = sorted(map_response.cards.values(), key=lambda item: item.score, reverse=True)
    return ArchiveResponse(items=items)


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
