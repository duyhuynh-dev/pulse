from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.events import CanonicalEvent, EventOccurrence, EventSource, Venue, VenueGeocode
from app.models.profile import (
    ProfileRun,
    RedditActivity,
    UserInterestOverride,
    UserInterestProfile,
)
from app.models.recommendation import (
    DigestDelivery,
    FeedbackEvent,
    RecommendationRun,
    VenueRecommendation,
)
from app.models.user import (
    EmailPreference,
    OAuthConnection,
    User,
    UserAnchorLocation,
    UserConstraint,
)
from app.services.recommendations import refresh_recommendations_for_user

DEMO_SOURCE_NAME = "Pulse Demo Source"
DEFAULT_ORIGIN = (40.7315, -73.9897)
NYC_TZ = ZoneInfo("America/New_York")

DEMO_TOPICS = [
    {
        "key": "underground_dance",
        "label": "Underground dance",
        "confidence": 0.94,
        "signals": ["r/aves", "saved venue lineups", "commented on DJ threads"],
        "boosted": True,
        "muted": False,
    },
    {
        "key": "indie_live_music",
        "label": "Indie live music",
        "confidence": 0.88,
        "signals": ["r/indieheads", "Brooklyn venue threads"],
        "boosted": False,
        "muted": False,
    },
    {
        "key": "gallery_nights",
        "label": "Gallery nights",
        "confidence": 0.72,
        "signals": ["saved art-weekend posts", "commented on openings"],
        "boosted": False,
        "muted": False,
    },
]

DEMO_REDDIT_ACTIVITIES = [
    {
        "activity_type": "comment",
        "subreddit": "aves",
        "title": "Warehouse set recommendations",
        "body": "Looking for darker late-night sets in Brooklyn",
    },
    {
        "activity_type": "saved",
        "subreddit": "indieheads",
        "title": "Tour calendar in NYC",
        "body": "Shortlist of intimate room shows",
    },
    {
        "activity_type": "comment",
        "subreddit": "AskNYC",
        "title": "Best gallery nights this month",
        "body": "Prefer neighborhoods with multiple openings in one evening",
    },
]

DEMO_VENUES = [
    {
        "name": "Elsewhere",
        "neighborhood": "Bushwick",
        "address": "599 Johnson Ave, Brooklyn, NY",
        "postal_code": "11237",
        "latitude": 40.7063,
        "longitude": -73.9232,
        "apple_place_id": "elsewhere-demo",
    },
    {
        "name": "Knockdown Center",
        "neighborhood": "Maspeth",
        "address": "52-19 Flushing Ave, Queens, NY",
        "postal_code": "11378",
        "latitude": 40.7144,
        "longitude": -73.9180,
        "apple_place_id": "knockdown-demo",
    },
    {
        "name": "Le Poisson Rouge",
        "neighborhood": "Greenwich Village",
        "address": "158 Bleecker St, New York, NY",
        "postal_code": "10012",
        "latitude": 40.7285,
        "longitude": -74.0005,
        "apple_place_id": "lpr-demo",
    },
]

DEMO_EVENT_SPECS = [
    {
        "key": "pulse-demo-0",
        "title": "After-Hours Techno Showcase",
        "category": "Live music",
        "summary": "A late-night lineup of warehouse-leaning selectors.",
        "min_price": 25,
        "max_price": 35,
        "source_confidence": 0.93,
        "topic_keys": ["underground_dance"],
    },
    {
        "key": "pulse-demo-1",
        "title": "Hybrid Club Night And Visual Installation",
        "category": "Culture",
        "summary": "Dance floor energy with projection-heavy visuals.",
        "min_price": 35,
        "max_price": 50,
        "source_confidence": 0.88,
        "topic_keys": ["underground_dance", "gallery_nights"],
    },
    {
        "key": "pulse-demo-2",
        "title": "Intimate Alt-Pop Performance",
        "category": "Live music",
        "summary": "Small-room set with strong songwriting and crowd energy.",
        "min_price": 45,
        "max_price": 65,
        "source_confidence": 0.83,
        "topic_keys": ["indie_live_music"],
    },
]

DEMO_EVENT_SLOTS = [
    (3, 21, 0),   # Thursday 9:00 PM
    (4, 20, 30),  # Friday 8:30 PM
    (5, 19, 30),  # Saturday 7:30 PM
]


def _next_demo_local_start(
    weekday: int,
    hour: int,
    minute: int,
    *,
    now_local: datetime | None = None,
) -> datetime:
    current_local = now_local or datetime.now(tz=NYC_TZ)
    days_ahead = (weekday - current_local.weekday()) % 7
    candidate_date = current_local.date() + timedelta(days=days_ahead)
    candidate = datetime.combine(candidate_date, time(hour=hour, minute=minute), tzinfo=NYC_TZ)
    if candidate <= current_local:
        candidate += timedelta(days=7)
    return candidate.astimezone(UTC)

async def seed_demo_state(session: AsyncSession, only_missing: bool = False) -> None:
    settings = get_settings()
    user = await session.scalar(select(User).where(User.email == settings.default_user_email))
    if user is None:
        user = User(email=settings.default_user_email, display_name="Pulse Beta User")
        session.add(user)
        await session.flush()

    if only_missing:
        await ensure_user_defaults(session, user)
        await session.commit()
        return

    await bootstrap_user_with_mock_reddit(session, user, create_connection=False)


async def bootstrap_user_with_mock_reddit(
    session: AsyncSession,
    user: User,
    *,
    create_connection: bool = True,
) -> None:
    await ensure_user_defaults(session, user)
    await ensure_demo_catalog(session)
    await reset_user_demo_state(session, user)

    if create_connection:
        connection = await session.scalar(
            select(OAuthConnection).where(
                OAuthConnection.user_id == user.id,
                OAuthConnection.provider == "reddit_mock",
            )
        )
        if connection is None:
            connection = OAuthConnection(user_id=user.id, provider="reddit_mock")
            session.add(connection)

        connection.provider_user_id = "pulse-mock-reddit-user"
        connection.access_token_encrypted = "mock"
        connection.refresh_token_encrypted = "mock"
        connection.scope_csv = "identity history mysubreddits"

    session.add(
        ProfileRun(
            user_id=user.id,
            provider="mock",
            model_name="pulse-demo-seed",
            summary_json={"summary": "Sample Reddit profile loaded while live Reddit API access is pending."},
        )
    )

    session.add_all(
        [
            UserInterestProfile(
                user_id=user.id,
                topic_key=topic["key"],
                label=topic["label"],
                confidence=topic["confidence"],
                source_signals_json=topic["signals"],
                boosted=topic["boosted"],
                muted=topic["muted"],
            )
            for topic in DEMO_TOPICS
        ]
    )

    now = datetime.now(tz=UTC).isoformat()
    session.add_all(
        [
            RedditActivity(
                user_id=user.id,
                activity_type=item["activity_type"],
                subreddit=item["subreddit"],
                title=item["title"],
                body=item["body"],
                occurred_at=now,
            )
            for item in DEMO_REDDIT_ACTIVITIES
        ]
    )
    await session.flush()
    await refresh_recommendations_for_user(
        session,
        user,
        force=True,
        provider="mock",
        model_name="pulse-demo-seed",
    )


async def ensure_user_defaults(session: AsyncSession, user: User) -> None:
    constraint = await session.scalar(select(UserConstraint).where(UserConstraint.user_id == user.id))
    if constraint is None:
        session.add(
            UserConstraint(
                user_id=user.id,
                city="New York City",
                neighborhood="East Village",
                zip_code="10003",
                radius_miles=8,
                budget_level="under_75",
                preferred_days_csv="Thursday,Friday,Saturday",
                social_mode="either",
            )
        )

    anchor = await session.scalar(
        select(UserAnchorLocation)
        .where(UserAnchorLocation.user_id == user.id)
        .order_by(UserAnchorLocation.created_at.desc())
        .limit(1)
    )
    if anchor is None:
        session.add(
            UserAnchorLocation(
                user_id=user.id,
                source="zip",
                neighborhood="East Village",
                zip_code="10003",
                latitude=DEFAULT_ORIGIN[0],
                longitude=DEFAULT_ORIGIN[1],
                is_session_only=False,
            )
        )

    preference = await session.scalar(select(EmailPreference).where(EmailPreference.user_id == user.id))
    if preference is None:
        session.add(EmailPreference(user_id=user.id))

    await session.flush()


async def ensure_demo_catalog(session: AsyncSession) -> tuple[list[Venue], list[EventOccurrence]]:
    source = await session.scalar(select(EventSource).where(EventSource.name == DEMO_SOURCE_NAME))
    if source is None:
        source = EventSource(kind="curated", name=DEMO_SOURCE_NAME, base_url="https://pulse.local")
        session.add(source)
        await session.flush()

    venues: list[Venue] = []
    for spec in DEMO_VENUES:
        venue = await session.scalar(
            select(Venue).where(Venue.name == spec["name"], Venue.address == spec["address"])
        )
        if venue is None:
            venue = Venue(**spec)
            session.add(venue)
            await session.flush()
        else:
            venue.neighborhood = spec["neighborhood"]
            venue.postal_code = spec["postal_code"]
            venue.latitude = spec["latitude"]
            venue.longitude = spec["longitude"]
            venue.apple_place_id = spec["apple_place_id"]

        geocode = await session.scalar(select(VenueGeocode).where(VenueGeocode.venue_id == venue.id))
        if geocode is None:
            session.add(
                VenueGeocode(
                    venue_id=venue.id,
                    provider_place_id=venue.apple_place_id or venue.id,
                    raw_response_json={"source": "demo"},
                )
            )
        venues.append(venue)

    await session.flush()

    now_local = datetime.now(tz=NYC_TZ)
    starts = [
        _next_demo_local_start(weekday, hour, minute, now_local=now_local)
        for weekday, hour, minute in DEMO_EVENT_SLOTS
    ]

    occurrences: list[EventOccurrence] = []
    for spec, venue, starts_at in zip(DEMO_EVENT_SPECS, venues, starts, strict=True):
        event = await session.scalar(
            select(CanonicalEvent).where(CanonicalEvent.source_event_key == spec["key"])
        )
        if event is None:
            event = CanonicalEvent(
                source_id=source.id,
                source_event_key=spec["key"],
                title=spec["title"],
                category=spec["category"],
                summary=spec["summary"],
            )
            session.add(event)
            await session.flush()
        else:
            event.source_id = source.id
            event.title = spec["title"]
            event.category = spec["category"]
            event.summary = spec["summary"]

        occurrence = await session.scalar(
            select(EventOccurrence).where(
                EventOccurrence.event_id == event.id,
                EventOccurrence.venue_id == venue.id,
            )
        )
        if occurrence is None:
            occurrence = EventOccurrence(
                event_id=event.id,
                venue_id=venue.id,
                starts_at=starts_at.isoformat(),
                ends_at=(starts_at + timedelta(hours=5)).isoformat(),
                min_price=spec["min_price"],
                max_price=spec["max_price"],
                ticket_url="https://pulse.local/tickets",
                metadata_json={
                    "sourceConfidence": spec["source_confidence"],
                    "topicKeys": spec["topic_keys"],
                },
            )
            session.add(occurrence)
        else:
            occurrence.starts_at = starts_at.isoformat()
            occurrence.ends_at = (starts_at + timedelta(hours=5)).isoformat()
            occurrence.min_price = spec["min_price"]
            occurrence.max_price = spec["max_price"]
            occurrence.ticket_url = "https://pulse.local/tickets"
            occurrence.metadata_json = {
                "sourceConfidence": spec["source_confidence"],
                "topicKeys": spec["topic_keys"],
            }
            occurrence.is_active = True

        occurrences.append(occurrence)

    await session.flush()
    return venues, occurrences


async def reset_user_demo_state(session: AsyncSession, user: User) -> None:
    run_ids = list(
        (
            await session.scalars(
                select(RecommendationRun.id).where(RecommendationRun.user_id == user.id)
            )
        ).all()
    )
    if run_ids:
        await session.execute(
            delete(VenueRecommendation).where(VenueRecommendation.run_id.in_(run_ids))
        )
        await session.execute(
            delete(DigestDelivery).where(DigestDelivery.recommendation_run_id.in_(run_ids))
        )

    await session.execute(delete(FeedbackEvent).where(FeedbackEvent.user_id == user.id))
    await session.execute(delete(RecommendationRun).where(RecommendationRun.user_id == user.id))
    await session.execute(delete(UserInterestOverride).where(UserInterestOverride.user_id == user.id))
    await session.execute(delete(UserInterestProfile).where(UserInterestProfile.user_id == user.id))
    await session.execute(delete(ProfileRun).where(ProfileRun.user_id == user.id))
    await session.execute(delete(RedditActivity).where(RedditActivity.user_id == user.id))
    await session.flush()
