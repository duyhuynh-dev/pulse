from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.events import CanonicalEvent, EventOccurrence, EventSource, Venue, VenueGeocode
from app.schemas.ingestion import CandidateIngestPayload, CandidateIngestResponse, IngestCandidateItem


async def upsert_ingested_candidates(
    session: AsyncSession,
    payload: CandidateIngestPayload,
) -> CandidateIngestResponse:
    response = CandidateIngestResponse(accepted=len(payload.items))

    for item in payload.items:
        source, source_created = await _upsert_source(session, item)
        venue, venue_created = await _upsert_venue(session, item)
        event, event_created = await _upsert_event(session, item, source.id)
        _, occurrence_created = await _upsert_occurrence(session, item, event.id, venue.id)

        if item.apple_place_id:
            await _upsert_geocode(session, venue.id, item.apple_place_id)

        response.sources_created += int(source_created)
        response.venues_created += int(venue_created)
        response.events_created += int(event_created)
        response.occurrences_created += int(occurrence_created)

    await session.commit()
    return response


async def _upsert_source(session: AsyncSession, item: IngestCandidateItem) -> tuple[EventSource, bool]:
    source = await session.scalar(select(EventSource).where(EventSource.name == item.source))
    created = source is None
    if source is None:
        source = EventSource(name=item.source, kind=item.source_kind)
        session.add(source)
        await session.flush()
    else:
        source.kind = item.source_kind

    return source, created


async def _upsert_venue(session: AsyncSession, item: IngestCandidateItem) -> tuple[Venue, bool]:
    venue = await session.scalar(
        select(Venue).where(Venue.name == item.venue_name, Venue.address == item.address)
    )
    created = venue is None
    if venue is None:
        venue = Venue(
            name=item.venue_name,
            neighborhood=item.neighborhood,
            address=item.address,
            city=item.city,
            state=item.state,
            postal_code=item.postal_code,
            latitude=item.latitude,
            longitude=item.longitude,
            apple_place_id=item.apple_place_id,
        )
        session.add(venue)
        await session.flush()
    else:
        venue.neighborhood = item.neighborhood
        venue.city = item.city
        venue.state = item.state
        venue.postal_code = item.postal_code
        venue.latitude = item.latitude
        venue.longitude = item.longitude
        if item.apple_place_id:
            venue.apple_place_id = item.apple_place_id

    return venue, created


async def _upsert_event(
    session: AsyncSession,
    item: IngestCandidateItem,
    source_id: str,
) -> tuple[CanonicalEvent, bool]:
    event = await session.scalar(
        select(CanonicalEvent).where(CanonicalEvent.source_event_key == item.source_event_key)
    )
    created = event is None
    if event is None:
        event = CanonicalEvent(
            source_id=source_id,
            source_event_key=item.source_event_key,
            title=item.title,
            category=item.category,
            summary=item.summary,
        )
        session.add(event)
        await session.flush()
    else:
        event.source_id = source_id
        event.title = item.title
        event.category = item.category
        event.summary = item.summary

    return event, created


async def _upsert_occurrence(
    session: AsyncSession,
    item: IngestCandidateItem,
    event_id: str,
    venue_id: str,
) -> tuple[EventOccurrence, bool]:
    await _retire_competing_occurrences(session, event_id, venue_id, keep_starts_at=item.starts_at)
    occurrence = await session.scalar(
        select(EventOccurrence).where(
            EventOccurrence.event_id == event_id,
            EventOccurrence.venue_id == venue_id,
            EventOccurrence.starts_at == item.starts_at,
        )
    )
    created = occurrence is None
    metadata = {
        "sourceConfidence": item.source_confidence,
        "topicKeys": item.topic_keys,
        "tags": item.tags,
    }
    if occurrence is None:
        occurrence = EventOccurrence(
            event_id=event_id,
            venue_id=venue_id,
            starts_at=item.starts_at,
            ends_at=item.ends_at,
            min_price=item.min_price,
            max_price=item.max_price,
            ticket_url=item.ticket_url,
            metadata_json=metadata,
            is_active=True,
        )
        session.add(occurrence)
        await session.flush()
    else:
        occurrence.ends_at = item.ends_at
        occurrence.min_price = item.min_price
        occurrence.max_price = item.max_price
        occurrence.ticket_url = item.ticket_url
        occurrence.metadata_json = metadata
        occurrence.is_active = True

    return occurrence, created


async def _retire_competing_occurrences(
    session: AsyncSession,
    event_id: str,
    venue_id: str,
    *,
    keep_starts_at: str,
) -> None:
    competing_occurrences = list(
        (
            await session.scalars(
                select(EventOccurrence).where(
                    EventOccurrence.event_id == event_id,
                    EventOccurrence.venue_id == venue_id,
                    EventOccurrence.starts_at != keep_starts_at,
                    EventOccurrence.is_active.is_(True),
                )
            )
        ).all()
    )
    for occurrence in competing_occurrences:
        occurrence.is_active = False


async def _upsert_geocode(session: AsyncSession, venue_id: str, provider_place_id: str) -> None:
    geocode = await session.scalar(
        select(VenueGeocode).where(VenueGeocode.venue_id == venue_id, VenueGeocode.provider == "apple")
    )
    if geocode is None:
        session.add(
            VenueGeocode(
                venue_id=venue_id,
                provider="apple",
                provider_place_id=provider_place_id,
                raw_response_json={"source": "connector"},
            )
        )
        await session.flush()
        return

    geocode.provider_place_id = provider_place_id
    geocode.raw_response_json = {"source": "connector"}
