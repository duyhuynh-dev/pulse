import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.base import Base
from app.models.events import CanonicalEvent, EventOccurrence, EventSource, Venue
from app.schemas.ingestion import IngestCandidateItem
from app.services.ingestion import _upsert_occurrence


@pytest.mark.asyncio
async def test_upsert_occurrence_retires_active_sibling_occurrences() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        source = EventSource(name="ticketmaster", kind="api_connector")
        venue = Venue(
            name="Le Poisson Rouge",
            neighborhood="Greenwich Village",
            address="158 Bleecker St, New York, NY",
            city="New York City",
            state="NY",
            latitude=40.7285,
            longitude=-74.0005,
        )
        event = CanonicalEvent(
            source_id="pending",
            source_event_key="ticketmaster:event-1",
            title="Intimate Alt-Pop Performance",
            category="live music",
            summary="Small room show",
        )
        session.add(source)
        await session.flush()
        event.source_id = source.id
        session.add_all([venue, event])
        await session.flush()

        old_occurrence = EventOccurrence(
            event_id=event.id,
            venue_id=venue.id,
            starts_at="2026-04-27T08:45:24+00:00",
            is_active=True,
            metadata_json={},
        )
        session.add(old_occurrence)
        await session.flush()

        item = IngestCandidateItem(
            source="ticketmaster",
            source_event_key="ticketmaster:event-1",
            title="Intimate Alt-Pop Performance",
            starts_at="2026-04-28T00:30:00+00:00",
            venue_name=venue.name,
            address=venue.address,
            latitude=venue.latitude,
            longitude=venue.longitude,
        )

        _, created = await _upsert_occurrence(session, item, event.id, venue.id)
        await session.commit()

        occurrences = list(
            (
                await session.scalars(
                    select(EventOccurrence)
                    .where(EventOccurrence.event_id == event.id)
                    .order_by(EventOccurrence.starts_at.asc())
                )
            ).all()
        )

        assert created is True
        assert len(occurrences) == 2
        assert occurrences[0].starts_at == "2026-04-27T08:45:24+00:00"
        assert occurrences[0].is_active is False
        assert occurrences[1].starts_at == "2026-04-28T00:30:00+00:00"
        assert occurrences[1].is_active is True

    await engine.dispose()
