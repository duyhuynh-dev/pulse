from __future__ import annotations

from datetime import UTC, datetime, timedelta
import httpx

from app.connectors.curated_venues import CuratedVenueConnector
from app.connectors.ticketmaster import TicketmasterConnector
from app.core.config import get_settings
from app.models.contracts import CandidateEvent, RetrievalQuery

DEFAULT_SUPPLY_QUERIES = [
    RetrievalQuery(query="techno brooklyn", source="ticketmaster", category="live music"),
    RetrievalQuery(query="indie live music nyc", source="ticketmaster", category="live music"),
    RetrievalQuery(query="gallery installation nyc", source="curated_venues", category="culture"),
    RetrievalQuery(query="indie songwriter brooklyn", source="curated_venues", category="live music"),
]
SUPPLY_LOOKAHEAD = timedelta(days=90)


def build_daily_supply_queries() -> list[RetrievalQuery]:
    return DEFAULT_SUPPLY_QUERIES.copy()


async def collect_supply_candidates() -> list[CandidateEvent]:
    connectors = {
        "ticketmaster": TicketmasterConnector(),
        "curated_venues": CuratedVenueConnector(),
    }
    seen_keys: set[str] = set()
    seen_fingerprints: set[str] = set()
    candidates: list[CandidateEvent] = []

    for query in build_daily_supply_queries():
        connector = connectors.get(query.source)
        if connector is None:
            continue

        for candidate in await connector.search(query):
            if not _candidate_is_usable(candidate):
                continue
            fingerprint = _dedupe_fingerprint(candidate)
            if candidate.source_event_key in seen_keys or fingerprint in seen_fingerprints:
                continue
            seen_keys.add(candidate.source_event_key)
            seen_fingerprints.add(fingerprint)
            candidates.append(candidate)

    return candidates


def _dedupe_fingerprint(candidate: CandidateEvent) -> str:
    normalized_title = _normalize_fingerprint_text(candidate.title)
    normalized_venue = _normalize_fingerprint_text(candidate.venue_name)
    starts_on = candidate.starts_at[:10]
    return f"{normalized_venue}|{normalized_title}|{starts_on}"


def _normalize_fingerprint_text(value: str) -> str:
    normalized = "".join(character.lower() if character.isalnum() else " " for character in value)
    return " ".join(normalized.split())


def _candidate_is_usable(candidate: CandidateEvent) -> bool:
    try:
        starts_at = datetime.fromisoformat(candidate.starts_at.replace("Z", "+00:00"))
    except ValueError:
        return False

    if starts_at.tzinfo is None:
        starts_at = starts_at.replace(tzinfo=UTC)

    now = datetime.now(tz=UTC)
    if starts_at < now - timedelta(hours=4):
        return False
    if starts_at > now + SUPPLY_LOOKAHEAD:
        return False
    if candidate.latitude == 0.0 or candidate.longitude == 0.0:
        return False
    return bool(candidate.title.strip() and candidate.venue_name.strip())


async def sync_supply_to_api(candidates: list[CandidateEvent]) -> dict:
    settings = get_settings()
    if not settings.api_base_url:
        return {"status": "skipped", "reason": "missing_api_base_url", "accepted": 0}

    headers: dict[str, str] = {}
    if settings.internal_ingest_secret:
        headers["x-pulse-ingest-secret"] = settings.internal_ingest_secret

    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            f"{settings.api_base_url}/v1/internal/ingest/candidates",
            json={"items": [candidate.model_dump(mode="json") for candidate in candidates]},
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()

    payload["status"] = "synced"
    return payload


async def run_daily_supply_sync() -> dict:
    candidates = await collect_supply_candidates()
    payload = await sync_supply_to_api(candidates)
    payload["candidate_count"] = len(candidates)
    return payload
