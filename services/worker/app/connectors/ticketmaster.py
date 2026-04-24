from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from app.core.config import get_settings
from app.models.contracts import CandidateEvent, RetrievalQuery

TOPIC_HINTS = {
    "underground_dance": ["techno", "warehouse", "dj", "rave", "dance"],
    "indie_live_music": ["indie", "band", "gig", "concert", "songwriter"],
    "gallery_nights": ["gallery", "art", "opening", "installation", "visual"],
    "creative_meetups": ["meetup", "creative", "networking", "community"],
    "collector_marketplaces": ["market", "swap", "fair", "collector", "vintage", "expo"],
    "student_intellectual_scene": ["reading", "lecture", "campus", "book", "talk", "discussion"],
    "ambitious_professional_scene": ["networking", "founder", "industry", "career", "business", "panel"],
    "style_design_shopping": ["design", "fashion", "boutique", "shopping", "thrift", "popup"],
}


class TicketmasterConnector:
    source_name = "ticketmaster"

    async def search(self, query: RetrievalQuery) -> list[CandidateEvent]:
        settings = get_settings()
        if not settings.ticketmaster_api_key:
            return []

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                "https://app.ticketmaster.com/discovery/v2/events.json",
                params={
                    "apikey": settings.ticketmaster_api_key,
                    "keyword": query.query,
                    "city": "New York",
                    "size": 10,
                },
            )
            response.raise_for_status()
            payload = response.json()

        events = []
        for item in payload.get("_embedded", {}).get("events", []):
            venue = item.get("_embedded", {}).get("venues", [{}])[0]
            location = venue.get("location", {})
            dates = item.get("dates", {}) or {}
            event_timezone = dates.get("timezone")
            starts_at = _normalize_ticketmaster_datetime(dates.get("start", {}), event_timezone)
            if starts_at is None:
                continue
            latitude = _coerce_coordinate(location.get("latitude"))
            longitude = _coerce_coordinate(location.get("longitude"))
            if latitude is None or longitude is None:
                continue
            address = venue.get("address", {}).get("line1") or venue.get("name", "Unknown venue")
            city = venue.get("city", {}).get("name") or "New York City"
            state = venue.get("state", {}).get("stateCode") or "NY"
            postal_code = venue.get("postalCode")
            classification = (item.get("classifications") or [{}])[0]
            genre = classification.get("genre", {}).get("name")
            segment = classification.get("segment", {}).get("name")
            text_blob = " ".join(filter(None, [query.query.lower(), genre.lower() if genre else "", segment.lower() if segment else ""]))
            topic_keys = [
                key for key, hints in TOPIC_HINTS.items() if any(hint in text_blob for hint in hints)
            ]
            events.append(
                CandidateEvent(
                    source=self.source_name,
                    source_kind="api_connector",
                    source_event_key=f"ticketmaster:{item.get('id', item.get('name', 'event'))}",
                    venue_name=venue.get("name", "Unknown venue"),
                    neighborhood=city,
                    address=address,
                    city=city,
                    state=state,
                    postal_code=postal_code,
                    title=item.get("name", "Untitled event"),
                    summary=item.get("info") or item.get("pleaseNote"),
                    category=segment or query.category,
                    starts_at=starts_at,
                    ends_at=_normalize_ticketmaster_datetime(dates.get("end", {}), event_timezone),
                    latitude=latitude,
                    longitude=longitude,
                    ticket_url=item.get("url"),
                    min_price=(item.get("priceRanges") or [{}])[0].get("min"),
                    max_price=(item.get("priceRanges") or [{}])[0].get("max"),
                    source_confidence=0.92,
                    topic_keys=topic_keys,
                    tags=[value for value in [query.category, query.query, segment, genre] if value],
                )
            )
        return events


def _coerce_coordinate(value: object) -> float | None:
    try:
        coordinate = float(value)
    except (TypeError, ValueError):
        return None
    if coordinate == 0.0:
        return None
    return coordinate


def _normalize_ticketmaster_datetime(payload: dict | None, fallback_timezone: str | None = None) -> str | None:
    if not payload:
        return None

    date_time = payload.get("dateTime")
    if isinstance(date_time, str) and date_time.strip():
        try:
            parsed = datetime.fromisoformat(date_time.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC).isoformat()

    local_date = payload.get("localDate")
    local_time = payload.get("localTime")
    if not isinstance(local_date, str) or not local_date.strip():
        return None
    if not isinstance(local_time, str) or not local_time.strip():
        return None

    timezone_name = payload.get("timezone") or fallback_timezone or "America/New_York"
    try:
        timezone = ZoneInfo(str(timezone_name))
    except ZoneInfoNotFoundError:
        timezone = ZoneInfo("America/New_York")

    for time_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            naive = datetime.strptime(f"{local_date} {local_time}", time_format)
            return naive.replace(tzinfo=timezone).astimezone(UTC).isoformat()
        except ValueError:
            continue

    return None
