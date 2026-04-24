from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Iterable
import re
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from selectolax.parser import HTMLParser

from app.models.contracts import CandidateEvent, RetrievalQuery

NYC_TZ = ZoneInfo("America/New_York")
QUERY_STOPWORDS = {
    "and",
    "brooklyn",
    "city",
    "for",
    "live",
    "music",
    "new",
    "nyc",
    "the",
}
TOPIC_HINTS = {
    "underground_dance": ["club", "dance", "dj", "electronic", "house", "rave", "techno", "warehouse"],
    "indie_live_music": ["alt-pop", "band", "concert", "indie", "live", "performance", "show", "songwriter"],
    "gallery_nights": ["art", "exhibition", "film", "gallery", "installation", "opening", "screening", "visual"],
    "creative_meetups": ["community", "conversation", "meetup", "networking", "talk", "workshop"],
    "collector_marketplaces": ["market", "swap", "fair", "collector", "vintage", "bazaar"],
    "student_intellectual_scene": ["book", "campus", "discussion", "lecture", "reading", "seminar", "talk"],
    "ambitious_professional_scene": ["career", "founder", "industry", "networking", "panel", "professional"],
    "style_design_shopping": ["boutique", "design", "fashion", "popup", "shopping", "thrift", "vintage"],
}
MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
PUBLIC_RECORDS_SPACE_PREFIXES = [
    "Sound Room The Atrium Upstairs",
    "The Atrium Upstairs",
    "Sound Room The Atrium",
    "Sound Room Upstairs",
    "The Atrium",
    "Sound Room",
    "Upstairs",
    "Nursery",
]
PUBLIC_RECORDS_LISTING_PATTERN = re.compile(
    r"^(?P<weekday>[A-Za-z]{3})\s+"
    r"(?P<month>\d{1,2})\.(?P<day>\d{1,2})\s+"
    r"(?P<category>[A-Za-z/& ]+?),\s+"
    r"(?P<time>\d{1,2}:\d{2}\s*[ap]m),\s+"
    r"(?P<rest>.+)$"
)
PIONEER_DATE_PATTERN = re.compile(
    r"^(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+"
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+"
    r"(?P<day>\d{1,2})$"
)
PIONEER_START_PATTERN = re.compile(
    r"Start:\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})\s*\|\s*"
    r"(?P<time>\d{1,2}:\d{2}\s*[APMapm]{2})"
)


@dataclass(frozen=True)
class VenueMetadata:
    venue_name: str
    neighborhood: str
    address: str
    city: str
    state: str
    postal_code: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class CuratedVenueSource:
    key: str
    listing_url: str
    venue: VenueMetadata
    default_category: str
    default_duration_hours: int
    parser_name: str


PUBLIC_RECORDS = CuratedVenueSource(
    key="public-records",
    listing_url="https://publicrecords.nyc/",
    venue=VenueMetadata(
        venue_name="Public Records",
        neighborhood="Gowanus",
        address="233 Butler St, Brooklyn, NY",
        city="New York City",
        state="NY",
        postal_code="11217",
        latitude=40.6784,
        longitude=-73.9896,
    ),
    default_category="culture",
    default_duration_hours=3,
    parser_name="public_records",
)
PIONEER_WORKS = CuratedVenueSource(
    key="pioneer-works",
    listing_url="https://pioneerworks.org/calendar",
    venue=VenueMetadata(
        venue_name="Pioneer Works",
        neighborhood="Red Hook",
        address="159 Pioneer St, Brooklyn, NY",
        city="New York City",
        state="NY",
        postal_code="11231",
        latitude=40.6778,
        longitude=-74.0128,
    ),
    default_category="culture",
    default_duration_hours=3,
    parser_name="pioneer_works",
)
CURATED_SOURCES = [PUBLIC_RECORDS, PIONEER_WORKS]


class CuratedVenueConnector:
    source_name = "curated_venues"

    def __init__(self) -> None:
        self._cached_candidates: list[CandidateEvent] | None = None

    async def search(self, query: RetrievalQuery) -> list[CandidateEvent]:
        if self._cached_candidates is None:
            self._cached_candidates = await self._load_candidates()

        return [
            candidate
            for candidate in self._cached_candidates
            if _matches_query(candidate, query)
        ]

    async def _load_candidates(self) -> list[CandidateEvent]:
        source_candidates: list[CandidateEvent] = []

        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "PulseWorker/0.1 (+https://github.com/duyhuynh-dev/pulse)"},
        ) as client:
            for source in CURATED_SOURCES:
                try:
                    response = await client.get(source.listing_url)
                    response.raise_for_status()
                except httpx.HTTPError:
                    continue

                html = response.text
                if source.parser_name == "public_records":
                    source_candidates.extend(_parse_public_records_html(html, source))
                    continue

                if source.parser_name == "pioneer_works":
                    source_candidates.extend(await _parse_pioneer_works_calendar(client, html, source))

        return _dedupe_candidates(source_candidates) or _demo_fallback_candidates(self.source_name)


def _dedupe_candidates(candidates: list[CandidateEvent]) -> list[CandidateEvent]:
    seen_source_keys: set[str] = set()
    deduped: list[CandidateEvent] = []

    for candidate in sorted(candidates, key=lambda item: item.starts_at):
        if candidate.source_event_key in seen_source_keys:
            continue
        seen_source_keys.add(candidate.source_event_key)
        deduped.append(candidate)

    return deduped


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _query_terms(value: str) -> list[str]:
    return [
        term
        for term in re.split(r"[^a-z0-9]+", value.lower())
        if len(term) > 2 and term not in QUERY_STOPWORDS
    ]


def _derive_topic_keys(*parts: str) -> list[str]:
    text = " ".join(part.lower() for part in parts if part)
    return [
        key
        for key, hints in TOPIC_HINTS.items()
        if any(hint in text for hint in hints)
    ]


def _infer_event_year(month: int, day: int) -> int:
    now = datetime.now(tz=NYC_TZ)
    candidate = datetime(now.year, month, day, tzinfo=NYC_TZ)
    if candidate < now - timedelta(days=90):
        return now.year + 1
    return now.year


def _parse_local_datetime(month: int, day: int, time_text: str, *, year: int | None = None) -> datetime:
    resolved_year = year or _infer_event_year(month, day)
    naive = datetime.strptime(
        f"{resolved_year}-{month:02d}-{day:02d} {time_text.upper()}",
        "%Y-%m-%d %I:%M %p",
    )
    return naive.replace(tzinfo=NYC_TZ)


def _split_public_records_listing(rest: str) -> tuple[str, str]:
    cleaned = re.sub(r"(Get tickets\s*)+$", "", _normalize_text(rest), flags=re.IGNORECASE).strip(" ,")
    for prefix in PUBLIC_RECORDS_SPACE_PREFIXES:
        if cleaned.startswith(prefix):
            title = cleaned[len(prefix) :].strip(" ,-")
            if title:
                return prefix, title
    return "Public Records", cleaned


def _candidate_event(
    *,
    source_name: str,
    source_key: str,
    venue: VenueMetadata,
    title: str,
    summary: str,
    category: str,
    starts_at: datetime,
    ends_at: datetime,
    ticket_url: str,
    min_price: float | None,
    max_price: float | None,
    source_confidence: float,
    tags: Iterable[str],
) -> CandidateEvent:
    tag_list = [_normalize_text(tag) for tag in tags if tag]
    topic_keys = _derive_topic_keys(title, summary, category, *tag_list)
    source_event_key = f"{source_key}:{_slugify(title)}:{starts_at.date().isoformat()}"
    return CandidateEvent(
        source=source_name,
        source_kind="curated_calendar",
        source_event_key=source_event_key,
        venue_name=venue.venue_name,
        neighborhood=venue.neighborhood,
        address=venue.address,
        city=venue.city,
        state=venue.state,
        postal_code=venue.postal_code,
        title=title,
        summary=summary,
        category=category,
        starts_at=starts_at.astimezone(UTC).isoformat(),
        ends_at=ends_at.astimezone(UTC).isoformat(),
        latitude=venue.latitude,
        longitude=venue.longitude,
        ticket_url=ticket_url,
        min_price=min_price,
        max_price=max_price,
        source_confidence=source_confidence,
        topic_keys=topic_keys,
        tags=tag_list,
    )


def _matches_query(candidate: CandidateEvent, query: RetrievalQuery) -> bool:
    terms = _query_terms(query.query)
    blob = " ".join(
        [
            candidate.title.lower(),
            (candidate.summary or "").lower(),
            candidate.category.lower(),
            candidate.venue_name.lower(),
            candidate.neighborhood.lower(),
            " ".join(candidate.tags).lower(),
            " ".join(candidate.topic_keys).lower(),
        ]
    )
    if not terms:
        return True
    return any(term in blob for term in terms)


def _extract_meta_description(document: HTMLParser) -> str:
    for selector in ('meta[name="description"]', 'meta[property="og:description"]'):
        node = document.css_first(selector)
        if node and node.attributes.get("content"):
            return _normalize_text(node.attributes["content"])
    return ""


def _parse_public_records_html(html: str, source: CuratedVenueSource) -> list[CandidateEvent]:
    document = HTMLParser(html)
    parsed: list[CandidateEvent] = []
    seen: set[str] = set()

    for anchor in document.css("a"):
        href = anchor.attributes.get("href", "")
        text = _normalize_text(anchor.text(separator=" ", strip=True))
        match = PUBLIC_RECORDS_LISTING_PATTERN.match(text)
        if not match:
            continue

        month = int(match.group("month"))
        day = int(match.group("day"))
        category = _normalize_text(match.group("category"))
        time_text = _normalize_text(match.group("time"))
        room_name, title = _split_public_records_listing(match.group("rest"))
        if not title:
            continue

        starts_at = _parse_local_datetime(month, day, time_text)
        candidate = _candidate_event(
            source_name="curated_venues",
            source_key=source.key,
            venue=source.venue,
            title=title,
            summary=f"{category} program in {room_name} at Public Records.",
            category="live music" if category.lower() == "live" else source.default_category,
            starts_at=starts_at,
            ends_at=starts_at + timedelta(hours=source.default_duration_hours),
            ticket_url=urljoin(source.listing_url, href),
            min_price=None,
            max_price=None,
            source_confidence=0.86,
            tags=["public records", category.lower(), room_name.lower()],
        )
        if candidate.source_event_key in seen:
            continue
        seen.add(candidate.source_event_key)
        parsed.append(candidate)

    return parsed


def _anchor_href_map(document: HTMLParser, source: CuratedVenueSource) -> dict[str, str]:
    href_map: dict[str, str] = {}
    for anchor in document.css("a"):
        href = anchor.attributes.get("href", "")
        text = _normalize_text(anchor.text(separator=" ", strip=True))
        if not text or not href:
            continue
        href_map[text.lower()] = urljoin(source.listing_url, href)
    return href_map


def _calendar_lines(document: HTMLParser) -> list[str]:
    body = document.body
    if body is None:
        return []
    return [
        _normalize_text(line)
        for line in body.text(separator="\n", strip=True).splitlines()
        if _normalize_text(line)
    ]


async def _parse_pioneer_works_calendar(
    client: httpx.AsyncClient,
    html: str,
    source: CuratedVenueSource,
) -> list[CandidateEvent]:
    document = HTMLParser(html)
    href_map = _anchor_href_map(document, source)
    lines = _calendar_lines(document)
    current_month: int | None = None
    current_day: int | None = None
    parsed: list[CandidateEvent] = []
    seen: set[str] = set()

    for line in lines:
        heading_match = PIONEER_DATE_PATTERN.match(line.upper())
        if heading_match:
            month_name = line.split(", ", 1)[1].rsplit(" ", 1)[0]
            current_month = MONTH_NAME_TO_NUMBER[month_name.lower()]
            current_day = int(heading_match.group("day"))
            continue

        if current_month is None or current_day is None:
            continue
        if not line.lower().endswith(" program"):
            continue

        title = re.sub(r"\s+program$", "", line, flags=re.IGNORECASE).strip(" -")
        detail_url = href_map.get(line.lower()) or href_map.get(title.lower())
        detail = await _fetch_pioneer_works_detail(client, detail_url, current_month, current_day)
        candidate = _candidate_event(
            source_name="curated_venues",
            source_key=source.key,
            venue=source.venue,
            title=title,
            summary=detail["summary"],
            category=detail["category"],
            starts_at=detail["starts_at"],
            ends_at=detail["ends_at"],
            ticket_url=detail_url or source.listing_url,
            min_price=None,
            max_price=None,
            source_confidence=0.82,
            tags=["pioneer works", detail["category"], "calendar"],
        )
        if candidate.source_event_key in seen:
            continue
        seen.add(candidate.source_event_key)
        parsed.append(candidate)

    return parsed


async def _fetch_pioneer_works_detail(
    client: httpx.AsyncClient,
    detail_url: str | None,
    month: int,
    day: int,
) -> dict[str, object]:
    default_start = _parse_local_datetime(month, day, "7:00 PM")
    fallback = {
        "summary": "Program at Pioneer Works in Red Hook.",
        "category": "culture",
        "starts_at": default_start,
        "ends_at": default_start + timedelta(hours=3),
    }
    if not detail_url:
        return fallback

    try:
        response = await client.get(detail_url)
        response.raise_for_status()
    except httpx.HTTPError:
        return fallback

    document = HTMLParser(response.text)
    body = document.body
    if body is None:
        return fallback

    text = _normalize_text(body.text(separator="\n", strip=True))
    meta_description = _extract_meta_description(document)
    start_match = PIONEER_START_PATTERN.search(text)
    if start_match:
        month_number = MONTH_NAME_TO_NUMBER[start_match.group("month").lower()]
        starts_at = _parse_local_datetime(
            month_number,
            int(start_match.group("day")),
            _normalize_text(start_match.group("time")),
            year=int(start_match.group("year")),
        )
    else:
        starts_at = default_start

    summary = meta_description or fallback["summary"]
    category = "live music" if any(word in summary.lower() for word in ("music", "performance", "concert")) else "culture"
    return {
        "summary": summary,
        "category": category,
        "starts_at": starts_at,
        "ends_at": starts_at + timedelta(hours=3),
    }


def _demo_fallback_candidates(source_name: str) -> list[CandidateEvent]:
    return [
        CandidateEvent(
            source=source_name,
            source_kind="curated_feed",
            source_event_key="curated:elsewhere-late-night-textures",
            venue_name="Elsewhere",
            neighborhood="Bushwick",
            address="599 Johnson Ave, Brooklyn, NY",
            city="New York City",
            state="NY",
            postal_code="11237",
            title="Late-night warehouse textures",
            summary="A deeper late-night lineup with warehouse techno energy and visual atmosphere.",
            category="live music",
            starts_at="2026-04-25T23:30:00+00:00",
            ends_at="2026-04-26T05:00:00+00:00",
            latitude=40.7063,
            longitude=-73.9232,
            ticket_url="https://www.elsewherebrooklyn.com/events",
            min_price=32,
            max_price=40,
            source_confidence=0.84,
            topic_keys=["underground_dance"],
            tags=["underground dance", "brooklyn"],
        ),
        CandidateEvent(
            source=source_name,
            source_kind="curated_feed",
            source_event_key="curated:public-records-listening-room",
            venue_name="Public Records",
            neighborhood="Gowanus",
            address="233 Butler St, Brooklyn, NY",
            city="New York City",
            state="NY",
            postal_code="11217",
            title="Ambient listening room session",
            summary="A seated, art-forward listening-room program with light visuals and experimental sets.",
            category="culture",
            starts_at="2026-04-27T00:00:00+00:00",
            ends_at="2026-04-27T03:00:00+00:00",
            latitude=40.6784,
            longitude=-73.9896,
            ticket_url="https://publicrecords.nyc/calendar",
            min_price=20,
            max_price=28,
            source_confidence=0.8,
            topic_keys=["gallery_nights"],
            tags=["listening room", "culture"],
        ),
        CandidateEvent(
            source=source_name,
            source_kind="curated_feed",
            source_event_key="curated:lpr-intimate-alt-pop",
            venue_name="Le Poisson Rouge",
            neighborhood="Greenwich Village",
            address="158 Bleecker St, New York, NY",
            city="New York City",
            state="NY",
            postal_code="10012",
            title="Intimate alt-pop songwriter night",
            summary="A small-room performance built around indie songwriting and close crowd energy.",
            category="live music",
            starts_at="2026-04-28T00:30:00+00:00",
            ends_at="2026-04-28T03:00:00+00:00",
            latitude=40.7285,
            longitude=-74.0005,
            ticket_url="https://lpr.com/",
            min_price=30,
            max_price=45,
            source_confidence=0.82,
            topic_keys=["indie_live_music"],
            tags=["indie live music", "songwriter"],
        ),
    ]
