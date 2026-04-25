from pydantic import BaseModel, Field


class RecommendationReason(BaseModel):
    title: str
    detail: str


class TravelEstimate(BaseModel):
    mode: str
    label: str
    minutes: int


class RecommendationFreshness(BaseModel):
    discoveredAt: str | None = None
    lastVerifiedAt: str | None = None
    freshnessLabel: str = "Recently refreshed"


class RecommendationProvenance(BaseModel):
    sourceName: str
    sourceKind: str
    sourceConfidence: float
    sourceConfidenceLabel: str
    sourceBaseUrl: str | None = None
    hasTicketUrl: bool = False


class RecommendationScoreBreakdownItem(BaseModel):
    key: str
    label: str
    impactLabel: str
    detail: str
    contribution: float
    direction: str = "positive"


class SecondaryEvent(BaseModel):
    eventId: str
    title: str
    startsAt: str


class VenueRecommendationCard(BaseModel):
    venueId: str
    venueName: str
    neighborhood: str
    address: str
    eventTitle: str
    eventId: str
    startsAt: str
    priceLabel: str
    scoreBand: str
    score: float
    travel: list[TravelEstimate] = Field(default_factory=list)
    reasons: list[RecommendationReason] = Field(default_factory=list)
    freshness: RecommendationFreshness = Field(default_factory=RecommendationFreshness)
    provenance: RecommendationProvenance
    scoreSummary: str | None = None
    scoreBreakdown: list[RecommendationScoreBreakdownItem] = Field(default_factory=list)
    secondaryEvents: list[SecondaryEvent] = Field(default_factory=list)


class MapVenuePin(BaseModel):
    venueId: str
    venueName: str
    latitude: float
    longitude: float
    scoreBand: str
    selected: bool = False


class MapViewport(BaseModel):
    latitude: float
    longitude: float
    latitudeDelta: float
    longitudeDelta: float


class MapContext(BaseModel):
    serviceArea: str = "New York City"
    activeAnchorLabel: str = "NYC"
    activeAnchorSource: str = "default"
    requestedAnchorLabel: str | None = None
    requestedAnchorSource: str | None = None
    requestedAnchorWithinServiceArea: bool = True
    usedFallbackAnchor: bool = False
    fallbackReason: str | None = None


class RecommendationsMapResponse(BaseModel):
    viewport: MapViewport
    pins: list[MapVenuePin]
    cards: dict[str, VenueRecommendationCard]
    generatedAt: str
    displayTimezone: str = "America/New_York"
    userConstraint: dict
    mapContext: MapContext = Field(default_factory=MapContext)


class ArchiveSnapshot(BaseModel):
    runId: str
    kind: str
    title: str
    generatedAt: str
    deliveredAt: str | None = None
    items: list[VenueRecommendationCard] = Field(default_factory=list)


class ArchiveResponse(BaseModel):
    items: list[VenueRecommendationCard]
    displayTimezone: str = "America/New_York"
    history: list[ArchiveSnapshot] = Field(default_factory=list)


class FeedbackReason(BaseModel):
    key: str
    label: str


class FeedbackPayload(BaseModel):
    action: str
    reasons: list[FeedbackReason] = Field(default_factory=list)
