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


class RecommendationDriverSummary(BaseModel):
    key: str
    label: str
    impactLabel: str
    averageContribution: float
    venueCount: int
    topVenues: list[str] = Field(default_factory=list)


class RecommendationFeedbackReasonSummary(BaseModel):
    key: str
    label: str
    count: int
    weightedStrength: float


class RecommendationMovementCue(BaseModel):
    key: str
    label: str
    delta: float
    direction: str = "positive"


class RecommendationDebugVenue(BaseModel):
    rank: int
    venueId: str
    venueName: str
    score: float
    scoreBand: str
    scoreSummary: str | None = None
    topDrivers: list[RecommendationScoreBreakdownItem] = Field(default_factory=list)


class RecommendationDebugSummary(BaseModel):
    runId: str | None = None
    generatedAt: str | None = None
    rankingModel: str | None = None
    contextHash: str | None = None
    shortlistSize: int = 0
    summary: str | None = None
    mapContext: MapContext = Field(default_factory=MapContext)
    activeTopics: list[str] = Field(default_factory=list)
    mutedTopics: list[str] = Field(default_factory=list)
    topSaveReasons: list[RecommendationFeedbackReasonSummary] = Field(default_factory=list)
    topConfirmedSaveReasons: list[RecommendationFeedbackReasonSummary] = Field(default_factory=list)
    topDismissReasons: list[RecommendationFeedbackReasonSummary] = Field(default_factory=list)
    topPositiveDrivers: list[RecommendationDriverSummary] = Field(default_factory=list)
    topNegativeDrivers: list[RecommendationDriverSummary] = Field(default_factory=list)
    venues: list[RecommendationDebugVenue] = Field(default_factory=list)


class RecommendationRunComparisonItem(BaseModel):
    venueId: str
    venueName: str
    neighborhood: str
    currentRank: int | None = None
    previousRank: int | None = None
    rankDelta: int | None = None
    currentScore: float | None = None
    previousScore: float | None = None
    scoreDelta: float | None = None
    scoreBand: str | None = None
    scoreSummary: str | None = None
    movementCues: list[RecommendationMovementCue] = Field(default_factory=list)
    movement: str


class RecommendationRunComparison(BaseModel):
    currentRunId: str | None = None
    previousRunId: str | None = None
    currentGeneratedAt: str | None = None
    previousGeneratedAt: str | None = None
    currentContextHash: str | None = None
    previousContextHash: str | None = None
    summary: str | None = None
    shortlistSize: int = 0
    comparableVenueCount: int = 0
    newEntrants: list[RecommendationRunComparisonItem] = Field(default_factory=list)
    droppedVenues: list[RecommendationRunComparisonItem] = Field(default_factory=list)
    movers: list[RecommendationRunComparisonItem] = Field(default_factory=list)
    steadyLeaders: list[RecommendationRunComparisonItem] = Field(default_factory=list)


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


class RecommendationInteractionEvent(BaseModel):
    recommendationId: str
    action: str


class RecommendationInteractionsPayload(BaseModel):
    events: list[RecommendationInteractionEvent] = Field(default_factory=list)
