export type ScoreBand = "high" | "medium" | "low";

export interface InterestTopic {
  id: string;
  label: string;
  confidence: number;
  sourceSignals: string[];
  boosted: boolean;
  muted: boolean;
}

export interface UserConstraint {
  city: string;
  neighborhood?: string | null;
  zipCode?: string | null;
  radiusMiles: number;
  budgetLevel: "free" | "under_30" | "under_75" | "flexible";
  preferredDays: string[];
  socialMode: "solo" | "group" | "either";
}

export interface RecommendationReason {
  title: string;
  detail: string;
}

export interface TravelEstimate {
  mode: "walk" | "transit";
  label: string;
  minutes: number;
}

export interface VenueRecommendationCard {
  venueId: string;
  venueName: string;
  neighborhood: string;
  address: string;
  eventTitle: string;
  eventId: string;
  startsAt: string;
  priceLabel: string;
  scoreBand: ScoreBand;
  score: number;
  travel: TravelEstimate[];
  reasons: RecommendationReason[];
  secondaryEvents: Array<{
    eventId: string;
    title: string;
    startsAt: string;
  }>;
}

export interface MapVenuePin {
  venueId: string;
  venueName: string;
  latitude: number;
  longitude: number;
  scoreBand: ScoreBand;
  selected: boolean;
}

export interface MapViewport {
  latitude: number;
  longitude: number;
  latitudeDelta: number;
  longitudeDelta: number;
}

export interface FeedbackReason {
  key: string;
  label: string;
}

export interface AuthViewer {
  email: string;
  displayName?: string | null;
  isAuthenticated: boolean;
  isDemo: boolean;
  redditConnected: boolean;
  redditConnectionMode: "none" | "live" | "sample";
}

export interface RecommendationsMapResponse {
  viewport: MapViewport;
  pins: MapVenuePin[];
  cards: Record<string, VenueRecommendationCard>;
  generatedAt: string;
  displayTimezone: string;
  userConstraint: UserConstraint;
}

export interface ArchiveSnapshot {
  runId: string;
  kind: "live" | "preview" | "scheduled" | "snapshot";
  title: string;
  generatedAt: string;
  deliveredAt?: string | null;
  items: VenueRecommendationCard[];
}

export interface ArchiveResponse {
  items: VenueRecommendationCard[];
  displayTimezone: string;
  history: ArchiveSnapshot[];
}

export interface LocationAnchorPayload {
  neighborhood?: string;
  zipCode?: string;
  latitude?: number;
  longitude?: number;
  source: "live" | "zip" | "neighborhood";
}

export interface SupplySyncResponse {
  ok: boolean;
  candidateCount: number;
  accepted: number;
  sourcesCreated: number;
  venuesCreated: number;
  eventsCreated: number;
  occurrencesCreated: number;
  status: string;
}

export interface DigestPreviewResponse {
  recipientEmail: string;
  subject: string;
  preheader: string;
  html: string;
  text: string;
  generatedAt: string;
  items: VenueRecommendationCard[];
}

export interface DigestSendResponse {
  ok: boolean;
  recipientEmail: string;
  provider: string;
  status: string;
}

export interface EmailPreferences {
  weeklyDigestEnabled: boolean;
  digestDay: string;
  digestTimeLocal: string;
  timezone: string;
}
