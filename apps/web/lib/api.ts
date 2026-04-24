import type {
  ArchiveResponse,
  AuthViewer,
  DigestPreviewResponse,
  DigestSendResponse,
  EmailPreferences,
  FeedbackReason,
  InterestTopic,
  LocationAnchorPayload,
  RecommendationsMapResponse,
  SupplySyncResponse,
  ThemeCatalogItem,
  TasteProfileResponse,
  UserConstraint
} from "@/lib/types";
import { getSupabaseBrowserClient } from "@/lib/supabase-browser";

function resolveApiUrl() {
  if (process.env.NEXT_PUBLIC_API_URL) {
    return process.env.NEXT_PUBLIC_API_URL;
  }

  if (typeof window !== "undefined") {
    const host = window.location.hostname;
    if (host === "127.0.0.1" || host === "localhost") {
      return `${window.location.protocol}//${host}:8000`;
    }
  }

  return "http://localhost:8000";
}

const API_URL = resolveApiUrl();

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  headers.set("Content-Type", "application/json");

  if (typeof window !== "undefined") {
    const supabase = getSupabaseBrowserClient();
    if (supabase) {
      const {
        data: { session }
      } = await supabase.auth.getSession();
      if (session?.access_token) {
        headers.set("Authorization", `Bearer ${session.access_token}`);
      }
    }
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...init,
    headers,
    cache: "no-store",
    credentials: "include",
  });

  if (!response.ok) {
    let detail = `Request failed for ${path} with status ${response.status}`;
    const contentType = response.headers.get("content-type") ?? "";

    if (contentType.includes("application/json")) {
      const payload = await response.json().catch(() => null);
      if (payload && typeof payload === "object" && "detail" in payload && typeof payload.detail === "string") {
        detail = payload.detail;
      }
    } else {
      const text = await response.text().catch(() => "");
      if (text) {
        detail = text;
      }
    }

    throw new Error(detail);
  }

  return response.json() as Promise<T>;
}

export function getMapRecommendations() {
  return request<RecommendationsMapResponse>("/v1/recommendations/map");
}

export function refreshRecommendations() {
  return request<{ ok: true }>("/v1/recommendations/refresh", {
    method: "POST"
  });
}

export function syncSupply() {
  return request<SupplySyncResponse>("/v1/supply/sync", {
    method: "POST"
  });
}

export function getDigestPreview() {
  return request<DigestPreviewResponse>("/v1/digest/preview");
}

export function sendDigestPreview() {
  return request<DigestSendResponse>("/v1/digest/send-preview", {
    method: "POST"
  });
}

export function getEmailPreferences() {
  return request<EmailPreferences>("/v1/profile/email-preferences");
}

export function saveEmailPreferences(payload: EmailPreferences) {
  return request<EmailPreferences>("/v1/profile/email-preferences", {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

export function getArchive() {
  return request<ArchiveResponse>("/v1/recommendations/archive");
}

export function getInterests() {
  return request<{ topics: InterestTopic[] }>("/v1/profile/interests");
}

export function patchInterests(topics: InterestTopic[]) {
  return request<{ topics: InterestTopic[] }>("/v1/profile/interests", {
    method: "PATCH",
    body: JSON.stringify({ topics })
  });
}

export function submitFeedback(
  recommendationId: string,
  action: "save" | "dismiss",
  reasons: FeedbackReason[],
) {
  return request<{ ok: true }>(`/v1/recommendations/${recommendationId}/feedback`, {
    method: "POST",
    body: JSON.stringify({ action, reasons })
  });
}

export function getMapToken() {
  return request<{ enabled: boolean; token: string | null }>("/v1/maps/token");
}

export function saveAnchor(payload: LocationAnchorPayload) {
  return request<{ ok: true }>("/v1/profile/anchor", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function saveConstraints(payload: UserConstraint) {
  return request<{ ok: true }>("/v1/profile/constraints", {
    method: "POST",
    body: JSON.stringify(payload)
  });
}

export function getAuthViewer() {
  return request<AuthViewer>("/v1/auth/me");
}

export function signOutPulseSession() {
  return request<{ ok: true }>("/v1/auth/sign-out", {
    method: "POST"
  });
}

export function startRedditConnection() {
  return request<{ authorizeUrl: string }>("/v1/reddit/connect/start", {
    method: "POST"
  });
}

export function startMockRedditConnection() {
  return request<{ ok: true }>("/v1/reddit/mock-connect", {
    method: "POST"
  });
}

export function startSpotifyConnection() {
  return request<{ authorizeUrl: string }>("/v1/spotify/connect/start", {
    method: "POST"
  });
}

export function getTasteThemes() {
  return request<{ items: ThemeCatalogItem[] }>("/v1/taste/themes");
}

export function previewManualTaste(selectedThemeIds: string[]) {
  return request<TasteProfileResponse>("/v1/taste/manual/preview", {
    method: "POST",
    body: JSON.stringify({ selectedThemeIds })
  });
}

export function applyManualTaste(selectedThemeIds: string[]) {
  return request<TasteProfileResponse>("/v1/taste/manual/apply", {
    method: "POST",
    body: JSON.stringify({ selectedThemeIds })
  });
}

export function getSpotifyTastePreview() {
  return request<TasteProfileResponse>("/v1/taste/spotify/preview");
}

export function applySpotifyTaste() {
  return request<TasteProfileResponse>("/v1/taste/spotify/apply", {
    method: "POST"
  });
}
