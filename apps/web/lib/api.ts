import type {
  ArchiveResponse,
  AuthViewer,
  DigestPreviewResponse,
  DigestSendResponse,
  FeedbackReason,
  InterestTopic,
  LocationAnchorPayload,
  RecommendationsMapResponse,
  SupplySyncResponse,
  UserConstraint
} from "@/lib/types";
import { getSupabaseBrowserClient } from "@/lib/supabase-browser";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

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
    cache: "no-store"
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
