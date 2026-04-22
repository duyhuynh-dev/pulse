import type {
  ArchiveResponse,
  AuthViewer,
  FeedbackReason,
  InterestTopic,
  LocationAnchorPayload,
  RecommendationsMapResponse,
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
    throw new Error(`Request failed for ${path} with status ${response.status}`);
  }

  return response.json() as Promise<T>;
}

export function getMapRecommendations() {
  return request<RecommendationsMapResponse>("/v1/recommendations/map");
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
  return request<{ token: string }>("/v1/maps/token");
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
