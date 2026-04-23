"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  sendDigestPreview,
  getAuthViewer,
  getEmailPreferences,
  getInterests,
  getMapRecommendations,
  patchInterests,
  refreshRecommendations,
  syncSupply,
  submitFeedback
} from "@/lib/api";
import type { InterestTopic, VenueRecommendationCard } from "@/lib/types";
import { useAuth } from "@/components/auth-provider";
import { AccountDock } from "@/components/account-dock";
import { InterestProfilePanel } from "@/components/interest-profile-panel";
import { RecommendationDrawer } from "@/components/recommendation-drawer";
import { PulseMap } from "@/components/pulse-map";
import { SettingsDock } from "@/components/settings-dock";
import { formatDigestTime } from "@/lib/utils";

export function PulseShell() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [selectedVenueId, setSelectedVenueId] = useState<string | null>(null);
  const [surfaceStatus, setSurfaceStatus] = useState<string | null>(null);
  const identityKey = user?.id ?? "demo";

  const viewerQuery = useQuery({
    queryKey: ["auth-viewer", identityKey],
    queryFn: getAuthViewer
  });
  const isAuthenticated = Boolean(viewerQuery.data?.isAuthenticated);
  const emailPreferencesQuery = useQuery({
    queryKey: ["email-preferences", identityKey],
    queryFn: getEmailPreferences,
    enabled: isAuthenticated,
  });

  const mapQuery = useQuery({
    queryKey: ["map-recommendations", identityKey],
    queryFn: getMapRecommendations
  });

  const interestsQuery = useQuery({
    queryKey: ["interests", identityKey],
    queryFn: getInterests
  });

  const toggleTopicMutation = useMutation({
    mutationFn: (topics: InterestTopic[]) => patchInterests(topics),
    onSuccess: (data) => {
      queryClient.setQueryData(["interests", identityKey], data);
      void queryClient.invalidateQueries({ queryKey: ["map-recommendations"] });
      void queryClient.invalidateQueries({ queryKey: ["archive"] });
    }
  });

  const feedbackMutation = useMutation({
    mutationFn: ({
      recommendationId,
      action
    }: {
      recommendationId: string;
      action: "save" | "dismiss";
    }) => submitFeedback(recommendationId, action, []),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["map-recommendations"] });
      void queryClient.invalidateQueries({ queryKey: ["archive"] });
    }
  });

  const refreshMutation = useMutation({
    mutationFn: refreshRecommendations,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["map-recommendations"] });
      void queryClient.invalidateQueries({ queryKey: ["archive"] });
    }
  });

  const syncSupplyMutation = useMutation({
    mutationFn: syncSupply,
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: ["map-recommendations"] });
      void queryClient.invalidateQueries({ queryKey: ["archive"] });
      setSurfaceStatus(
        `Synced ${data.candidateCount} candidates and saved ${data.accepted} fresh events into the catalog.`,
      );
    },
    onError: (error) => {
      setSurfaceStatus(error instanceof Error ? error.message : "Unable to sync fresh venue supply right now.");
    }
  });

  const digestPreviewMutation = useMutation({
    mutationFn: sendDigestPreview,
    onSuccess: (data) => {
      setSurfaceStatus(`Sent a digest preview to ${data.recipientEmail}.`);
    },
    onError: (error) => {
      setSurfaceStatus(error instanceof Error ? error.message : "Unable to send the digest preview right now.");
    }
  });

  const selectedCard: VenueRecommendationCard | null = useMemo(() => {
    if (!mapQuery.data) {
      return null;
    }

    const initialVenueId = mapQuery.data.pins.at(0)?.venueId ?? null;
    const activeVenueId = selectedVenueId ?? initialVenueId;
    if (!activeVenueId) {
      return null;
    }

    return mapQuery.data.cards[activeVenueId] ?? null;
  }, [mapQuery.data, selectedVenueId]);

  const applyTopicAction = (
    topic: InterestTopic,
    action: "boost" | "mute" | "reset"
  ) => {
    const topics = (interestsQuery.data?.topics ?? []).map((current) => {
      if (current.id !== topic.id) {
        return current;
      }

      if (action === "reset") {
        return {
          ...current,
          boosted: false,
          muted: false
        };
      }

      return {
        ...current,
        boosted: action === "boost" ? !current.boosted : false,
        muted: action === "mute" ? !current.muted : false
      };
    });

    toggleTopicMutation.mutate(topics);
  };

  const cadenceValue =
    isAuthenticated && emailPreferencesQuery.data
      ? emailPreferencesQuery.data.weeklyDigestEnabled
        ? `${emailPreferencesQuery.data.digestDay} ${formatDigestTime(emailPreferencesQuery.data.digestTimeLocal)}`
        : "Digest paused"
      : null;
  const locationSummary = mapQuery.data?.userConstraint?.neighborhood || mapQuery.data?.userConstraint?.zipCode || "NYC";
  const radiusSummary = mapQuery.data?.userConstraint?.radiusMiles ? `${mapQuery.data.userConstraint.radiusMiles} mi radius` : null;
  const topbarMessage =
    surfaceStatus ??
    (isAuthenticated
      ? [
          locationSummary !== "NYC" ? `Anchored to ${locationSummary}` : "Anchored to NYC",
          radiusSummary,
          cadenceValue ? (cadenceValue === "Digest paused" ? "Weekly digest paused" : `Digest sends ${cadenceValue}`) : null,
        ]
          .filter(Boolean)
          .join(" · ")
      : "Open Profile to sign in, save this map, and keep setup tucked behind Settings.");

  return (
    <main className="min-h-screen px-4 py-4 md:px-6 md:py-6">
      <div className="mx-auto flex max-w-[1680px] flex-col gap-4 xl:h-[calc(100vh-3rem)]">
        <header className="rounded-[1.5rem] border border-stroke/80 bg-card/80 px-5 py-3.5 shadow-float backdrop-blur">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-3 text-sm font-medium text-slate-600">
                <span className="text-lg font-semibold text-slate-900">Pulse</span>
                <span className="h-1.5 w-1.5 rounded-full bg-slate-300" />
                <span>{locationSummary}</span>
                {radiusSummary ? (
                  <>
                    <span className="h-1.5 w-1.5 rounded-full bg-slate-300" />
                    <span>{radiusSummary}</span>
                  </>
                ) : null}
                {cadenceValue ? (
                  <>
                    <span className="h-1.5 w-1.5 rounded-full bg-slate-300" />
                    <span>{cadenceValue === "Digest paused" ? "Digest paused" : `Digest ${cadenceValue}`}</span>
                  </>
                ) : null}
              </div>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-500">
                {topbarMessage}
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <Link
                href="/archive"
                className="rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white"
              >
                Weekly archive
              </Link>
              <SettingsDock showDigest={isAuthenticated} />
              <AccountDock />
            </div>
          </div>
        </header>

        <section className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,1.58fr)_minmax(23rem,0.82fr)]">
          <div className="flex min-h-[58vh] min-w-0 flex-col overflow-hidden rounded-[2rem] border border-stroke/80 bg-card/80 shadow-float">
            <div className="flex flex-col gap-4 border-b border-stroke/70 bg-white/84 px-5 py-4 backdrop-blur md:flex-row md:items-start md:justify-between">
              <div className="max-w-2xl">
                <h1 className="text-[1.9rem] font-semibold leading-tight text-slate-900">Your shortlist, on the map</h1>
                <p className="mt-2 text-sm leading-6 text-slate-500">
                  {mapQuery.data?.pins.length
                    ? `See the venues currently leading your map, then use signals on the right to reshape the stack live.`
                    : "Pulse will populate the map as soon as recommendations are ready."}
                </p>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={() => digestPreviewMutation.mutate()}
                  disabled={!isAuthenticated || digestPreviewMutation.isPending}
                  className="rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white disabled:opacity-60"
                  title={isAuthenticated ? "Email the current shortlist to yourself." : "Sign in to email this list."}
                >
                  {digestPreviewMutation.isPending ? "Emailing..." : "Email me this list"}
                </button>
                <button
                  type="button"
                  onClick={() => syncSupplyMutation.mutate()}
                  disabled={syncSupplyMutation.isPending || digestPreviewMutation.isPending}
                  className="rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white disabled:opacity-60"
                >
                  {syncSupplyMutation.isPending ? "Checking..." : "Check for new events"}
                </button>
                <button
                  type="button"
                  onClick={() => refreshMutation.mutate()}
                  disabled={refreshMutation.isPending || syncSupplyMutation.isPending || digestPreviewMutation.isPending}
                  className="rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white disabled:opacity-60"
                >
                  {refreshMutation.isPending ? "Re-ranking..." : "Re-rank now"}
                </button>
              </div>
            </div>

            <div className="map-surface flex-1 min-h-[540px] bg-card/80 xl:min-h-0">
              <PulseMap
                pins={mapQuery.data?.pins ?? []}
                viewport={mapQuery.data?.viewport ?? null}
                selectedVenueId={selectedCard?.venueId ?? null}
                onSelectVenue={setSelectedVenueId}
              />
            </div>
          </div>

          <div className="grid min-h-0 gap-4 xl:grid-rows-[auto,minmax(0,1fr)]">
            <InterestProfilePanel
              topics={interestsQuery.data?.topics ?? []}
              isLoading={interestsQuery.isLoading}
              isSaving={toggleTopicMutation.isPending}
              onAction={(topicId, action) => {
                const topic = (interestsQuery.data?.topics ?? []).find((current) => current.id === topicId);
                if (!topic) {
                  return;
                }
                applyTopicAction(topic, action);
              }}
            />

            <RecommendationDrawer
              loading={mapQuery.isLoading}
              cards={mapQuery.data?.cards ?? {}}
              selectedVenueId={selectedCard?.venueId ?? null}
              onSelectVenue={setSelectedVenueId}
              onSave={(card) =>
                feedbackMutation.mutate({ recommendationId: card.eventId, action: "save" })
              }
              onDismiss={(card) =>
                feedbackMutation.mutate({ recommendationId: card.eventId, action: "dismiss" })
              }
            />
          </div>
        </section>
      </div>
    </main>
  );
}
