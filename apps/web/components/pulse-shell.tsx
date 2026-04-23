"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MapPinned, Compass, CalendarDays } from "lucide-react";
import {
  getAuthViewer,
  getInterests,
  getMapRecommendations,
  patchInterests,
  refreshRecommendations,
  submitFeedback
} from "@/lib/api";
import type { InterestTopic, VenueRecommendationCard } from "@/lib/types";
import { useAuth } from "@/components/auth-provider";
import { InterestProfilePanel } from "@/components/interest-profile-panel";
import { MagicLinkCard } from "@/components/sign-in-card";
import { LocationOnboardingCard } from "@/components/location-onboarding-card";
import { RecommendationDrawer } from "@/components/recommendation-drawer";
import { PulseMap } from "@/components/pulse-map";

export function PulseShell() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [selectedVenueId, setSelectedVenueId] = useState<string | null>(null);
  const identityKey = user?.id ?? "demo";

  const viewerQuery = useQuery({
    queryKey: ["auth-viewer", identityKey],
    queryFn: getAuthViewer
  });
  const isAuthenticated = Boolean(viewerQuery.data?.isAuthenticated);

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

  return (
    <main className="min-h-screen px-4 py-4 md:px-6 md:py-6">
      <div className="mx-auto flex max-w-[1600px] flex-col gap-4">
        <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-[2rem] border border-stroke/80 bg-card/80 p-6 shadow-float backdrop-blur">
            <div className="grid gap-6 xl:grid-cols-[1.18fr_0.82fr]">
              <div>
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <span className="inline-flex rounded-full bg-accentSoft px-3 py-1 text-xs font-semibold uppercase tracking-[0.22em] text-accent">
                      {viewerQuery.data?.isAuthenticated ? "Signed-In Beta" : "Private Beta Demo"}
                    </span>
                    <h1 className="mt-4 text-4xl font-semibold tracking-tight md:text-5xl">
                      Your Reddit taste, turned into a city map.
                    </h1>
                    <p className="mt-4 max-w-2xl text-sm leading-6 text-slate-600 md:text-base">
                      Pulse ranks NYC venues by how well their upcoming events match your current interests, then
                      highlights the best places directly on the map.
                    </p>
                    <p className="mt-3 text-sm text-slate-500">
                      {viewerQuery.data?.isAuthenticated
                        ? `Personalized for ${viewerQuery.data.email}.`
                        : "Demo mode is active until you sign in with Supabase magic link."}
                    </p>
                  </div>
                  <Link
                    href="/archive"
                    className="hidden rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white lg:inline-flex"
                  >
                    Weekly archive
                  </Link>
                </div>

                <div className="mt-6 grid gap-3 sm:grid-cols-3">
                  <StatCard icon={MapPinned} label="Map-first picks" value="3-5 primary spots" />
                  <StatCard icon={Compass} label="Travel aware" value="Approx walk + transit" />
                  <StatCard icon={CalendarDays} label="Weekly cadence" value="Tuesday 9 AM digest" />
                </div>

                <div className="mt-6 rounded-[1.75rem] border border-stroke/70 bg-white/45 px-4 py-4">
                  <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Recommendation Lens</p>
                  <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-600">
                    Pulse balances durable taste, your current edits, and practical city constraints so the map stays
                    personal without turning setup into the main event.
                  </p>
                </div>
              </div>

              <aside className="rounded-[1.75rem] border border-stroke/80 bg-white/50 p-4 shadow-[0_18px_36px_rgba(17,24,39,0.08)] backdrop-blur">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Setup Rail</p>
                    <h2 className="mt-1 text-2xl font-semibold">
                      {isAuthenticated ? "Already in place" : "Personalize quietly"}
                    </h2>
                  </div>
                  <span className="rounded-full bg-accentSoft px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-accent">
                    {isAuthenticated ? "Live account" : "Ready when you are"}
                  </span>
                </div>

                <p className="mt-2 text-sm leading-6 text-slate-600">
                  {isAuthenticated
                    ? "Your identity is already handled. Keep location and account controls off to the side, then let the map stay center stage."
                    : "Keep identity and location setup off to the side, then let the map stay center stage."}
                </p>

                <div className="mt-4 space-y-4">
                  {isAuthenticated ? (
                    <>
                      <LocationOnboardingCard compact />
                      <MagicLinkCard compact />
                    </>
                  ) : (
                    <>
                      <MagicLinkCard compact />
                      <LocationOnboardingCard compact />
                    </>
                  )}
                </div>
              </aside>
            </div>
          </div>

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
        </section>

        <section className="grid gap-4 xl:grid-cols-[1.45fr_0.55fr]">
          <div className="map-surface overflow-hidden rounded-[2rem] border border-stroke/80 shadow-float">
            <div className="flex items-center justify-between border-b border-stroke/70 px-5 py-4">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Map View</p>
                <h2 className="mt-1 text-2xl font-semibold">Recommended venues across NYC</h2>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => refreshMutation.mutate()}
                  disabled={refreshMutation.isPending}
                  className="rounded-full border border-stroke bg-white/70 px-4 py-2 text-sm text-slate-700 transition hover:bg-white disabled:opacity-60"
                >
                  {refreshMutation.isPending ? "Refreshing..." : "Refresh picks"}
                </button>
                <Link href="/archive" className="rounded-full border border-stroke px-4 py-2 text-sm text-slate-700 lg:hidden">
                  Archive
                </Link>
              </div>
            </div>

            <PulseMap
              pins={mapQuery.data?.pins ?? []}
              viewport={mapQuery.data?.viewport ?? null}
              selectedVenueId={selectedCard?.venueId ?? null}
              onSelectVenue={setSelectedVenueId}
            />
          </div>

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
        </section>
      </div>
    </main>
  );
}

function StatCard({
  icon: Icon,
  label,
  value
}: {
  icon: typeof MapPinned;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-3xl border border-stroke bg-white/70 p-4">
      <Icon className="h-5 w-5 text-accent" />
      <p className="mt-3 text-xs uppercase tracking-[0.2em] text-slate-500">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-800">{value}</p>
    </div>
  );
}
