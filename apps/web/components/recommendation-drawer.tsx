"use client";

import { useEffect, useRef } from "react";
import { Bookmark, MapPin, MoveRight, XCircle } from "lucide-react";
import type { VenueRecommendationCard } from "@/lib/types";
import { formatEventStart } from "@/lib/utils";

export function RecommendationDrawer({
  loading,
  cards,
  timezone,
  selectedVenueId,
  onSelectVenue,
  onSave,
  onDismiss
}: {
  loading: boolean;
  cards: Record<string, VenueRecommendationCard>;
  timezone: string;
  selectedVenueId: string | null;
  onSelectVenue: (venueId: string) => void;
  onSave: (card: VenueRecommendationCard) => void;
  onDismiss: (card: VenueRecommendationCard) => void;
}) {
  const orderedCards = Object.values(cards).sort((left, right) => right.score - left.score);
  const cardRefs = useRef<Record<string, HTMLElement | null>>({});

  useEffect(() => {
    if (!selectedVenueId) {
      return;
    }

    const target = cardRefs.current[selectedVenueId];
    target?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [selectedVenueId]);

  return (
    <aside className="flex h-full min-h-0 flex-col rounded-[2rem] border border-stroke/80 bg-card/80 p-4 shadow-float backdrop-blur">
      <div className="px-2 pb-3">
        <h2 className="text-2xl font-semibold">Top spots this week</h2>
        <p className="mt-1 text-sm text-slate-500">Choose a card to focus the map and compare this week&apos;s strongest venue fits.</p>
      </div>

      <div className="flex-1 space-y-3 overflow-y-auto pr-1">
        {loading ? (
          <div className="rounded-3xl border border-stroke bg-white/70 p-5 text-sm text-slate-500">
            Loading your current venue shortlist...
          </div>
        ) : null}

        {!loading && !orderedCards.length ? (
          <div className="rounded-3xl border border-dashed border-stroke bg-white/70 p-5 text-sm text-slate-500">
            No saved recommendation run yet. Use Sync supply to pull in fresh events, then refresh picks to populate this drawer.
          </div>
        ) : null}

        {orderedCards.map((card) => {
          const selected = card.venueId === selectedVenueId;

          return (
            <article
              key={card.venueId}
              ref={(node) => {
                cardRefs.current[card.venueId] = node;
              }}
              role="button"
              tabIndex={0}
              onClick={() => onSelectVenue(card.venueId)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  onSelectVenue(card.venueId);
                }
              }}
              className={[
                "w-full rounded-[1.75rem] border p-4 text-left transition",
                selected ? "border-accent bg-accentSoft/60" : "border-stroke bg-white/80 hover:bg-white"
              ].join(" ")}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.22em] text-slate-500">{card.neighborhood}</p>
                  <h3 className="mt-2 text-lg font-semibold text-slate-900">{card.venueName}</h3>
                  <p className="mt-1 text-sm text-slate-600">{card.eventTitle}</p>
                </div>
                <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-accent">
                  {card.scoreBand}
                </span>
              </div>

              <div className="mt-4 flex flex-wrap items-center gap-3 text-sm text-slate-600">
                <span>{formatEventStart(card.startsAt, timezone)}</span>
                <span>{card.priceLabel}</span>
                <span className="inline-flex items-center gap-1">
                  <MapPin className="h-4 w-4" />
                  {card.address}
                </span>
              </div>

              <div className="mt-4 space-y-2">
                {card.reasons.filter((reason) => reason.title !== "Travel fit").map((reason) => (
                  <div key={reason.title} className="rounded-2xl bg-white/70 px-3 py-2 text-sm text-slate-700">
                    <span className="font-semibold">{reason.title}:</span> {reason.detail}
                  </div>
                ))}
              </div>

              <div className="mt-4 flex flex-wrap gap-2 text-sm text-slate-600">
                {card.travel.map((travel) => (
                  <span key={`${card.venueId}-${travel.mode}`} className="rounded-full bg-white px-3 py-1">
                    {travel.label}
                  </span>
                ))}
              </div>

              <div className="mt-4 flex gap-2">
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onSave(card);
                  }}
                  className="inline-flex flex-1 items-center justify-center gap-2 rounded-full bg-accent px-4 py-2 text-sm font-medium text-white"
                >
                  <Bookmark className="h-4 w-4" />
                  Save
                </button>
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    onDismiss(card);
                  }}
                  className="inline-flex items-center justify-center gap-2 rounded-full border border-stroke bg-white px-4 py-2 text-sm font-medium text-slate-700"
                  title="Hide from this run"
                  aria-label={`Hide ${card.venueName} from this run`}
                >
                  <XCircle className="h-4 w-4" />
                  Hide
                </button>
              </div>

              {card.secondaryEvents.length ? (
                <div className="mt-4 rounded-2xl border border-stroke/80 bg-white/70 p-3 text-sm text-slate-600">
                  <p className="font-semibold text-slate-800">Also upcoming at {card.venueName}</p>
                  <div className="mt-2 space-y-1">
                    {card.secondaryEvents.map((event) => (
                      <div key={event.eventId} className="flex items-center justify-between gap-2">
                        <span>{event.title}</span>
                        <MoveRight className="h-4 w-4 shrink-0 text-slate-400" />
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
            </article>
          );
        })}
      </div>
    </aside>
  );
}
