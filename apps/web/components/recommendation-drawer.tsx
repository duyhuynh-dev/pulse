"use client";

import { useEffect, useRef } from "react";
import { Bookmark, MapPin, MoveRight, XCircle } from "lucide-react";
import type { RecommendationRunComparisonItem, VenueRecommendationCard } from "@/lib/types";
import { formatEventStart, formatRelativeTimestamp } from "@/lib/utils";

export function RecommendationDrawer({
  loading,
  cards,
  timezone,
  selectedVenueId,
  onSelectVenue,
  onSave,
  onDismiss,
  comparisonByVenueId = {},
  mode = "rail",
  previewCount = 2,
  isExpanded = false,
  onToggleExpanded
}: {
  loading: boolean;
  cards: Record<string, VenueRecommendationCard>;
  timezone: string;
  selectedVenueId: string | null;
  onSelectVenue: (venueId: string) => void;
  onSave: (card: VenueRecommendationCard) => void;
  onDismiss: (card: VenueRecommendationCard) => void;
  comparisonByVenueId?: Record<string, RecommendationRunComparisonItem>;
  mode?: "rail" | "modal";
  previewCount?: number;
  isExpanded?: boolean;
  onToggleExpanded?: () => void;
}) {
  const orderedCards = Object.values(cards).sort((left, right) => right.score - left.score);
  const visibleCards = mode === "rail" ? orderedCards.slice(0, previewCount) : orderedCards;
  const canShowAll = mode === "rail" && orderedCards.length > visibleCards.length;
  const breakdownPreviewCount = mode === "rail" ? 3 : 5;
  const cardRefs = useRef<Record<string, HTMLElement | null>>({});

  useEffect(() => {
    if (!selectedVenueId) {
      return;
    }

    const target = cardRefs.current[selectedVenueId];
    target?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [selectedVenueId]);

  return (
    <aside
      className={[
        "flex min-h-0 flex-col",
        mode === "rail"
          ? "h-full rounded-[2rem] border border-stroke/80 bg-card/80 p-4 shadow-float backdrop-blur"
          : ""
      ].join(" ")}
    >
      <div className="px-2 pb-3">
        <button
          type="button"
          onClick={mode === "rail" ? onToggleExpanded : undefined}
          className={[
            "min-w-0 text-left",
            mode === "rail" ? "transition hover:opacity-80" : ""
          ].join(" ")}
          aria-expanded={mode === "rail" ? isExpanded : undefined}
        >
          <h2 className="text-2xl font-semibold">Top spots this week</h2>
          <p className="mt-1 text-sm text-slate-500">
            Choose a card to focus the map and compare this week&apos;s strongest venue fits.
          </p>
        </button>
      </div>

      <div className={["min-h-0 pr-1", mode === "rail" ? "flex-1 space-y-3 overflow-y-auto" : "space-y-4"].join(" ")}>
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

        {visibleCards.map((card) => {
          const selected = card.venueId === selectedVenueId;
          const comparison = comparisonByVenueId[card.venueId];
          const movementCues = comparison?.movementCues ?? [];
          const movementLabel =
            comparison?.movement === "new"
              ? "New"
              : comparison?.movement === "up"
                ? `Up ${Math.abs(comparison.rankDelta ?? 0)}`
                : comparison?.movement === "down"
                  ? `Down ${Math.abs(comparison.rankDelta ?? 0)}`
                  : null;

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

              <div className="mt-4 flex flex-wrap gap-2 text-xs font-medium text-slate-600">
                <span className="rounded-full border border-stroke/80 bg-white px-3 py-1">
                  Source: {card.provenance.sourceName}
                </span>
                <span className="rounded-full border border-stroke/80 bg-white px-3 py-1">
                  {card.provenance.sourceConfidenceLabel}
                </span>
                <span className="rounded-full border border-stroke/80 bg-white px-3 py-1">
                  {card.freshness.freshnessLabel}
                  {card.freshness.lastVerifiedAt ? ` · ${formatRelativeTimestamp(card.freshness.lastVerifiedAt)}` : ""}
                </span>
              </div>

              {card.scoreSummary ? (
                <p className="mt-4 text-sm leading-6 text-slate-700">{card.scoreSummary}</p>
              ) : null}

              {movementLabel || movementCues.length ? (
                <div className="mt-3 flex flex-wrap gap-2 text-xs font-medium">
                  {movementLabel ? (
                    <span
                      className={[
                        "rounded-full border px-3 py-1",
                        comparison?.movement === "down"
                          ? "border-amber-200 bg-amber-50 text-amber-800"
                          : "border-sky-200 bg-sky-50 text-sky-800"
                      ].join(" ")}
                    >
                      {movementLabel}
                    </span>
                  ) : null}
                  {movementCues.slice(0, mode === "rail" ? 2 : 3).map((cue) => (
                    <span
                      key={`${card.venueId}-movement-${cue.key}`}
                      className={[
                        "rounded-full border px-3 py-1",
                        cue.direction === "negative"
                          ? "border-amber-200 bg-amber-50 text-amber-800"
                          : "border-sky-200 bg-sky-50 text-sky-800"
                      ].join(" ")}
                      title={`Contribution delta ${cue.delta > 0 ? "+" : ""}${cue.delta.toFixed(3)}`}
                    >
                      {cue.label} {cue.direction === "positive" ? "\u2191" : "\u2193"}
                    </span>
                  ))}
                </div>
              ) : null}

              {card.scoreBreakdown.length ? (
                <div className="mt-3 flex flex-wrap gap-2 text-xs font-medium">
                  {card.scoreBreakdown.slice(0, breakdownPreviewCount).map((item) => (
                    <span
                      key={`${card.venueId}-${item.key}`}
                      className={[
                        "rounded-full border px-3 py-1",
                        item.direction === "negative"
                          ? "border-amber-200 bg-amber-50 text-amber-800"
                          : "border-stroke/80 bg-white text-slate-700"
                      ].join(" ")}
                      title={item.detail}
                    >
                      {item.label} · {item.impactLabel}
                    </span>
                  ))}
                </div>
              ) : null}

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

      {canShowAll ? (
        <button
          type="button"
          onClick={onToggleExpanded}
          className="mt-4 inline-flex self-start rounded-full border border-stroke bg-white/75 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-white"
        >
          Show all ({orderedCards.length}) &rarr;
        </button>
      ) : null}
    </aside>
  );
}
