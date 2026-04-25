"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { getArchive } from "@/lib/api";
import type { ArchiveSnapshot, VenueRecommendationCard } from "@/lib/types";
import { formatEventStart, formatRelativeTimestamp, formatTimestamp } from "@/lib/utils";

function SnapshotSection({
  title,
  subtitle,
  items,
  timezone,
}: {
  title: string;
  subtitle: string;
  items: VenueRecommendationCard[];
  timezone: string;
}) {
  return (
    <section className="rounded-[1.75rem] border border-stroke bg-white/75 p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-2xl font-semibold">{title}</h2>
          <p className="mt-1 text-sm text-slate-500">{subtitle}</p>
        </div>
        <span className="rounded-full bg-canvas px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
          {items.length} picks
        </span>
      </div>

      <div className="mt-4 grid gap-3">
        {items.map((item) => (
          <article key={`${title}-${item.venueId}-${item.eventId}`} className="rounded-[1.5rem] border border-stroke/80 bg-white/80 p-5">
            <p className="text-xs uppercase tracking-[0.22em] text-slate-500">{item.neighborhood}</p>
            <h3 className="mt-2 text-2xl font-semibold">{item.venueName}</h3>
            <p className="mt-1 text-slate-700">{item.eventTitle}</p>
            <div className="mt-3 flex flex-wrap gap-2 text-sm text-slate-600">
              <span>{formatEventStart(item.startsAt, timezone)}</span>
              <span>{item.priceLabel}</span>
              {item.travel.map((travel) => (
                <span key={`${item.eventId}-${travel.mode}`} className="rounded-full bg-canvas px-3 py-1">
                  {travel.label}
                </span>
              ))}
            </div>
            <div className="mt-3 flex flex-wrap gap-2 text-xs font-medium text-slate-600">
              <span className="rounded-full border border-stroke/80 bg-canvas px-3 py-1">
                {item.provenance.sourceName}
              </span>
              <span className="rounded-full border border-stroke/80 bg-canvas px-3 py-1">
                {item.provenance.sourceConfidenceLabel}
              </span>
              <span className="rounded-full border border-stroke/80 bg-canvas px-3 py-1">
                {item.freshness.freshnessLabel}
                {item.freshness.lastVerifiedAt ? ` · ${formatRelativeTimestamp(item.freshness.lastVerifiedAt)}` : ""}
              </span>
            </div>
            {item.scoreSummary ? (
              <p className="mt-3 text-sm leading-6 text-slate-700">{item.scoreSummary}</p>
            ) : null}
            {item.scoreBreakdown.length ? (
              <div className="mt-3 flex flex-wrap gap-2 text-xs font-medium">
                {item.scoreBreakdown.slice(0, 4).map((factor) => (
                  <span
                    key={`${item.eventId}-${factor.key}`}
                    className={[
                      "rounded-full border px-3 py-1",
                      factor.direction === "negative"
                        ? "border-amber-200 bg-amber-50 text-amber-800"
                        : "border-stroke/80 bg-canvas text-slate-700"
                    ].join(" ")}
                    title={factor.detail}
                  >
                    {factor.label} · {factor.impactLabel}
                  </span>
                ))}
              </div>
            ) : null}
          </article>
        ))}
      </div>
    </section>
  );
}

function snapshotSubtitle(snapshot: ArchiveSnapshot, timezone: string) {
  const delivered = formatTimestamp(snapshot.deliveredAt, timezone);
  const generated = formatTimestamp(snapshot.generatedAt, timezone);
  if (snapshot.kind === "scheduled" && delivered) {
    return `Sent to your inbox on ${delivered}.`;
  }
  if (snapshot.kind === "preview" && delivered) {
    return `Previewed on ${delivered}.`;
  }
  if (generated) {
    return `Generated on ${generated}.`;
  }
  return "Saved from an earlier Pulse run.";
}

export function ArchivePageContent() {
  const archiveQuery = useQuery({
    queryKey: ["archive"],
    queryFn: getArchive
  });
  const timezone = archiveQuery.data?.displayTimezone ?? "America/New_York";

  return (
    <main className="min-h-screen px-4 py-6 md:px-6">
      <div className="mx-auto max-w-5xl rounded-[2rem] border border-stroke/80 bg-card/80 p-6 shadow-float">
        <div className="flex items-center justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Archive</p>
            <h1 className="mt-2 text-4xl font-semibold tracking-tight">Weekly venue picks</h1>
          </div>
          <Link href="/" className="rounded-full border border-stroke px-4 py-2 text-sm text-slate-700">
            Back to map
          </Link>
        </div>

        <div className="mt-6 grid gap-3">
          {!!archiveQuery.data?.items.length && (
            <SnapshotSection
              title="Current shortlist"
              subtitle="This is the live recommendation stack behind your map right now."
              items={archiveQuery.data.items}
              timezone={timezone}
            />
          )}

          {archiveQuery.data?.history.map((snapshot) => (
            <SnapshotSection
              key={snapshot.runId}
              title={snapshot.title}
              subtitle={snapshotSubtitle(snapshot, timezone)}
              items={snapshot.items}
              timezone={timezone}
            />
          ))}

          {!archiveQuery.data?.items.length && !archiveQuery.data?.history.length && (
            <div className="rounded-[1.75rem] border border-dashed border-stroke bg-white/70 p-5 text-sm text-slate-500">
              The archive will fill in after the first recommendation run is generated and the first preview or weekly digest snapshot is saved.
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
