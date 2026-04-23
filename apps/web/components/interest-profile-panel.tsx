"use client";

import { RotateCcw, Sparkles, VolumeX } from "lucide-react";
import type { InterestTopic } from "@/lib/types";

type InterestAction = "boost" | "mute" | "reset";

interface InterestProfilePanelProps {
  topics: InterestTopic[];
  isLoading: boolean;
  isSaving: boolean;
  onAction: (topicId: string, action: InterestAction) => void;
}

export function InterestProfilePanel({
  topics,
  isLoading,
  isSaving,
  onAction
}: InterestProfilePanelProps) {
  return (
    <div className="rounded-[2rem] border border-stroke/80 bg-card/70 p-5 shadow-float backdrop-blur">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">Interest signals</h2>
          <p className="mt-1 text-sm text-slate-500">
            Boost or mute a theme, then watch the map and shortlist respond around that edit.
          </p>
        </div>
        <button
          type="button"
          title="More signal sources are coming soon."
          className="rounded-full border border-stroke bg-white/70 px-3 py-2 text-sm font-medium text-slate-600 transition hover:bg-white"
        >
          Add signal
        </button>
      </div>

      <div className="mt-5 space-y-3">
        {isLoading ? (
          <div className="rounded-3xl border border-dashed border-stroke bg-white/50 p-5 text-sm text-slate-500">
            Loading inferred interests...
          </div>
        ) : null}

        {!isLoading && topics.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-stroke bg-white/50 p-5 text-sm leading-6 text-slate-500">
            Connect Reddit or load the sample profile to see inferred themes like underground dance, indie gigs,
            gallery nights, and creative meetups.
          </div>
        ) : null}

        {topics.map((topic) => {
          const confidencePercent = Math.round(topic.confidence * 100);
          return (
            <article
              key={topic.id}
              className={[
                "rounded-3xl border bg-white/75 p-4 transition",
                topic.muted ? "border-slate-200" : topic.boosted ? "border-accent/35" : "border-stroke"
              ].join(" ")}
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <h3 className="text-base font-semibold text-slate-900">{topic.label}</h3>
                  <p className="mt-2 text-sm text-slate-500">Confidence {confidencePercent}% from observed signals.</p>
                </div>

                <div className="flex flex-wrap gap-2">
                  <ActionButton
                    label="Boost"
                    active={topic.boosted}
                    disabled={isSaving}
                    title="Lift venues that match this theme higher in the ranking."
                    onClick={() => onAction(topic.id, "boost")}
                  />
                  <ActionButton
                    label="Mute"
                    active={topic.muted}
                    disabled={isSaving}
                    tone="muted"
                    title="Downrank venues that match this theme."
                    onClick={() => onAction(topic.id, "mute")}
                  />
                  <ActionButton
                    label="Reset"
                    active={false}
                    disabled={isSaving || (!topic.boosted && !topic.muted)}
                    tone="ghost"
                    title="Clear your manual override for this theme."
                    onClick={() => onAction(topic.id, "reset")}
                  />
                </div>
              </div>

              <div className="mt-4 h-2 overflow-hidden rounded-full bg-slate-100">
                <div
                  className={[
                    "h-full rounded-full transition-all",
                    topic.muted ? "bg-slate-300" : topic.boosted ? "bg-accent" : "bg-slate-700"
                  ].join(" ")}
                  style={{ width: `${confidencePercent}%` }}
                />
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                {topic.sourceSignals.map((signal) => (
                  <span
                    key={signal}
                    className="rounded-full border border-stroke bg-slate-50 px-3 py-1 text-xs text-slate-600"
                  >
                    {signal}
                  </span>
                ))}
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function ActionButton({
  label,
  active,
  disabled,
  onClick,
  tone = "accent",
  title,
}: {
  label: string;
  active: boolean;
  disabled: boolean;
  onClick: () => void;
  tone?: "accent" | "muted" | "ghost";
  title: string;
}) {
  const className =
    tone === "muted"
      ? active
        ? "border-slate-300 bg-slate-900 text-white"
        : "border-stroke bg-white text-slate-700"
      : tone === "ghost"
        ? "border-stroke bg-transparent text-slate-500"
        : active
          ? "border-accent bg-accent text-white"
          : "border-accent/20 bg-accentSoft text-accent";

  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      title={title}
      className={`rounded-full border px-3 py-2 text-sm font-medium transition disabled:cursor-not-allowed disabled:opacity-60 ${className}`}
    >
      {label}
    </button>
  );
}
