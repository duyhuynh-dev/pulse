"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronDown, ChevronUp, Settings2, X } from "lucide-react";
import { DigestSettingsCard } from "@/components/digest-settings-card";
import { LocationOnboardingCard } from "@/components/location-onboarding-card";

export function SettingsDock({ showDigest = false }: { showDigest?: boolean }) {
  const [open, setOpen] = useState(false);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);

  useEffect(() => {
    if (!open) {
      return;
    }

    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!target) {
        return;
      }
      if (panelRef.current?.contains(target) || triggerRef.current?.contains(target)) {
        return;
      }
      setOpen(false);
    };

    window.addEventListener("pointerdown", onPointerDown);
    return () => window.removeEventListener("pointerdown", onPointerDown);
  }, [open]);

  return (
    <div className="relative">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex items-center gap-3 rounded-full border border-stroke/80 bg-white/85 px-3 py-2 text-left shadow-[0_14px_36px_rgba(17,24,39,0.08)] transition hover:bg-white"
      >
        <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-canvas text-slate-700">
          <Settings2 className="h-4 w-4" />
        </span>
        <span className="min-w-0">
          <span className="block text-sm font-semibold text-slate-900">Settings</span>
          <span className="block text-xs uppercase tracking-[0.18em] text-slate-500">Location and digest</span>
        </span>
        {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
      </button>

      {open ? (
        <div
          ref={panelRef}
          className="absolute right-0 top-[calc(100%+0.85rem)] z-30 w-[min(27rem,calc(100vw-2rem))] rounded-[1.75rem] border border-stroke/80 bg-white/95 p-4 shadow-[0_28px_60px_rgba(15,23,42,0.18)] backdrop-blur"
        >
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Settings</p>
              <h3 className="mt-1 text-xl font-semibold text-slate-900">Map context</h3>
            </div>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-stroke bg-white text-slate-500 transition hover:text-slate-900"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <p className="mt-3 text-sm leading-6 text-slate-600">Adjust location and weekly delivery without pulling setup into the main map view.</p>

          <div className="mt-4 grid gap-4">
            <LocationOnboardingCard compact />
            {showDigest ? <DigestSettingsCard compact /> : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
