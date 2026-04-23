"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronDown, ChevronUp, X } from "lucide-react";
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

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false);
      }
    };

    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  return (
    <div className="relative z-[70] overflow-visible">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="inline-flex h-11 items-center gap-2 rounded-full border border-stroke bg-white/70 px-4 py-2 text-left text-sm font-medium text-slate-700 transition hover:bg-white"
      >
        <span className="text-sm font-medium text-slate-900">Settings</span>
        {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
      </button>

      {open ? (
        <div
          ref={panelRef}
          className="absolute right-0 top-[calc(100%+0.5rem)] z-[90] w-[min(27rem,calc(100vw-2rem))] max-h-[calc(100vh-120px)] overflow-y-auto rounded-[1.5rem] border border-stroke bg-white p-4 shadow-[0_22px_60px_rgba(15,23,42,0.18)]"
        >
          <div className="absolute right-6 top-0 h-3.5 w-3.5 -translate-y-1/2 rotate-45 border-l border-t border-stroke bg-white" />

          <div className="relative flex items-start justify-between gap-3">
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
