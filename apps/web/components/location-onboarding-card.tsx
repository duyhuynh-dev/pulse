"use client";

import { useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import { zodResolver } from "@hookform/resolvers/zod";
import { MapPinned } from "lucide-react";
import { saveAnchor, saveConstraints } from "@/lib/api";

const schema = z.object({
  neighborhood: z.string().optional(),
  zipCode: z.string().min(5).max(5),
  radiusMiles: z.coerce.number().min(1).max(30)
});

type FormValues = z.infer<typeof schema>;

const defaultConstraint = {
  city: "New York City",
  radiusMiles: 8,
  budgetLevel: "under_75" as const,
  preferredDays: ["Thursday", "Friday", "Saturday"],
  socialMode: "either" as const
};

export function LocationOnboardingCard({ compact = false }: { compact?: boolean }) {
  const [status, setStatus] = useState<string>("Use your current location for a better map center, or save a ZIP fallback.");
  const form = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: {
      zipCode: "10003",
      radiusMiles: 8,
      neighborhood: "East Village"
    }
  });

  const askForLocation = async () => {
    if (!navigator.geolocation) {
      setStatus("This browser does not support geolocation. Use ZIP or neighborhood instead.");
      return;
    }

    navigator.geolocation.getCurrentPosition(
      async (position) => {
        await saveAnchor({
          latitude: position.coords.latitude,
          longitude: position.coords.longitude,
          source: "live"
        });
        setStatus("Live location saved for this session. Exact coordinates are not persisted.");
      },
      () => {
        setStatus("Location access denied. Save a ZIP or neighborhood to keep planning coarse and private.");
      },
      {
        enableHighAccuracy: false,
        timeout: 10_000
      },
    );
  };

  const onSubmit = async (values: FormValues) => {
    await saveAnchor({
      neighborhood: values.neighborhood,
      zipCode: values.zipCode,
      source: values.neighborhood ? "neighborhood" : "zip"
    });

    await saveConstraints({
      ...defaultConstraint,
      neighborhood: values.neighborhood,
      zipCode: values.zipCode,
      radiusMiles: values.radiusMiles
    });

    setStatus("Saved your coarse planning anchor. Travel hints will now use your ZIP or neighborhood centroid.");
  };

  return (
    <div className={compact ? "rounded-[1.5rem] border border-stroke/80 bg-white/60 p-4 backdrop-blur" : "rounded-[1.75rem] border border-stroke bg-white/70 p-4"}>
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <MapPinned className="h-5 w-5 text-accent" />
          <h3 className={compact ? "text-base font-semibold" : "text-lg font-semibold"}>Location anchor</h3>
        </div>
        {compact ? (
          <button
            type="button"
            onClick={askForLocation}
            className="rounded-full bg-accent px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-white"
          >
            Use live
          </button>
        ) : null}
      </div>
      <p className={compact ? "mt-2 text-xs leading-5 text-slate-500" : "mt-2 text-sm leading-6 text-slate-600"}>{status}</p>

      {!compact ? (
        <div className="mt-4 flex gap-2">
          <button
            type="button"
            onClick={askForLocation}
            className="rounded-full bg-accent px-4 py-2 text-sm font-medium text-white"
          >
            Use current location
          </button>
        </div>
      ) : null}

      <form onSubmit={form.handleSubmit(onSubmit)} className={compact ? "mt-4 grid gap-3 sm:grid-cols-2" : "mt-4 grid gap-3"}>
        <label className="grid gap-1">
          <span className={compact ? "text-xs font-semibold uppercase tracking-[0.18em] text-slate-500" : "text-sm font-medium text-slate-700"}>ZIP code</span>
          <input
            {...form.register("zipCode")}
            className="rounded-[1.1rem] border border-stroke bg-white px-3 py-2.5 text-sm text-slate-700 outline-none transition focus:border-accent/35 focus:ring-2 focus:ring-accent/15"
          />
        </label>
        <label className={compact ? "grid gap-1 sm:col-span-1" : "grid gap-1"}>
          <span className={compact ? "text-xs font-semibold uppercase tracking-[0.18em] text-slate-500" : "text-sm font-medium text-slate-700"}>Radius in miles</span>
          <input
            type="number"
            {...form.register("radiusMiles")}
            className="rounded-[1.1rem] border border-stroke bg-white px-3 py-2.5 text-sm text-slate-700 outline-none transition focus:border-accent/35 focus:ring-2 focus:ring-accent/15"
          />
        </label>
        <label className={compact ? "grid gap-1 sm:col-span-2" : "grid gap-1"}>
          <span className={compact ? "text-xs font-semibold uppercase tracking-[0.18em] text-slate-500" : "text-sm font-medium text-slate-700"}>Neighborhood</span>
          <input
            {...form.register("neighborhood")}
            className="rounded-[1.1rem] border border-stroke bg-white px-3 py-2.5 text-sm text-slate-700 outline-none transition focus:border-accent/35 focus:ring-2 focus:ring-accent/15"
          />
        </label>

        <button type="submit" className={compact ? "rounded-full border border-stroke bg-white px-4 py-2 text-sm font-medium shadow-sm transition hover:bg-canvas sm:col-span-2" : "rounded-full border border-stroke bg-white px-4 py-2 text-sm font-medium transition hover:bg-canvas"}>
          Save anchor
        </button>
      </form>
    </div>
  );
}
