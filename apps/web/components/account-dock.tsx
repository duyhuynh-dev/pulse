"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Link2,
  LogOut,
  Mail,
  ShieldCheck,
  UserRound,
  X,
} from "lucide-react";
import { getAuthViewer, startMockRedditConnection, startRedditConnection } from "@/lib/api";
import { getSupabaseBrowserClient } from "@/lib/supabase-browser";
import { useAuth } from "@/components/auth-provider";

function statusCopy(connectionMode: "none" | "live" | "sample", isSignedIn: boolean) {
  if (connectionMode === "live") {
    return {
      eyebrow: "Live Reddit",
      detail: "Connected and ready for full personalization.",
    };
  }
  if (connectionMode === "sample") {
    return {
      eyebrow: "Sample profile",
      detail: "Attached while Reddit API approval is pending.",
    };
  }
  if (isSignedIn) {
    return {
      eyebrow: "Identity only",
      detail: "Connect Reddit whenever you want live signals.",
    };
  }
  return {
    eyebrow: "Demo mode",
    detail: "Sign in here and keep the setup out of the main canvas.",
  };
}

export function AccountDock() {
  const { isConfigured, isLoading, session, user, signOut } = useAuth();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("Send a magic link to unlock personalized digests, saved account state, and Reddit connection.");
  const [showSwitchForm, setShowSwitchForm] = useState(false);
  const [isConnectingReddit, setIsConnectingReddit] = useState(false);
  const [isLoadingSample, setIsLoadingSample] = useState(false);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const supabase = getSupabaseBrowserClient();

  const viewerQuery = useQuery({
    queryKey: ["auth-viewer", user?.id ?? "demo"],
    queryFn: getAuthViewer,
  });

  const isSignedIn = Boolean(session && !isLoading);
  const connectionMode = viewerQuery.data?.redditConnectionMode ?? "none";
  const status = statusCopy(connectionMode, isSignedIn);

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

  const sendMagicLink = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!supabase) {
      setMessage("Supabase browser keys are missing. Configure them to enable account sign-in.");
      return;
    }

    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: {
        emailRedirectTo: typeof window !== "undefined" ? window.location.origin : undefined,
      },
    });

    setMessage(error ? error.message : `Magic link sent to ${email}. Check your inbox and come right back.`);
  };

  const connectReddit = async () => {
    if (!session) {
      setMessage("Sign in first so Pulse can attach the Reddit connection to your account.");
      return;
    }

    setIsConnectingReddit(true);
    try {
      const response = await startRedditConnection();
      window.location.assign(response.authorizeUrl);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to start Reddit connection.");
      setIsConnectingReddit(false);
    }
  };

  const connectSampleProfile = async () => {
    if (!session) {
      setMessage("Sign in first so Pulse can attach the sample profile to your account.");
      return;
    }

    setIsLoadingSample(true);
    try {
      await startMockRedditConnection();
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["auth-viewer"] }),
        queryClient.invalidateQueries({ queryKey: ["interests"] }),
        queryClient.invalidateQueries({ queryKey: ["map-recommendations"] }),
        queryClient.invalidateQueries({ queryKey: ["archive"] }),
      ]);
      setMessage("Sample Reddit profile attached. Swap to live Reddit whenever approval lands.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to attach the sample profile.");
    } finally {
      setIsLoadingSample(false);
    }
  };

  return (
    <div className="relative">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex items-center gap-3 rounded-full border border-stroke/80 bg-white/85 px-3 py-2 text-left shadow-[0_14px_36px_rgba(17,24,39,0.08)] transition hover:bg-white"
      >
        <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-accentSoft text-accent">
          {isSignedIn ? <UserRound className="h-4 w-4" /> : <Mail className="h-4 w-4" />}
        </span>
        <span className="min-w-0">
          <span className="block text-sm font-semibold text-slate-900">Profile</span>
          <span className="block text-xs uppercase tracking-[0.18em] text-slate-500">
            {isSignedIn ? status.eyebrow : "Sign in"}
          </span>
        </span>
        {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
      </button>

      {open ? (
        <div
          ref={panelRef}
          className="absolute right-0 top-[calc(100%+0.85rem)] z-30 w-[min(23rem,calc(100vw-2rem))] rounded-[1.75rem] border border-stroke/80 bg-white/95 p-4 shadow-[0_28px_60px_rgba(15,23,42,0.18)] backdrop-blur"
        >
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Account</p>
              <h3 className="mt-1 text-xl font-semibold text-slate-900">{isSignedIn ? "Profile" : "Sign in quietly"}</h3>
            </div>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-stroke bg-white text-slate-500 transition hover:text-slate-900"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <div className="mt-4 rounded-[1.35rem] border border-stroke/80 bg-canvas/70 p-4">
            {isConfigured ? (
              <>
                <div className="flex flex-wrap items-center gap-2">
                  {isSignedIn ? (
                    <span className="inline-flex items-center gap-2 rounded-full bg-white px-3 py-1.5 text-sm font-medium text-slate-900 shadow-sm">
                      <CheckCircle2 className="h-4 w-4 text-accent" />
                      <span className="max-w-[12rem] truncate">{user?.email}</span>
                    </span>
                  ) : (
                    <span className="inline-flex items-center gap-2 rounded-full bg-white px-3 py-1.5 text-sm font-medium text-slate-900 shadow-sm">
                      <Mail className="h-4 w-4 text-accent" />
                      No active session
                    </span>
                  )}
                  <span className="inline-flex items-center gap-2 rounded-full border border-stroke bg-white px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                    <ShieldCheck className="h-3.5 w-3.5 text-accent" />
                    {status.eyebrow}
                  </span>
                </div>
                <p className="mt-3 text-sm leading-6 text-slate-600">{status.detail}</p>
              </>
            ) : (
              <p className="text-sm leading-6 text-slate-600">
                Supabase browser keys are missing, so the app stays in demo mode until those env vars are added.
              </p>
            )}
          </div>

          {isConfigured ? (
            <>
              {isSignedIn ? (
                <div className="mt-4 space-y-3">
                <div className="grid gap-2 sm:grid-cols-2">
                    {connectionMode !== "live" ? (
                      <button
                        type="button"
                        onClick={() => void connectReddit()}
                        disabled={isConnectingReddit}
                        className="inline-flex items-center justify-center gap-2 rounded-full bg-slate-900 px-4 py-2.5 text-sm font-medium text-white disabled:opacity-60"
                      >
                        <Link2 className="h-4 w-4" />
                        {isConnectingReddit ? "Redirecting..." : connectionMode === "sample" ? "Connect live Reddit" : "Connect Reddit"}
                      </button>
                    ) : (
                      <div className="inline-flex items-center justify-center rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2.5 text-sm font-medium text-emerald-700">
                        Live Reddit connected
                      </div>
                    )}

                    {connectionMode === "none" ? (
                      <button
                        type="button"
                        onClick={() => void connectSampleProfile()}
                        disabled={isLoadingSample}
                        className="inline-flex items-center justify-center gap-2 rounded-full border border-stroke bg-white px-4 py-2.5 text-sm font-medium text-slate-700 disabled:opacity-60"
                      >
                        <Link2 className="h-4 w-4" />
                        {isLoadingSample ? "Loading sample..." : "Use sample profile"}
                      </button>
                    ) : null}
                  </div>

                  <div className="flex flex-wrap items-center justify-between gap-3 border-t border-stroke/80 pt-3">
                    <button
                      type="button"
                      onClick={() => setShowSwitchForm((value) => !value)}
                      className="inline-flex items-center gap-2 text-sm font-medium text-slate-600 transition hover:text-slate-900"
                    >
                      {showSwitchForm ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                      Use another email
                    </button>
                    <button
                      type="button"
                      onClick={() => void signOut()}
                      className="inline-flex items-center gap-2 text-sm font-medium text-slate-500 transition hover:text-slate-900"
                    >
                      <LogOut className="h-4 w-4" />
                      Sign out
                    </button>
                  </div>
                </div>
              ) : null}

              {!isSignedIn || showSwitchForm ? (
                <form
                  onSubmit={sendMagicLink}
                  className="mt-4 grid gap-2 rounded-[1.25rem] border border-dashed border-stroke bg-white/70 p-3"
                >
                  {isSignedIn ? (
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Switch account</p>
                  ) : (
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Magic-link sign in</p>
                  )}
                  <input
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    type="email"
                    required
                    placeholder="you@example.com"
                    className="rounded-2xl border border-stroke bg-white px-3 py-2 text-sm"
                  />
                  <button type="submit" className="rounded-full bg-slate-900 px-4 py-2.5 text-sm font-medium text-white">
                    {isSignedIn ? "Send magic link to another email" : "Send magic link"}
                  </button>
                </form>
              ) : null}
            </>
          ) : null}

          <p className="mt-4 text-xs leading-5 text-slate-500">{message}</p>
        </div>
      ) : null}
    </div>
  );
}
