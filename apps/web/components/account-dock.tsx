"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Disc3,
  Link2,
  LogOut,
  Mail,
  ShieldCheck,
  X,
} from "lucide-react";
import {
  applySpotifyTaste,
  getAuthViewer,
  getSpotifyTastePreview,
  startMockRedditConnection,
  startRedditConnection,
  startSpotifyConnection,
} from "@/lib/api";
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
      detail: "Add providers whenever you want Pulse to sharpen the map around your tastes.",
    };
  }
  return {
    eyebrow: "Demo mode",
    detail: "Start with Spotify for the fastest path in, or use a magic link if you prefer email.",
  };
}

export function AccountDock() {
  const { isConfigured, isLoading, isAuthenticated, user, signOut, authMethod } = useAuth();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState(
    "Continue with Spotify to jump into Pulse fast, or use a magic link if you prefer email.",
  );
  const [showSwitchForm, setShowSwitchForm] = useState(false);
  const [isConnectingReddit, setIsConnectingReddit] = useState(false);
  const [isLoadingSample, setIsLoadingSample] = useState(false);
  const [isConnectingSpotify, setIsConnectingSpotify] = useState(false);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const supabase = getSupabaseBrowserClient();

  const viewerQuery = useQuery({
    queryKey: ["auth-viewer", user?.id ?? "demo"],
    queryFn: getAuthViewer,
  });

  const isSignedIn = Boolean(isAuthenticated && !isLoading);
  const connectionMode = viewerQuery.data?.redditConnectionMode ?? "none";
  const status = statusCopy(connectionMode, isSignedIn);
  const spotifyConnected = Boolean(viewerQuery.data?.spotifyConnected);

  const spotifyPreviewQuery = useQuery({
    queryKey: ["spotify-taste-preview", user?.id ?? "demo"],
    queryFn: getSpotifyTastePreview,
    enabled: isSignedIn && spotifyConnected,
  });
  const spotifyThemes = spotifyPreviewQuery.data?.themes ?? [];
  const hasSpotifyThemes = spotifyThemes.length > 0;
  const spotifyLowSignalReason =
    typeof spotifyPreviewQuery.data?.unmatchedActivity?.reason === "string"
      ? spotifyPreviewQuery.data.unmatchedActivity.reason
      : "Pulse needs a little more Spotify listening history before it can infer strong nightlife themes.";

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

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const searchParams = new URLSearchParams(window.location.search);
    const spotifyStatus = searchParams.get("spotify");
    if (spotifyStatus !== "connected") {
      return;
    }

    setOpen(true);
    setMessage("Spotify connected. Review the inferred taste themes and apply them when they look right.");
    void queryClient.invalidateQueries({ queryKey: ["auth-viewer"] });
    void queryClient.invalidateQueries({ queryKey: ["spotify-taste-preview"] });
    window.history.replaceState({}, "", window.location.pathname);
  }, [queryClient]);

  useEffect(() => {
    if (!spotifyPreviewQuery.error) {
      return;
    }

    setMessage(
      spotifyPreviewQuery.error instanceof Error
        ? spotifyPreviewQuery.error.message
        : "Unable to read a Spotify taste preview right now.",
    );
  }, [spotifyPreviewQuery.error]);

  const sendMagicLink = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!supabase) {
      setMessage("Supabase browser keys are missing. Configure them to enable magic-link sign-in.");
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
    if (!isAuthenticated) {
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
    if (!isAuthenticated) {
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

  const connectSpotify = async () => {
    setIsConnectingSpotify(true);
    try {
      const response = await startSpotifyConnection();
      window.location.assign(response.authorizeUrl);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to start Spotify connection.");
      setIsConnectingSpotify(false);
    }
  };

  const applySpotifyProfile = async () => {
    try {
      await applySpotifyTaste();
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["auth-viewer"] }),
        queryClient.invalidateQueries({ queryKey: ["interests"] }),
        queryClient.invalidateQueries({ queryKey: ["map-recommendations"] }),
        queryClient.invalidateQueries({ queryKey: ["archive"] }),
        queryClient.invalidateQueries({ queryKey: ["spotify-taste-preview"] }),
      ]);
      setMessage("Spotify taste applied. The map and signals just refreshed with the new provider profile.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to apply the Spotify taste profile.");
    }
  };

  return (
    <div className="relative z-[70] overflow-visible">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="inline-flex h-11 items-center gap-2 rounded-full border border-stroke bg-white/70 px-4 py-2 text-left text-sm font-medium text-slate-700 transition hover:bg-white"
      >
        <span className="text-sm font-medium text-slate-900">Profile</span>
        {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
      </button>

      {open ? (
        <div
          ref={panelRef}
          className="absolute right-0 top-[calc(100%+0.5rem)] z-[90] w-[min(24rem,calc(100vw-2rem))] max-h-[calc(100vh-120px)] overflow-y-auto rounded-[1.5rem] border border-stroke bg-white p-4 shadow-[0_22px_60px_rgba(15,23,42,0.18)]"
        >
          <div className="absolute right-6 top-0 h-3.5 w-3.5 -translate-y-1/2 rotate-45 border-l border-t border-stroke bg-white" />

          <div className="relative flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Account</p>
              <h3 className="mt-1 text-xl font-semibold text-slate-900">{isSignedIn ? "Profile" : "Start quietly"}</h3>
            </div>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-stroke bg-white text-slate-500 transition hover:text-slate-900"
              aria-label="Close"
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
                Pulse auth is not configured in this environment yet.
              </p>
            )}
          </div>

          {isSignedIn ? (
            <div className="mt-4 space-y-3">
              <div className="grid gap-2 sm:grid-cols-2">
                {!spotifyConnected ? (
                  <button
                    type="button"
                    onClick={() => void connectSpotify()}
                    disabled={isConnectingSpotify}
                    className="inline-flex items-center justify-center gap-2 rounded-full bg-emerald-600 px-4 py-2.5 text-sm font-medium text-white disabled:opacity-60"
                  >
                    <Disc3 className="h-4 w-4" />
                    {isConnectingSpotify ? "Redirecting..." : authMethod === "pulse_session" ? "Reconnect Spotify" : "Connect Spotify"}
                  </button>
                ) : hasSpotifyThemes ? (
                  <button
                    type="button"
                    onClick={() => void applySpotifyProfile()}
                    disabled={spotifyPreviewQuery.isLoading}
                    className="inline-flex items-center justify-center gap-2 rounded-full bg-emerald-600 px-4 py-2.5 text-sm font-medium text-white disabled:opacity-60"
                  >
                    <Disc3 className="h-4 w-4" />
                    {spotifyPreviewQuery.isLoading ? "Reading Spotify..." : "Use Spotify taste"}
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={() => void spotifyPreviewQuery.refetch()}
                    disabled={spotifyPreviewQuery.isLoading}
                    className="inline-flex items-center justify-center gap-2 rounded-full border border-stroke bg-white px-4 py-2.5 text-sm font-medium text-slate-700 disabled:opacity-60"
                  >
                    <Disc3 className="h-4 w-4" />
                    {spotifyPreviewQuery.isLoading ? "Reading Spotify..." : "Refresh Spotify read"}
                  </button>
                )}

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

              {spotifyConnected && spotifyPreviewQuery.data ? (
                <div className="rounded-[1.15rem] border border-stroke/80 bg-white/80 px-3 py-3">
                  <div className="flex items-center gap-2">
                    <span className="inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-700">
                      <Disc3 className="h-3.5 w-3.5" />
                      Spotify connected
                    </span>
                  </div>
                  {hasSpotifyThemes ? (
                    <>
                      <p className="mt-3 text-sm leading-6 text-slate-600">
                        Pulse found {spotifyThemes.length} possible themes from your listening history.
                      </p>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {spotifyThemes.slice(0, 3).map((theme) => (
                          <span
                            key={theme.id}
                            className="rounded-full border border-stroke bg-white px-3 py-1.5 text-sm font-medium text-slate-700"
                          >
                            {theme.label} · {theme.confidenceLabel}
                          </span>
                        ))}
                      </div>
                    </>
                  ) : (
                    <>
                      <p className="mt-3 text-sm leading-6 text-slate-600">{spotifyLowSignalReason}</p>
                      <p className="mt-2 text-xs leading-5 text-slate-500">
                        Keep listening on Spotify for a bit longer, then tap refresh and Pulse will try again.
                      </p>
                    </>
                  )}
                </div>
              ) : null}
              <div className="flex flex-wrap items-center justify-between gap-3 border-t border-stroke/80 pt-3">
                {authMethod === "supabase" ? (
                  <button
                    type="button"
                    onClick={() => setShowSwitchForm((value) => !value)}
                    className="inline-flex items-center gap-2 text-sm font-medium text-slate-600 transition hover:text-slate-900"
                  >
                    {showSwitchForm ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
                    Use another email
                  </button>
                ) : (
                  <span className="text-xs leading-5 text-slate-500">
                    Spotify started this Pulse session. You can still add magic-link sign-in anytime.
                  </span>
                )}
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
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Add magic-link sign-in</p>
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
                {isSignedIn ? "Send magic link to this email" : "Send magic link"}
              </button>
            </form>
          ) : null}

          {!isSignedIn ? (
            <div className="mt-4 grid gap-2">
              <button
                type="button"
                onClick={() => void connectSpotify()}
                disabled={isConnectingSpotify}
                className="inline-flex items-center justify-center gap-2 rounded-full bg-emerald-600 px-4 py-2.5 text-sm font-medium text-white disabled:opacity-60"
              >
                <Disc3 className="h-4 w-4" />
                {isConnectingSpotify ? "Redirecting to Spotify..." : "Continue with Spotify"}
              </button>
            </div>
          ) : null}

          <p className="mt-4 text-xs leading-5 text-slate-500">{message}</p>
        </div>
      ) : null}
    </div>
  );
}
