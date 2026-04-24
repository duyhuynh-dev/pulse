"use client";

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronDown, ChevronUp, Link2, LogOut, Mail, UserRound } from "lucide-react";
import { getAuthViewer, startMockRedditConnection, startRedditConnection } from "@/lib/api";
import { getSupabaseBrowserClient } from "@/lib/supabase-browser";
import { useAuth } from "@/components/auth-provider";

export function MagicLinkCard({ compact = false }: { compact?: boolean }) {
  const { isConfigured, isLoading, isAuthenticated, user, signOut, authMethod } = useAuth();
  const queryClient = useQueryClient();
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("Use email magic links for Pulse identity, then connect Reddit as a separate signal source.");
  const [isConnectingReddit, setIsConnectingReddit] = useState(false);
  const [isLoadingSample, setIsLoadingSample] = useState(false);
  const [showSwitchForm, setShowSwitchForm] = useState(false);
  const supabase = getSupabaseBrowserClient();

  const viewerQuery = useQuery({
    queryKey: ["auth-viewer", user?.id ?? "demo"],
    queryFn: getAuthViewer
  });
  const redditStatusLabel =
    viewerQuery.data?.redditConnectionMode === "live"
      ? "Reddit connected"
      : viewerQuery.data?.redditConnectionMode === "sample"
        ? "Sample profile attached"
        : isAuthenticated
          ? "Ready for Reddit"
          : "Identity only";
  const accountTitle = isAuthenticated ? "Account status" : "Magic-link sign in";
  const containerClass = compact
    ? "rounded-[1.5rem] border border-stroke/80 bg-white/60 p-4 backdrop-blur"
    : "rounded-[1.75rem] border border-stroke bg-white/70 p-4";

  const sendMagicLink = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!supabase) {
      setMessage("Supabase environment variables are missing. Configure them to enable magic-link auth.");
      return;
    }

    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: {
        emailRedirectTo: typeof window !== "undefined" ? window.location.origin : undefined
      }
    });

    setMessage(error ? error.message : "Check your inbox for a sign-in link.");
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
      setMessage("Sample Reddit profile loaded. You can keep building with the real user flow while approval is pending.");
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to load the sample Reddit profile.");
    } finally {
      setIsLoadingSample(false);
    }
  };

  const isLiveRedditConnected = viewerQuery.data?.redditConnectionMode === "live";
  const isSampleAttached = viewerQuery.data?.redditConnectionMode === "sample";
  const showSignedInAccount = Boolean(isAuthenticated && !isLoading);
  const identityLabel = showSignedInAccount ? "Identity set" : redditStatusLabel;
  const signedInMessage = isLiveRedditConnected
    ? "Live Reddit is attached, so this account can stay in the background while Pulse personalizes quietly."
    : isSampleAttached
      ? "A sample Reddit profile is attached while approval is pending. Swap to a live Reddit connection whenever you are ready."
      : "Your Pulse identity is already set. Connect Reddit only when you want live personalization.";

  return (
    <div className={containerClass}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2">
          {showSignedInAccount ? <UserRound className="h-5 w-5 text-accent" /> : <Mail className="h-5 w-5 text-accent" />}
          <h3 className={compact ? "text-base font-semibold" : "text-lg font-semibold"}>{accountTitle}</h3>
        </div>
        {compact ? (
          <span className="rounded-full bg-canvas px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
            {identityLabel}
          </span>
        ) : null}
      </div>
      <p className={compact ? "mt-2 text-sm leading-5 text-slate-600" : "mt-2 text-sm leading-6 text-slate-600"}>
        {showSignedInAccount
          ? "You are already in. Keep account controls tucked here and let the map do the talking."
          : compact
          ? "Keep account setup tucked away here, then attach Reddit when you want live personalization."
          : message}
      </p>

      <div className={compact ? "mt-3 rounded-[1.25rem] bg-canvas/80 px-3 py-3 text-sm text-slate-700" : "mt-3 rounded-2xl bg-canvas px-3 py-3 text-sm text-slate-700"}>
        {!isConfigured ? (
          <p>Supabase browser keys are missing, so the app stays in demo mode until those env vars are added.</p>
        ) : isLoading ? (
          <p>Checking your session...</p>
        ) : isAuthenticated ? (
          <div className={compact ? "space-y-3" : "space-y-2"}>
            <div className="rounded-[1.15rem] border border-stroke/80 bg-white/80 px-3 py-3">
              <div className="flex flex-wrap items-center gap-2">
                <span className="inline-flex items-center gap-2 rounded-full bg-accentSoft px-3 py-1.5 text-sm font-medium text-slate-900">
                  <CheckCircle2 className="h-4 w-4 text-accent" />
                  <span className="font-semibold">{user?.email}</span>
                </span>
                <span className="rounded-full border border-stroke bg-white px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                  {redditStatusLabel}
                </span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-600">{signedInMessage}</p>
            </div>

            <div className="grid gap-2 sm:grid-cols-2">
              {!isLiveRedditConnected ? (
                <button
                  type="button"
                  onClick={() => void connectReddit()}
                  disabled={isConnectingReddit}
                  className="inline-flex items-center justify-center gap-2 rounded-full bg-slate-900 px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
                >
                  <Link2 className="h-4 w-4" />
                  {isConnectingReddit ? "Redirecting..." : isSampleAttached ? "Connect live Reddit" : "Connect Reddit"}
                </button>
              ) : (
                <div className="inline-flex items-center justify-center rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm font-medium text-emerald-700">
                  Live Reddit connected
                </div>
              )}

              {!isSampleAttached && !isLiveRedditConnected ? (
                <button
                  type="button"
                  onClick={() => void connectSampleProfile()}
                  disabled={isLoadingSample}
                  className="inline-flex items-center justify-center gap-2 rounded-full border border-stroke bg-white px-4 py-2 text-sm font-medium text-slate-700 disabled:opacity-60"
                >
                  <Link2 className="h-4 w-4" />
                  {isLoadingSample ? "Loading sample..." : "Use sample profile"}
                </button>
              ) : null}
            </div>

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
                  This Pulse session was started by a provider. Add magic-link sign-in any time.
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
        ) : (
          <p>No active session yet. Send a magic link, then come back here to connect Reddit.</p>
        )}
      </div>

      {!showSignedInAccount || showSwitchForm ? (
        <div className={showSignedInAccount ? "mt-3 rounded-[1.25rem] border border-dashed border-stroke bg-white/50 p-3" : ""}>
          {showSignedInAccount ? (
            <p className="mb-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
              Switch account
            </p>
          ) : null}

          <form onSubmit={sendMagicLink} className={compact ? "grid gap-2" : "grid gap-3"}>
            <input
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              type="email"
              required
              placeholder="you@example.com"
              className="rounded-2xl border border-stroke bg-white px-3 py-2 text-sm"
            />

            <button type="submit" className="rounded-full bg-slate-900 px-4 py-2 text-sm font-medium text-white">
              {showSignedInAccount ? "Send magic link to another email" : "Send magic link"}
            </button>
          </form>
        </div>
      ) : null}

      {!showSignedInAccount ? (
        <div className={compact ? "mt-3 grid gap-2" : undefined}>
          <button
            type="button"
            onClick={() => void connectReddit()}
            disabled={!isAuthenticated || isConnectingReddit}
            className={
              compact
                ? "inline-flex items-center justify-center gap-2 rounded-full border border-stroke px-4 py-2 text-sm font-medium text-slate-700 disabled:opacity-60"
                : "mt-4 inline-flex items-center justify-center gap-2 rounded-full border border-stroke px-4 py-2 text-sm font-medium text-slate-700 disabled:opacity-60"
            }
          >
            <Link2 className="h-4 w-4" />
            {isConnectingReddit ? "Redirecting to Reddit..." : "Connect Reddit"}
          </button>

          <button
            type="button"
            onClick={() => void connectSampleProfile()}
            disabled={!isAuthenticated || isLoadingSample}
            className={
              compact
                ? "inline-flex items-center justify-center gap-2 rounded-full bg-accent px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
                : "mt-3 inline-flex items-center justify-center gap-2 rounded-full bg-accent px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
            }
          >
            <Link2 className="h-4 w-4" />
            {isLoadingSample ? "Loading sample profile..." : "Load sample Reddit profile"}
          </button>
        </div>
      ) : null}

      {compact ? (
        <p className="mt-3 text-xs leading-5 text-slate-500">
          {showSignedInAccount
            ? "Identity is already handled, so you only come back here when you want to switch or connect."
            : message}
        </p>
      ) : null}
    </div>
  );
}
