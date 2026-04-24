"use client";

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { Session } from "@supabase/supabase-js";
import { getSupabaseBrowserClient } from "@/lib/supabase-browser";
import { signOutPulseSession } from "@/lib/api";
import type { AuthViewer } from "@/lib/types";

interface PulseAuthUser {
  id: string;
  email: string;
  displayName?: string | null;
}

interface AuthContextValue {
  session: Session | null;
  user: PulseAuthUser | null;
  isConfigured: boolean;
  isLoading: boolean;
  isAuthenticated: boolean;
  authMethod: AuthViewer["authMethod"];
  signOut: () => Promise<void>;
  refresh: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);
const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function fetchAuthViewer(session: Session | null): Promise<AuthViewer | null> {
  const headers = new Headers();
  if (session?.access_token) {
    headers.set("Authorization", `Bearer ${session.access_token}`);
  }

  const response = await fetch(`${API_URL}/v1/auth/me`, {
    headers,
    cache: "no-store",
    credentials: "include",
  });

  if (!response.ok) {
    return null;
  }

  return response.json() as Promise<AuthViewer>;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(null);
  const [viewer, setViewer] = useState<AuthViewer | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const client = getSupabaseBrowserClient();

  const refresh = async (nextSession: Session | null = session) => {
    const nextViewer = await fetchAuthViewer(nextSession);
    setViewer(nextViewer);
    setIsLoading(false);
  };

  useEffect(() => {
    let mounted = true;

    const syncViewer = async (nextSession: Session | null) => {
      const nextViewer = await fetchAuthViewer(nextSession);
      if (!mounted) {
        return;
      }
      setViewer(nextViewer);
      setIsLoading(false);
    };

    if (!client) {
      void syncViewer(null);
      return () => {
        mounted = false;
      };
    }

    void client.auth.getSession().then(({ data }) => {
      if (!mounted) {
        return;
      }

      setSession(data.session);
      void syncViewer(data.session);
    });

    const {
      data: { subscription },
    } = client.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      void syncViewer(nextSession);
    });

    return () => {
      mounted = false;
      subscription.unsubscribe();
    };
  }, [client]);

  const value = useMemo<AuthContextValue>(
    () => ({
      session,
      user: viewer?.isAuthenticated
        ? {
            id: viewer.userId,
            email: viewer.email,
            displayName: viewer.displayName,
          }
        : null,
      isConfigured: true,
      isLoading,
      isAuthenticated: Boolean(viewer?.isAuthenticated),
      authMethod: viewer?.authMethod ?? "demo",
      signOut: async () => {
        await signOutPulseSession().catch(() => undefined);
        if (client) {
          await client.auth.signOut();
        }
        setSession(null);
        await refresh(null);
      },
      refresh: async () => {
        await refresh();
      },
    }),
    [client, isLoading, refresh, session, viewer],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider.");
  }

  return context;
}
