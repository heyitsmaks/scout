import { useCallback, useEffect, useRef, useState } from "react";
import { EventStream } from "../lib/event-stream";
import { API_BASE } from "../lib/utils";
import { getAccessToken } from "../lib/supabase";
import type { ScoutEvent } from "../data/events";
import type { Profile } from "./useFeedSync";

export type SourceTab = "picks" | "major" | "search";

export interface SourceState {
  events: ScoutEvent[];
  loading: boolean;
  loaded: boolean;
  error: string | null;
}

const EMPTY: SourceState = { events: [], loading: false, loaded: false, error: null };

// Normalize a batch payload into a ScoutEvent[]. Accepts either an array or
// `{ events: [...] }`.
function extractEvents(data: unknown): ScoutEvent[] {
  if (Array.isArray(data)) return data as ScoutEvent[];
  if (data && typeof data === "object" && Array.isArray((data as { events?: unknown }).events)) {
    return (data as { events: ScoutEvent[] }).events;
  }
  return [];
}

// Owns lazy, per-tab SSE streaming for the three event source endpoints.
// Each tab opens an EventSource only when first requested; events arrive
// progressively and are appended as each batch streams in.
export function useEventSources(profile: Profile | null) {
  const [picks, setPicks] = useState<SourceState>(EMPTY);
  const [major, setMajor] = useState<SourceState>(EMPTY);
  const [search, setSearch] = useState<SourceState>(EMPTY);

  const esRef = useRef<Record<SourceTab, EventStream | null>>({
    picks: null,
    major: null,
    search: null,
  });

  // Tracks in-flight / completed state synchronously, independent of React's
  // (possibly deferred) state updater execution. Used to gate duplicate streams.
  const statusRef = useRef<Record<SourceTab, { loading: boolean; loaded: boolean }>>({
    picks: { loading: false, loaded: false },
    major: { loading: false, loaded: false },
    search: { loading: false, loaded: false },
  });

  const generation = useRef(0);
  const searchRequest = useRef(0);

  const closeStream = useCallback((tab: SourceTab) => {
    esRef.current[tab]?.close();
    esRef.current[tab] = null;
  }, []);

  // Opens an SSE connection for a tab and wires progressive updates.
  const openStream = useCallback(
    (
      tab: SourceTab,
      url: string,
      token: string,
      setState: React.Dispatch<React.SetStateAction<SourceState>>,
      errorMessage: string,
    ) => {
      closeStream(tab);
      statusRef.current[tab] = { loading: true, loaded: false };
      setState({ events: [], loading: true, loaded: false, error: null });

      const es = new EventStream(url, token);
      esRef.current[tab] = es;

      es.onmessage = (e) => {
        if (esRef.current[tab] !== es) return;
        let payload: { events?: unknown; status?: string; message?: string };
        try {
          payload = JSON.parse(e.data);
        } catch {
          return;
        }
        const batch = extractEvents(payload);
        // Backend signals failures as {status: "error", message} before the
        // stream ends — without this branch the UI sits on skeletons until
        // the connection close fires onerror (or forever behind a proxy).
        if (payload.status === "error") {
          statusRef.current[tab] = { loading: false, loaded: true };
          setState((prev) => ({
            events: prev.events,
            loading: false,
            loaded: true,
            // Keep partial results and expose the failure for manual retry.
            error: (typeof payload.message === "string" && payload.message) || errorMessage,
          }));
          closeStream(tab);
          return;
        }
        if (payload.status === "complete") {
          statusRef.current[tab] = { loading: false, loaded: true };
          setState((prev) => ({
            events: batch.length ? [...prev.events, ...batch] : prev.events,
            loading: false,
            loaded: true,
            error: null,
          }));
          closeStream(tab);
          return;
        }
        // status "searching" (or anything else with events) → append batch.
        if (batch.length) {
          setState((prev) => ({ ...prev, events: [...prev.events, ...batch] }));
        }
      };

      es.onerror = () => {
        if (esRef.current[tab] !== es) return;
        // A normal close after "complete" also fires onerror; ignore if done.
        if (statusRef.current[tab].loaded) {
          closeStream(tab);
          return;
        }
        statusRef.current[tab] = { loading: false, loaded: true };
        setState((prev) => ({
          events: prev.events,
          loading: false,
          loaded: true,
          error: errorMessage,
        }));
        closeStream(tab);
      };
    },
    [closeStream],
  );

  // Reset all streams/caches whenever the profile changes.
  useEffect(() => {
    generation.current++;
    statusRef.current = {
      picks: { loading: false, loaded: false },
      major: { loading: false, loaded: false },
      search: { loading: false, loaded: false },
    };
    closeStream("picks");
    closeStream("major");
    closeStream("search");
    setPicks(EMPTY);
    setMajor(EMPTY);
    setSearch(EMPTY);
  }, [profile, closeStream]);

  // Close every connection on unmount.
  useEffect(
    () => () => {
      generation.current++;
      esRef.current.picks?.close();
      esRef.current.major?.close();
      esRef.current.search?.close();
    },
    [],
  );

  const city = profile?.city || "Miami";
  const radius = profile?.radius ?? 25;

  const loadPicks = useCallback(
    async (force = false) => {
      const status = statusRef.current.picks;
      if (status.loading || (status.loaded && !force)) return;
      status.loading = true;

      const version = generation.current;
      const token = await getAccessToken().catch(() => null);
      if (version !== generation.current) return;
      if (!token) {
        statusRef.current.picks = { loading: false, loaded: true };
        setPicks({ ...EMPTY, loaded: true, error: "Please sign in again." });
        return;
      }
      const params = new URLSearchParams({
        city,
        preferences: profile?.vibes ?? "",
        radius: String(radius),
      });
      for (const interest of profile?.interests ?? []) params.append("interests", interest);
      if (profile?.university) params.append("university", profile.university);
      if (profile?.major) params.append("major", profile.major);

      openStream(
        "picks",
        `${API_BASE}/api/events/vibe/stream?${params}`,
        token,
        setPicks,
        "Couldn't load your picks. Try again.",
      );
    },
    [
      city,
      radius,
      profile?.vibes,
      profile?.interests,
      profile?.university,
      profile?.major,
      openStream,
    ],
  );

  const loadMajor = useCallback(
    async (force = false) => {
      if (!profile?.major) return;
      const status = statusRef.current.major;
      if (status.loading || (status.loaded && !force)) return;
      status.loading = true;

      const version = generation.current;
      const token = await getAccessToken().catch(() => null);
      if (version !== generation.current) return;
      if (!token) {
        statusRef.current.major = { loading: false, loaded: true };
        setMajor({ ...EMPTY, loaded: true, error: "Please sign in again." });
        return;
      }
      const params = new URLSearchParams({
        city,
        major: profile.major,
        radius: String(radius),
      });
      if (profile?.university) params.append("university", profile.university);

      openStream(
        "major",
        `${API_BASE}/api/events/major/stream?${params}`,
        token,
        setMajor,
        "Couldn't load major events. Try again.",
      );
    },
    [city, radius, profile?.major, profile?.university, openStream],
  );

  const runSearch = useCallback(
    async (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;

      if (statusRef.current.search.loading) return;
      statusRef.current.search.loading = true;
      const version = generation.current;
      const request = ++searchRequest.current;
      const token = await getAccessToken().catch(() => null);
      if (version !== generation.current || request !== searchRequest.current) return;
      if (!token) {
        statusRef.current.search = { loading: false, loaded: true };
        setSearch({ ...EMPTY, loaded: true, error: "Please sign in again." });
        return;
      }
      const params = new URLSearchParams({
        city,
        query: trimmed,
        radius: String(radius),
      });

      openStream(
        "search",
        `${API_BASE}/api/events/search/stream?${params}`,
        token,
        setSearch,
        "Search failed. Try again.",
      );
    },
    [city, radius, openStream],
  );

  return { picks, major, search, loadPicks, loadMajor, runSearch };
}
