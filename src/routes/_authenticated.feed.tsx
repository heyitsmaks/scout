import { useEffect, useMemo, useState, useRef, type ReactNode } from "react";
import { createFileRoute, Link } from "@tanstack/react-router";
import {
  MapPin,
  Calendar,
  Tag,
  Loader2,
  CalendarX,
  Bookmark,
  Sparkles,
  CalendarDays,
  Search as SearchIcon,
  X,
} from "lucide-react";
import { format } from "date-fns";
import type { DateRange } from "react-day-picker";

import { EventCard } from "../components/EventCard";
import { ScoutLogoLink } from "../components/ScoutLogoLink";
import { PostGenerator } from "../components/PostGenerator";
import { AttendedSurvey, type SurveyAnswers } from "../components/AttendedSurvey";
import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { Input } from "../components/ui/input";
import { Textarea } from "../components/ui/textarea";
import { Skeleton } from "../components/ui/skeleton";
import { toast } from "sonner";
import { supabase } from "@/lib/supabase";
import { eventLink } from "../lib/event-link";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "../components/ui/dialog";
import { Popover, PopoverContent, PopoverTrigger } from "../components/ui/popover";
import { Calendar as CalendarPicker } from "../components/ui/calendar";
import { markAttended, dbMarkAttended, getAttendance } from "../lib/saved-events";
import { type ScoutEvent } from "../data/events";
import { getStorageUser } from "../lib/user-storage";
import { useAuth } from "../lib/auth";
import { useFeedSync, writeStoredProfile } from "../hooks/useFeedSync";
import { useEventSources, type SourceTab } from "../hooks/useEventSources";
import { useFeedFilters, type SortOption } from "../hooks/useFeedFilters";

export const Route = createFileRoute("/_authenticated/feed")({
  head: () => ({
    meta: [
      { title: "Your Scout feed — Miami events" },
      {
        name: "description",
        content: "Browse personalized Miami events: concerts, meetups, games, food and careers.",
      },
    ],
  }),
  component: Feed,
});

function EmptyState({ icon, message }: { icon: ReactNode; message: string }) {
  return (
    <div className="flex flex-col items-center gap-4 py-20 text-center">
      {icon}
      <p className="max-w-md text-muted-foreground">{message}</p>
    </div>
  );
}

const COMPANY_MAP: Record<string, string> = {
  Solo: "solo",
  Friends: "friends",
  "Work/Networking": "work",
};

// Active tab/pill: vivid red gradient with a warm glow.
const RED_PILL = {
  backgroundImage: "linear-gradient(135deg, #FF2D2D, #E60000)",
  boxShadow: "0 4px 15px rgba(255, 45, 45, 0.4)",
};

export function Feed() {
  const { session } = useAuth();
  const [detailsEvent, setDetailsEvent] = useState<ScoutEvent | null>(null);
  const detailsLink = detailsEvent ? eventLink(detailsEvent) : null;
  const [postEvent, setPostEvent] = useState<ScoutEvent | null>(null);
  const [postOpen, setPostOpen] = useState(false);
  const [surveyEvent, setSurveyEvent] = useState<ScoutEvent | null>(null);
  const [surveyOpen, setSurveyOpen] = useState(false);
  const attendanceSaving = useRef(false);
  const [surveyAnswers, setSurveyAnswers] = useState<SurveyAnswers | null>(null);
  const [tab, setTab] = useState("discover");

  // Inner source tab (which backend endpoint feeds the Discover grid).
  const [source, setSource] = useState<SourceTab>("picks");
  const [searchInput, setSearchInput] = useState("");

  // Vibes gate for My Picks: hide events behind a "what's your vibe?" prompt
  // until the user has saved preferences.
  const [editingVibe, setEditingVibe] = useState(false);
  const [vibeInput, setVibeInput] = useState("");
  const [savingVibe, setSavingVibe] = useState(false);

  const { profile, savedEvents, attendedEvents } = useFeedSync();
  const { picks, major, search, loadPicks, loadMajor, runSearch } = useEventSources(profile);

  const currentVibes = (profile?.vibes ?? "").trim();
  const hasVibes = currentVibes.length > 0;
  const showVibeGate = !!profile && source === "picks" && (!hasVibes || editingVibe);

  const saveVibe = async () => {
    const text = vibeInput.trim();
    if (!text) return;
    setSavingVibe(true);
    try {
      const { data, error: authError } = await supabase.auth.getUser();
      if (authError || !data.user) throw authError ?? new Error("Please sign in again.");
      const userId = data.user.id;
      const { data: updated, error } = await supabase
        .from("profiles")
        .update({ vibes: text })
        .eq("id", userId)
        .select("id")
        .single();
      if (error || !updated) throw error ?? new Error("Profile was not updated.");
      if (getStorageUser() !== userId) return;
      // Updates the stored profile AND notifies useFeedSync — the profile
      // object changes, useEventSources resets its streams, and the load
      // effect below re-opens the picks stream with the new preferences.
      writeStoredProfile({ vibes: text });
      setEditingVibe(false);
    } catch (err) {
      console.error("[scout] save vibe failed:", err);
      toast.error("Couldn't save your vibe — please try again.");
    } finally {
      setSavingVibe(false);
    }
  };

  const { sort, setSort, dateRange, setDateRange, applyFilters, sortEvents } = useFeedFilters();

  // Lazily load the active source tab on first activation / profile change.
  // Gated on the profile being loaded (a fresh login must not stream the
  // default city while the DB sync is in flight) and, for picks, on vibes
  // existing (no point streaming behind the vibe gate with empty preferences).
  useEffect(() => {
    if (!profile) return;
    if (source === "picks" && hasVibes) loadPicks();
    else if (source === "major") loadMajor();
  }, [profile, hasVibes, source, loadPicks, loadMajor]);

  const user = session?.user;
  const avatarUrl = user?.user_metadata?.avatar_url as string | undefined;
  const profileInitials = (() => {
    const src =
      (user?.user_metadata?.full_name as string | undefined)?.trim() ||
      user?.email?.split("@")[0] ||
      "";
    const parts = src.split(/[\s._-]+/).filter(Boolean);
    if (parts.length === 0) return "?";
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
  })();

  const openPost = (event: ScoutEvent) => {
    setSurveyAnswers(getAttendance(event.id));
    setPostEvent(event);
    setPostOpen(true);
  };

  const openSurvey = (event: ScoutEvent) => {
    setSurveyEvent(event);
    setSurveyOpen(true);
  };

  const handleSurveySubmit = async (answers: SurveyAnswers) => {
    if (attendanceSaving.current) return;
    attendanceSaving.current = true;
    try {
      if (surveyEvent) {
        if (session?.user?.id) {
          const ok = await dbMarkAttended(
            session.user.id,
            surveyEvent,
            answers.rating || null,
            answers.highlight || null,
            answers.recommend,
            COMPANY_MAP[answers.companions ?? ""] ?? null,
          );
          if (!ok || getStorageUser() !== session.user.id) return;
          markAttended(surveyEvent, answers);
        }
      }
      setSurveyOpen(false);
      setSurveyAnswers(answers);
      setPostEvent(surveyEvent);
      setPostOpen(true);
    } finally {
      attendanceSaving.current = false;
    }
  };

  const upcomingSaved = useMemo(() => sortEvents(savedEvents), [savedEvents, sortEvents]);

  // Active source state + filtered list.
  const activeState = source === "picks" ? picks : source === "major" ? major : search;
  const events = useMemo(
    () => applyFilters(activeState.events),
    [applyFilters, activeState.events],
  );

  const submitSearch = () => runSearch(searchInput);

  const sortDropdown = (
    <Select value={sort} onValueChange={(v) => setSort(v as SortOption)}>
      <SelectTrigger className="w-44 border-white/[0.15] bg-black/[0.35] text-white">
        <SelectValue placeholder="Sort events" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="soonest">Soonest first</SelectItem>
        <SelectItem value="latest">Latest first</SelectItem>
        <SelectItem value="free">Free first</SelectItem>
      </SelectContent>
    </Select>
  );

  const hasDateFilter = !!dateRange.from;
  const dateLabel = hasDateFilter
    ? dateRange.to && dateRange.to.getTime() !== dateRange.from!.getTime()
      ? `${format(dateRange.from!, "MMM d")} - ${format(dateRange.to, "MMM d")}`
      : format(dateRange.from!, "MMM d")
    : "Pick a date";

  const dateFilterButton = (
    <div
      className={`flex shrink-0 items-center gap-1 rounded-full pr-1.5 transition-colors ${
        hasDateFilter
          ? "text-white"
          : "border border-white/[0.15] bg-black/[0.35] text-white hover:bg-black/[0.45] pr-0"
      }`}
      style={hasDateFilter ? RED_PILL : undefined}
    >
      <Popover>
        <PopoverTrigger asChild>
          <button
            type="button"
            className="flex shrink-0 items-center gap-1.5 rounded-full px-4 py-1.5 text-sm font-medium max-sm:px-3 max-sm:text-[13px]"
          >
            <CalendarDays className="h-4 w-4" />
            {dateLabel}
          </button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <CalendarPicker
            mode="range"
            selected={dateRange.from ? { from: dateRange.from, to: dateRange.to } : undefined}
            onSelect={(range: DateRange | undefined) =>
              setDateRange({ from: range?.from, to: range?.to })
            }
            numberOfMonths={1}
            initialFocus
            className="pointer-events-auto"
          />
        </PopoverContent>
      </Popover>
      {hasDateFilter && (
        <button
          type="button"
          aria-label="Clear date filter"
          onClick={() => setDateRange({})}
          className="flex shrink-0 items-center border-l border-white/40 py-0.5 pr-1 pl-1.5 text-white transition-colors hover:text-white/80"
        >
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );

  const sourceTabClass = (active: boolean) =>
    `rounded-full px-5 py-2 text-sm font-medium transition-colors max-sm:px-3 max-sm:text-[13px] ${
      active
        ? "text-white"
        : "border border-white/[0.15] bg-black/[0.35] text-white hover:bg-black/[0.45]"
    }`;

  const sourceTabs = (
    <div className="relative z-10">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="relative">
          <div className="flex flex-nowrap items-center gap-2 overflow-x-auto whitespace-nowrap scrollbar-hide sm:flex-wrap">
            <button
              type="button"
              onClick={() => {
                setTab("discover");
                setSource("picks");
              }}
              className={sourceTabClass(tab === "discover" && source === "picks")}
              style={tab === "discover" && source === "picks" ? RED_PILL : undefined}
            >
              My Picks
            </button>
            {profile?.major && (
              <button
                type="button"
                onClick={() => {
                  setTab("discover");
                  setSource("major");
                }}
                className={sourceTabClass(tab === "discover" && source === "major")}
                style={tab === "discover" && source === "major" ? RED_PILL : undefined}
              >
                {profile.major}
              </button>
            )}
            <button
              type="button"
              onClick={() => {
                setTab("discover");
                setSource("search");
              }}
              className={sourceTabClass(tab === "discover" && source === "search")}
              style={tab === "discover" && source === "search" ? RED_PILL : undefined}
            >
              Search
            </button>
            <div className="mx-1 h-6 w-px shrink-0 bg-white/20" aria-hidden="true" />
            {dateFilterButton}
            <div className="mx-1 h-6 w-px shrink-0 bg-white/20" aria-hidden="true" />
            <button
              type="button"
              onClick={() => setTab("myevents")}
              className={sourceTabClass(tab === "myevents")}
              style={tab === "myevents" ? RED_PILL : undefined}
            >
              My Events
            </button>
          </div>
        </div>
        <div className="shrink-0">{sortDropdown}</div>
      </div>
    </div>
  );

  const searchBar = source === "search" && (
    <div className="mb-3 flex items-center gap-2">
      <div className="relative flex-1 max-w-md">
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submitSearch();
          }}
          placeholder="Search events…"
          className="pl-9"
        />
      </div>
      <Button onClick={submitSearch} disabled={!searchInput.trim() || search.loading}>
        Search
      </Button>
    </div>
  );

  const renderGrid = (list: ScoutEvent[]) => (
    <div
      className="grid gap-4"
      style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 320px), 1fr))" }}
    >
      {list.map((event) => (
        <div key={event.id} className="relative h-full">
          <EventCard
            event={event}
            onViewDetails={setDetailsEvent}
            onCreatePost={openPost}
            onAttended={openSurvey}
          />
        </div>
      ))}
    </div>
  );

  const loadingSkeletons = (
    <div
      className="grid gap-4"
      style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 320px), 1fr))" }}
    >
      {Array.from({ length: 3 }).map((_, i) => (
        <div
          key={i}
          className="flex h-full flex-col gap-4 rounded-xl border border-border bg-card p-6 shadow-[var(--shadow-soft)]"
        >
          <div className="flex items-center justify-between gap-3">
            <Skeleton className="h-6 w-28 rounded-full" />
            <Skeleton className="h-5 w-12 rounded-md" />
          </div>
          <div className="space-y-2">
            <Skeleton className="h-5 w-3/4 rounded-md" />
            <Skeleton className="h-4 w-full rounded-md" />
            <Skeleton className="h-4 w-5/6 rounded-md" />
          </div>
          <div className="mt-auto space-y-2">
            <Skeleton className="h-4 w-1/2 rounded-md" />
            <Skeleton className="h-4 w-2/3 rounded-md" />
          </div>
          <div className="flex gap-2 pt-1">
            <Skeleton className="h-9 flex-1 rounded-md" />
            <Skeleton className="h-9 flex-1 rounded-md" />
            <Skeleton className="h-9 flex-1 rounded-md" />
          </div>
        </div>
      ))}
    </div>
  );

  const renderSourceContent = () => {
    if (source === "search" && !activeState.loaded && !activeState.loading) {
      return (
        <EmptyState
          icon={<SearchIcon className="h-12 w-12 text-muted-foreground" />}
          message="Search for events by keyword to get started."
        />
      );
    }
    // Streaming with no results yet → full skeleton placeholder.
    if (activeState.loading && events.length === 0) return loadingSkeletons;
    if (activeState.error && events.length === 0) {
      return (
        <div className="flex flex-col items-center gap-4 py-20 text-center">
          <CalendarX className="h-12 w-12 text-muted-foreground" />
          <p className="max-w-md text-muted-foreground">{activeState.error}</p>
          <Button
            onClick={() => {
              if (source === "picks") loadPicks(true);
              else if (source === "major") loadMajor(true);
              else submitSearch();
            }}
          >
            Try again
          </Button>
        </div>
      );
    }
    if (events.length === 0) {
      return (
        <EmptyState
          icon={<CalendarX className="h-12 w-12 text-muted-foreground" />}
          message={
            source === "search"
              ? "No events matched your search. Try a different keyword."
              : `No events found for ${profile?.city || "your city"}. Try adjusting your filters.`
          }
        />
      );
    }
    return (
      <>
        {activeState.error && (
          <div role="status" className="mb-4 rounded-lg border p-4">
            <p>Showing partial results. {activeState.error}</p>
            <Button
              variant="outline"
              onClick={() => {
                if (source === "picks") loadPicks(true);
                else if (source === "major") loadMajor(true);
                else submitSearch();
              }}
            >
              Try again
            </Button>
          </div>
        )}
        {renderGrid(events)}
        {activeState.loading && (
          <div className="flex items-center justify-center gap-2 py-8 text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
            <span className="text-sm">Loading more events…</span>
          </div>
        )}
      </>
    );
  };

  const onDark = tab === "discover" || tab === "myevents";

  const feedHeader = (
    <header className="relative z-10">
      <div className="flex w-full items-center justify-between px-6 py-5">
        <ScoutLogoLink
          className={onDark ? "text-2xl font-extrabold text-white" : "text-2xl font-extrabold"}
        />
        <Link
          to="/profile"
          aria-label="Your profile"
          className={`flex items-center gap-2 rounded-full text-sm font-semibold transition-colors ${
            onDark
              ? "text-white hover:text-white/90"
              : "text-muted-foreground hover:text-foreground"
          }`}
        >
          <span
            className={`flex h-9 w-9 items-center justify-center overflow-hidden rounded-full transition-colors ${
              onDark
                ? "bg-white/10 text-white hover:bg-white/20"
                : "bg-muted text-muted-foreground hover:bg-muted/80"
            }`}
          >
            {avatarUrl ? (
              <img src={avatarUrl} alt="Your profile" className="h-full w-full object-cover" />
            ) : (
              profileInitials
            )}
          </span>
          <span>Profile</span>
        </Link>
      </div>
    </header>
  );

  return (
    <div className="relative min-h-screen bg-background">
      {/* Dark gradient header zone with subtle grain, fading into the card area */}
      {tab === "discover" || tab === "myevents" ? (
        <>
          <div className="relative z-10">
            <div className="relative z-10 flex min-h-[280px] flex-col">
              <div
                aria-hidden
                className="feed-header-zone grain-overlay pointer-events-none absolute inset-0 overflow-visible"
              >
                {/* Warm amber glow orb for depth */}
                <div
                  className="pointer-events-none absolute left-[58%] top-[35%] h-[240px] w-[240px] -translate-x-1/2 -translate-y-1/2"
                  style={{
                    background:
                      "radial-gradient(circle, rgba(255, 107, 0, 0.1) 0%, rgba(255, 107, 0, 0.05) 40%, transparent 70%)",
                    filter: "blur(60px)",
                  }}
                />
              </div>

              {feedHeader}

              <div className="relative z-10 flex flex-col flex-grow w-full px-6 py-4">
                <div className="flex flex-col flex-grow justify-between">
                  <div className="-mt-2 pb-6 text-center">
                    <p className="text-4xl font-bold tracking-tight text-white">
                      {tab === "discover"
                        ? profile?.city
                          ? `Events in ${profile.city}`
                          : "Upcoming in Miami"
                        : "My Events"}
                    </p>
                    {tab === "myevents" && (
                      <p className="mt-2 text-sm text-white/60">
                        Your upcoming and attended events, all in one place.
                      </p>
                    )}
                    {hasVibes && tab === "discover" && (
                      <button
                        type="button"
                        onClick={() => {
                          setSource("picks");
                          setVibeInput(currentVibes);
                          setEditingVibe(true);
                        }}
                        className="mt-2 text-sm font-medium text-white/70 underline underline-offset-4 transition-colors hover:text-white"
                      >
                        Edit vibe
                      </button>
                    )}
                  </div>
                </div>
              </div>
            </div>
            <div className="absolute bottom-0 left-0 right-0 z-10 px-6 pb-4">{sourceTabs}</div>
          </div>

          <main className="relative z-0 w-full px-6 py-4">
            {tab === "discover" ? (
              showVibeGate ? (
                <div className="mx-auto max-w-lg py-16 text-center">
                  <h2 className="text-2xl font-bold tracking-tight">What's your vibe?</h2>
                  <p className="mt-2 text-muted-foreground">
                    Tell us what you're into and we'll find events that actually match you.
                  </p>
                  <Textarea
                    aria-label="Your vibe"
                    autoFocus
                    maxLength={1000}
                    value={vibeInput}
                    onChange={(e) => setVibeInput(e.target.value)}
                    placeholder={`e.g. rooftop bars, Shakira, watch FIFA with a crowd,\njazz brunches, startup pitch nights...`}
                    className="mt-6 min-h-[120px] rounded-2xl p-4 text-base leading-relaxed"
                  />
                  <Button
                    onClick={saveVibe}
                    disabled={!vibeInput.trim() || savingVibe}
                    className="mt-4 w-full rounded-full border-0 font-semibold text-white"
                    style={RED_PILL}
                  >
                    {savingVibe ? "Saving…" : "Find My Events"}
                  </Button>
                  {editingVibe && hasVibes && (
                    <button
                      type="button"
                      disabled={savingVibe}
                      onClick={() => setEditingVibe(false)}
                      className="mt-3 text-sm text-muted-foreground transition-colors hover:text-foreground"
                    >
                      Cancel
                    </button>
                  )}
                </div>
              ) : (
                <>
                  {searchBar}
                  {renderSourceContent()}
                </>
              )
            ) : (
              <>
                {upcomingSaved.length === 0 && attendedEvents.length === 0 ? (
                  <div className="flex justify-center">
                    <div className="relative z-0 mx-auto max-w-md rounded-2xl border border-black/[0.1] bg-black/[0.06] p-8 text-center backdrop-blur-sm">
                      <Sparkles className="mx-auto h-10 w-10 text-[#111111]" />
                      <h3 className="mt-4 whitespace-normal text-center text-[20px] font-bold leading-tight text-[#111111]">
                        Your events, your story.
                      </h3>
                      <p className="mt-3 text-center text-sm leading-relaxed text-[#111111]/60">
                        Save events from your feed and they'll appear here. Turn any event into a
                        ready-to-post for LinkedIn, Instagram, or X — in one tap.
                      </p>
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="mb-8">
                      <h1 className="text-3xl font-extrabold tracking-tight">My Events</h1>
                      <p className="mt-2 text-muted-foreground">
                        Everything you've saved and attended, all in one place.
                      </p>
                    </div>

                    <section className="mb-12">
                      <h2 className="mb-4 text-xl font-bold tracking-tight">Upcoming</h2>
                      {upcomingSaved.length === 0 ? (
                        <EmptyState
                          icon={<Bookmark className="h-12 w-12 text-muted-foreground" />}
                          message="No upcoming events saved yet"
                        />
                      ) : (
                        renderGrid(upcomingSaved)
                      )}
                    </section>

                    <section>
                      <h2 className="mb-4 text-xl font-bold tracking-tight">Attended</h2>
                      {attendedEvents.length === 0 ? (
                        <EmptyState
                          icon={<CalendarX className="h-12 w-12 text-muted-foreground" />}
                          message="No attended events yet"
                        />
                      ) : (
                        renderGrid(attendedEvents)
                      )}
                    </section>
                  </>
                )}
              </>
            )}
          </main>
        </>
      ) : (
        <>
          {feedHeader}

          <main className="relative z-0 w-full px-6 py-4" />
        </>
      )}

      <Dialog open={!!detailsEvent} onOpenChange={(o) => !o && setDetailsEvent(null)}>
        <DialogContent className="sm:max-w-md">
          {detailsEvent && (
            <>
              <DialogHeader>
                <Badge variant="secondary" className="mb-2 w-fit font-medium">
                  <Tag className="mr-1 h-3 w-3" />
                  {detailsEvent.category}
                </Badge>
                <DialogTitle className="text-xl">{detailsEvent.name}</DialogTitle>
                <DialogDescription className="text-base leading-relaxed">
                  {detailsEvent.description}
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-2 text-sm text-muted-foreground">
                <p className="flex items-center gap-2">
                  <Calendar className="h-4 w-4" />
                  {detailsEvent.date}
                </p>
                <p className="flex items-center gap-2">
                  <MapPin className="h-4 w-4" />
                  {detailsEvent.venue} · {detailsEvent.neighborhood}
                </p>
                {detailsLink && (
                  <a
                    href={detailsLink.href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="block text-sky-600 hover:underline"
                  >
                    {detailsLink.verified ? "View event →" : "Find event on Google →"}
                  </a>
                )}
              </div>
            </>
          )}
        </DialogContent>
      </Dialog>

      <AttendedSurvey
        event={surveyEvent}
        open={surveyOpen}
        onOpenChange={setSurveyOpen}
        onSubmit={handleSurveySubmit}
      />

      <PostGenerator
        event={postEvent}
        open={postOpen}
        onOpenChange={setPostOpen}
        survey={surveyAnswers}
      />
    </div>
  );
}
