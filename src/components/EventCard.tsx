import { useEffect, useState, useRef } from "react";
import { MapPin, Calendar, Tag, Check, Bookmark, Star } from "lucide-react";

import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import { Card, CardContent } from "./ui/card";
import {
  isSaved,
  toggleSaved,
  onSavedUpdated,
  dbSaveEvent,
  dbUnsaveEvent,
} from "../lib/saved-events";
import { getStorageUser } from "../lib/user-storage";
import { useAuth } from "../lib/auth";
import type { ScoutEvent, EventCategory } from "../data/events";
import { eventLink } from "../lib/event-link";

const categoryColorMap: Record<EventCategory, string> = {
  "Tech & Startups": "bg-sky-100 text-sky-800 border-sky-200",
  "Concerts & Music": "bg-violet-100 text-violet-800 border-violet-200",
  "Football/Soccer": "bg-emerald-100 text-emerald-800 border-emerald-200",
  Networking: "bg-orange-100 text-orange-800 border-orange-200",
  "Art & Culture": "bg-pink-100 text-pink-800 border-pink-200",
  Sports: "bg-teal-100 text-teal-800 border-teal-200",
  "Food & Drinks": "bg-amber-100 text-amber-800 border-amber-200",
  "Career & Jobs": "bg-indigo-100 text-indigo-800 border-indigo-200",
  "Economics & Finance": "bg-yellow-100 text-yellow-800 border-yellow-200",
  "Law & Politics": "bg-slate-200 text-slate-800 border-slate-300",
  "Health & Medicine": "bg-lime-100 text-lime-800 border-lime-200",
  Engineering: "bg-stone-200 text-stone-800 border-stone-300",
  "Marketing & Business": "bg-rose-100 text-rose-800 border-rose-200",
  "Real Estate": "bg-[#ede0d4] text-[#5c4033] border-[#dccbb8]",
  Gaming: "bg-fuchsia-100 text-fuchsia-800 border-fuchsia-200",
  Photography: "bg-neutral-200 text-neutral-800 border-neutral-300",
  Travel: "bg-cyan-100 text-cyan-800 border-cyan-200",
};

interface EventCardProps {
  event: ScoutEvent;
  onViewDetails: (event: ScoutEvent) => void;
  onCreatePost: (event: ScoutEvent) => void;
  onAttended?: (event: ScoutEvent) => void;
}

export function EventCard({ event, onViewDetails, onCreatePost, onAttended }: EventCardProps) {
  const isFree = event.price === "Free";
  const categoryColor =
    categoryColorMap[event.category] ?? "bg-secondary text-secondary-foreground";
  const savingRef = useRef(false);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const { session } = useAuth();

  useEffect(() => {
    const sync = () => setSaved(isSaved(event.id));
    sync();
    return onSavedUpdated(sync);
  }, [event.id]);

  const handleToggle = async () => {
    const userId = session?.user.id;
    if (!userId || savingRef.current) return;
    savingRef.current = true;
    setSaving(true);
    try {
      const wasSaved = isSaved(event.id);
      const ok = wasSaved
        ? await dbUnsaveEvent(userId, event.id)
        : await dbSaveEvent(userId, event);
      if (ok && getStorageUser() === userId) setSaved(toggleSaved(event));
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  };

  const hasDescription =
    !!event.description && event.description.trim().toLowerCase() !== "no description available";

  return (
    <Card className="relative flex h-full flex-col border-border shadow-[var(--shadow-soft)] transition-shadow hover:shadow-lg">
      <CardContent className="flex flex-1 flex-col gap-4 p-6">
        <div className="flex items-start justify-between gap-3">
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge variant="outline" className={`font-medium ${categoryColor}`}>
              <Tag className="mr-1 h-3 w-3" />
              {event.category}
            </Badge>
            {event.is_student_deal && (
              <Badge className="border-0 bg-emerald-500 text-[11px] font-semibold text-white hover:bg-emerald-500">
                Student Deal
              </Badge>
            )}
            {!eventLink(event).verified && (
              <Badge
                variant="outline"
                className="border-amber-300 bg-amber-50 text-[11px] font-medium text-amber-700"
              >
                Unverified
              </Badge>
            )}
          </div>
          {event.price && event.price !== "See tickets" && (
            <span
              className={
                isFree
                  ? "text-sm font-semibold text-foreground"
                  : "text-sm font-semibold text-muted-foreground"
              }
            >
              {event.price}
            </span>
          )}
        </div>

        <div className="space-y-2">
          <h3 className="text-lg font-bold leading-snug tracking-tight">{event.name}</h3>
          {hasDescription && (
            <p className="text-sm leading-relaxed text-muted-foreground">{event.description}</p>
          )}
        </div>

        <div className="mt-auto space-y-1.5 text-sm text-muted-foreground">
          <p className="flex items-center gap-2">
            <Calendar className="h-4 w-4 shrink-0" />
            {event.date}
          </p>
          <p className="flex items-center gap-2">
            <MapPin className="h-4 w-4 shrink-0" />
            {event.venue} · {event.neighborhood}
          </p>
        </div>

        <div className="flex flex-wrap gap-2 pt-1">
          <Button variant="outline" className="flex-1" onClick={() => onViewDetails(event)}>
            View Details
          </Button>
          <Button
            variant="outline"
            className="flex-1"
            onClick={handleToggle}
            disabled={saving}
            aria-pressed={saved}
            style={saved ? { borderColor: "#FF6B6B", color: "#FF6B6B" } : undefined}
          >
            {saved ? (
              <>
                <Check className="mr-1 h-4 w-4" />
                Saved
              </>
            ) : (
              <>
                <Bookmark className="mr-1 h-4 w-4" />
                Save
              </>
            )}
          </Button>
          <Button
            className="flex-1 text-white"
            onClick={() => onCreatePost(event)}
            style={{ backgroundColor: "#FF2D2D" }}
          >
            Create Post
          </Button>
        </div>

        {saved && onAttended && (
          <button
            type="button"
            className="mt-1 inline-flex items-center gap-1.5 self-start text-xs font-medium text-muted-foreground transition-colors hover:text-foreground"
            onClick={() => onAttended(event)}
          >
            <Star className="h-3.5 w-3.5" />
            Mark as attended
          </button>
        )}
      </CardContent>
    </Card>
  );
}
