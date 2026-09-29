import { useMemo, useState } from "react";
import type { ScoutEvent } from "../data/events";


export type SortOption = "soonest" | "latest" | "free";
export type DateRangeFilter = { from?: Date; to?: Date };

function parseEventDate(date: string): number {
  const [datePart, timePart] = date.split("·").map((s) => s.trim());
  const withoutWeekday = datePart.replace(/^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s+/i, "");
  // Cross-year events already contain a 4-digit year — don't append current year.
  const hasYear = /\b\d{4}\b/.test(withoutWeekday);
  const str = hasYear
    ? timePart
      ? `${withoutWeekday} ${timePart}`
      : withoutWeekday
    : timePart
      ? `${withoutWeekday} ${new Date().getFullYear()} ${timePart}`
      : `${withoutWeekday} ${new Date().getFullYear()}`;
  const parsed = new Date(str).getTime();
  return Number.isNaN(parsed) ? 0 : parsed;
}

// An event has an "exact" date when it isn't TBD, isn't a date range,
// and parses to a real calendar date.
function hasExactDate(date: string): boolean {
  if (!date) return false;
  const datePart = date.split("·")[0].trim();
  if (/tbd|tba/i.test(date)) return false;
  // Date range markers: dash variants or "to"
  if (/[-–—]|\bto\b/i.test(datePart)) return false;
  return parseEventDate(date) > 0;
}

function eventMatchesDateRange(eventDateStr: string, range: DateRangeFilter): boolean {
  const ts = parseEventDate(eventDateStr);
  if (ts <= 0) return false;
  const eventDate = new Date(ts);
  const eventDay = new Date(eventDate.getFullYear(), eventDate.getMonth(), eventDate.getDate());

  const from = range.from
    ? new Date(range.from.getFullYear(), range.from.getMonth(), range.from.getDate())
    : null;
  const to = range.to
    ? new Date(range.to.getFullYear(), range.to.getMonth(), range.to.getDate())
    : from; // single-date selection: match that day only

  if (!from) return true;
  return eventDay >= from && eventDay <= (to ?? from);
}

// Owns sort/exact-date/date-range filter state and exposes helpers to apply
// them to any event list.
export function useFeedFilters() {
  const [sort, setSort] = useState<SortOption>("soonest");
  const [exactOnly, setExactOnly] = useState(false);
  const [dateRange, setDateRange] = useState<DateRangeFilter>({});

  // Sort that always pushes undated/unparseable events to the very end,
  // regardless of the chosen sort direction.
  const sortEvents = useMemo(() => {
    return (list: ScoutEvent[]) => {
      const base = [...list];
      base.sort((a, b) => {
        const ae = hasExactDate(a.date);
        const be = hasExactDate(b.date);
        if (ae !== be) return ae ? -1 : 1; // undated go last
        if (!ae && !be) return 0;
        if (sort === "free") {
          const af = a.price.toLowerCase() === "free" ? 0 : 1;
          const bf = b.price.toLowerCase() === "free" ? 0 : 1;
          if (af !== bf) return af - bf;
          return parseEventDate(a.date) - parseEventDate(b.date);
        }
        if (sort === "latest") return parseEventDate(b.date) - parseEventDate(a.date);
        return parseEventDate(a.date) - parseEventDate(b.date);
      });
      return base;
    };
  }, [sort]);

  const applyFilters = useMemo(() => {
    return (list: ScoutEvent[]) => {
      let base = [...list];
      if (exactOnly) base = base.filter((e) => hasExactDate(e.date));
      if (dateRange.from) {
        base = base.filter((e) => eventMatchesDateRange(e.date, dateRange));
      }
      return sortEvents(base);
    };
  }, [exactOnly, dateRange, sortEvents]);

  return {
    sort,
    setSort,
    exactOnly,
    setExactOnly,
    dateRange,
    setDateRange,
    applyFilters,
    sortEvents,
  };
}
