import { renderHook } from "@testing-library/react";
import { expect, test } from "vitest";
import { eventLink } from "../src/lib/event-link";
import { useFeedFilters } from "../src/hooks/useFeedFilters";
import type { ScoutEvent } from "../src/data/events";

const event = {
  name: "AI Workshop",
  venue: "Campus",
  neighborhood: "Miami",
  price: "Free",
} as ScoutEvent;
test("legacy unverified events cannot open their model-written URL", () => {
  const link = eventLink({
    ...event,
    url: "https://invented.example/events/fake",
    url_verified: false,
  });
  expect(new URL(link.href).hostname).toBe("www.google.com");
  expect(link.verified).toBe(false);
  expect(new URL(link.href).searchParams.get("q")).toContain("AI Workshop");
});
test("a source URL opens directly; unsafe or malformed URLs do not", () => {
  const url = "https://calendar.fiu.edu/event/workshop";
  expect(eventLink({ ...event, url, url_verified: true })).toEqual({ href: url, verified: true });
  for (const url of [
    "javascript:alert(1)",
    "https://[invalid",
    "https://user:pass@example.com/event",
  ]) {
    expect(eventLink({ ...event, url, url_verified: true }).verified).toBe(false);
  }
});
test("old saved AI cards must reacquire source evidence before linking directly", () => {
  const saved = { ...event, id: "ai-old", url: "https://example.com/events/wrong", url_verified: true };
  expect(eventLink(saved).verified).toBe(false);
  expect(eventLink({ ...saved, url_source: "grounding" }).verified).toBe(true);
});
test("month names without weekdays survive chronological sorting", () => {
  const { result } = renderHook(useFeedFilters);
  expect(
    result.current
      .sortEvents([
        { ...event, id: "later", date: "Oct 10, 2026" },
        { ...event, id: "first", date: "Sep 29, 2026" },
        { ...event, id: "last", date: "TBA" },
      ])
      .map((e) => e.id),
  ).toEqual(["first", "later", "last"]);
});
