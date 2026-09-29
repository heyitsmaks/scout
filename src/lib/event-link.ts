import type { ScoutEvent } from "../data/events";
import { safeHttpUrl } from "./utils";

// Saved events may predate server validation. Never open a model-written URL
// under the label "Find event on Google".
export function eventLink(event: ScoutEvent): { href: string; verified: boolean } {
  const source = safeHttpUrl(event.url);
  const legacyAI = event.id?.startsWith("ai-") && event.url_source !== "grounding";
  if (event.url_verified === true && source && !legacyAI) return { href: source, verified: true };
  const query = [event.name, event.venue, event.neighborhood].filter(Boolean).join(" ");
  return { href: `https://www.google.com/search?q=${encodeURIComponent(query)}`, verified: false };
}
