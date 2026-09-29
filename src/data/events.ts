export type EventCategory =
  | "Tech & Startups"
  | "Concerts & Music"
  | "Football/Soccer"
  | "Networking"
  | "Art & Culture"
  | "Sports"
  | "Food & Drinks"
  | "Career & Jobs"
  | "Economics & Finance"
  | "Law & Politics"
  | "Health & Medicine"
  | "Engineering"
  | "Marketing & Business"
  | "Real Estate"
  | "Gaming"
  | "Photography"
  | "Travel";

export interface ScoutEvent {
  id: string;
  name: string;
  category: EventCategory;
  date: string;
  venue: string;
  neighborhood: string;
  price: string; // "Free" or e.g. "$25"
  description: string;
  url?: string;
  is_student_deal?: boolean;
  /** false → url is a Google search link, not a confirmed event page */
  url_verified?: boolean;
  url_source?: "grounding";
}
