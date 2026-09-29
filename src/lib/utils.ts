import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Use a local backend by default; set VITE_API_URL for deployment.
export const API_BASE =
  import.meta.env.VITE_API_URL || "http://localhost:8000";

/**
 * Returns the URL only if it is a safe http(s) link, otherwise null.
 * Prevents javascript: and other scheme injection from AI-generated URLs.
 */
export function safeHttpUrl(url?: string | null): string | null {
  if (!url) return null;
  const trimmed = url.trim();
  try {
    const parsed = new URL(trimmed);
    return ["https:", "http:"].includes(parsed.protocol) && !parsed.username && !parsed.password
      ? parsed.href
      : null;
  } catch {
    return null;
  }
}
