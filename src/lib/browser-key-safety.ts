/** Reject privileged Supabase credentials before any browser bundle is built. */
export function assertBrowserSafeEnvironment(env: Record<string, unknown>) {
  for (const [name, value] of Object.entries(env)) {
    if (!name.startsWith("VITE_") || typeof value !== "string") continue;
    const key = value.trim();
    let privileged = key.startsWith("sb_secret_");
    const parts = key.split(".");
    if (parts.length === 3 && parts[0].startsWith("eyJ")) {
      try {
        const payload = JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/")));
        privileged ||= typeof payload.role === "string" && payload.role !== "anon";
      } catch {
        // Other public configuration may contain dotted strings.
      }
    }
    if (privileged) {
      // Never put the credential itself in build logs.
      throw new Error(
        `${name} contains a server credential. Use a Supabase publishable key in browser configuration.`,
      );
    }
  }
}
