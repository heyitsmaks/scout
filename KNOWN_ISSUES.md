# Known limitations

- The offline suite does not verify OAuth configuration or Redis failover. The September audit exercised an existing authenticated production session, provider-backed search and post drafts, and transactional database writes. New signup, password recovery, and provider balances were not verified.
- An interrupted stream now retains partial results, shows an error and offers manual Retry. It does not resume from a cursor; retry can repeat unfinished AI work. There is no automatic retry loop.
- Saved/Attended writes use separate database calls. An attendance upsert followed by a failed Saved deletion can leave both rows in the database until the user retries; hydration excludes attended events from Saved. A transactional database RPC would improve this.
- The browser cache is scoped to account IDs. Legacy unscoped local-only data is deliberately not imported, since its owner cannot be established. Confirmed database records are restored after sign-in. There is no offline write queue.
- Ticketmaster radius is 1–50 miles. AI and campus sources do not enforce an exact geographic boundary.
- AI dates, descriptions and locations can still be incorrect. New AI candidates require an unambiguous grounding citation and a parseable date with a year. This is source attribution, not independent verification of the source page. Ambiguous candidates are excluded, reducing recall. Search constraints such as “free” or “this week” still need deterministic enforcement.
- New clients use header-authenticated fetch streaming. The backend still accepts legacy EventSource query tokens for already-open clients; redact historical access logs and remove that compatibility path once old clients have aged out.
- Daily AI limits count attempts, not dollars. Redis is needed to share counters across processes and restarts. Memory fallback is per-process and cannot enforce an account-wide spending ceiling. Set provider-side spending limits as well.
- Simultaneous cache misses across clients can duplicate paid work. Shared in-flight request coalescing is future work; daily and per-user limits mitigate, but do not remove, this cost.
- The repository still has an inherited lint backlog outside the changed files. Build, typecheck and regression tests are separate checks.
