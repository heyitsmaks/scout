# September 2026 reliability release

## Problems addressed

- Edit vibe opened an invisible editor outside My Picks. The control now switches to My Picks and focuses the editor. A missing session, zero-row update, or database error cannot be reported as a successful save.
- Provider exceptions were swallowed and cached as successful empty searches. Failed batches now produce a visible error, preserve already-streamed events, and skip AI cache writes. Missing provider credentials and exhausted budgets also produce a visible degraded state.
- A model-written event URL could remain clickable without a grounding source. New AI candidates must have an unambiguous citation tied to one event and one source URL. Whole-response citations, ambiguous sources, absent grounding, and definitively deleted Eventbrite pages are excluded. This reduces coverage deliberately; it does not prove all event details.
- Direct grounding URLs are supported, including URLs already equal to the model's output. Server-side redirect resolution is restricted to Google's grounding wrapper; arbitrary source URLs are not fetched by this resolver.
- Date-only past events expire at their local day boundary. September abbreviations parse correctly. Yearless AI dates are no longer rolled into next year. Unknown AI dates are excluded.
- Legacy saved AI records fall back to an actual Google search link unless they carry the new grounding provenance marker. The link label matches its destination.
- New SSE requests use an Authorization header instead of putting the session token in their URL. Connections are cancellable and do not reconnect automatically.
- Client database roles no longer have TRUNCATE, REFERENCES, or TRIGGER on application tables. The optional automatic-RLS event trigger is no longer executable by client roles. Older bigint event IDs retain authenticated sequence usage.
- Post generation instructions explicitly prohibit inventing times, dates, prices, personal history, or attendance. This is a prompt safeguard, not a deterministic factuality guarantee.

Cache version 6 invalidates older AI/campus/provider results after deployment. It does not delete saved events or profiles. `/api/health` includes the backend revision and cache version so deployment can be verified without credentials.

## Release verification

Run `npm test`, `npm run build`, `npm run typecheck`, and `npm audit --audit-level=high`. Build first on a fresh checkout to generate TanStack route types.
With backend dependencies installed and placeholder Supabase configuration, run:

```sh
python -m unittest discover -s backend -p 'test_*.py'
python backend/test_stale_events.py
python backend/test_gemini_budget.py
```

These tests use mock providers and do not spend API credits. `backend/test_gemini.py` is a separate live, potentially billable diagnostic and is intentionally excluded from CI execution. A ready-to-enable GitHub Actions workflow is saved at `docs/ci.workflow.yml`. Copy it to `.github/workflows/ci.yml` using an account/token with workflow write permission; the current OAuth connection cannot publish workflow files. Full lint remains a separate inherited backlog.

After deployment, confirm Edit vibe opens from Search and Major, saving survives reload, source links match their cards, and a fresh search finishes with results or an explicit error. Check both the frontend publication and the backend revision; merging GitHub does not by itself prove the public frontend was republished.

## Before broad promotion

1. Build a canonical event inventory with provider event ID, source URL, organizer, ISO start/end, timezone, coordinates, price/currency, observed time, and verification status. Prefer source APIs or source-page structured Event data; AI should rank/explain this inventory.
2. Enforce city/radius, date window, and price filters deterministically. The FIU school calendar includes remote and out-of-state events; an institution is not a location. The current AI prompt is not a geographic boundary.
3. Add a small evaluation set of real search requests with expected geography, dates, source identity, relevance, and broken-link measurements. Gate releases on it.
4. Verify provider spending controls, actual Redis availability, alerts and per-request cost telemetry. Application attempt counters are not a guaranteed monetary cap. Add distributed coalescing for concurrent cache misses.
5. Add account recovery, account/data deletion, privacy information describing AI-provider data use, and a visible support path. Verify signup and OAuth from a new browser session.
6. Make Saved → Attended transactional; add owner-isolation tests to migration CI and exercise backup restoration in a non-production environment.
7. Use one source-of-truth repository and automate the public snapshot. Existing repositories already differed in UI labels, sample data and campus fixes. Pin backend dependency resolution with a lockfile.
8. Validate mobile layout, keyboard flows, empty/error states, loading latency and accessibility. Add a report-wrong-event action and measure discovery → source click → save → attendance.

Do not describe this release as eliminating hallucinations. Grounding links can still cite an irrelevant page; source-field validation and deterministic search constraints remain release blockers for broad promotion.
