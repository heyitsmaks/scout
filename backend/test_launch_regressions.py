"""Offline tests for bugs reproduced in the September production audit."""
import json
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import main


def event(name="AI workshop", **extra):
    return main.ScoutEvent(id=name, name=name, category="Tech & Startups", date="Sep 29, 2099",
                           venue="Campus", neighborhood="Miami, FL", price="See tickets",
                           description="Workshop", **extra)


class SourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_generated_link_without_source_is_excluded(self):
        items = await main._apply_grounding_urls([("2099", event(url="https://example.com/events/fake"))], None, "test")
        self.assertEqual(await main._validate_ai_urls(items), [])

    async def test_matching_direct_grounded_link_is_recognized(self):
        url = "https://calendar.fiu.edu/event/ai-workshop"
        grounding = main._GroundingData([url], [("AI workshop is coming", [0])])
        results = await main._apply_grounding_urls([("2099", event(url=url))], grounding, "test")
        self.assertTrue(results[0][1].url_verified)
        self.assertEqual(len(await main._validate_ai_urls(results)), 1)

    async def test_citation_for_multiple_events_cannot_assign_first_link_to_both(self):
        items = [("2099", event()), ("2099", event("County training"))]
        grounding = main._GroundingData(["https://miamidade.gov/global/ai/training.page"],
                                       [("AI workshop, County training", [0])])
        results = await main._apply_grounding_urls(items, grounding, "test")
        self.assertEqual(await main._validate_ai_urls(results), [])

    async def test_ambiguous_sources_are_not_guessed(self):
        grounding = main._GroundingData(["https://example.com/events/a", "https://example.com/events/b"],
                                       [("AI workshop", [0, 1])])
        results = await main._apply_grounding_urls([("2099", event())], grounding, "test")
        self.assertFalse(results[0][1].url_verified)

    async def test_only_google_redirect_wrapper_is_fetched(self):
        client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=302, headers={"location": "http://127.0.0.1/private/event"})))
        with patch.object(main, "_http_client", client):
            results = await main._resolve_grounding_redirects({"http://127.0.0.1/private/event", "https://example.com/events/a"})
            self.assertEqual(results, {"https://example.com/events/a": "https://example.com/events/a"})
            client.get.assert_not_called()
            results = await main._resolve_grounding_redirects({"https://vertexaisearch.cloud.google.com/grounding-api-redirect/token"})
            self.assertEqual(results, {})
            self.assertFalse(client.get.call_args.kwargs["follow_redirects"])

    async def test_definitively_deleted_source_is_excluded(self):
        with patch.object(main, "_eb_head_status", AsyncMock(return_value=410)):
            self.assertEqual(await main._validate_ai_urls([("2099", event(url="https://eventbrite.com/e/workshop-tickets-123456789111"))]), [])


class FailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_provider_failure_is_not_cached_as_empty_success(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        cache = AsyncMock()
        with patch.object(main, "GEMINI_API_KEY", "test"), patch.object(main, "_gemini_budget_exhausted", AsyncMock(return_value=False)), patch.object(main, "_decompose_search_query", AsyncMock(return_value=["AI"])), patch.object(main, "_run_gemini", AsyncMock(side_effect=RuntimeError("billing failure"))), patch.object(main, "_cache_set", cache):
            frames = [json.loads(chunk[6:]) async for chunk in main._search_stream(request, "Miami", "AI", "test", None, 25)]
        self.assertEqual(frames[-1]["status"], "error")
        self.assertNotIn("billing failure", str(frames))
        cache.assert_not_called()

    async def test_picks_keeps_structured_events_when_ai_fails(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        cache = AsyncMock()
        with ExitStack() as stack:
            for name, replacement in {
                "GEMINI_API_KEY": "test", "_gemini_budget_exhausted": AsyncMock(return_value=False),
                "fetch_campus_events": AsyncMock(return_value=[event("Campus source")]),
                "_fetch_city_inventory": AsyncMock(return_value=[]),
                "_run_gemini": AsyncMock(side_effect=TimeoutError()), "_cache_set": cache,
            }.items(): stack.enter_context(patch.object(main, name, replacement))
            frames = [json.loads(chunk[6:]) async for chunk in main._vibe_stream(request, "Miami", "test", None, "music", main._build_local_signals(None, None), [], 25)]
        self.assertEqual(frames[0]["events"][0]["name"], "Campus source")
        self.assertEqual(frames[-1]["status"], "error")
        cache.assert_not_called()

    async def test_major_provider_failure_is_not_cached(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        cache = AsyncMock()
        with patch.object(main, "GEMINI_API_KEY", "test"), patch.object(main, "_gemini_budget_exhausted", AsyncMock(return_value=False)), patch.object(main, "_run_gemini", AsyncMock(side_effect=RuntimeError("quota"))), patch.object(main, "_cache_set", cache):
            frames = [json.loads(chunk[6:]) async for chunk in main._major_stream(request, "Miami", "test", [], None, "Data Science", None, 25)]
        self.assertEqual(frames[-1]["status"], "error")
        cache.assert_not_called()

    async def test_valid_empty_search_can_be_cached(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        cache = AsyncMock()
        with patch.object(main, "GEMINI_API_KEY", "test"), patch.object(main, "_gemini_budget_exhausted", AsyncMock(return_value=False)), patch.object(main, "_decompose_search_query", AsyncMock(return_value=["AI"])), patch.object(main, "_run_gemini", AsyncMock(return_value=("[]", None))), patch.object(main, "_cache_set", cache):
            frames = [json.loads(chunk[6:]) async for chunk in main._search_stream(request, "Miami", "AI", "test", None, 25)]
        self.assertEqual(frames[-1]["status"], "complete")
        cache.assert_awaited_once()


class DateAndCategoryTests(unittest.TestCase):
    def test_explicit_past_dates_are_not_left_in_cache_forever(self):
        now = datetime(2026, 9, 28, 20, tzinfo=timezone.utc)
        for date in ("Aug. 27, 2026", "Sept. 3, 2026", "Thu, Sep 24", "2026-09-24"):
            self.assertTrue(main._is_stale_event({"date": date}, "Miami", now), date)
        self.assertFalse(main._is_stale_event({"date": "Mon, Sep 28"}, "Miami", now))
        self.assertFalse(main._is_stale_event({"date": "TBA"}, "Miami", now))

    def test_yearless_dates_are_not_promoted_to_next_year(self):
        self.assertEqual(main._parse_ai_date("September 1", "Miami")[0], "0000-00-00")
        self.assertTrue(main._parse_ai_date("Sept. 3, 2026", "Miami")[0].startswith("2026-09-03"))

    def test_invalid_output_is_a_failure_not_no_results(self):
        with self.assertRaises(main.AIProviderUnavailable): main._parse_ai_response("Sorry, unavailable", "Miami", [])

    def test_category_keywords_match_words_not_fragments(self):
        self.assertNotEqual(main.pick_category_from_text("Pancreatic cancer development in the department of biology", []), "Art & Culture")
        self.assertNotEqual(main.pick_category_from_text("Physics colloquium on population dynamics", []), "Concerts & Music")


if __name__ == "__main__": unittest.main()
