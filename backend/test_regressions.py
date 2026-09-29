"""Offline regressions: no live API requests or database writes."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import main

class RegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_anthropic_limit_concurrent_and_no_sdk_retry(self):
        main._anthropic_day_local.update(date="", count=0)
        with patch.object(main, "ANTHROPIC_DAILY_CALL_LIMIT", 2), patch.object(main, "_redis_rate_limit_incr", AsyncMock(return_value=None)):
            results = await asyncio.gather(*[main._anthropic_budget_reserve() for _ in range(8)])
            self.assertEqual(sum(results), 2)
        client = SimpleNamespace(with_options=lambda **kw: self.assertEqual(kw, {"max_retries": 0}) or SimpleNamespace(messages=SimpleNamespace(create=AsyncMock(return_value="ok"))))
        with patch.object(main, "_anthropic_budget_reserve", AsyncMock(return_value=True)):
            self.assertEqual(await main._budgeted_anthropic_create(client, model="test"), "ok")

    async def test_missing_gemini_keeps_backbone_and_skips_paid_signal_extraction(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        signals = AsyncMock()
        with patch.object(main, "GEMINI_API_KEY", None), patch.object(main, "_cache_get", AsyncMock(return_value=None)), patch.object(main, "extract_vibe_signals", signals), patch.object(main, "fetch_campus_events", AsyncMock(return_value=[])), patch.object(main, "_fetch_city_inventory", AsyncMock(return_value=[])):
            response = await main.vibe_stream_events(request, "Miami", [], "music")
            chunks = [chunk async for chunk in response.body_iterator]
            self.assertIn('"error"', ''.join(chunks))
            signals.assert_not_called()

    async def test_exhausted_search_is_not_reported_as_empty_success(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        with patch.object(main, "GEMINI_API_KEY", None):
            chunks = [chunk async for chunk in main._search_stream(request, "Miami", "music", "key", None, 25)]
            self.assertIn('"error"', ''.join(chunks))

    async def test_model_url_is_not_verified_without_grounding(self):
        event = main.ScoutEvent(id="test", name="Test", category="Networking", date="Tomorrow", venue="Test", neighborhood="Miami", price="Free", description="Test", url="https://example.com/events/test")
        result = await main._apply_grounding_urls([("key", event)], None, "test")
        self.assertFalse(result[0][1].url_verified)
        self.assertFalse(main._is_specific_event_url("ftp://example.com/events/test"))
        self.assertFalse(main._is_specific_event_url("https://[bad/events/test"))

    async def test_cancelled_stream_cleans_up_backbone_tasks(self):
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        stopped = asyncio.Event()
        async def slow(*args):
            try:
                await asyncio.sleep(60)
            finally:
                stopped.set()
        with patch.object(main, "fetch_campus_events", AsyncMock(return_value=[])), patch.object(main, "_fetch_city_inventory", slow):
            stream = main._vibe_stream(request, "Miami", "key", None, "music", main._build_local_signals(None,None), [],25,None)
            task = asyncio.create_task(anext(stream)); await asyncio.sleep(0.01); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertTrue(stopped.is_set())

if __name__ == "__main__": unittest.main()
