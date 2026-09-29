"""Campus addresses must come from the event, including shared-cache hits."""
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import httpx
import main

URL = 'https://calendar.fiu.edu/event/career-fair'


def feed():
    date = format_datetime(datetime.now(timezone.utc) + timedelta(days=2))
    return f'''<rss><channel><item><title>Career Fair at OBCC - Ocean Bank Convocation Center</title>
    <link>{URL}</link><pubDate>{date}</pubDate><category>Career Readiness</category>
    </item></channel></rss>'''


def payload(city='Miami', state='FL', **extra):
    return {'events': [{'event': {'localist_url': URL, 'geo': {'city': city, 'state': state}, **extra}}]}


class CampusLocationTests(unittest.IsolatedAsyncioTestCase):
    def test_source_location_and_offsite_location(self):
        for city, state in [('Miami', 'FL'), ('Orlando', 'FL'), ('Boston', 'MA')]:
            events = main._parse_campus_feed(feed(), 'FIU', main._campus_locations(payload(city, state)))
            self.assertEqual(events[0].neighborhood, f'{city}, {state}')
            self.assertEqual(events[0].venue, 'OBCC - Ocean Bank Convocation Center')

    def test_virtual_missing_and_address_only(self):
        self.assertEqual(main._campus_locations(payload(experience='virtual'))[URL], 'Online')
        self.assertEqual(main._campus_locations(payload('', '', address='123 Example Road'))[URL], '123 Example Road')
        self.assertEqual(main._parse_campus_feed(feed(), 'FIU', {})[0].neighborhood, 'Location unconfirmed')

    async def test_profile_city_cannot_pollute_shared_cache(self):
        cache = {}
        async def get(key, ttl):
            return cache.get(key)
        async def put(key, events, ttl):
            cache[key] = events
        rss = SimpleNamespace(text=feed(), raise_for_status=lambda: None)
        api = SimpleNamespace(json=lambda: payload(), raise_for_status=lambda: None)
        client = SimpleNamespace(get=AsyncMock(side_effect=[rss, api]))
        with patch.object(main, '_http_client', client), patch.object(main, '_cache_get', get), patch.object(main, '_cache_set', put):
            first = await main.fetch_campus_events('FIU', 'Milford, IL')
            second = await main.fetch_campus_events('FIU', 'New York, NY')
        self.assertEqual(first[0].neighborhood, 'Miami, FL')
        self.assertEqual(second[0].neighborhood, 'Miami, FL')
        self.assertEqual(client.get.await_count, 2)
        self.assertTrue(all('source-location-v1' in key for key in cache))

    async def test_api_failure_never_falls_back_to_profile_city(self):
        rss = SimpleNamespace(text=feed(), raise_for_status=lambda: None)
        client = SimpleNamespace(get=AsyncMock(side_effect=[rss, httpx.ConnectError('offline')]))
        with patch.object(main, '_http_client', client), patch.object(main, '_cache_get', AsyncMock(return_value=None)), patch.object(main, '_cache_set', AsyncMock()):
            events = await main.fetch_campus_events('FIU', 'Milford, IL')
        self.assertEqual(events[0].neighborhood, 'Location unconfirmed')


if __name__ == '__main__':
    unittest.main()
