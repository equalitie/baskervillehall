import logging
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from baskervillehall.ai_bot_verificator import AiBotVerificator, _parse_prefixes

logger = logging.getLogger(__name__)

# Fake responses keyed by URL — injected into tests that mock HTTP
FAKE_RESPONSES = {
    "https://claude.com/crawling/bots.json": {
        "creationTime": "2026-01-01T00:00:00",
        "prefixes": [
            {"ipv4Prefix": "10.0.1.0/24"},
            {"ipv6Prefix": "2001:db8:1::/48"},
        ],
    },
    "https://openai.com/gptbot.json": {
        "creationTime": "2026-01-01T00:00:00",
        "prefixes": ["10.0.2.0/24", "10.0.3.0/28"],
    },
    "https://openai.com/searchbot.json": {
        "creationTime": "2026-01-01T00:00:00",
        "prefixes": ["10.0.4.0/24"],
    },
    "https://developers.google.com/static/crawling/ipranges/common-crawlers.json": {
        "creationTime": "2026-01-01T00:00:00",
        "prefixes": [
            {"ipv4Prefix": "10.0.5.0/24"},
            {"ipv6Prefix": "2001:db8:5::/48"},
        ],
    },
}


def _make_mock_session(responses=None):
    """Build a requests.Session mock that returns fake JSON for known URLs."""
    if responses is None:
        responses = FAKE_RESPONSES

    def fake_get(url, timeout=None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = responses[url]
        return resp

    mock_session = MagicMock()
    mock_session.get.side_effect = fake_get
    return mock_session


def _make_verificator(responses=None):
    """Create an AiBotVerificator with a mocked HTTP session."""
    with patch("baskervillehall.ai_bot_verificator.requests.Session") as MockSession:
        MockSession.return_value = _make_mock_session(responses)
        v = AiBotVerificator(logger=logger)
    return v


class TestParsePrefixes(unittest.TestCase):
    """Unit tests for the _parse_prefixes helper — no network."""

    def test_dict_format_ipv4(self):
        data = {"prefixes": [{"ipv4Prefix": "1.2.3.0/24"}]}
        self.assertEqual(_parse_prefixes(data), ["1.2.3.0/24"])

    def test_dict_format_ipv6(self):
        data = {"prefixes": [{"ipv6Prefix": "2001:db8::/32"}]}
        self.assertEqual(_parse_prefixes(data), ["2001:db8::/32"])

    def test_plain_string_format(self):
        data = {"prefixes": ["1.2.3.0/24", "5.6.7.0/28"]}
        self.assertEqual(_parse_prefixes(data), ["1.2.3.0/24", "5.6.7.0/28"])

    def test_mixed_format_skips_empty(self):
        # dict with neither ipv4Prefix nor ipv6Prefix → empty string → filtered out
        data = {"prefixes": [{"ipv4Prefix": "1.2.3.0/24"}, {}, "5.6.7.0/28"]}
        self.assertEqual(_parse_prefixes(data), ["1.2.3.0/24", "5.6.7.0/28"])

    def test_empty_prefixes(self):
        self.assertEqual(_parse_prefixes({}), [])
        self.assertEqual(_parse_prefixes({"prefixes": []}), [])


class TestAiBotVerificatorWithMocks(unittest.TestCase):
    """Tests using mocked HTTP — deterministic, no network required."""

    def setUp(self):
        self.v = _make_verificator()

    def test_claude_bot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.1.1"), "ClaudeBot")

    def test_gptbot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.2.50"), "GPTBot")

    def test_gptbot_second_prefix(self):
        self.assertEqual(self.v.get_bot_name("10.0.3.1"), "GPTBot")

    def test_searchbot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.4.100"), "OAISearchBot")

    def test_google_extended_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.5.200"), "GoogleExtended")

    def test_unknown_ip_returns_empty(self):
        self.assertEqual(self.v.get_bot_name("1.2.3.4"), "")

    def test_ipv6_claude_bot(self):
        self.assertEqual(self.v.get_bot_name("2001:db8:1::1"), "ClaudeBot")

    def test_ipv6_google_extended(self):
        self.assertEqual(self.v.get_bot_name("2001:db8:5::ff"), "GoogleExtended")

    def test_invalid_ip_returns_empty(self):
        self.assertEqual(self.v.get_bot_name("not-an-ip"), "")
        self.assertEqual(self.v.get_bot_name(""), "")

    def test_result_is_cached(self):
        # Two calls for the same IP should hit the lru_cache — the underlying
        # network scan (_nets iteration) should only happen once.
        first = self.v.get_bot_name("10.0.1.1")
        with patch.object(self.v, "_nets", wraps=self.v._nets) as mock_nets:
            second = self.v.get_bot_name("10.0.1.1")
        self.assertEqual(first, second)
        # _nets was never accessed on the second call (cache hit)
        mock_nets.__iter__.assert_not_called()

    def test_cache_cleared_after_refresh(self):
        self.assertEqual(self.v.get_bot_name("10.0.1.1"), "ClaudeBot")
        # Force a refresh with new data that removes ClaudeBot prefix
        new_responses = dict(FAKE_RESPONSES)
        new_responses["https://claude.com/crawling/bots.json"] = {
            "prefixes": [{"ipv4Prefix": "192.168.99.0/24"}]
        }
        self.v._session = _make_mock_session(new_responses)
        self.v.refresh(force=True)
        # Old ClaudeBot IP no longer recognised
        self.assertEqual(self.v.get_bot_name("10.0.1.1"), "")
        # New prefix is recognised
        self.assertEqual(self.v.get_bot_name("192.168.99.5"), "ClaudeBot")

    def test_refresh_skipped_within_interval(self):
        self.v.ts_refresh = datetime.now()
        with patch.object(self.v, "_refresh_one") as mock_refresh_one:
            self.v.refresh()
        mock_refresh_one.assert_not_called()

    def test_refresh_runs_after_interval(self):
        self.v.ts_refresh = datetime.now() - timedelta(hours=2)
        with patch.object(self.v, "_refresh_one") as mock_refresh_one:
            self.v.refresh()
        self.assertEqual(mock_refresh_one.call_count, len(AiBotVerificator._SOURCES))

    def test_failed_source_does_not_crash(self):
        """A network error for one source should log a warning and keep stale nets intact."""
        def flaky_get(url, timeout=None):
            if "claude" in url:
                raise ConnectionError("simulated failure")
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = FAKE_RESPONSES[url]
            return resp

        self.v._session = MagicMock()
        self.v._session.get.side_effect = flaky_get
        self.v.refresh(force=True)

        # ClaudeBot nets are stale but retained — we prefer stale over empty
        # (avoids wrongly challenging a legitimate ClaudeBot during a transient outage)
        self.assertEqual(self.v.get_bot_name("10.0.1.1"), "ClaudeBot")
        # Other bots refreshed successfully
        self.assertEqual(self.v.get_bot_name("10.0.2.1"), "GPTBot")


class TestAiBotVerificatorLive(unittest.TestCase):
    """
    Integration tests that hit the real endpoints.
    Skipped automatically if network is unavailable.
    """

    @classmethod
    def setUpClass(cls):
        import requests as req
        try:
            req.get("https://claude.com/crawling/bots.json", timeout=5)
            cls.skip = False
        except Exception:
            cls.skip = True
        if not cls.skip:
            cls.v = AiBotVerificator(logger=logger)

    def _skip_if_offline(self):
        if self.skip:
            self.skipTest("Network unavailable")

    def test_known_claude_ip(self):
        """216.73.216.0/22 is published by Anthropic."""
        self._skip_if_offline()
        self.assertEqual(self.v.get_bot_name("216.73.216.1"), "ClaudeBot")

    def test_random_ip_not_a_bot(self):
        self._skip_if_offline()
        self.assertEqual(self.v.get_bot_name("1.2.3.4"), "")

    def test_all_sources_loaded(self):
        """After init, every source should have at least one prefix."""
        self._skip_if_offline()
        for name, nets in self.v._nets.items():
            self.assertGreater(len(nets), 0, f"{name} loaded no prefixes")


if __name__ == "__main__":
    unittest.main()
