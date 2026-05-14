import logging
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from baskervillehall.ai_bot_verificator import AiBotVerificator, _parse_prefixes

logger = logging.getLogger(__name__)

# Fake responses keyed by URL — injected into tests that mock HTTP
FAKE_RESPONSES = {
    # Anthropic
    "https://claude.com/crawling/bots.json": {
        "prefixes": [{"ipv4Prefix": "10.0.1.0/24"}, {"ipv6Prefix": "2001:db8:1::/48"}],
    },
    # OpenAI
    "https://openai.com/gptbot.json": {
        "prefixes": ["10.0.2.0/24", "10.0.3.0/28"],
    },
    "https://openai.com/searchbot.json": {
        "prefixes": ["10.0.4.0/24"],
    },
    "https://openai.com/chatgpt-user.json": {
        "prefixes": ["10.0.9.0/24"],
    },
    # Google
    "https://developers.google.com/crawling/ipranges/common-crawlers.json": {
        "prefixes": [{"ipv4Prefix": "10.0.5.0/24"}, {"ipv6Prefix": "2001:db8:5::/48"}],
    },
    "https://developers.google.com/crawling/ipranges/special-crawlers.json": {
        "prefixes": [{"ipv4Prefix": "10.0.10.0/24"}],
    },
    "https://developers.google.com/crawling/ipranges/user-triggered-fetchers.json": {
        "prefixes": [{"ipv4Prefix": "10.0.11.0/24"}],
    },
    # Perplexity
    "https://www.perplexity.ai/perplexitybot.json": {
        "prefixes": [{"ipv4Prefix": "10.0.6.0/24"}],
    },
    "https://www.perplexity.ai/perplexity-user.json": {
        "prefixes": [{"ipv4Prefix": "10.0.12.0/24"}],
    },
    # Mistral
    "https://mistral.ai/mistralai-index-ips.json": {
        "prefixes": [{"ipv4Prefix": "10.0.7.0/24"}],
    },
    "https://mistral.ai/mistralai-user-ips.json": {
        "prefixes": [{"ipv4Prefix": "10.0.13.0/24"}],
    },
    # DuckDuckGo
    "https://duckduckgo.com/duckduckbot.json": {
        "prefixes": [{"ipv4Prefix": "10.0.14.0/24"}],
    },
    # Microsoft / Bing
    "https://www.bing.com/toolbox/bingbot.json": {
        "prefixes": [{"ipv4Prefix": "10.0.15.0/24"}],
    },
    # Common Crawl
    "https://index.commoncrawl.org/ccbot.json": {
        "prefixes": [{"ipv4Prefix": "10.0.16.0/24"}],
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

    def test_chatgpt_user_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.9.1"), "ChatGPT-User")

    def test_google_special_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.10.1"), "GoogleSpecial")

    def test_google_user_triggered_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.11.1"), "GoogleUserTriggered")

    def test_perplexity_user_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.12.1"), "Perplexity-User")

    def test_mistralai_user_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.13.1"), "MistralAI-User")

    def test_duckassistbot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.14.1"), "DuckAssistBot")

    def test_bingbot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.15.1"), "Bingbot")

    def test_ccbot_ip_in_range(self):
        self.assertEqual(self.v.get_bot_name("10.0.16.1"), "CCBot")

    def test_unknown_ip_returns_empty(self):
        self.assertEqual(self.v.get_bot_name("1.2.3.4"), "")

    def test_ipv6_claude_bot(self):
        self.assertEqual(self.v.get_bot_name("2001:db8:1::1"), "ClaudeBot")

    def test_ipv6_google_extended(self):
        self.assertEqual(self.v.get_bot_name("2001:db8:5::ff"), "GoogleExtended")

    def test_ipv6_full_form_matches_compressed(self):
        # "2001:0db8:0001:0000:0000:0000:0000:0001" == "2001:db8:1::1" (ClaudeBot)
        self.assertEqual(self.v.get_bot_name("2001:0db8:0001:0000:0000:0000:0000:0001"), "ClaudeBot")

    def test_ipv4_does_not_raise_against_ipv6_nets(self):
        # ClaudeBot has both IPv4 and IPv6 prefixes — IPv4 lookup must not raise TypeError
        self.assertEqual(self.v.get_bot_name("10.0.1.5"), "ClaudeBot")

    def test_ipv6_does_not_raise_against_ipv4_nets(self):
        # GPTBot has only IPv4 prefixes — IPv6 lookup must return "" without TypeError
        self.assertEqual(self.v.get_bot_name("2001:db8::dead"), "")

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


class TestFCrDNS(unittest.TestCase):
    """Unit tests for _verify_fcrdns — mocked socket, no network."""

    def setUp(self):
        self.v = _make_verificator()

    def _patch_socket(self, reverse_hostname, forward_ips):
        """
        Helper: patches gethostbyaddr and getaddrinfo.
        forward_ips: list of IP strings returned by getaddrinfo.
        """
        def fake_gethostbyaddr(ip):
            return (reverse_hostname, [], [ip])

        def fake_getaddrinfo(host, port):
            return [(None, None, None, None, (ip,)) for ip in forward_ips]

        return patch.multiple(
            "baskervillehall.ai_bot_verificator.socket",
            gethostbyaddr=fake_gethostbyaddr,
            getaddrinfo=fake_getaddrinfo,
        )

    # Meta
    def test_meta_ipv4_verified(self):
        ip = "69.171.250.1"
        suffixes = (".crawl.facebook.com", ".crawl.fbsearch.com")
        with self._patch_socket("spider-69-171-250-1.crawl.facebook.com", [ip]):
            self.assertTrue(self.v._verify_fcrdns(ip, suffixes))

    def test_meta_ipv6_verified(self):
        ip = "2a03:2880:f003::1"
        full_form = "2a03:2880:f003:0000:0000:0000:0000:0001"
        suffixes = (".crawl.facebook.com", ".crawl.fbsearch.com")
        with self._patch_socket("spider-1.crawl.facebook.com", [full_form]):
            self.assertTrue(self.v._verify_fcrdns(ip, suffixes))

    # Amazon
    def test_amazon_ipv4_verified(self):
        ip = "18.232.36.1"
        suffixes = (".crawl.amazonbot.amazon",)
        with self._patch_socket("18-232-36-1.crawl.amazonbot.amazon", [ip]):
            self.assertTrue(self.v._verify_fcrdns(ip, suffixes))

    def test_amazon_via_get_bot_name(self):
        ip = "18.232.36.1"
        suffixes = (".crawl.amazonbot.amazon",)
        with self._patch_socket("18-232-36-1.crawl.amazonbot.amazon", [ip]):
            self.assertEqual(self.v.get_bot_name(ip, check_fcrdns=True), "Amazonbot")

    # Security checks
    def test_wrong_hostname_suffix_rejected(self):
        suffixes = (".crawl.facebook.com",)
        with self._patch_socket("spider.evil.com", ["1.2.3.4"]):
            self.assertFalse(self.v._verify_fcrdns("1.2.3.4", suffixes))

    def test_forward_ip_mismatch_rejected(self):
        suffixes = (".crawl.facebook.com",)
        with self._patch_socket("spider.crawl.facebook.com", ["9.9.9.9"]):
            self.assertFalse(self.v._verify_fcrdns("1.2.3.4", suffixes))

    def test_reverse_dns_failure_returns_false(self):
        import socket as sock
        suffixes = (".crawl.facebook.com",)
        with patch("baskervillehall.ai_bot_verificator.socket.gethostbyaddr",
                   side_effect=sock.herror):
            self.assertFalse(self.v._verify_fcrdns("1.2.3.4", suffixes))

    def test_ipv6_compressed_vs_full_form(self):
        import ipaddress
        self.assertEqual(ipaddress.ip_address("::1"), ipaddress.ip_address("0:0:0:0:0:0:0:1"))


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
