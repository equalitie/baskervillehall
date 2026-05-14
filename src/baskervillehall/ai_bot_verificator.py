from datetime import datetime, timedelta
import ipaddress as ipa
import socket
import requests
from functools import lru_cache


def _parse_prefixes(data):
    """
    Parse IP prefixes from JSON that may use one of two formats:
      - Anthropic/Google: {"prefixes": [{"ipv4Prefix": "1.2.3.0/24"}, {"ipv6Prefix": "..."}]}
      - OpenAI:           {"prefixes": ["1.2.3.0/24", "2.3.4.0/28"]}
    Returns a list of CIDR strings.
    """
    result = []
    for item in data.get("prefixes", []):
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            result.append(item.get("ipv4Prefix") or item.get("ipv6Prefix") or "")
    return [p for p in result if p]


class AiBotVerificator:
    """
    IP-based verification of legitimate AI crawlers.
    Companies publish their crawler IP ranges as JSON; we download and cache them.

    Supported bots:
      - ClaudeBot           (Anthropic)   https://claude.com/crawling/bots.json
      - GPTBot              (OpenAI)      https://openai.com/gptbot.json
      - OAISearchBot        (OpenAI)      https://openai.com/searchbot.json
      - ChatGPT-User        (OpenAI)      https://openai.com/chatgpt-user.json
      - GoogleExtended      (Google)      https://developers.google.com/crawling/ipranges/common-crawlers.json
      - GoogleSpecial       (Google)      https://developers.google.com/crawling/ipranges/special-crawlers.json
      - GoogleUserTriggered (Google)      https://developers.google.com/crawling/ipranges/user-triggered-fetchers.json
      - PerplexityBot       (Perplexity)  https://www.perplexity.ai/perplexitybot.json
      - Perplexity-User     (Perplexity)  https://www.perplexity.ai/perplexity-user.json
      - MistralBot          (Mistral)     https://mistral.ai/mistralai-index-ips.json
      - MistralAI-User      (Mistral)     https://mistral.ai/mistralai-user-ips.json
      - DuckAssistBot       (DuckDuckGo)  https://duckduckgo.com/duckduckbot.json
      - Bingbot             (Microsoft)   https://www.bing.com/toolbox/bingbot.json
      - CCBot               (CommonCrawl) https://index.commoncrawl.org/ccbot.json
      - Meta-ExternalAgent  (Meta)        FCrDNS: hostname must end with .crawl.facebook.com or .crawl.fbsearch.com
    """

    # Bots verified via FCrDNS (no public IP prefix list).
    # Each entry: (bot_name, valid_hostname_suffixes)
    _FCRDNS_BOTS = [
        ("Meta-ExternalAgent", (".crawl.facebook.com", ".crawl.fbsearch.com")),
        ("Amazonbot",          (".crawl.amazonbot.amazon",)),
    ]

    # ASN fallback for bots that don't consistently set PTR records.
    # Used only when FCrDNS fails. Each entry: (bot_name, asn_name_keywords).
    # Keywords matched case-insensitively against the GeoIP ASN org name.
    _ASN_FALLBACK_BOTS = [
        ("Meta-ExternalAgent", ("meta platforms", "facebook")),
    ]

    _SOURCES = [
        # Anthropic
        {"name": "ClaudeBot", "url": "https://claude.com/crawling/bots.json"},
        # OpenAI
        {"name": "GPTBot", "url": "https://openai.com/gptbot.json"},
        {"name": "OAISearchBot", "url": "https://openai.com/searchbot.json"},
        {"name": "ChatGPT-User", "url": "https://openai.com/chatgpt-user.json"},
        # Google
        {"name": "GoogleExtended", "url": "https://developers.google.com/crawling/ipranges/common-crawlers.json"},
        {"name": "GoogleSpecial", "url": "https://developers.google.com/crawling/ipranges/special-crawlers.json"},
        {"name": "GoogleUserTriggered", "url": "https://developers.google.com/crawling/ipranges/user-triggered-fetchers.json"},
        # Perplexity
        {"name": "PerplexityBot", "url": "https://www.perplexity.ai/perplexitybot.json"},
        {"name": "Perplexity-User", "url": "https://www.perplexity.ai/perplexity-user.json"},
        # Mistral
        {"name": "MistralBot", "url": "https://mistral.ai/mistralai-index-ips.json"},
        {"name": "MistralAI-User", "url": "https://mistral.ai/mistralai-user-ips.json"},
        # DuckDuckGo
        {"name": "DuckAssistBot", "url": "https://duckduckgo.com/duckduckbot.json"},
        # Microsoft / Bing
        {"name": "Bingbot", "url": "https://www.bing.com/toolbox/bingbot.json"},
        # Common Crawl (major LLM training data source)
        {"name": "CCBot", "url": "https://index.commoncrawl.org/ccbot.json"},
    ]

    def __init__(self, logger, refresh_interval_hours=1):
        self.logger = logger
        self.refresh_interval = timedelta(hours=refresh_interval_hours)
        self.ts_refresh = None
        # bot_name -> tuple[ip_network, ...]
        self._nets: dict[str, tuple] = {s["name"]: () for s in self._SOURCES}
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "AiBotVerificator/1.0"})
        self.refresh()

    def _get_json(self, url, timeout=(3, 5)):
        r = self._session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _refresh_one(self, name, url):
        data = self._get_json(url)
        nets = []
        for prefix in _parse_prefixes(data):
            try:
                nets.append(ipa.ip_network(prefix, strict=False))
            except Exception as e:
                self.logger.warning(f"[AiBotVerificator] {name}: bad prefix {prefix!r}: {e}")
        self._nets[name] = tuple(nets)
        self.logger.info(f"[AiBotVerificator] {name}: loaded {len(nets)} prefixes")

    def refresh(self, force=False):
        if not force and self.ts_refresh and datetime.now() - self.ts_refresh < self.refresh_interval:
            return
        for src in self._SOURCES:
            try:
                self._refresh_one(src["name"], src["url"])
            except Exception as e:
                self.logger.warning(f"[AiBotVerificator] {src['name']} refresh failed: {e!r}")
        self.ts_refresh = datetime.now()
        self.get_bot_name.cache_clear()

    def _verify_fcrdns(self, ip: str, valid_suffixes: tuple) -> bool:
        """
        Forward-confirmed reverse DNS check.
        1. Reverse DNS: ip -> hostname
        2. Check hostname ends with one of valid_suffixes.
        3. Forward DNS: hostname -> IPs, confirm original IP is among them.
        IPv6 addresses are compared as ipa.ip_address objects to handle
        multiple canonical representations (e.g. "::1" vs "0:0:0:0:0:0:0:1").
        """
        try:
            hostname = socket.gethostbyaddr(ip)[0]
        except (socket.herror, socket.gaierror):
            return False
        if not any(hostname.endswith(suffix) for suffix in valid_suffixes):
            return False
        try:
            forward_ips = set()
            for r in socket.getaddrinfo(hostname, None):
                try:
                    forward_ips.add(ipa.ip_address(r[4][0]))
                except ValueError:
                    pass
        except socket.gaierror:
            return False
        try:
            return ipa.ip_address(ip) in forward_ips
        except ValueError:
            return False

    @lru_cache(maxsize=10000)
    def get_bot_name(self, ip: str, check_fcrdns: bool = False) -> str:
        """
        Returns the name of the verified AI bot (e.g. "ClaudeBot") if the IP
        belongs to a known AI crawler's published range, or "" otherwise.
        FCrDNS bots (Meta, Amazon) are checked only when check_fcrdns=True
        to avoid DNS lookups on every request — pass True only when the UA
        suggests one of these crawlers.
        Call refresh() separately on a timer — not here, to avoid it being
        suppressed by the cache hit path.
        """
        try:
            ip_obj = ipa.ip_address(ip)
        except ValueError:
            return ""
        for name, nets in self._nets.items():
            if any(ip_obj in net for net in nets if net.version == ip_obj.version):
                return name
        if check_fcrdns:
            for bot_name, suffixes in self._FCRDNS_BOTS:
                if self._verify_fcrdns(ip, suffixes):
                    return bot_name
        return ""

    def get_bot_name_by_asn(self, asn_name: str) -> str:
        """
        ASN-based fallback verification for bots that don't consistently publish
        PTR records (e.g. Meta). Called only when FCrDNS fails and the UA suggests
        an FCrDNS bot. Weaker than FCrDNS/prefix-list — use only as last resort.
        Returns bot name if asn_name matches a known company, "" otherwise.
        """
        if not asn_name:
            return ""
        asn_lower = asn_name.lower()
        for bot_name, keywords in self._ASN_FALLBACK_BOTS:
            if any(kw in asn_lower for kw in keywords):
                return bot_name
        return ""
