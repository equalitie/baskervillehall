from datetime import datetime, timedelta
import ipaddress as ipa
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
      - ClaudeBot  (Anthropic)   https://claude.com/crawling/bots.json
      - GPTBot     (OpenAI)      https://openai.com/gptbot.json
      - SearchBot  (OpenAI)      https://openai.com/searchbot.json
      - GoogleExtended (Google)  https://developers.google.com/static/crawling/ipranges/common-crawlers.json
    """

    _SOURCES = [
        {
            "name": "ClaudeBot",
            "url": "https://claude.com/crawling/bots.json",
        },
        {
            "name": "GPTBot",
            "url": "https://openai.com/gptbot.json",
        },
        {
            "name": "OAISearchBot",
            "url": "https://openai.com/searchbot.json",
        },
        {
            "name": "GoogleExtended",
            "url": "https://developers.google.com/static/crawling/ipranges/common-crawlers.json",
        },
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

    @lru_cache(maxsize=10000)
    def get_bot_name(self, ip: str) -> str:
        """
        Returns the name of the verified AI bot (e.g. "ClaudeBot") if the IP
        belongs to a known AI crawler's published range, or "" otherwise.
        Call refresh() separately on a timer — not here, to avoid it being
        suppressed by the cache hit path.
        """
        try:
            ip_obj = ipa.ip_address(ip)
        except ValueError:
            return ""
        for name, nets in self._nets.items():
            if any(ip_obj in net for net in nets):
                return name
        return ""
