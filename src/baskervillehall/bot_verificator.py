from datetime import datetime, timedelta
import ipaddress as ipa
import re
import requests
from functools import lru_cache

class BotVerificator:
    DUCK_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')

    def __init__(self, logger):
        self.logger = logger  # присваиваем раньше, чтобы логировать в refresh()
        self.ts_refresh = None
        self.google_nets = ()      # tuple[IPv4/6Network]
        self.bing_nets = ()        # tuple[IPv4/6Network]
        self.duck_ips = frozenset() # set[str] of dotted IPv4
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "BotVerificator/1.0"})
        self.refresh()  # можно убрать из __init__, если не хочешь блокироваться на старте

    def _get_json(self, url, timeout=(2, 3)):
        r = self._session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def _get_text(self, url, timeout=(2, 3)):
        r = self._session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text

    def refresh_google(self):
        data = self._get_json('https://developers.google.com/static/search/apis/ipranges/googlebot.json')
        prefixes = [list(d.values())[0] for d in data.get("prefixes", [])]
        nets = []
        for p in prefixes:
            try:
                nets.append(ipa.ip_network(p, strict=False))
            except Exception as e:
                self.logger.warning(f"Bad Google prefix {p!r}: {e}")
        self.google_nets = tuple(nets)

    def refresh_bing(self):
        data = self._get_json('https://www.bing.com/toolbox/bingbot.json')
        prefixes = [list(d.values())[0] for d in data.get("prefixes", [])]
        nets = []
        for p in prefixes:
            try:
                nets.append(ipa.ip_network(p, strict=False))
            except Exception as e:
                self.logger.warning(f"Bad Bing prefix {p!r}: {e}")
        self.bing_nets = tuple(nets)

    def refresh_duckduckgo(self):
        html = self._get_text('https://duckduckgo.com/duckduckgo-help-pages/results/duckduckbot/')
        ips = set(self.DUCK_RE.findall(html))
        self.duck_ips = frozenset(ips)

    def refresh(self, force=False):
        if not force and self.ts_refresh and datetime.now() - self.ts_refresh < timedelta(hours=1):
            return
        for name, func in (
            ('Google', self.refresh_google),
            ('Bing', self.refresh_bing),
            ('DuckDuckGo', self.refresh_duckduckgo),
        ):
            try:
                func()
            except Exception as e:
                self.logger.exception(f"{name} refresh failed: {e!r}")
        self.ts_refresh = datetime.now()
        # очистим кэш результатов после обновления списков
        self.is_verified_bot.cache_clear()

    def _in_any(self, ip_obj, nets):
        # быстрый линейный проход по уже-парсенным сетям
        return any(ip_obj in net for net in nets)

    @lru_cache(maxsize=10000)
    def is_verified_bot(self, ip: str) -> bool:
        # refresh() дешёвый (порог по времени), но защищает от устаревания
        self.refresh()

        ip_obj = ipa.ip_address(ip)

        # порядок: самые частые для твоего трафика — первыми
        if self._in_any(ip_obj, self.google_nets):
            return True
        if self._in_any(ip_obj, self.bing_nets):
            return True
        # duckduckgo публикует одиночные IP — простая проверка в множестве строк
        if ip_obj.version == 4 and ip_obj.exploded in self.duck_ips:
            return True

        return False
