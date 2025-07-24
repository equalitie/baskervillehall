from datetime import datetime
import ipaddress
import requests

class BotVerificator(object):

    def __init__(self, logger):
        super().__init__()
        self.ts_refresh = None
        self.google_ranges = None
        self.bing_ranges = None
        self.duckduckgo_ips = None
        self.refresh()
        self.logger = logger

    def refresh_google(self):
        r = requests.get('https://developers.google.com/static/search/apis/ipranges/googlebot.json')
        data = r.json().get("prefixes")
        self.google_ranges = [list(d.values())[0] for d in data]

    def refresh_duckduckgo(self):
        r = requests.get('https://duckduckgo.com/duckduckgo-help-pages/results/duckduckbot/')
        data = r.text
        self.duckduckgo_ips = []
        split1 = data.split('<li>')
        for s in split1:
            for ip in s.split('</li>\n  '):
                if len(ip) > 0 and len(ip) < 30:
                    self.duckduckgo_ips.append(ip)

    def refresh_bing(self):
        r = requests.get('https://www.bing.com/toolbox/bingbot.json')
        data = r.json().get("prefixes")
        self.bing_ranges = [list(d.values())[0] for d in data]

    def is_verified_google_bot(self, ip):
        for ip_range in self.google_ranges:
            if ipaddress.ip_address(ip) in ipaddress.ip_network(ip_range):
                return True
        return False

    def is_verified_bing_bot(self, ip):
        for ip_range in self.bing_ranges:
            if ipaddress.ip_address(ip) in ipaddress.ip_network(ip_range):
                return True
        return False

    def is_verified_duckduckgo_bot(self, ip):
        return ip in self.duckduckgo_ips


    def refresh(self):
        if self.ts_refresh is not None and (datetime.now()-self.ts_refresh).total_seconds() < 60*60:
            return

        for name, func in (
            ('Google', self.refresh_google),
            ('Bing',   self.refresh_bing),
            ('DuckDuckGo', self.refresh_duckduckgo),
        ):
            try:
                func()
            except Exception as e:
                self.logger.exception(f"{name} refresh failed: {e!r}")

        self.ts_refresh = datetime.now()


    def is_verified_bot(self, ip):
        self.refresh()

        if self.is_verified_google_bot(ip):
            return True

        if self.is_verified_bing_bot(ip):
            return True

        if self.is_verified_duckduckgo_bot(ip):
            return True

        return False
