from datetime import datetime
import ipaddress
import requests

class BotVerificator(object):

    def __init__(self):
        super().__init__()
        self.ts_refresh = None
        self.google_ranges = None
        self.bing_ranges = None
        self.refresh()

    def refresh_google(self):
        r = requests.get('https://developers.google.com/static/search/apis/ipranges/googlebot.json')
        data = r.json().get("prefixes")
        self.google_ranges = [list(d.values())[0] for d in data]

    def refresh_bing(self):
        r = requests.get('https://www.bing.com/toolbox/bingbot.json')
        data = r.json().get("prefixes")
        self.google_ranges = [list(d.values())[0] for d in data]

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

    def refresh(self):
        if self.ts_refresh is not None and (datetime.now()-self.ts_refresh).total_seconds() < 60*60:
            return

        self.refresh_google()
        self.refresh_bing()
        self.ts_refresh = datetime.now()


    def is_verified_bot(self, ip):
        self.refresh()
        if self.is_verified_google_bot(ip):
            return True
