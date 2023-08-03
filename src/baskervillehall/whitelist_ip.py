from baskervillehall.json_url_reader import JsonUrlReader


class WhitelistIP(object):

    def __init__(self, url, logger=None, refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.ips = None
        self.logger = logger
        self.host_ips = None

    def get_ips(self):
        data, fresh = self.reader.get()
        if data and fresh:
            self.host_ips = {}
            for k, v in data.items():
                if k == 'global' or k == 'global_updated_at':
                    continue
                ips = []
                for ip in v:
                    ips.append(ip.split('/')[0])
                ips = list(set(ips))
                self.host_ips[k] = self.host_ips.get(k, []) + ips
        return self.host_ips

    def is_in_whitelist(self, host, ip):
        ips = self.get_ips()
        if not ips:
            return False
        if host not in ips:
            return False
        return ip in ips[host]

