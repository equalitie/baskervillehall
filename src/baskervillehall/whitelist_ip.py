from baskervillehall.json_url_reader import JsonUrlReader


class WhitelistIP(object):

    def __init__(self, url, logger=None, refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.ips = None
        self.logger = logger
        self.host_ips = None
        self.global_ips = None

    def get_ips(self):
        data, fresh = self.reader.get()
        if data and fresh:
            self.host_ips = {}
            self.global_ips = set([ip.split('/')[0] for ip in data.get('global', [])])
            for k, v in data.items():
                if k == 'global' or k == 'global_updated_at':
                    continue
                self.host_ips[k] = set([ip.split('/')[0] for ip in v])
        return self.host_ips, self.global_ips

    def is_in_whitelist(self, host, ip):
        host_ips, global_ips = self.get_ips()
        if global_ips:
            if ip in global_ips:
                return True
        if host_ips:
            if host not in host_ips:
                return False
            return ip in host_ips[host]
        return False

