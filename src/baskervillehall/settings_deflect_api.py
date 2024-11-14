import logging

from baskervillehall.json_url_reader import JsonUrlReader


class SettingsDeflectAPI(object):

    def __init__(
            self,
            url,
            whitelist_default=[],
            whitelist_default_block_session=[],
            logger=None,
            refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.domains = []
        self.prefixes = []
        self.matches = []
        self.stars = []
        self.config =  {}
        self.double_stars = []
        self.whitelist_default = whitelist_default
        self.whitelist_default_block_session = whitelist_default_block_session
        self.whitelist_block_session = []

    def _refresh(self):
        data, fresh = self.reader.get()
        if data and fresh:
            self.whitelist_block_session = self.whitelist_default_block_session
            white_list = self.whitelist_default
            self.config = {}
            for host, v in data.items():
                self.whitelist_block_session += list(set(v.get('no_banjax_path_urls', [])))
                white_list += list(set(v['allowlist_urls']))
                self.config[host] = {
                    'sensitivity': v.get('sensitivity', 0)
                }

            self.domains = []
            self.prefixes = []
            self.matches = []
            self.stars = []
            for url in white_list:
                if url.find('/') < 0:
                    self.domains.append(url)
                else:
                    star_pos = url.find('*')
                    if url.find('*') < 0:
                        self.matches.append(url)
                    else:
                        if star_pos == len(url) - 1:
                            self.prefixes.append(url[:-1])
                        else:
                            star_pos2 = url.rfind('*')
                            if star_pos == star_pos2:
                                self.stars.append((url[:star_pos], url[star_pos + 1:]))
                            else:
                                self.double_stars.append((url[:star_pos], url[star_pos + 1:-1]))

    def is_host_whitelisted_block_session(self, host):
        self._refresh()
        return host in self.whitelist_block_session

    def is_host_whitelisted(self, host):
        self._refresh()
        for domain in self.domains:
            if domain in host:
                return True
        return False

    def is_in_whitelist(self, url):
        self._refresh()

        if url in self.matches:
            return True

        for url_prefix in self.prefixes:
            if url.startswith(url_prefix):
                return True

        for star in self.stars:
            if url and url.startswith(star[0]) and url.endswith(star[1]):
                return True
        for star in self.double_stars:
            if url and url.startswith(star[0]) and star[1] in url[len(star[0]):]:
                return True

        return False

    def remove_whitelisted(self, host, urls):
        self._refresh()

        result = []
        for ts, url in urls:
            if self.is_in_whitelist(host + '/' + url):
                continue
            result.append((ts, url))

        return result

    def get_sensitivity(self, host):
        self._refresh()
        if host == 'parniplus.com':
            return -2
        host_data = self.config.get(host, None)
        if not host_data:
            return 0
        return host_data.get('sensitivity', 0)