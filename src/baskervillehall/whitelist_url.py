import logging

from baskervillehall.json_url_reader import JsonUrlReader


class WhitelistURL(object):

    def __init__(self, url, whitelist_default=[], logger=None, refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.domains = []
        self.prefixes = []
        self.matches = []
        self.stars = []
        self.whitelist_default = whitelist_default

    def _refresh(self):
        data, fresh = self.reader.get()
        if data and fresh:
            white_list = list(set(data['white_list_urls']))
            white_list += self.whitelist_default

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
                            self.stars.append((url[:star_pos], url[star_pos + 1:]))

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

        return False

    def remove_whitelisted(self, host, urls):
        self._refresh()

        result = []
        for ts, url in urls:
            if self.is_in_whitelist(host + '/' + url):
                continue
            result.append((ts, url))

        return result
