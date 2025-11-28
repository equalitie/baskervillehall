import logging

from baskervillehall.json_url_reader import JsonUrlReader


class SettingsDeflectAPI(object):

    def __init__(
            self,
            url,
            auth,
            whitelist_default=[],
            whitelist_default_block_session=[],
            logger=None,
            refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url,
                                    headers={'Authorization': f'Bearer {auth}'},
                                    logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
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
        self._first_load = True  # Track first load for detailed logging

    def _refresh(self):
        data, fresh = self.reader.get()
        if data and fresh:
            self.whitelist_block_session = self.whitelist_default_block_session
            white_list = self.whitelist_default
            self.config = {}
            for host, v in data.items():
                self.whitelist_block_session += list(set(v.get('no_banjax_path_urls', [])))
                white_list += list(set(v['allowlist_urls']))

                # Parse sensitivity - handle both string and int
                sensitivity_raw = v.get('sensitivity', 0)
                try:
                    sensitivity = int(sensitivity_raw)
                except (ValueError, TypeError):
                    if self.logger:
                        self.logger.warning(f"Invalid sensitivity value for {host}: {sensitivity_raw!r}, using 0")
                    sensitivity = 0

                self.config[host] = {
                    'sensitivity': sensitivity
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

            # Log full configuration on first load
            if self._first_load:
                self.logger.info("=" * 80)
                self.logger.info("SettingsDeflectAPI - First Load Configuration")
                self.logger.info("=" * 80)

                # Statistics
                self.logger.info(f"\n=== Statistics ===")
                self.logger.info(f"Total hosts: {len(self.config)}")
                self.logger.info(f"Domains whitelist: {len(self.domains)}")
                self.logger.info(f"Prefixes whitelist: {len(self.prefixes)}")
                self.logger.info(f"Matches whitelist: {len(self.matches)}")
                self.logger.info(f"Stars patterns: {len(self.stars)}")
                self.logger.info(f"Double stars patterns: {len(self.double_stars)}")
                self.logger.info(f"Block session whitelist: {len(self.whitelist_block_session)}")

                # Sensitivity distribution
                self.logger.info(f"\n=== Sensitivity Distribution ===")
                sensitivity_counts = {}
                for host, config in self.config.items():
                    sens = config['sensitivity']
                    sensitivity_counts[sens] = sensitivity_counts.get(sens, 0) + 1

                for sens, count in sorted(sensitivity_counts.items()):
                    self.logger.info(f"  sensitivity={sens}: {count} hosts")

                # Show first 10 hosts with their config
                self.logger.info(f"\n=== First 10 Hosts Configuration ===")
                for i, (host, config) in enumerate(list(self.config.items())[:10]):
                    self.logger.info(f"  {i+1}. {host}")
                    self.logger.info(f"     sensitivity: {config['sensitivity']} (type: {type(config['sensitivity']).__name__})")

                # Show hosts with non-zero sensitivity
                non_zero_sens = [(host, config['sensitivity']) for host, config in self.config.items()
                                 if config['sensitivity'] != 0]
                if non_zero_sens:
                    self.logger.info(f"\n=== Hosts with Non-Zero Sensitivity ({len(non_zero_sens)}) ===")
                    for host, sens in sorted(non_zero_sens, key=lambda x: x[1]):
                        self.logger.info(f"  {host}: {sens}")

                # Show whitelist examples
                if self.domains:
                    self.logger.info(f"\n=== Whitelisted Domains (first 10 of {len(self.domains)}) ===")
                    for domain in self.domains[:10]:
                        self.logger.info(f"  - {domain}")

                if self.prefixes:
                    self.logger.info(f"\n=== Whitelisted Prefixes (first 10 of {len(self.prefixes)}) ===")
                    for prefix in self.prefixes[:10]:
                        self.logger.info(f"  - {prefix}*")

                if self.matches:
                    self.logger.info(f"\n=== Exact Matches (first 10 of {len(self.matches)}) ===")
                    for match in self.matches[:10]:
                        self.logger.info(f"  - {match}")

                if self.stars:
                    self.logger.info(f"\n=== Star Patterns (first 10 of {len(self.stars)}) ===")
                    for start, end in self.stars[:10]:
                        self.logger.info(f"  - {start}*{end}")

                if self.whitelist_block_session:
                    self.logger.info(f"\n=== Block Session Whitelist (first 10 of {len(self.whitelist_block_session)}) ===")
                    for url in self.whitelist_block_session[:10]:
                        self.logger.info(f"  - {url}")

                self.logger.info("=" * 80)
                self.logger.info("End of Configuration Dump")
                self.logger.info("=" * 80)

                self._first_load = False

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