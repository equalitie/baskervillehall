import ipaddress
import logging

from baskervillehall.json_url_reader import JsonUrlReader


class SettingsDeflectAPI(object):

    def __init__(
            self,
            url,
            auth,
            ip_whitelist_url=None,
            ip_whitelist_auth=None,
            global_allowlist_url=None,
            global_allowlist_auth=None,
            origin_ips_url=None,
            origin_ips_auth=None,
            whitelist_default=[],
            whitelist_default_block_session=[],
            logger=None,
            refresh_period_in_seconds=300):
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.reader = JsonUrlReader(url=url,
                                    headers={'Authorization': f'Bearer {auth}'},
                                    logger=self.logger, refresh_period_in_seconds=refresh_period_in_seconds)

        # IP whitelist reader (optional) — /banjax_global_per_site_allowlist
        self.ip_whitelist_reader = None
        if ip_whitelist_url and ip_whitelist_auth:
            self.ip_whitelist_reader = JsonUrlReader(
                url=ip_whitelist_url,
                headers={'Authorization': f'Bearer {ip_whitelist_auth}'},
                logger=logger,
                refresh_period_in_seconds=refresh_period_in_seconds
            )
        self.ip_whitelist_data = {}
        self.ip_whitelist_parsed = {}  # Cache of parsed IP networks per host

        # Global allowlist reader (optional) — /global_allowlist
        # Contains verified bot IPs and custom globally-allowlisted IPs.
        self.global_allowlist_reader = None
        if global_allowlist_url and global_allowlist_auth:
            self.global_allowlist_reader = JsonUrlReader(
                url=global_allowlist_url,
                headers={'Authorization': f'Bearer {global_allowlist_auth}'},
                logger=logger,
                refresh_period_in_seconds=refresh_period_in_seconds
            )
        self.global_allowlist_networks = []  # list[ip_network]

        # Origin IPs reader (optional) — /origin_ips
        # Contains per-site origin IPs that must never be challenged.
        self.origin_ips_reader = None
        if origin_ips_url and origin_ips_auth:
            self.origin_ips_reader = JsonUrlReader(
                url=origin_ips_url,
                headers={'Authorization': f'Bearer {origin_ips_auth}'},
                logger=logger,
                refresh_period_in_seconds=refresh_period_in_seconds
            )
        self.origin_ips_parsed = {}  # host -> list[ip_network]
        self.domains = []
        self.prefixes = []
        self.matches = set()
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

                if v.get('allowlist_domain', False):
                    white_list.append(host)

                self.config[host] = {
                    'sensitivity': sensitivity,
                    'country_mode': v.get('country_mode', None),
                    'country_blacklist': set(v.get('country_blacklist', [])),
                    'country_whitelist': set(v.get('country_whitelist', [])),
                    'country_action_type': v.get('country_action_type', 'challenge'),
                    'ttl_ban_time': v.get('ttl_ban_time', 300),
                }

            self.domains = []
            self.prefixes = []
            self.matches = set()
            self.stars = []
            for url in white_list:
                if url.find('/') < 0:
                    self.domains.append(url)
                else:
                    star_pos = url.find('*')
                    if url.find('*') < 0:
                        self.matches.add(url)
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

                # Country blocking distribution
                country_block_hosts = [(host, config['country_mode']) for host, config in self.config.items()
                                       if config.get('country_mode') is not None]
                if country_block_hosts:
                    self.logger.info(f"\n=== Hosts with Country Blocking ({len(country_block_hosts)}) ===")
                    for host, mode in country_block_hosts:
                        cfg = self.config[host]
                        action = cfg.get('country_action_type', 'challenge')
                        ttl = cfg.get('ttl_ban_time', 300)
                        if mode == 'whitelist':
                            countries = cfg.get('country_whitelist', set())
                            self.logger.info(f"  {host}: mode={mode}, allowed={countries}, action={action}, ttl={ttl}")
                        elif mode == 'blacklist':
                            countries = cfg.get('country_blacklist', set())
                            self.logger.info(f"  {host}: mode={mode}, blocked={countries}, action={action}, ttl={ttl}")

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
                    for match in list(self.matches)[:10]:
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

    def _parse_ip_list(self, ip_list, context):
        """Parse a list of IP/CIDR strings into ip_network objects, skipping invalid entries."""
        networks = []
        for ip_entry in ip_list:
            try:
                if ip_entry.startswith('/'):
                    continue
                networks.append(ipaddress.ip_network(ip_entry, strict=False))
            except ValueError as e:
                self.logger.warning(f"Invalid IP/CIDR in {context}: {ip_entry!r} - {e}")
        return networks

    def _refresh_ip_whitelist(self):
        if not self.ip_whitelist_reader:
            return

        data, fresh = self.ip_whitelist_reader.get()
        if data and fresh:
            self.ip_whitelist_data = data
            self.ip_whitelist_parsed = {}

            for host, ip_list in data.items():
                self.ip_whitelist_parsed[host] = self._parse_ip_list(ip_list, f"banjax_global_per_site_allowlist/{host}")

            self.logger.info(f"IP whitelist refreshed: {len(self.ip_whitelist_parsed)} hosts loaded")

    def _refresh_global_allowlist(self):
        """Refresh /global_allowlist — globally whitelisted IPs (bots + custom)."""
        if not self.global_allowlist_reader:
            return

        data, fresh = self.global_allowlist_reader.get()
        if data and fresh:
            # API may return a list of IPs or a dict with an 'ips'/'allowlist' key
            if isinstance(data, list):
                ip_list = data
            elif isinstance(data, dict):
                # Real API returns {"global_allowlist": [...], "global_updated_at": "..."}
                ip_list = (
                    data.get('global_allowlist')
                    or data.get('ips')
                    or data.get('allowlist')
                    or []
                )
                if not ip_list:
                    # fallback: flatten all list values
                    for v in data.values():
                        if isinstance(v, list):
                            ip_list.extend(v)
            else:
                ip_list = []
            self.global_allowlist_networks = self._parse_ip_list(ip_list, "global_allowlist")
            self.logger.info(f"Global allowlist refreshed: {len(self.global_allowlist_networks)} IPs/CIDRs loaded")

    def _refresh_origin_ips(self):
        """Refresh /origin_ips — per-site origin IPs that must never be challenged."""
        if not self.origin_ips_reader:
            return

        data, fresh = self.origin_ips_reader.get()
        if data and fresh:
            self.origin_ips_parsed = {}
            if isinstance(data, dict):
                for host, ip_list in data.items():
                    if isinstance(ip_list, list):
                        self.origin_ips_parsed[host] = self._parse_ip_list(ip_list, f"origin_ips/{host}")
                    elif isinstance(ip_list, str):
                        # Some APIs return a single IP as a string
                        self.origin_ips_parsed[host] = self._parse_ip_list([ip_list], f"origin_ips/{host}")
            total = sum(len(v) for v in self.origin_ips_parsed.values())
            self.logger.info(f"Origin IPs refreshed: {len(self.origin_ips_parsed)} hosts, {total} IPs loaded")

    def is_ip_whitelisted(self, host, ip):
        """
        Check if an IP address is whitelisted for a given host.

        Checks (in order):
          1. /banjax_global_per_site_allowlist  — per-site + 'global' key
          2. /global_allowlist                  — globally whitelisted IPs (bots, custom)
          3. /origin_ips                        — per-site origin IPs

        Args:
            host: The hostname to check (e.g., 'kavkaz-uzel.eu')
            ip: The IP address to check (e.g., '104.131.108.241')

        Returns:
            True if the IP is whitelisted for this host, False otherwise
        """
        self._refresh_ip_whitelist()
        self._refresh_global_allowlist()
        self._refresh_origin_ips()

        try:
            ip_addr = ipaddress.ip_address(ip)
        except ValueError as e:
            self.logger.warning(f"Invalid IP address: {ip!r} - {e}")
            return False

        # 1. Per-site allowlist + 'global' key from /banjax_global_per_site_allowlist
        for network in self.ip_whitelist_parsed.get(host, []) + self.ip_whitelist_parsed.get('global', []):
            if ip_addr in network:
                return True

        # 2. Global allowlist — /global_allowlist
        for network in self.global_allowlist_networks:
            if ip_addr in network:
                return True

        # 3. Origin IPs — /origin_ips
        for network in self.origin_ips_parsed.get(host, []):
            if ip_addr in network:
                return True

        return False

    def is_country_blocked(self, host, country):
        """
        Check if a country is blocked for a given host based on the
        country_mode setting from the Deflect API.

        Modes:
          - None: no country blocking, returns False
          - "whitelist": only countries in country_whitelist are allowed;
                         everything else is blocked
          - "blacklist": countries in country_blacklist are blocked;
                         everything else is allowed

        Args:
            host: The hostname to check
            country: ISO country code (e.g., 'US', 'RU')

        Returns:
            True if the country is blocked for this host, False otherwise
        """
        self._refresh()
        host_data = self.config.get(host)
        if not host_data:
            return False

        mode = host_data.get('country_mode')
        if mode is None:
            return False

        if mode == 'whitelist':
            return country not in host_data.get('country_whitelist', set())

        if mode == 'blacklist':
            return country in host_data.get('country_blacklist', set())

        return False

    def get_country_action_type(self, host):
        """Get the country blocking action type for a host ('challenge' or 'block')."""
        self._refresh()
        host_data = self.config.get(host)
        if not host_data:
            return 'challenge'
        return host_data.get('country_action_type', 'challenge')

    def get_ttl_ban_time(self, host):
        """Get the TTL ban time in seconds for country blocking for a host."""
        self._refresh()
        host_data = self.config.get(host)
        if not host_data:
            return 300
        return host_data.get('ttl_ban_time', 300)