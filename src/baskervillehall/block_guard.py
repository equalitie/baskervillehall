# -*- coding: utf-8 -*-
import logging
import os

# Kill switch for IP/subnet/session blocks (Nov 2026: too much collateral, since banjax
# applies these to every site on the edge). When on, blocks are sent as challenges.
BLOCKS_DISABLED = os.environ.get('BASKERVILLE_BLOCKS_DISABLED', 'false').lower() == 'true'

_BLOCK_TO_CHALLENGE = {
    'block_ip': 'challenge_ip',
    'block_subnet': 'challenge_subnet',
    'block_session': 'challenge_session',
}


def guard_command_name(name):
    if BLOCKS_DISABLED:
        return _BLOCK_TO_CHALLENGE.get(name, name)
    return name


# Sites Baskerville never sends any command for: the exact host and all its subdomains.
# FIRST_RESPONDER_HOST_WHITELIST is kept for backward compatibility.
HOST_WHITELIST = tuple(
    h.strip().lower()
    for h in (os.environ.get('BASKERVILLE_HOST_WHITELIST', '') + ','
              + os.environ.get('FIRST_RESPONDER_HOST_WHITELIST', '')).split(',')
    if h.strip()
)


def _in_env_whitelist(host):
    return any(host == h or host.endswith('.' + h) for h in HOST_WHITELIST)


# Sites that opted out of Baskerville in the Deflect dashboard (allowlist_domain in
# baskerville/config_v3). Banjax ignores our decisions for them (sites_to_disable_baskerville),
# so we send nothing: host-scoped commands would be dropped anyway, and IP commands would
# apply to every other site on the edge. Exact host, as banjax matches it.
OPT_OUT_REFRESH_SECONDS = 600
_opt_out_reader = None


def _opted_out_hosts():
    global _opt_out_reader
    url = os.environ.get('DEFLECT_CONFIG_URL')
    if not url:
        return set()
    if _opt_out_reader is None:
        from baskervillehall.json_url_reader import JsonUrlReader
        _opt_out_reader = JsonUrlReader(
            url=url,
            headers={'Authorization': f"Bearer {os.environ.get('DEFLECT_CONFIG_AUTH', '')}"},
            logger=logging.getLogger('block_guard'),
            refresh_period_in_seconds=OPT_OUT_REFRESH_SECONDS)
        _opt_out_reader.hosts = set()
    data, fresh = _opt_out_reader.get()
    if fresh and isinstance(data, dict):
        _opt_out_reader.hosts = {h.strip().lower() for h, v in data.items()
                                 if isinstance(v, dict) and v.get('allowlist_domain')}
        logging.getLogger('block_guard').info(
            f'Opted-out hosts (dashboard allowlist_domain): {len(_opt_out_reader.hosts)}')
    return _opt_out_reader.hosts


def is_host_opted_out(host):
    host = (host or '').strip().lower()
    hosts = _opted_out_hosts()
    return host in hosts or (host.startswith('www.') and host[4:] in hosts)


def is_host_whitelisted(host):
    host = (host or '').strip().lower()
    return _in_env_whitelist(host) or is_host_opted_out(host)
