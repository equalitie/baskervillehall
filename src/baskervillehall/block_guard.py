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


# AltCha instead of the sha-inv challenge for every automatic challenge (banjax >= v0.13.3 on all
# edges). sha-inv is ~1000 hashes, nothing for a headless browser; AltCha's work is tunable, and
# its cookie also passes sha-inv. Both fail the same way (Block), so the switch adds no new risk
# besides the solve time, which BASKERVILLE_ALTCHA_MAX_NUMBER sets (banjax clamps to 1000..20000;
# its own default, 10000, is too slow for cheap phones, so we always send it).
ALTCHA_ENABLED = os.environ.get('BASKERVILLE_ALTCHA', 'false').lower() == 'true'
ALTCHA_MAX_NUMBER = int(os.environ.get('BASKERVILLE_ALTCHA_MAX_NUMBER', '1000'))

_CHALLENGE_TO_ALTCHA = {
    'challenge_ip': 'altcha_challenge_ip',
    'challenge_subnet': 'altcha_challenge_subnet',
    'challenge_session': 'altcha_challenge_session',
    'challenge_ua': 'altcha_challenge_ua',
    'challenge_all': 'altcha_challenge_all',
}


def finalize_command(command):
    """Last step before a command dict is sent: with BASKERVILLE_ALTCHA on, a challenge_* becomes
    its altcha_challenge_* with our difficulty (an explicit altcha_max_number is kept). The name in
    meta/source ("... [challenge_ip]") follows, so dashboards split by it. Returns the command."""
    name = command.get('Name')
    altcha = _CHALLENGE_TO_ALTCHA.get(name)
    if not ALTCHA_ENABLED or altcha is None:
        return command
    command['Name'] = altcha
    command.setdefault('altcha_max_number', ALTCHA_MAX_NUMBER)
    for f in ('meta', 'source'):
        if isinstance(command.get(f), str):
            command[f] = command[f].replace(f'[{name}]', f'[{altcha}]')
    return command


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
