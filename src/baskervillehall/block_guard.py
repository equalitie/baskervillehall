# -*- coding: utf-8 -*-
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


def is_host_whitelisted(host):
    host = (host or '').strip().lower()
    return any(host == h or host.endswith('.' + h) for h in HOST_WHITELIST)
