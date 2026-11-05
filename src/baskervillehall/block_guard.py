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
