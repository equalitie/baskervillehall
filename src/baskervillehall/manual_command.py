# -*- coding: utf-8 -*-
"""Send one banjax command by hand (support analysts), with the same routing and guards as the
First Responder. Runs inside the incident-first-responder pod, which has the Kafka, Postgres and
dnet settings; analysts call it through analyst/bin/send_command.sh.

  python manual_command.py challenge_ip --host example.org --value 1.2.3.4 --ttl 3600 \
      --reason "scraper hammering /search" --operator vladimir
  python manual_command.py clear_rules --host example.org --value 1.2.3.4 --reason "false positive" ...

Every command sent is recorded in the manual_commands table.
"""
import argparse
import ipaddress
import json
import os
import sys

from baskervillehall.block_guard import _in_env_whitelist, guard_command_name, is_host_opted_out

MAX_TTL = 24 * 3600
MIN_SUBNET_PREFIX = 16

# command name -> what it targets
COMMANDS = {
    'challenge_ip': 'ip', 'block_ip': 'ip', 'altcha_challenge_ip': 'ip',
    'challenge_subnet': 'subnet', 'block_subnet': 'subnet', 'altcha_challenge_subnet': 'subnet',
    'challenge_session': 'session', 'block_session': 'session', 'altcha_challenge_session': 'session',
    'challenge_ua': 'ua', 'block_ua': 'ua', 'altcha_challenge_ua': 'ua',
    'challenge_all': 'host', 'altcha_challenge_all': 'host',
    'clear_rules': 'clear',
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS manual_commands (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    operator    TEXT NOT NULL,
    name        TEXT NOT NULL,          -- as requested
    sent_name   TEXT NOT NULL,          -- after the block kill switch
    host        TEXT NOT NULL,
    value       TEXT,
    ua          TEXT,
    session_id  TEXT,
    ttl         INTEGER,
    dnet        TEXT,
    partitions  TEXT,
    reason      TEXT NOT NULL,
    message     JSONB NOT NULL
)
"""


class CommandError(ValueError):
    pass


def build_command(name, host, value='', ua='', session_id='', ttl=None, reason='', operator='',
                  clear_host=False, allow_block=False):
    """Validate the request and return (banjax message, list of warnings)."""
    warnings = []
    if name not in COMMANDS:
        raise CommandError(f"unknown command {name}; allowed: {', '.join(sorted(COMMANDS))}")
    host = (host or '').strip().lower()
    if not host:
        raise CommandError('--host is required (it decides which dnet gets the command)')
    if not reason.strip():
        raise CommandError('--reason is required')
    if not operator.strip():
        raise CommandError('--operator is required')
    if _in_env_whitelist(host.strip().lower()):
        raise CommandError(f'{host} is in BASKERVILLE_HOST_WHITELIST, no commands are sent for it')
    # clear_rules stays allowed: it lifts IP decisions that apply to the whole edge.
    if name != 'clear_rules' and is_host_opted_out(host):
        raise CommandError(f'{host} opted out of Baskerville in the Deflect dashboard '
                           f'(allowlist_domain); banjax ignores our commands for it')

    kind = COMMANDS[name]
    # The kill switch is for automatic blocks; an analyst can override it for one command.
    sent_name = name if allow_block else guard_command_name(name)
    if sent_name != name:
        warnings.append(f'blocks are disabled (BASKERVILLE_BLOCKS_DISABLED): {name} is sent as {sent_name}; '
                        f'add --allow-block to send a real block')
    elif allow_block and sent_name != guard_command_name(name):
        warnings.append(f'--allow-block: a real {name} is sent although automatic blocks are disabled')

    meta = f'manual [{operator}] {reason}'
    # "Value" (not "value"): banjax reads either, but the Kibana pipeline maps only "Value" to client_ip.
    message = {'Name': sent_name, 'source': meta, 'meta': meta}

    if kind != 'clear':
        if ttl is None:
            raise CommandError('--ttl is required')
        if not 0 < ttl <= MAX_TTL:
            raise CommandError(f'--ttl must be between 1 and {MAX_TTL} seconds')
        message['ttl'] = ttl

    if kind == 'ip':
        message['Value'] = _ip(value)
        message['host'] = host
        warnings.append('an IP decision applies to every site of the dnet that receives it, not only this host')
    elif kind == 'subnet':
        message['Value'] = _subnet(value)
        message['host'] = host
        warnings.append('a subnet decision applies to every site of the dnet that receives it, not only this host')
    elif kind == 'session':
        if not session_id:
            raise CommandError('--session-id is required')
        message['session_id'] = session_id
        message['host'] = host
    elif kind == 'ua':
        if len(ua or '') < 5:
            raise CommandError('--ua is required (the full, exact user agent)')
        message['ua'] = ua
        message['host'] = host
    elif kind == 'host':
        message['host'] = host
    else:
        # clear_rules: banjax clears every target present in the message, and "host" alone means
        # the host's challenge_all. So host goes into the message only when asked for.
        targets = 0
        if value:
            message['Value'] = _subnet(value) if '/' in value else _ip(value)
            warnings.append(f'clears {message["Value"]} on every site, in every dnet')
            targets += 1
        if session_id:
            message['session_id'] = session_id
            targets += 1
        if ua:
            message['ua'] = ua
            targets += 1
            if clear_host:
                warnings.append('banjax clears the ua on this host AND the host-wide challenge_all')
            else:
                warnings.append('clears this ua on every site (add --clear-host to limit it to the host)')
        if clear_host:
            message['host'] = host
            targets += 1
        if targets == 0:
            raise CommandError('clear_rules needs --value (ip or subnet), --session-id, --ua or --clear-host')
    return message, warnings


def _ip(value):
    try:
        return str(ipaddress.ip_address((value or '').strip()))
    except ValueError:
        raise CommandError(f'--value must be an IP address, got {value!r}')


def _subnet(value):
    try:
        net = ipaddress.ip_network((value or '').strip(), strict=False)
    except ValueError:
        raise CommandError(f'--value must be a subnet like 1.2.3.0/24, got {value!r}')
    if net.version != 4:
        raise CommandError('banjax supports IPv4 subnets only')
    if net.prefixlen < MIN_SUBNET_PREFIX:
        raise CommandError(f'subnet is wider than /{MIN_SUBNET_PREFIX}')
    return str(net)


def _host_dnet(conn, host):
    with conn.cursor() as cur:
        cur.execute("SELECT dnet FROM host_dnet WHERE host = %s", (host,))
        row = cur.fetchone()
    return row[0] if row else None


# Each banjax reads only its own dnet's partition. Host-scoped commands and clear_rules go to every
# dnet: harmless where the host is not served or nothing is set, and independent of host_dnet, which
# keeps one dnet per host (the last one seen) and can be missing or stale. IP and subnet decisions
# apply to every site of the dnet that receives them, so they stay on the host's dnet by default.
ALL_DNETS_KINDS = {'clear', 'host', 'ua', 'session'}


def choose_partitions(name, host_dnet, partition_map, dnet=None, all_dnets=False):
    """Return (dnet label, sorted partitions) for a command, or raise CommandError."""
    if not partition_map:
        raise CommandError('DNET_PARTITION_MAP is empty')
    if dnet:
        if dnet not in partition_map:
            raise CommandError(f'unknown dnet {dnet}; known: {", ".join(sorted(partition_map))}')
        return dnet, [partition_map[dnet]]
    if all_dnets or COMMANDS[name] in ALL_DNETS_KINDS:
        return 'all', sorted(set(partition_map.values()))
    if host_dnet in partition_map:
        return host_dnet, [partition_map[host_dnet]]
    raise CommandError(f'the host has no known dnet (host_dnet={host_dnet}). Check the host name, '
                       f'or choose --dnet <name> or --all-dnets (all of Deflect)')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Send one banjax command by hand.')
    parser.add_argument('name', help=', '.join(sorted(COMMANDS)))
    parser.add_argument('--host', required=True)
    parser.add_argument('--value', default='', help='IP or IPv4 subnet')
    parser.add_argument('--ua', default='', help='exact user agent')
    parser.add_argument('--session-id', default='')
    parser.add_argument('--ttl', type=int, help=f'seconds, max {MAX_TTL}')
    parser.add_argument('--reason', required=True)
    parser.add_argument('--operator', required=True)
    parser.add_argument('--allow-block', action='store_true',
                        help='send block_* as is, even while BASKERVILLE_BLOCKS_DISABLED turns automatic blocks into challenges')
    parser.add_argument('--clear-host', action='store_true',
                        help="clear_rules: also clear the host's challenge_all (and limit --ua to this host)")
    parser.add_argument('--all-dnets', action='store_true',
                        help='ip/subnet commands: send to every dnet (all of Deflect), not only the host\'s dnet')
    parser.add_argument('--dnet', default='',
                        help='send only to this dnet (by name, e.g. dnext4)')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    try:
        message, warnings = build_command(
            args.name, args.host, value=args.value, ua=args.ua, session_id=args.session_id,
            ttl=args.ttl, reason=args.reason, operator=args.operator, clear_host=args.clear_host,
            allow_block=args.allow_block)
    except CommandError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 2

    import psycopg2
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST'), user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'), database=os.environ.get('POSTGRES_DATABASE_NAME'),
        port=int(os.environ.get('POSTGRES_PORT', '5432')))
    conn.autocommit = True

    host = args.host.strip().lower()
    partition_map = json.loads(os.environ.get('DNET_PARTITION_MAP', '{}'))
    host_dnet = _host_dnet(conn, host)
    try:
        dnet, partitions = choose_partitions(args.name, host_dnet, partition_map,
                                             dnet=args.dnet.strip(), all_dnets=args.all_dnets)
    except CommandError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 2

    message['dnet'] = dnet
    if dnet == 'all' and COMMANDS[args.name] in ('ip', 'subnet'):
        warnings.append('--all-dnets: this IP/subnet decision applies on every Deflect site')
    for w in warnings:
        print(f'NOTE: {w}')
    print(f'host dnet={host_dnet or "-"} -> sent to dnet={dnet} partitions={partitions}')
    print(json.dumps(message, ensure_ascii=False))
    if args.dry_run:
        print('dry run, nothing sent')
        return 0

    from kafka import KafkaProducer
    topic = os.environ.get('TOPIC_COMMANDS', 'banjax_command_topic')
    encoded = json.dumps(message).encode('utf-8')
    producer_output = KafkaProducer(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS_OUTPUT'))
    for p in partitions:
        producer_output.send(topic=topic, value=encoded, partition=p)
    producer_output.flush()
    # The internal cluster gets a copy too, as with First Responder commands.
    if os.environ.get('BOOTSTRAP_SERVERS'):
        producer = KafkaProducer(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
        producer.send(topic=topic, value=encoded, key=host.encode('utf-8'))
        producer.flush()

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(
            """INSERT INTO manual_commands (operator, name, sent_name, host, value, ua, session_id, ttl,
                                            dnet, partitions, reason, message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
            (args.operator, args.name, message['Name'], host, message.get('Value'), message.get('ua'),
             message.get('session_id'), message.get('ttl'), dnet, ','.join(map(str, partitions)),
             args.reason, json.dumps(message)))
        row_id = cur.fetchone()[0]
    print(f'sent, manual_commands.id={row_id}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
