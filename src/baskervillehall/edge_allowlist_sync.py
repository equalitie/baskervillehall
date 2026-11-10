# -*- coding: utf-8 -*-
"""Keep loadBalancerSourceRanges of the public LoadBalancers (logstash, kafka9, kafkab) equal to the
Deflect edge IP list, plus a few fixed addresses.

The Kafka client certificate leaked and logstash beats has no client auth, so these ports must only
accept the edges. The edge list changes; this job follows it (edge_allowlist_sync_cronjob.yaml).

A bad answer from the API must never cut the edges off: the list is applied only when it is valid,
has at least MIN_EDGES addresses and keeps at least MIN_RATIO of the edges allowed now.

Env: EDGE_API_TOKEN (Secret deflect-edge-api), EDGE_API_URL, EXTRA_ALLOWED_CIDRS (comma separated),
SERVICES (comma separated), NAMESPACE, MIN_EDGES, MIN_RATIO, DRY_RUN=true to only print the diff.
"""
import ipaddress
import logging
import os
import sys

import requests

DEFAULT_URL = 'https://dashboard.deflect.network/api/integration/edge_ip_list'
DEFAULT_SERVICES = ('logstash-lb,kafka9-lb-0,kafka9-lb-1,kafka9-lb-2,'
                    'kafkab-lb-0,kafkab-lb-1,kafkab-lb-2')

logger = logging.getLogger('edge_allowlist_sync')


class SyncError(Exception):
    pass


def parse_edge_list(payload):
    """Edge IPs from the API answer {"last_updated": ..., "list": ["1.2.3.4", ...]} as /32 CIDRs."""
    if not isinstance(payload, dict) or not isinstance(payload.get('list'), list):
        raise SyncError('unexpected answer: no "list"')
    cidrs = set()
    for item in payload['list']:
        try:
            ip = ipaddress.ip_address(str(item).strip())
        except ValueError:
            raise SyncError(f'not an IP address in the list: {item!r}')
        cidrs.add(f'{ip}/{ip.max_prefixlen}')
    return cidrs


def parse_cidrs(text):
    out = set()
    for part in (text or '').split(','):
        part = part.strip()
        if part:
            out.add(str(ipaddress.ip_network(part, strict=False)))
    return out


def check_edges(new_edges, current_edges, min_edges, min_ratio):
    """Raise SyncError when the new list looks broken compared to what is allowed now."""
    if len(new_edges) < min_edges:
        raise SyncError(f'only {len(new_edges)} edges in the API answer (minimum {min_edges})')
    if current_edges:
        kept = len(new_edges & current_edges) / len(current_edges)
        if kept < min_ratio:
            raise SyncError(f'the new list keeps only {kept:.0%} of the {len(current_edges)} edges allowed now '
                            f'(minimum {min_ratio:.0%}); not applying')


def desired_ranges(edges, extra):
    return sorted(edges | extra, key=lambda c: ipaddress.ip_network(c))


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    token = os.environ.get('EDGE_API_TOKEN', '')
    if not token:
        logger.error('EDGE_API_TOKEN is not set')
        return 2
    url = os.environ.get('EDGE_API_URL', DEFAULT_URL)
    namespace = os.environ.get('NAMESPACE', 'default')
    services = [s.strip() for s in os.environ.get('SERVICES', DEFAULT_SERVICES).split(',') if s.strip()]
    extra = parse_cidrs(os.environ.get('EXTRA_ALLOWED_CIDRS', ''))
    min_edges = int(os.environ.get('MIN_EDGES', '10'))
    min_ratio = float(os.environ.get('MIN_RATIO', '0.7'))
    dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'

    from kubernetes import client, config
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    v1 = client.CoreV1Api()

    try:
        r = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
        if r.status_code != 200:
            raise SyncError(f'edge list API returned HTTP {r.status_code}')
        edges = parse_edge_list(r.json())
    except (requests.RequestException, ValueError, SyncError) as e:
        logger.error(f'cannot get the edge list, nothing changed: {e}')
        return 1

    current = {name: set(v1.read_namespaced_service(name, namespace).spec.load_balancer_source_ranges or [])
               for name in services}
    # Edges allowed now = what the services allow, minus the fixed extras.
    current_edges = set().union(*current.values()) - extra
    try:
        check_edges(edges, current_edges, min_edges, min_ratio)
    except SyncError as e:
        logger.error(str(e))
        return 1

    want = desired_ranges(edges, extra)
    changed = 0
    for name in services:
        added, removed = set(want) - current[name], current[name] - set(want)
        if not added and not removed:
            logger.info(f'{name}: up to date ({len(want)} ranges)')
            continue
        changed += 1
        logger.info(f'{name}: +{sorted(added)} -{sorted(removed)}{" (dry run)" if dry_run else ""}')
        if not dry_run:
            v1.patch_namespaced_service(name, namespace, {'spec': {'loadBalancerSourceRanges': want}})
    logger.info(f'{len(edges)} edges + {len(extra)} extra ranges; {changed} of {len(services)} services '
                f'{"would change" if dry_run else "updated"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
