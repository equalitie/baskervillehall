# -*- coding: utf-8 -*-
"""Verified bots JSON shared by banjax and Baskerville: user agent tokens and published IP ranges
per operator and category, served at https://api.baskerville.ai/v1/verified_bots.json
(verified_bots.yaml). A CronJob runs this every 6 h and writes the JSON to the ConfigMap that
nginx serves.

  {"updated_at": "...", "operators": {
     "OpenAI": {
       "training":   {"ua_tokens": ["GPTBot"], "ip_ranges": ["20.171.206.0/24", ...],
                      "sources": ["https://openai.com/gptbot.json"], "updated_at": "..."},
       "assistance": {...}},
     "Anthropic": {"training": {"ua_tokens": ["ClaudeBot"], "ip_ranges": null, ...}}}}

Categories: search, training, assistance (fetches for a user's chat request), fetcher (link
previews, feeds, site verification), seo.

Matching: a UA matches a category when it contains one of its ua_tokens (case-insensitive). With
ip_ranges, the client IP must be in them, otherwise it is a spoofer. ip_ranges null means the
operator publishes no ranges: the UA cannot be verified, so "allow" must not be applied to it.

A category whose source fails, or looks too small, keeps the ranges of the previous JSON (or is
left out on the first run), so one broken vendor list never empties the file. Nothing is
written when more than MAX_FAILED_SOURCES sources fail.

Env: VERIFIED_BOTS_CONFIGMAP (default verified-bots), NAMESPACE (default), DRY_RUN=true to print.
"""
import ipaddress
import json
import logging
import os
import sys
from collections import OrderedDict
from datetime import datetime, timezone

import requests

logger = logging.getLogger('verified_bots')

CONFIGMAP_KEY = 'verified_bots.json'
MAX_FAILED_SOURCES = 4

# (operator, category, ua_tokens, source url, min ranges). Several rows of one operator and
# category are merged (Google's fetchers).
SOURCES = (
    ('Google', 'search', ['Googlebot', 'Googlebot-Image', 'Googlebot-News', 'Googlebot-Video',
                          'Storebot-Google', 'Google-InspectionTool', 'GoogleOther'],
     'https://developers.google.com/static/crawling/ipranges/common-crawlers.json', 50),
    ('Google', 'fetcher', ['AdsBot-Google', 'Mediapartners-Google', 'APIs-Google', 'Google-Safety',
                           'FeedFetcher-Google', 'Google-Site-Verification', 'Google-Read-Aloud'],
     'https://developers.google.com/static/crawling/ipranges/special-crawlers.json', 50),
    ('Google', 'fetcher', [],
     'https://developers.google.com/static/crawling/ipranges/user-triggered-fetchers.json', 100),
    ('Google', 'fetcher', [],
     'https://developers.google.com/static/crawling/ipranges/user-triggered-fetchers-google.json', 50),
    ('Microsoft', 'search', ['bingbot', 'BingPreview', 'adidxbot'],
     'https://www.bing.com/toolbox/bingbot.json', 5),
    ('Apple', 'search', ['Applebot'], 'https://search.developer.apple.com/applebot.json', 5),
    ('DuckDuckGo', 'search', ['DuckDuckBot'], 'https://duckduckgo.com/duckduckbot.json', 50),
    ('DuckDuckGo', 'assistance', ['DuckAssistBot'], 'https://duckduckgo.com/duckassistbot.json', 1),
    ('OpenAI', 'training', ['GPTBot'], 'https://openai.com/gptbot.json', 3),
    ('OpenAI', 'search', ['OAI-SearchBot'], 'https://openai.com/searchbot.json', 3),
    ('OpenAI', 'assistance', ['ChatGPT-User'], 'https://openai.com/chatgpt-user.json', 10),
    ('Perplexity', 'search', ['PerplexityBot'], 'https://www.perplexity.ai/perplexitybot.json', 2),
    ('Perplexity', 'assistance', ['Perplexity-User'], 'https://www.perplexity.ai/perplexity-user.json', 2),
    ('Mistral AI', 'assistance', ['MistralAI-User'], 'https://mistral.ai/mistralai-user-ips.json', 1),
    ('Common Crawl', 'training', ['CCBot'], 'https://index.commoncrawl.org/ccbot.json', 1),
    ('Ahrefs', 'seo', ['AhrefsBot', 'AhrefsSiteAudit'],
     'https://api.ahrefs.com/v3/public/crawler-ip-ranges', 10),
)

# Known bots with no published IP list (verified at best by reverse DNS, or not at all).
UA_ONLY = (
    ('Anthropic', 'training', ['ClaudeBot']),
    ('Anthropic', 'search', ['Claude-SearchBot']),
    ('Anthropic', 'assistance', ['Claude-User']),
    ('Meta', 'training', ['meta-externalagent']),
    ('Meta', 'fetcher', ['facebookexternalhit', 'meta-externalfetcher']),
    ('ByteDance', 'training', ['Bytespider']),
    ('Amazon', 'training', ['Amazonbot']),
    ('Yandex', 'search', ['YandexBot']),
    ('Baidu', 'search', ['Baiduspider']),
)

CATEGORIES = ('search', 'training', 'assistance', 'fetcher', 'seo')


class SourceError(Exception):
    pass


def parse_prefixes(payload):
    """CIDRs from the vendor format {"prefixes": [{"ipv4Prefix": ...} | {"ipv6Prefix": ...}]}."""
    if not isinstance(payload, dict) or not isinstance(payload.get('prefixes'), list):
        raise SourceError('unexpected answer: no "prefixes"')
    nets = set()
    for item in payload['prefixes']:
        if not isinstance(item, dict):
            continue
        for key in ('ipv4Prefix', 'ipv6Prefix'):
            if item.get(key):
                try:
                    nets.add(ipaddress.ip_network(str(item[key]).strip(), strict=False))
                except ValueError:
                    logger.warning(f'bad prefix {item[key]!r}')
    return nets


def _sorted(nets):
    return [str(n) for n in sorted(nets, key=lambda n: (n.version, n.network_address, n.prefixlen))]


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def fetch(session, url, min_ranges):
    r = session.get(url, timeout=(5, 20), allow_redirects=True)
    r.raise_for_status()
    nets = parse_prefixes(r.json())
    if len(nets) < min_ranges:
        raise SourceError(f'{len(nets)} ranges, expected at least {min_ranges}')
    return nets


def build(session=None, previous=None, sources=SOURCES, ua_only=UA_ONLY):
    """Return (document, failed source urls). previous is the last published document."""
    session = session or requests.Session()
    session.headers.update({'User-Agent': 'Baskerville verified-bots (+https://baskerville.ai)'})
    prev_ops = (previous or {}).get('operators', {})
    now = _now()
    cats = OrderedDict()  # (operator, category) -> entry under construction
    failed = []
    for operator, category, tokens, url, min_ranges in sources:
        e = cats.setdefault((operator, category), {'ua_tokens': [], 'nets': set(), 'sources': [],
                                                   'failed': False})
        e['ua_tokens'] += [t for t in tokens if t not in e['ua_tokens']]
        e['sources'].append(url)
        try:
            e['nets'] |= fetch(session, url, min_ranges)
        except Exception as ex:
            logger.error(f'{operator}/{category}: {url}: {ex}')
            failed.append(url)
            e['failed'] = True
    operators = OrderedDict()
    for (operator, category), e in cats.items():
        if e['failed']:
            old = prev_ops.get(operator, {}).get(category)
            if not old:
                logger.error(f'{operator}/{category}: left out (no previous data)')
                continue
            entry = dict(old)  # last good ranges, with their own updated_at
        else:
            entry = {'ua_tokens': e['ua_tokens'], 'ip_ranges': _sorted(e['nets']),
                     'sources': e['sources'], 'updated_at': now}
        operators.setdefault(operator, OrderedDict())[category] = entry
    for operator, category, tokens in ua_only:
        operators.setdefault(operator, OrderedDict())[category] = {
            'ua_tokens': tokens, 'ip_ranges': None, 'sources': [], 'updated_at': now}
    return {'updated_at': now, 'categories': list(CATEGORIES), 'operators': operators}, failed


def match(document, ua, ip):
    """(operator, category, verified) for a request, or None when the UA is not a known bot.
    verified is True (IP in the ranges), False (spoofer) or None (no published ranges).
    The reference for banjax and the predictor."""
    ua_l = (ua or '').lower()
    for operator, cats in document.get('operators', {}).items():
        for category, e in cats.items():
            if any(t.lower() in ua_l for t in e.get('ua_tokens') or []):
                ranges = e.get('ip_ranges')
                if ranges is None:
                    return operator, category, None
                addr = ipaddress.ip_address(ip)
                return operator, category, any(addr in ipaddress.ip_network(r) for r in ranges)
    return None


def _k8s():
    from kubernetes import client, config
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CoreV1Api()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    name = os.environ.get('VERIFIED_BOTS_CONFIGMAP', 'verified-bots')
    namespace = os.environ.get('NAMESPACE', 'default')
    dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    previous, v1 = None, None
    if not dry_run:
        v1 = _k8s()
        try:
            cm = v1.read_namespaced_config_map(name, namespace)
            previous = json.loads((cm.data or {}).get(CONFIGMAP_KEY) or 'null')
        except Exception as e:
            logger.warning(f'no previous {name}: {e}')
    doc, failed = build(previous=previous)
    for operator, cats in doc['operators'].items():
        for category, e in cats.items():
            n = 'no ranges' if e['ip_ranges'] is None else f"{len(e['ip_ranges'])} ranges"
            logger.info(f"{operator}/{category}: {', '.join(e['ua_tokens']) or '-'}: {n}")
    body = json.dumps(doc, separators=(',', ':'))
    if len(failed) > MAX_FAILED_SOURCES:
        logger.error(f'{len(failed)} sources failed, not publishing')
        return 1
    if dry_run:
        print(json.dumps(doc, indent=1)[:3000])
        return 1 if failed else 0
    v1.patch_namespaced_config_map(name, namespace, {'data': {CONFIGMAP_KEY: body}})
    logger.info(f'{name}/{CONFIGMAP_KEY}: {len(body)} bytes, {len(failed)} sources failed')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
