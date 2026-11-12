# -*- coding: utf-8 -*-
"""Publish the IP ranges of verified crawlers to a compacted Kafka topic, so banjax does not have to
fetch and parse the vendor lists itself (crawler_ranges_cronjob.yaml).

One message per crawler, key = crawler name:
  {"name": "googlebot", "operator": "Google", "category": "search", "ua": "Googlebot",
   "ranges": ["66.249.64.0/27", ...], "source": "<url>", "updated_at": "2026-11-12T11:00:00Z"}

category is what the crawler is for; whether to allow it is the consumer's policy:
  search      search engine indexing
  fetcher     fetches on behalf of a user or a product (link previews, site verification, feeds)
  ai_training collects data to train models
  ai_search   indexes for an AI search product
  ai_user     fetches a page for a user's chat request
  seo         SEO tool crawler

A crawler whose list cannot be fetched, or comes back empty or much smaller than expected, is not
published: the topic keeps its last good snapshot. Read the topic from the beginning on start.

Env: CRAWLER_RANGES_TOPIC (default verified_crawlers), CRAWLER_RANGES_BOOTSTRAPS (';'-separated
bootstrap lists, default BOOTSTRAP_SERVERS_OUTPUT;BOOTSTRAP_SERVERS), DRY_RUN=true to only log.
"""
import ipaddress
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone

import requests

logger = logging.getLogger('crawler_ranges_publisher')

# name, operator, category, user agent token, url, format, min_ranges (sanity floor)
CRAWLERS = (
    ('googlebot', 'Google', 'search', 'Googlebot',
     'https://developers.google.com/static/crawling/ipranges/common-crawlers.json', 'prefixes', 50),
    ('google-special-crawlers', 'Google', 'fetcher', 'AdsBot-Google, Mediapartners-Google, APIs-Google',
     'https://developers.google.com/static/crawling/ipranges/special-crawlers.json', 'prefixes', 50),
    ('google-user-triggered-fetchers', 'Google', 'fetcher', 'FeedFetcher-Google, Google-Site-Verification',
     'https://developers.google.com/static/crawling/ipranges/user-triggered-fetchers.json', 'prefixes', 100),
    ('google-user-triggered-fetchers-google', 'Google', 'fetcher', 'Google-Read-Aloud, Google-InspectionTool',
     'https://developers.google.com/static/crawling/ipranges/user-triggered-fetchers-google.json',
     'prefixes', 50),
    ('bingbot', 'Microsoft', 'search', 'bingbot',
     'https://www.bing.com/toolbox/bingbot.json', 'prefixes', 5),
    ('applebot', 'Apple', 'search', 'Applebot',
     'https://search.developer.apple.com/applebot.json', 'prefixes', 5),
    ('duckduckbot', 'DuckDuckGo', 'search', 'DuckDuckBot',
     'https://duckduckgo.com/duckduckbot.json', 'prefixes', 50),
    ('duckassistbot', 'DuckDuckGo', 'ai_user', 'DuckAssistBot',
     'https://duckduckgo.com/duckassistbot.json', 'prefixes', 1),
    ('gptbot', 'OpenAI', 'ai_training', 'GPTBot',
     'https://openai.com/gptbot.json', 'prefixes', 3),
    ('oai-searchbot', 'OpenAI', 'ai_search', 'OAI-SearchBot',
     'https://openai.com/searchbot.json', 'prefixes', 3),
    ('chatgpt-user', 'OpenAI', 'ai_user', 'ChatGPT-User',
     'https://openai.com/chatgpt-user.json', 'prefixes', 10),
    ('perplexitybot', 'Perplexity', 'ai_search', 'PerplexityBot',
     'https://www.perplexity.ai/perplexitybot.json', 'prefixes', 2),
    ('perplexity-user', 'Perplexity', 'ai_user', 'Perplexity-User',
     'https://www.perplexity.ai/perplexity-user.json', 'prefixes', 2),
    ('mistralai-user', 'Mistral AI', 'ai_user', 'MistralAI-User',
     'https://mistral.ai/mistralai-user-ips.json', 'prefixes', 1),
    ('ccbot', 'Common Crawl', 'ai_training', 'CCBot',
     'https://index.commoncrawl.org/ccbot.json', 'prefixes', 1),
    ('ahrefsbot', 'Ahrefs', 'seo', 'AhrefsBot, AhrefsSiteAudit',
     'https://api.ahrefs.com/v3/public/crawler-ip-ranges', 'prefixes', 10),
)

IPV4_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')


class CrawlerError(Exception):
    pass


def parse_prefixes(payload):
    """CIDRs from the common vendor format {"prefixes": [{"ipv4Prefix": ...} | {"ipv6Prefix": ...}]}."""
    if not isinstance(payload, dict) or not isinstance(payload.get('prefixes'), list):
        raise CrawlerError('unexpected answer: no "prefixes"')
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


def parse_html_ips(text):
    nets = set()
    for ip in IPV4_RE.findall(text or ''):
        try:
            nets.add(ipaddress.ip_network(ip + '/32'))
        except ValueError:
            pass
    return nets


def _sorted(nets):
    return [str(n) for n in sorted(nets, key=lambda n: (n.version, n.network_address, n.prefixlen))]


def fetch_crawler(crawler, session, now):
    name, operator, category, ua, url, fmt, min_ranges = crawler
    r = session.get(url, timeout=(5, 20), allow_redirects=True)
    r.raise_for_status()
    nets = parse_prefixes(r.json()) if fmt == 'prefixes' else parse_html_ips(r.text)
    if len(nets) < min_ranges:
        raise CrawlerError(f'{len(nets)} ranges, expected at least {min_ranges}')
    return {'name': name, 'operator': operator, 'category': category, 'ua': ua,
            'ranges': _sorted(nets), 'source': url, 'updated_at': now}


def collect(session=None, crawlers=CRAWLERS):
    session = session or requests.Session()
    session.headers.update({'User-Agent': 'Baskerville crawler-ranges (+https://equalit.ie)'})
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    out, failed = [], []
    for c in crawlers:
        try:
            out.append(fetch_crawler(c, session, now))
        except Exception as e:
            logger.error(f'{c[0]}: not published, keeping the last snapshot: {e}')
            failed.append(c[0])
    return out, failed


def publish(records, bootstraps, topic):
    from kafka import KafkaProducer
    for bootstrap in bootstraps:
        producer = KafkaProducer(bootstrap_servers=bootstrap, max_request_size=4 * 1024 * 1024)
        for rec in records:
            producer.send(topic, key=rec['name'].encode(), value=json.dumps(rec).encode())
        producer.flush()
        producer.close()
        logger.info(f'{len(records)} crawlers -> {topic} @ {bootstrap.split(",")[0]}')


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    topic = os.environ.get('CRAWLER_RANGES_TOPIC', 'verified_crawlers')
    raw = os.environ.get('CRAWLER_RANGES_BOOTSTRAPS') or ';'.join(
        b for b in (os.environ.get('BOOTSTRAP_SERVERS_OUTPUT'), os.environ.get('BOOTSTRAP_SERVERS')) if b)
    bootstraps = [b.strip() for b in raw.split(';') if b.strip()]
    records, failed = collect()
    for rec in records:
        logger.info(f"{rec['name']} ({rec['category']}): {len(rec['ranges'])} ranges")
    if os.environ.get('DRY_RUN', 'false').lower() == 'true':
        logger.info('DRY_RUN: nothing published')
    elif records:
        publish(records, bootstraps, topic)
    # fail the job when a source broke, so it shows up in failedJobsHistory
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
