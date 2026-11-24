# -*- coding: utf-8 -*-
"""Chronic attacks: a browser UA that takes a large share of a site's traffic for hours and passes
the SHA challenge, with no incident open.

The First Responder acts on incidents, and an incident needs a spike over the baseline. A botnet
that runs at a steady rate for days becomes the baseline (Nov 2026: one Linux Chrome UA sent 1.5M of
a site's 2M requests a day, 99% of its addresses passed SHA, one 8-minute incident in three days).
The one-click AltCha stops it, but expires after 6 hours.

Input: hourly mitigation counts per host and per host+UA (host_mitigation_hour,
host_ua_mitigation_hour, written by deflect_alerts for every host). A UA is a candidate on a host
when, in each of the last hours, it sends at least min_ua_requests_per_hour and min_share % of the
host's traffic, it passes the challenge (passed >= min_passed_per_hour * hours and pass rate >=
min_pass_rate), and its share is at least min_typical_ratio times its typical share across Deflect
(median over busy hosts, 0 where it is not among the host's top UAs). The last condition keeps out
common browsers: Windows Chrome is 20-40% almost everywhere, a bot UA is near 0 on most sites.
Readers of a challenged site pass too, so all conditions are needed.
"""
import os
import statistics
from collections import defaultdict

MODE = os.environ.get('FIRST_RESPONDER_CHRONIC_CLICK', 'dry_run').lower()  # off | dry_run
HOURS = int(os.environ.get('FIRST_RESPONDER_CHRONIC_CLICK_HOURS', '3'))

PARAMS = {
    'min_host_requests_per_hour': 1000,  # hosts below this are not judged and not in the median
    'min_ua_requests_per_hour': 500,
    'min_share': 20.0,                   # % of the host's traffic, in every hour
    'min_typical_ratio': 5.0,            # share / median share of the UA over busy hosts
    'min_passed_per_hour': 100,
    'min_pass_rate': 0.5,                # passed / (passed + challenged)
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chronic_click_log (
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    window_start   TIMESTAMP   NOT NULL,
    window_end     TIMESTAMP   NOT NULL,
    host           TEXT        NOT NULL,
    ua             TEXT        NOT NULL,
    mode           TEXT        NOT NULL,
    skipped        TEXT,       -- why nothing would be sent (whitelisted, open incident, click active)
    requests       BIGINT,
    share_min      REAL,       -- lowest hourly share, %
    typical_share  REAL,       -- median share over busy hosts, %
    passed         BIGINT,
    challenged     BIGINT,
    PRIMARY KEY (window_end, host, ua)
);
"""


def _total(row):
    return sum(int(v or 0) for v in row)


def find_candidates(host_rows, ua_rows, hours, params=None, is_candidate_ua=lambda ua: True):
    """host_rows: (host, hour, served, passed, challenged, blocked, rate_limited).
    ua_rows: (host, hour, ua, served, passed, challenged, blocked, rate_limited).
    hours: the hours of the window (each must be present for a host to be judged).
    Returns dicts sorted by requests, largest first."""
    p = dict(PARAMS, **(params or {}))
    hours = list(hours)
    n = len(hours)

    host_hour = {(h, hour): _total(r) for h, hour, *r in host_rows}
    busy = {h for h, _ in host_hour
            if all(host_hour.get((h, hour), 0) >= p['min_host_requests_per_hour'] for hour in hours)}
    if not busy:
        return []
    host_total = {h: sum(host_hour[(h, hour)] for hour in hours) for h in busy}

    # per host+UA: hourly totals and summed passed/challenged
    ua_hour = defaultdict(dict)
    ua_pass = defaultdict(lambda: [0, 0])
    for h, hour, ua, served, passed, challenged, blocked, rate_limited in ua_rows:
        if h not in busy or hour not in hours:
            continue
        ua_hour[(h, ua)][hour] = _total((served, passed, challenged, blocked, rate_limited))
        ua_pass[(h, ua)][0] += int(passed or 0)
        ua_pass[(h, ua)][1] += int(challenged or 0)

    shares_by_ua = defaultdict(list)
    for (h, ua), per_hour in ua_hour.items():
        shares_by_ua[ua].append(100.0 * sum(per_hour.values()) / host_total[h])

    def typical(ua):
        s = shares_by_ua[ua] + [0.0] * (len(busy) - len(shares_by_ua[ua]))
        return statistics.median(s)

    out = []
    for (h, ua), per_hour in ua_hour.items():
        if not is_candidate_ua(ua):
            continue
        hourly_shares = []
        ok = True
        for hour in hours:
            c = per_hour.get(hour, 0)
            share = 100.0 * c / host_hour[(h, hour)]
            if c < p['min_ua_requests_per_hour'] or share < p['min_share']:
                ok = False
                break
            hourly_shares.append(share)
        if not ok:
            continue
        requests = sum(per_hour.values())
        share = 100.0 * requests / host_total[h]
        typ = typical(ua)
        if share < p['min_typical_ratio'] * typ:
            continue
        passed, challenged = ua_pass[(h, ua)]
        if passed < p['min_passed_per_hour'] * n or passed < p['min_pass_rate'] * (passed + challenged):
            continue
        out.append({'host': h, 'ua': ua, 'requests': requests, 'share_min': min(hourly_shares),
                    'typical_share': typ, 'passed': passed, 'challenged': challenged})
    return sorted(out, key=lambda c: -c['requests'])
