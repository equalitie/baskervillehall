# -*- coding: utf-8 -*-
"""Site dossiers, step 2 of BASKERVILLE_AGENT_DESIGN.md: one row per site that persists between days.

This job fills only what code can compute: `profile` (normal traffic, top countries/ASNs/UAs/URLs,
incident and First Responder history, the manual baseline from deflect_alerts'
INCIDENT_BASELINE_OVERRIDES, so nobody forgets it is there). `known_clients`, `campaigns`, `open_questions` and
`narrative` are for the LLM step and for people; a field listed in `human_fields` was set by a person and no job overwrites it.

Hosts are stored without "www." (incidents already are). A site gets a dossier when it had
DOSSIER_MIN_REQUESTS requests in the last 7 days or an incident in the last 30.

Site groups (sites of one client): group_id is the registrable domain (Public Suffix List, so
a.org.co and b.org.co stay apart) when 2+ dossier sites share it: eng.kavkaz-uzel.eu and
kavkaz-uzel.eu. Not across TLDs (fergana.ru and fergana.news stay apart) and not under the
DOSSIER_SHARED_ROOTS domains, whose subdomains belong to different clients. Not from origin IPs
either: one IP is often a shared server. Merging two clients is worse than having no group; the
Deflect dashboard may give us real owners later.

Versions: `version` goes up when the group changes or the last version is older than HISTORY_EVERY_DAYS; each version
is copied to site_dossier_history. The profile in site_dossier itself is refreshed on every run.
"""
import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import tldextract

from baskervillehall.block_guard import is_host_whitelisted

MIN_REQUESTS = int(os.environ.get('DOSSIER_MIN_REQUESTS', '1000'))
TRAFFIC_DAYS = 28  # host_hourly_traffic keeps ~10 days today; the profile says how many it saw
HISTORY_EVERY_DAYS = 7
TOP_N = 5
UA_CHARS = 200
# Subdomains of these are different clients: Deflect-hosted client sites, Hetzner reverse DNS
SHARED_ROOTS = frozenset(d.strip().lower() for d in os.environ.get(
    'DOSSIER_SHARED_ROOTS', 'deflect.network,your-server.de,digsec.org').split(',') if d.strip())
# The PSL snapshot bundled with tldextract: no download at run time
_tld = tldextract.TLDExtract(suffix_list_urls=(), include_psl_private_domains=True)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS site_dossier (
    host            text PRIMARY KEY,
    version         int  NOT NULL DEFAULT 1,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    updated_by      text NOT NULL,
    group_id        text,
    profile         jsonb NOT NULL DEFAULT '{}',
    known_clients   jsonb NOT NULL DEFAULT '[]',
    campaigns       jsonb NOT NULL DEFAULT '[]',
    open_questions  jsonb NOT NULL DEFAULT '[]',
    narrative       text  NOT NULL DEFAULT '',
    human_fields    text[] NOT NULL DEFAULT '{}',
    version_at      timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS site_dossier_history (LIKE site_dossier INCLUDING DEFAULTS,
                                                 PRIMARY KEY (host, version));
"""

_H = r"regexp_replace(lower(host), '^www\.', '')"

QUERIES = {
    'hosts': f"""
        SELECT h FROM (SELECT {_H} h, sum(count) s FROM host_hourly_traffic
                       WHERE epoch_hour >= %(since_7d_h)s GROUP BY 1) t WHERE s >= %(min_req)s
        UNION
        SELECT DISTINCT {_H} FROM incidents WHERE started_at > now() - interval '30 days'""",
    'traffic': f"""
        WITH hh AS (SELECT {_H} h, epoch_hour, sum(count) c FROM host_hourly_traffic
                    WHERE epoch_hour >= %(since_h)s GROUP BY 1, 2)
        SELECT h, count(*) hours, min(epoch_hour) first_hour, sum(c) total,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY c) p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY c) p95, max(c) max
        FROM hh GROUP BY h""",
    'hour_of_day': f"""
        WITH hh AS (SELECT {_H} h, epoch_hour, sum(count) c FROM host_hourly_traffic
                    WHERE epoch_hour >= %(since_h)s GROUP BY 1, 2)
        SELECT h, epoch_hour %% 24 hod, avg(c) avg FROM hh GROUP BY 1, 2""",
    'incidents': f"""
        SELECT {_H} h,
               count(*) FILTER (WHERE started_at > now() - interval '7 days') n_7d,
               count(*) FILTER (WHERE started_at > now() - interval '30 days') n_30d,
               count(*) n_90d,
               count(DISTINCT started_at::date) FILTER (WHERE started_at > now() - interval '30 days') days_30d,
               max(spike_ratio) FILTER (WHERE started_at > now() - interval '30 days') max_spike_30d,
               max(traffic_peak_count) FILTER (WHERE started_at > now() - interval '30 days') max_peak_req_min_30d,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY baseline_avg) baseline_req_min_p50,
               min(started_at) first_90d, max(started_at) last
        FROM incidents WHERE started_at > now() - interval '90 days' GROUP BY 1""",
    'actions': f"""
        SELECT {_H} h, action, count(*) n FROM first_responder_actions
        WHERE created_at > now() - interval '30 days' GROUP BY 1, 2""",
    'exposures': f"""
        SELECT {_H} h, count(DISTINCT url) n FROM exposures_daily
        WHERE day > current_date - 7 GROUP BY 1""",
    'sqli': f"""
        SELECT {_H} h, sum(attempts) n FROM sqli_daily WHERE day > current_date - 7 GROUP BY 1""",
    'dnet': f"SELECT {_H} h, dnet FROM host_dnet",
}
# Snapshots (percentages) kept by the stats pipeline: top TOP_N per host
for _name, _col in (('countries', 'country'), ('asns', 'asn_name'), ('uas', 'ua'), ('urls', 'url')):
    _table = {'countries': 'host_country_stats', 'asns': 'host_asn_stats',
              'uas': 'host_ua_stats', 'urls': 'host_url_stats'}[_name]
    QUERIES[_name] = f"""
        SELECT h, k, pct FROM (
            SELECT {_H} h, left({_col}, {UA_CHARS}) k, pct,
                   row_number() OVER (PARTITION BY {_H} ORDER BY pct DESC) rn
            FROM {_table}) t
        WHERE rn <= {TOP_N}"""

# Tables other pipelines create on first use
_OPTIONAL = ('exposures', 'sqli', 'dnet', 'countries', 'asns', 'uas', 'urls')


def normalize_host(host):
    host = (host or '').strip().lower()
    return host[4:] if host.startswith('www.') else host


def root_domain(host):
    """Registrable domain of host ('kavkaz-uzel.eu' for amp.eng.kavkaz-uzel.eu), or None."""
    return _tld(normalize_host(host)).top_domain_under_public_suffix or None


def group_ids(hosts):
    """{host: group_id} for hosts whose registrable domain at least one other host shares."""
    by_root = {}
    for h in hosts:
        r = root_domain(h)
        if r and r not in SHARED_ROOTS:
            by_root.setdefault(r, []).append(h)
    return {h: r for r, hs in by_root.items() if len(hs) > 1 for h in hs}


def baseline_overrides(value=None):
    """{host: baseline} from INCIDENT_BASELINE_OVERRIDES ("v2026.org:500,tapol.org:1100"), parsed as
    deflect_alerts parses it."""
    value = os.environ.get('INCIDENT_BASELINE_OVERRIDES', '') if value is None else value
    out = {}
    for part in value.split(','):
        if ':' not in part:
            continue
        k, v = part.strip().split(':', 1)
        try:
            out[normalize_host(k)] = float(v)
        except ValueError:
            continue
    return out


def _num(v):
    return round(float(v), 2) if v is not None else None


def build_profiles(hosts, rows, now=None, overrides=None):
    """Profile dict per host from the QUERIES results (rows: name -> list of dicts)."""
    now = now or datetime.now(timezone.utc)
    by = {name: {} for name in rows}
    for name, lst in rows.items():
        for r in lst:
            by[name].setdefault(r['h'], []).append(r)

    profiles = {}
    for h in hosts:
        p = {'computed_at': now.isoformat(timespec='seconds')}
        t = (by.get('traffic', {}).get(h) or [None])[0]
        if t:
            hod = sorted(by['hour_of_day'].get(h, []), key=lambda r: r['avg'])
            p['traffic'] = {
                'days_seen': round(t['hours'] / 24, 1),
                'since': datetime.fromtimestamp(t['first_hour'] * 3600, timezone.utc).date().isoformat(),
                'req_per_hour_p50': _num(t['p50']), 'req_per_hour_p95': _num(t['p95']),
                'req_per_hour_max': int(t['max']), 'total': int(t['total']),
                'peak_hour_utc': int(hod[-1]['hod']) if hod else None,
                'quiet_hour_utc': int(hod[0]['hod']) if hod else None,
            }
        for name in ('countries', 'asns', 'uas', 'urls'):
            lst = sorted(by.get(name, {}).get(h, []), key=lambda r: -float(r['pct'] or 0))[:TOP_N]
            if lst:
                p['top_' + name] = [{'key': r['k'], 'pct': _num(r['pct'])} for r in lst]
        i = (by.get('incidents', {}).get(h) or [None])[0]
        if i:
            p['incidents'] = {
                'n_7d': i['n_7d'], 'n_30d': i['n_30d'], 'n_90d': i['n_90d'],
                'days_with_incidents_30d': i['days_30d'],
                'max_spike_30d': _num(i['max_spike_30d']),
                'max_peak_req_min_30d': i['max_peak_req_min_30d'],
                'baseline_req_min_p50': _num(i['baseline_req_min_p50']),
                'first_in_90d': i['first_90d'].isoformat(timespec='seconds') if i['first_90d'] else None,
                'last': i['last'].isoformat(timespec='seconds') if i['last'] else None,
            }
        acts = by.get('actions', {}).get(h)
        if acts:
            p['fr_actions_30d'] = {r['action']: r['n'] for r in sorted(acts, key=lambda r: -r['n'])}
        for name in ('exposures', 'sqli'):
            r = (by.get(name, {}).get(h) or [None])[0]
            if r and r['n']:
                p[name + '_7d'] = int(r['n'])
        dn = sorted({r['dnet'] for r in by.get('dnet', {}).get(h, []) if r['dnet']})
        if dn:
            p['dnet'] = dn
        if h in (overrides or {}):
            p['manual_baseline'] = overrides[h]
        p['opted_out_or_whitelisted'] = is_host_whitelisted(h)
        profiles[h] = p
    return profiles


class SiteDossier:
    def __init__(self, postgres_connection, logger=None):
        self.pg = postgres_connection
        self.logger = logger or logging.getLogger(__name__)

    def _query(self, conn, params):
        rows = {}
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")
        for name, sql in QUERIES.items():
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    cols = [c[0] for c in cur.description]
                    rows[name] = [dict(zip(cols, r)) for r in cur.fetchall()]
            except Exception as e:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = '300s'")
                if name in _OPTIONAL and 'does not exist' in str(e):
                    rows[name] = []
                    continue
                raise
        return rows

    def run(self):
        now = datetime.now(timezone.utc)
        hour = int(now.timestamp() // 3600)
        params = {'since_h': hour - TRAFFIC_DAYS * 24, 'since_7d_h': hour - 7 * 24, 'min_req': MIN_REQUESTS}
        conn = psycopg2.connect(**self.pg)
        try:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()
            rows = self._query(conn, params)
            overrides = baseline_overrides()
            hosts = sorted({r['h'] for r in rows['hosts'] if r['h']} | set(overrides))
            profiles = build_profiles(hosts, rows, now, overrides)
            groups = group_ids(hosts)
            new, bumped = self._write_dossiers(conn, profiles, groups)
            conn.commit()
        finally:
            conn.close()
        self.logger.info(f'[DOSSIER] {len(profiles)} dossiers ({new} new, {bumped} new versions), '
                         f'{len(groups)} sites in {len(set(groups.values()))} groups, manual baselines: {overrides}')

    def _write_dossiers(self, conn, profiles, groups):
        new = bumped = 0
        with conn.cursor() as cur:
            cur.execute("SELECT host, group_id, version_at FROM site_dossier")
            current = {h: (gid, vat) for h, gid, vat in cur.fetchall()}
            now = datetime.now(timezone.utc)
            for h, p in profiles.items():
                gid = groups.get(h)
                if h not in current:
                    cur.execute("""INSERT INTO site_dossier (host, updated_by, group_id, profile)
                                   VALUES (%s, 'dossier_job', %s, %s)""", (h, gid, json.dumps(p)))
                    new += 1
                    snapshot = True
                else:
                    old_gid, vat = current[h]
                    snapshot = old_gid != gid or (now - vat).days >= HISTORY_EVERY_DAYS
                    cur.execute("""UPDATE site_dossier SET profile = %s, group_id = %s, updated_at = now(),
                                     updated_by = 'dossier_job',
                                     version = version + %s, version_at = CASE WHEN %s THEN now() ELSE version_at END
                                   WHERE host = %s""", (json.dumps(p), gid, int(snapshot), snapshot, h))
                    bumped += snapshot
                if snapshot:
                    cur.execute("""INSERT INTO site_dossier_history SELECT * FROM site_dossier WHERE host = %s
                                   ON CONFLICT DO NOTHING""", (h,))
        return new, bumped

