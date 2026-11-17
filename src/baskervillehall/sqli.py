# -*- coding: utf-8 -*-
"""Passive SQL injection check on the edge log: attempts, and signs that one worked.

We only see the URL and the query string (no POST bodies). Each query parameter (name and value,
URL-decoded) goes through libinjection (the SQL tokenizer behind ModSecurity CRS rule 942100: it
matches the token "fingerprint" of the value against known attacks, so obfuscation, comments and
odd characters don't hide it), then our regexes over the whole decoded URL, which also name the
kind of attack and give N of SLEEP(N). On 66k real query strings (Nov 2026) libinjection flagged 20,
all attacks; the regexes 18. Without the library (tests, local runs) only the regexes work.

An attempt alone says nothing about the site: every site gets them. Two signs that the database
ran the payload (the time-based one needs request_time, which the Kafka edge log lacks so far):

- time-based: the origin took about N seconds to answer a SLEEP(N) / pg_sleep(N) / WAITFOR DELAY
  payload. A slow site is slow for any N, so "likely" needs 2+ different N that each match;
- error-based: the origin answered 500 (the SQL error reached the app). Many apps answer 500 to any
  junk, so this is only a "suspect" for a person to check.

IPs that send SQLI_CHALLENGE_THRESHOLD attempts within SQLI_WINDOW_SECONDS get an IP challenge
(AltCha with BASKERVILLE_ALTCHA): sqlmap and the like run no JavaScript.
"""
import ctypes
import logging
import os
import re
from urllib.parse import parse_qsl, unquote_plus

from cachetools import TTLCache

CHALLENGE_ENABLED = os.environ.get('SQLI_CHALLENGE', 'true').lower() == 'true'
CHALLENGE_THRESHOLD = int(os.environ.get('SQLI_CHALLENGE_THRESHOLD', '3'))
WINDOW_SECONDS = int(os.environ.get('SQLI_WINDOW_SECONDS', '600'))
CHALLENGE_TTL = int(os.environ.get('SQLI_CHALLENGE_TTL', '3600'))

# Response time vs SLEEP(N): N - SLOW_MARGIN_BELOW <= time <= N + SLOW_MARGIN_ABOVE
SAMPLE_CHARS = 500
MIN_SLEEP = 3
SLOW_MARGIN_BELOW = 0.5
SLOW_MARGIN_ABOVE = 3.0

# SQL constructs, not words: a search for "select" or "sleep" is not an attempt.
_SIGNATURES = (
    ('union_select', re.compile(r'\bunion\b[\s(/*]+(all[\s(/*]+)?select\b')),
    ('time_based', re.compile(r'\b(sleep|pg_sleep)\s*\(\s*\d|\bwaitfor\s+delay\b|\bbenchmark\s*\(\s*\d')),
    ('information_schema', re.compile(r'\binformation_schema\b')),
    ('error_based', re.compile(r'\b(extractvalue|updatexml)\s*\(|\bfloor\s*\(\s*rand\s*\(')),
    ('boolean', re.compile(r"['\"]\s*\)?\s*(or|and)\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+|\b(or|and)\s+\d+\s*=\s*\d+\s*(--|#|/\*)")),
    ('select_from', re.compile(r"['\"]\s*\)*\s*;?\s*\(?\s*select\b.{1,200}\bfrom\b")),
    ('xp_cmdshell', re.compile(r'\bxp_cmdshell\b')),
)
_SLEEP_RE = re.compile(r"\b(?:sleep|pg_sleep)\s*\(\s*(\d{1,3})|\bwaitfor\s+delay\s+'\d{1,2}:\d{1,2}:(\d{1,2})'")


LIBINJECTION_PATH = os.environ.get('LIBINJECTION_PATH', '/usr/local/lib/libinjection.so')


def _load_libinjection():
    try:
        lib = ctypes.CDLL(LIBINJECTION_PATH)
    except OSError:
        logging.getLogger('sqli').warning(f'libinjection not found at {LIBINJECTION_PATH}: regexes only')
        return None
    lib.libinjection_sqli.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p]
    lib.libinjection_sqli.restype = ctypes.c_int
    return lib


_lib = _load_libinjection()


def libinjection_fingerprint(value):
    """libinjection's fingerprint when value is SQL injection, else None (also without the library)."""
    if _lib is None or not value or len(value) < 2:
        return None
    b = value.encode('utf-8', 'surrogateescape')
    fp = ctypes.create_string_buffer(8)
    if _lib.libinjection_sqli(b, len(b), fp) == 1:
        return fp.value.decode('ascii', 'replace')
    return None


def _decode(text):
    text = text or ''
    for _ in range(2):  # payloads are often encoded twice
        decoded = unquote_plus(text)
        if decoded == text:
            break
        text = decoded
    return text.lower().replace('/**/', ' ')


def detect(url, query):
    """Name of the signature found in url + query string ('libinjection:<fingerprint>' or one of
    _SIGNATURES), else None."""
    if not query and '%' not in (url or '') and "'" not in (url or ''):
        return None
    text = _decode(f'{url or ""}?{query or ""}')
    for name, rx in _SIGNATURES:
        if rx.search(text):
            return name
    if query:
        for k, v in parse_qsl(query, keep_blank_values=True):
            for part in (v, k):
                if '%' in part:
                    part = unquote_plus(part)
                fp = libinjection_fingerprint(part)
                if fp:
                    return f'libinjection:{fp}'
    return None


def sleep_seconds(url, query):
    """N of a SLEEP(N) / pg_sleep(N) / WAITFOR DELAY '0:0:N' payload, else None."""
    m = _SLEEP_RE.search(_decode(f'{url or ""}?{query or ""}'))
    if not m:
        return None
    return int(m.group(1) or m.group(2))


def sleep_matched(n, request_time):
    """The origin took about the N seconds the payload asked for."""
    if n is None or n < MIN_SLEEP or request_time is None:
        return False
    try:
        t = float(request_time)
    except (TypeError, ValueError):
        return False
    return n - SLOW_MARGIN_BELOW <= t <= n + SLOW_MARGIN_ABOVE


def level(code_500, sleep_values_matched):
    """'likely' (2+ different SLEEP(N) answered in ~N s), 'suspect' (one match or a 500), else ''."""
    if len(set(sleep_values_matched or ())) >= 2:
        return 'likely'
    if sleep_values_matched or code_500:
        return 'suspect'
    return ''


class SqliCounter:
    """Attempts per IP for the challenge, and per host+path for the daily report (sqli_daily)."""

    MAX_IPS = 20

    def __init__(self):
        self._ip_attempts = TTLCache(maxsize=200_000, ttl=WINDOW_SECONDS)
        self._challenged = TTLCache(maxsize=100_000, ttl=CHALLENGE_TTL)
        # (host, url) -> {'attempts', 'origin', 'code_500', 'max_time', 'sleep_matched': set, 'ips': set,
        #                 'first', 'last' (log time), 'sample' (first query string), 'signature', 'challenged'}
        self.counts = {}

    def add(self, host, url, query, ip, status, request_time, origin_answered, ts=None):
        """Count one request. Returns the signature name when it is an attempt, else None."""
        sig = detect(url, query)
        if not sig:
            return None
        c = self.counts.get((host, url))
        if c is None:
            c = self.counts[(host, url)] = {'attempts': 0, 'origin': 0, 'code_500': 0, 'max_time': 0.0,
                                             'sleep_matched': set(), 'ips': set(), 'first': ts, 'last': ts,
                                             'sample': (query or '')[:SAMPLE_CHARS], 'signature': sig,
                                             'challenged': 0}
        c['attempts'] += 1
        if ts is not None:
            c['first'] = ts if c['first'] is None else min(c['first'], ts)
            c['last'] = ts if c['last'] is None else max(c['last'], ts)
        if len(c['ips']) < self.MAX_IPS:
            c['ips'].add(ip)
        if origin_answered:
            c['origin'] += 1
            try:
                status = int(float(status or 0))
            except (TypeError, ValueError):
                status = 0
            if status == 500:
                c['code_500'] += 1
            try:
                t = float(request_time) if request_time is not None else None
            except (TypeError, ValueError):
                t = None
            if t is not None:
                c['max_time'] = max(c['max_time'], t)
                n = sleep_seconds(url, query)
                if sleep_matched(n, t):
                    c['sleep_matched'].add(n)
        self._ip_attempts[ip] = self._ip_attempts.get(ip, 0) + 1
        return sig

    def should_challenge(self, ip):
        """True once per CHALLENGE_TTL when the IP reached the threshold in the window."""
        if not CHALLENGE_ENABLED or ip in self._challenged:
            return False
        if self._ip_attempts.get(ip, 0) >= CHALLENGE_THRESHOLD:
            self._challenged[ip] = True
            return True
        return False

    def mark_challenged(self, host, url):
        c = self.counts.get((host, url))
        if c is not None:
            c['challenged'] += 1

    def snapshot(self):
        counts, self.counts = self.counts, {}
        return counts
