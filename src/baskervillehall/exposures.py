# -*- coding: utf-8 -*-
"""Passive exposure check: files that scanners ask for and the origin really serves.

Scanners probe every Deflect site for .env, .git, database dumps, debug logs and the like. We
never request anything ourselves: the session pipeline only looks at the edge log of those
requests and counts the ones the origin answered with the file (status 200, not an HTML page).
The daily report lists them for support (exposures_daily, written every 5 min).

False positives: many sites answer every unknown path with their home page (text/html, ignored
here) or with one catch-all response of another type (same size for every path). The report
drops a host whose findings of a day share one size across 3+ paths.
"""
import ipaddress
import re

HIGH, MEDIUM = 'high', 'medium'

# Smaller bodies are status strings ("OK"), not files: a real .env line, git config or log is longer.
MIN_BYTES = 20

# Upper bounds for small files: a bigger answer is a page or a catch-all, not the file
# (a real .git/HEAD is ~23 bytes; one site answers every path with the same 200 KB text).
_MAX_BYTES = (
    ('/.git/HEAD', 200),
    ('/.git/config', 10000),
    ('/.aws/credentials', 10000),
    ('/.htpasswd', 50000),
    ('/.DS_Store', 100000),
    ('/.env', 50000),  # also /.env.*
)

# exact paths -> (severity, what it is)
_EXACT = {
    '/.env': (HIGH, 'environment file (keys, passwords)'),
    '/.git/config': (HIGH, 'git repository config (the whole repo may be downloadable)'),
    '/.git/HEAD': (HIGH, 'git repository (the whole repo may be downloadable)'),
    '/.aws/credentials': (HIGH, 'AWS credentials'),
    '/.htpasswd': (HIGH, 'password hashes'),
    '/wp-content/debug.log': (MEDIUM, 'WordPress debug log (paths, errors, sometimes data)'),
    '/debug.log': (MEDIUM, 'debug log'),
    '/docker-compose.yml': (MEDIUM, 'service layout, sometimes passwords'),
    '/docker-compose.yaml': (MEDIUM, 'service layout, sometimes passwords'),
    '/Dockerfile': (MEDIUM, 'build file'),
    '/.svn/entries': (MEDIUM, 'svn repository'),
    '/.DS_Store': (MEDIUM, 'directory listing (macOS)'),
    '/web.config': (MEDIUM, 'IIS config'),
    '/server-status': (MEDIUM, 'Apache status page (visitor IPs and URLs)'),
}
# /.env.local, /.env.production, ...
_ENV_RE = re.compile(r'^/\.env\.[A-Za-z0-9_-]{1,20}$')
# config backups: /wp-config.php.bak, /config.php~, /wp-config.php.old ...
_CONFIG_BACKUP_RE = re.compile(r'^/(wp-)?config\.php(\.bak|\.old|\.save|\.orig|\.txt|~)$')
# dumps and archives in the site root: /backup.sql, /db.sql.gz, /site.zip, /backup.tar.gz
_DUMP_RE = re.compile(r'^/[A-Za-z0-9_.-]{1,40}\.(sql|sql\.gz|sql\.zip|tar\.gz|tgz|zip)$')


def sensitive_path(url):
    """(severity, description) when url is a path scanners ask for to find leaked files, else None."""
    url = url or ''
    if url in _EXACT:
        return _EXACT[url]
    if _ENV_RE.match(url):
        return HIGH, 'environment file (keys, passwords)'
    if _CONFIG_BACKUP_RE.match(url):
        return HIGH, 'config backup (database password)'
    if _DUMP_RE.match(url):
        return HIGH, 'database dump or site archive'
    return None


def _is_ip(host):
    try:
        ipaddress.ip_address((host or '').split(':')[0])
        return True
    except ValueError:
        return False


def is_exposure(url, status, content_type, length, host=None):
    """True when the origin served the sensitive file itself: 200, at least MIN_BYTES, and not an
    HTML page (Apache's server-status is HTML in ISO-8859-1, unlike the sites' own pages).
    Requests to an edge by IP instead of a site name don't count."""
    if not sensitive_path(url) or _is_ip(host):
        return False
    try:
        if int(status or 0) != 200 or int(float(length or 0)) < MIN_BYTES:
            return False
    except (TypeError, ValueError):
        return False
    for prefix, limit in _MAX_BYTES:
        if url == prefix or (prefix == '/.env' and url.startswith('/.env.')):
            if int(float(length)) > limit:
                return False
    ct = (content_type or '').lower()
    if url == '/server-status':
        return ct.startswith('text/html') and 'iso-8859-1' in ct
    return not ct.startswith('text/html')
