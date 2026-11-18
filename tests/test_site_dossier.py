from datetime import datetime, timezone

from baskervillehall.site_dossier import build_profiles, normalize_host


def test_normalize_host():
    assert normalize_host('WWW.Example.org ') == 'example.org'
    assert normalize_host('www2.example.org') == 'www2.example.org'


def test_profile_from_rows():
    now = datetime(2026, 11, 18, tzinfo=timezone.utc)
    rows = {
        'traffic': [{'h': 'a.org', 'hours': 48, 'first_hour': 491000, 'total': 4800,
                     'p50': 100, 'p95': 180.5, 'max': 900}],
        'hour_of_day': [{'h': 'a.org', 'hod': 3, 'avg': 10}, {'h': 'a.org', 'hod': 15, 'avg': 300}],
        'countries': [{'h': 'a.org', 'k': 'US', 'pct': 20}, {'h': 'a.org', 'k': 'UA', 'pct': 60}],
        'actions': [{'h': 'a.org', 'action': 'monitor_only', 'n': 2},
                    {'h': 'a.org', 'action': 'challenge_all', 'n': 5}],
        'sqli': [{'h': 'a.org', 'n': 7}],
    }
    p = build_profiles(['a.org', 'quiet.org'], rows, now)
    a = p['a.org']
    assert a['traffic']['days_seen'] == 2.0
    assert a['traffic']['peak_hour_utc'] == 15 and a['traffic']['quiet_hour_utc'] == 3
    assert [c['key'] for c in a['top_countries']] == ['UA', 'US']
    assert list(a['fr_actions_30d']) == ['challenge_all', 'monitor_only']
    assert a['sqli_7d'] == 7
    assert 'group' not in a
    assert 'traffic' not in p['quiet.org']

