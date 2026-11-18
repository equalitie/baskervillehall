from datetime import datetime, timezone

from baskervillehall.site_dossier import build_profiles, group_by_origin_ips, group_kind, normalize_host, site_name


def test_normalize_host():
    assert normalize_host('WWW.Example.org ') == 'example.org'
    assert normalize_host('www2.example.org') == 'www2.example.org'


def test_groups_join_through_shared_ips():
    g = group_by_origin_ips({
        'b.org': ['10.0.0.1'],
        'www.a.org': ['10.0.0.1', '10.0.0.2'],
        'c.org': '10.0.0.2',
        'alone.org': ['10.0.0.9'],
        'bad.org': None,
    })
    assert g['a.org'] == ('a.org', ['10.0.0.1', '10.0.0.2'], 3, 'client')
    assert g['b.org'][0] == g['c.org'][0] == 'a.org'
    assert g['alone.org'] == ('alone.org', ['10.0.0.9'], 1, 'client')
    assert 'bad.org' not in g


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
    p = build_profiles(['a.org', 'quiet.org'], rows, {'a.org': ('a.org', ['10.0.0.1'], 2, 'client'), 'quiet.org': ('h.org', ['10.0.0.5'], 300, 'shared_hosting')}, now)
    a = p['a.org']
    assert a['traffic']['days_seen'] == 2.0
    assert a['traffic']['peak_hour_utc'] == 15 and a['traffic']['quiet_hour_utc'] == 3
    assert [c['key'] for c in a['top_countries']] == ['UA', 'US']
    assert list(a['fr_actions_30d']) == ['challenge_all', 'monitor_only']
    assert a['sqli_7d'] == 7
    assert a['group'] == {'id': 'a.org', 'size': 2}
    assert 'traffic' not in p['quiet.org']
    assert p['quiet.org']['group'] == {'shared_hosting': True, 'sites_on_origin': 300}


def test_group_kind():
    assert site_name('de.fergana.news') == 'fergana' and site_name('7dniv.rv.ua') == '7dniv'
    fergana = [f'{l}.fergana.{t}' for l in ('ar', 'de', 'en', 'fr') for t in ('news', 'me', 'ru')]
    assert group_kind(fergana + ['other.org', 'x.com']) == 'client'
    assert group_kind([f'site{i}.org' for i in range(11)]) == 'shared_hosting'
    assert group_kind([f'site{i}.org' for i in range(10)]) == 'client'
