import pytest

from baskervillehall import crawler_ranges_publisher as crp


class FakeResponse:
    def __init__(self, json_data=None, text=''):
        self._json, self.text = json_data, text

    def raise_for_status(self):
        pass

    def json(self):
        return self._json


class FakeSession:
    def __init__(self, responses):
        self.responses, self.headers = responses, {}

    def get(self, url, **kw):
        r = self.responses[url]
        if isinstance(r, Exception):
            raise r
        return r


def test_parse_prefixes_v4_v6_and_bad():
    nets = crp.parse_prefixes({'prefixes': [{'ipv4Prefix': '66.249.64.0/27'},
                                            {'ipv6Prefix': '2001:4860:4801:10::/64'},
                                            {'ipv4Prefix': 'garbage'}, 'x']})
    assert {str(n) for n in nets} == {'66.249.64.0/27', '2001:4860:4801:10::/64'}


def test_parse_prefixes_rejects_other_shapes():
    with pytest.raises(crp.CrawlerError):
        crp.parse_prefixes({'list': []})


def test_parse_html_ips():
    assert {str(n) for n in crp.parse_html_ips('<li>20.191.45.212</li> 999 <b>40.88.21.235</b>')} == \
        {'20.191.45.212/32', '40.88.21.235/32'}


def test_collect_skips_broken_and_too_small_sources():
    ok = ('okbot', 'Op', 'search', 'OkBot', 'https://ok', 'prefixes', 1)
    small = ('smallbot', 'Op', 'search', 'SmallBot', 'https://small', 'prefixes', 5)
    down = ('downbot', 'Op', 'search', 'DownBot', 'https://down', 'prefixes', 1)
    session = FakeSession({
        'https://ok': FakeResponse({'prefixes': [{'ipv4Prefix': '10.0.1.0/24'}, {'ipv4Prefix': '10.0.0.0/24'}]}),
        'https://small': FakeResponse({'prefixes': [{'ipv4Prefix': '10.1.0.0/24'}]}),
        'https://down': RuntimeError('timeout'),
    })
    records, failed = crp.collect(session, crawlers=(ok, small, down))
    assert [r['name'] for r in records] == ['okbot']
    assert records[0]['ranges'] == ['10.0.0.0/24', '10.0.1.0/24']
    assert records[0]['category'] == 'search' and records[0]['updated_at'].endswith('Z')
    assert failed == ['smallbot', 'downbot']


def test_crawler_names_unique():
    names = [c[0] for c in crp.CRAWLERS]
    assert len(names) == len(set(names))
