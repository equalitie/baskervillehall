from baskervillehall import incident_first_responder as ifr

BOT_UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Safari/537.36'
READER_UA = 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/157.0.0.0 Mobile Safari/537.36'
GOOGLEBOT = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'


class FakeCursor:
    def __init__(self, results):
        self.results, self.last = results, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, args=None):
        self.last = 'ua' if 'host_ua_mitigation_minute' in sql else 'host'

    def fetchone(self):
        return (self.results['host'],)

    def fetchall(self):
        return self.results['ua']


class FakeConn:
    def __init__(self, results):
        self.results = results

    def cursor(self):
        return FakeCursor(self.results)


def _fr(bot_pct, normal):
    fr = object.__new__(ifr.IncidentFirstResponder)
    fr._get_ua_stats = lambda conn, incident_id: [{'ua': u, 'pct': p} for u, p in bot_pct.items()]
    fr._get_normal_ua_traffic = lambda conn, host: normal
    return fr


def test_attack_ua_passing_challenge_is_selected():
    conn = FakeConn({'host': 10000, 'ua': [
        (BOT_UA, 8000, 7000, 500),        # attack UA, passes the challenge
        (READER_UA, 1200, 900, 100),      # readers: pass too, but normal share and no bot commands
        (GOOGLEBOT, 500, 0, 0),
    ]})
    fr = _fr({BOT_UA: 90.0, READER_UA: 2.0}, {READER_UA: 10.0})
    got = fr._altcha_click_candidates(conn, {'id': 1, 'host': 'example.org'})
    assert [c['ua'] for c in got] == [BOT_UA]


def test_ua_that_does_not_pass_is_left_alone():
    conn = FakeConn({'host': 10000, 'ua': [(BOT_UA, 8000, 50, 7900)]})
    fr = _fr({BOT_UA: 90.0}, {})
    assert fr._altcha_click_candidates(conn, {'id': 1, 'host': 'example.org'}) == []


def test_ua_at_its_normal_share_is_left_alone():
    conn = FakeConn({'host': 10000, 'ua': [(BOT_UA, 3000, 2500, 100)]})
    fr = _fr({BOT_UA: 90.0}, {BOT_UA: 25.0})  # 30% now vs 25% normally
    assert fr._altcha_click_candidates(conn, {'id': 1, 'host': 'example.org'}) == []
