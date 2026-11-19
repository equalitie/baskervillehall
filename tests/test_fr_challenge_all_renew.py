import logging
from datetime import datetime, timedelta, timezone

from baskervillehall import incident_first_responder as fr


class _Cur:
    def __init__(self, prev):
        self.prev = prev

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        pass

    def fetchone(self):
        return (self.prev,)


class _Conn:
    def __init__(self, prev):
        self.prev = prev

    def cursor(self):
        return _Cur(self.prev)


def _responder():
    r = object.__new__(fr.IncidentFirstResponder)
    r.logger = logging.getLogger('test')
    r._deferred_challenge_all = {}
    r.sent = []
    r._send_banjax_command = lambda conn=None, **kw: r.sent.append(kw)
    return r


def test_no_previous_sends_now():
    r = _responder()
    r._send_challenge_all(_Conn(None), 'example.org', 3600, 'x')
    assert len(r.sent) == 1 and not r._deferred_challenge_all


def test_renewal_right_after_expiry_waits():
    r = _responder()
    prev = datetime.now(timezone.utc) - timedelta(seconds=9)  # expired 9 s ago (tapol, Nov 16 05:53)
    r._send_challenge_all(_Conn(prev), 'example.org', 3600, 'x')
    assert r.sent == []
    send_at, kwargs = r._deferred_challenge_all['example.org']
    assert send_at == prev + timedelta(seconds=fr.CHALLENGE_ALL_RENEW_GAP_SECONDS)
    r._send_deferred_challenge_all(None)
    assert r.sent == []  # not yet
    r._deferred_challenge_all['example.org'] = (datetime.now(timezone.utc) - timedelta(seconds=1), kwargs)
    r._send_deferred_challenge_all(None)
    assert len(r.sent) == 1 and r.sent[0]['command_name'] == 'challenge_all'
    assert not r._deferred_challenge_all


def test_long_expired_sends_now():
    r = _responder()
    # the query only returns expiries within the gap; an older one comes back as NULL
    r._send_challenge_all(_Conn(None), 'example.org', 3600, 'x')
    assert len(r.sent) == 1
