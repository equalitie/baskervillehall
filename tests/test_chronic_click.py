from datetime import datetime, timedelta

from baskervillehall.chronic_click import find_candidates

H = [datetime(2026, 11, 24, 9) + timedelta(hours=k) for k in range(3)]
BOT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36'
WIN = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36'


def host(h, total, hours=H):
    return [(h, hour, total, 0, 0, 0, 0) for hour in hours]


def ua(h, name, served, passed, challenged, hours=H):
    return [(h, hour, name, served, passed, challenged, 0, 0) for hour in hours]


def background(n=10):
    """Busy hosts where Windows Chrome is a third of the traffic and the bot UA is absent."""
    hosts, uas = [], []
    for i in range(n):
        hosts += host(f'site{i}.org', 10000)
        uas += ua(f'site{i}.org', WIN, 3000, 300, 30)
    return hosts, uas


def test_chronic_bot_passing_sha_is_found():
    hosts, uas = background()
    hosts += host('attacked.org', 70000)
    uas += ua('attacked.org', BOT, 1000, 50000, 500)
    found = find_candidates(hosts, uas, H)
    assert [(c['host'], c['ua']) for c in found] == [('attacked.org', BOT)]
    assert found[0]['typical_share'] == 0.0
    assert round(found[0]['share_min']) == 74


def test_common_browser_with_big_share_is_not_found():
    hosts, uas = background()
    hosts += host('news.org', 10000)
    uas += ua('news.org', WIN, 2000, 2000, 100)  # 40%, but Windows Chrome is ~33% everywhere
    assert find_candidates(hosts, uas, H) == []


def test_one_quiet_hour_breaks_the_chain():
    hosts, uas = background()
    hosts += host('attacked.org', 70000)
    uas += ua('attacked.org', BOT, 1000, 50000, 500, hours=H[:2])  # gone in the last hour
    assert find_candidates(hosts, uas, H) == []


def test_ua_that_does_not_pass_is_not_found():
    hosts, uas = background()
    hosts += host('attacked.org', 70000)
    uas += ua('attacked.org', BOT, 100, 50, 40000)  # stuck on the challenge: the challenge works
    assert find_candidates(hosts, uas, H) == []


def test_small_host_is_ignored():
    hosts, uas = background()
    hosts += host('tiny.org', 800)
    uas += ua('tiny.org', BOT, 0, 700, 10)
    assert find_candidates(hosts, uas, H) == []


def test_ua_filter_is_applied():
    hosts, uas = background()
    hosts += host('attacked.org', 70000)
    uas += ua('attacked.org', BOT, 1000, 50000, 500)
    assert find_candidates(hosts, uas, H, is_candidate_ua=lambda u: 'Linux' not in u) == []
