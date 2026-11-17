from baskervillehall import sqli

# Real payloads from deflect.log, Nov 17 2026
SQLMAP = ('amp=true%20AND%20%28SELECT%208538%20FROM%28SELECT%20COUNT%28*%29%2CCONCAT%28%27hbkgth%27%2C'
          '%28%28SELECT%20%28ELT%288538%3D8538%2C1%29%29%29%29%2C%27tbudjp%27%2CFLOOR%28RAND%280%29*2%29%29x'
          '%20FROM%20INFORMATION_SCHEMA.PLUGINS%20GROUP%20BY%20x%29a%29')
UNION = ('inline=1&UWQF=9025%20AND%201%3D1%20UNION%20ALL%20SELECT%201%2CNULL%2C%27%3Cscript%3Ealert%28%22XSS'
         '%22%29%3C%2Fscript%3E%27%2Ctable_name%20FROM%20information_schema.tables')
PMPRO = 'rest_route=/pmpro/v1/order&code=a%27%20OR%20(SELECT%201%20FROM%20(SELECT(SLEEP(7)))a)--%20-'
WPS = 'page=wps_pages_page&ID=0+AND+(SELECT+1+FROM+(SELECT(SLEEP(7)))test)&type=home'


def test_real_payloads():
    assert sqli.detect('/articles/354228', SQLMAP)
    assert sqli.detect('/index.php/atrjournal/article/download/709/532', UNION) == 'union_select'
    assert sqli.detect('/', PMPRO) == 'time_based'
    assert sqli.detect('/wp-admin/admin.php', WPS) == 'time_based'
    assert sqli.detect('/', "id=1' or '1'='1") == 'boolean'
    assert sqli.detect('/', 'id=1%2527%2520UNION%2520SELECT%2520password') == 'union_select'  # double-encoded


def test_ordinary_requests_are_not_attempts():
    for url, q in [('/search', 's=how+to+select+a+union+lawyer'), ('/search', 'q=sleep+well'),
                   ('/', ''), ('/blog/union-select-explained', ''), ('/search', 'q=select+from+menu'),
                   ('/', 'utm_source=newsletter&page=2'), ('/news', 'q=rock+and+roll+1=1')]:
        assert sqli.detect(url, q) is None, (url, q)


def test_sleep_seconds():
    assert sqli.sleep_seconds('/', PMPRO) == 7
    assert sqli.sleep_seconds('/', "id=1';waitfor delay '0:0:5'--") == 5
    assert sqli.sleep_seconds('/', UNION) is None


def test_level_needs_two_different_sleeps():
    assert sqli.level(0, []) == ''
    assert sqli.level(1, []) == 'suspect'
    assert sqli.level(0, [7]) == 'suspect'
    assert sqli.level(0, [7, 7]) == 'suspect'  # a slow site is slow for any N
    assert sqli.level(0, [5, 7]) == 'likely'


def test_counter_challenges_once_after_threshold():
    c = sqli.SqliCounter()
    ip = '37.67.207.79'
    for i in range(2):
        assert c.add('kavkaz-uzel.eu', '/articles/354228', SQLMAP, ip, 200, 0.2, True)
        assert not c.should_challenge(ip)
    c.add('kavkaz-uzel.eu', '/articles/354228', SQLMAP, ip, 200, 0.2, True)
    assert c.should_challenge(ip)
    c.add('kavkaz-uzel.eu', '/articles/354228', SQLMAP, ip, 200, 0.2, True)
    assert not c.should_challenge(ip)  # already sent
    assert c.add('kavkaz-uzel.eu', '/', 'page=2', ip, 200, 0.2, True) is None


def test_counter_sleep_match_only_for_origin_answers():
    c = sqli.SqliCounter()
    c.add('a.org', '/', PMPRO, '1.1.1.1', 200, 7.3, True)
    c.add('a.org', '/', PMPRO.replace('SLEEP(7)', 'SLEEP(5)'), '1.1.1.1', 200, 5.1, True)
    c.add('b.org', '/', PMPRO, '1.1.1.1', 429, 7.3, False)  # banjax answered, not the origin
    c.add('c.org', '/', PMPRO, '1.1.1.1', 500, 0.1, True)
    snap = c.snapshot()
    assert snap[('a.org', '/')]['sleep_matched'] == {5, 7}
    assert snap[('b.org', '/')]['sleep_matched'] == set() and snap[('b.org', '/')]['origin'] == 0
    assert snap[('c.org', '/')]['code_500'] == 1
    assert c.snapshot() == {}


import pytest

needs_lib = pytest.mark.skipif(sqli._lib is None, reason='libinjection not built (LIBINJECTION_PATH)')


@needs_lib
def test_libinjection_catches_obfuscation_regexes_miss():
    for q in ['id=1/*!50000UNION*/+/*!50000SELECT*/+1', "id=1%27%7C%7C(SELECT+CHAR(113)%7C%7CCHAR(120))",
              'id=1+AND+ASCII(SUBSTRING((SELECT+database()),1,1))>64', 'id=1;DROP+TABLE+users--',
              'id=1%20AnD%208%3d8', 'id=1+or+true--', "sb_category=%27)+AND+false--+-"]:
        assert (sqli.detect('/', q) or '').startswith('libinjection:'), q


@needs_lib
def test_libinjection_ordinary_values():
    for q in ['s=O%27Reilly', 'q=Hello+(world);+%22quoted%22+--+note', 'data=%7B%22a%22:1,%22b%22:[1,2]%7D',
              'h=e3b0c44298fc1c149afbf4c8996fb924', 'email=user@example.com', 'd=2026-11-17+10:00',
              'r=https://x.org/a?b=1%26c=2', 't=dGVzdA==', 'n=-1', 'q=1+or+2']:
        assert sqli.detect('/', q) is None, q


def test_counter_keeps_times_sample_signature_and_challenges():
    from datetime import datetime
    c = sqli.SqliCounter()
    t1, t2 = datetime(2026, 11, 17, 14, 0), datetime(2026, 11, 17, 14, 5)
    c.add('a.org', '/', PMPRO, '1.1.1.1', 200, None, True, t2)
    c.add('a.org', '/', UNION, '1.1.1.1', 200, None, True, t1)
    c.mark_challenged('a.org', '/')
    row = c.snapshot()[('a.org', '/')]
    assert (row['first'], row['last']) == (t1, t2)
    assert row['sample'] == PMPRO and row['signature'] == 'time_based'
    assert row['challenged'] == 1


@needs_lib
def test_dashes_in_a_value_are_not_a_comment_attack():
    assert sqli.detect('/project.shtml', 'apc=147581-----1&w=147581&s=190365') is None  # bankwatch.org


def test_triple_encoded():
    q = ('lang=%252527%29%2F**%2FAND%2F**%2F%28%252527ZQXoaJ%252527%3D%252527zQXOaj%252527AND%2F**%2F'
         'EXTRACTVALUE%283370%2CCONCAT%280x7e%2C%28%28SELECT%2F**%2F%28ELT%283370%3D3370%2C1%29%29%29%29%2C0x7e%29%29')
    assert sqli.detect('/hebergement-fsm-2026/', q)  # fsm2026benin.org, sqlmap


def test_oracle_dbms_pipe():
    q = 'at2=128)%3BSELECT%20DBMS_PIPE.RECEIVE_MESSAGE(CHR(85)%7C%7CCHR(70)%7C%7CCHR(71),7)%20FROM%20DUAL--'
    assert sqli.detect('/', q) == 'time_based'  # rote-hilfe.de, Nov 17
    assert sqli.sleep_seconds('/', q) == 7
