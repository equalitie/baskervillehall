import logging

from baskervillehall import incident_first_responder as fr

FB_IPHONE = ('Mozilla/5.0 (iPhone; CPU iPhone OS 27_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) '
             'Mobile/24B156 Safari/604.1 [FBAN/FBIOS;FBAV/587.0.0.31.71;FBBV/1110946418;FBDV/iPhone17,2;'
             'FBMD/iPhone;FBSN/iOS;FBSV/27.2.1;FBSS/3;FBID/phone;FBLC/es_LA;FBOP/5;FBRV/1119923411;IABMV/1]')
BOT = 'Go-http-client/2.0'


def _responder():
    r = object.__new__(fr.IncidentFirstResponder)
    r.logger = logging.getLogger('test')
    return r


def test_jetpack_and_wordpress_com_are_protected():
    assert fr.is_protected_ua('Jetpack by WordPress.com', 'example.org')
    assert fr.is_protected_ua('WordPress.com; https://wordpress.com', 'example.org')
    # a site's pingback flood from other WordPress sites stays unprotected
    assert not fr.is_protected_ua('WordPress/6.8; https://other.example', 'example.org')


def test_jetpack_target_is_dropped():
    rec = {'action': 'challenge_ua', 'target': ['Jetpack by WordPress.com', BOT], 'reasoning': ''}
    stats = [{'ua': 'Jetpack by WordPress.com', 'pct': 30.0}, {'ua': BOT, 'pct': 40.0}]
    out = _responder()._resolve_ua_targets(rec, stats, host='example.org')
    assert out['target'] == [BOT]


def test_in_app_browser_below_threshold_is_dropped():
    assert fr.is_in_app_ua(FB_IPHONE)
    rec = {'action': 'challenge_ua', 'target': [FB_IPHONE, BOT], 'reasoning': ''}
    stats = [{'ua': FB_IPHONE, 'pct': 7.7}, {'ua': BOT, 'pct': 40.0}]
    out = _responder()._resolve_ua_targets(rec, stats, host='example.org')
    assert out['target'] == [BOT]
    assert 'in-app browser' in out['reasoning']


def test_in_app_browser_dominating_the_attack_is_kept():
    rec = {'action': 'challenge_ua', 'target': [FB_IPHONE], 'reasoning': ''}
    stats = [{'ua': FB_IPHONE, 'pct': 60.0}]
    fps = [{'fingerprint': 'abc', 'pct': 90.0}]
    out = _responder()._resolve_ua_targets(rec, stats, host='example.org', fingerprint_stats=fps)
    assert out['action'] == 'challenge_ua_fingerprint'
    assert FB_IPHONE in out['target']


def test_ordinary_browser_is_not_in_app():
    assert not fr.is_in_app_ua('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                               '(KHTML, like Gecko) Chrome/158.0.0.0 Safari/537.36')


def test_ios_in_app_browsers_are_browsers():
    assert fr.is_browser_ua(FB_IPHONE)
    assert fr.is_browser_ua('Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 '
                            '(KHTML, like Gecko) Mobile/22B83 Instagram 356.0.0.30.83 (iPhone15,2; iOS 18_1)')
    assert not fr.is_browser_ua('Go-http-client/2.0')
