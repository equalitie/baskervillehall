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


def test_jetpack_is_verified_by_automattic_asn_not_protected():
    jetpack = 'Jetpack by WordPress.com'
    assert not fr.is_protected_ua(jetpack, 'example.org')
    assert fr.is_verifiable_crawler_ua(jetpack)
    assert fr.is_verifiable_crawler_ua('WordPress.com; https://wordpress.com')
    assert fr.is_asn_verified_service(jetpack, 2635)
    assert fr.is_asn_verified_service(jetpack, '2635')
    assert not fr.is_asn_verified_service(jetpack, '16509')  # a fake from AWS
    # a site's pingback flood from other WordPress sites is not a verifiable service
    assert not fr.is_verifiable_crawler_ua('WordPress/6.8; https://other.example')


def test_archive_org_and_ai_crawlers_are_verifiable():
    assert fr.is_verifiable_crawler_ua('Mozilla/5.0 (compatible; archive.org_bot +http://archive.org/details/archive.org_bot)')
    assert fr.is_asn_verified_service('archive.org_bot', '7941')
    assert fr.is_verifiable_crawler_ua('Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; '
                                       'PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)')
    assert not fr.is_verifiable_crawler_ua(BOT)


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


def test_checkable_crawlers_are_verified_by_network():
    yandex = 'Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)'
    fb = 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'
    for ua, asn in ((yandex, '13238'), (fb, '32934')):
        assert not fr.is_protected_ua(ua, 'example.org')
        assert fr.is_verifiable_crawler_ua(ua)
        assert fr.is_asn_verified_service(ua, asn)
        assert not fr.is_asn_verified_service(ua, '14061')  # DigitalOcean
    assert not fr.is_asn_verified_service(yandex, '200350')  # Yandex Cloud is rentable
    # people using the Yandex search app are not crawlers
    assert not fr.is_verifiable_crawler_ua(
        'Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 '
        'YaApp_Android/24.1 YaSearchBrowser/24.1 BroPP/1.0 SA/3 Mobile Safari/537.36 YandexSearch/24.1')


def test_monitor_is_protected_only_while_small():
    ua = 'Mozilla/5.0+(compatible; UptimeRobot/2.0; http://www.uptimerobot.com/)'
    assert fr.is_protected_ua(ua, 'example.org')
    assert fr.is_protected_ua(ua, 'example.org', 0.5)
    assert not fr.is_protected_ua(ua, 'example.org', 40.0)
    rec = {'action': 'challenge_ua', 'target': [ua, BOT], 'reasoning': ''}
    small = _responder()._resolve_ua_targets(dict(rec), [{'ua': ua, 'pct': 1.0}, {'ua': BOT, 'pct': 60.0}],
                                             host='example.org')
    assert small['target'] == [BOT]
    big = _responder()._resolve_ua_targets(dict(rec, target=[ua]), [{'ua': ua, 'pct': 45.0}], host='example.org')
    assert big['target'] == [ua]
    assert 'not protected' in big['reasoning']
