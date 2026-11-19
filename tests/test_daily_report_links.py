from baskervillehall.daily_report import DailyReport, defang_links


def test_image_removed():
    out = defang_links('see ![x](https://evil.example/p.png?d=secret) here')
    assert 'evil' not in out and '(image removed)' in out


def test_foreign_link_defanged():
    out = defang_links('[click](https://evil.example/login) and http://bad.example/x?y=1')
    assert out == 'click (hxxps://evil[.]example/login) and hxxp://bad[.]example/x?y=1'


def test_own_links_kept():
    md = '[dash](https://dashboard.deflect.network/a) https://equalit.ie/x'
    assert defang_links(md) == md


def test_http_own_domain_defanged_and_lookalike():
    assert 'hxxp' in defang_links('http://equalit.ie/x')
    assert 'hxxps' in defang_links('https://equalit.ie.evil.example/x')


def test_html_has_no_foreign_href_or_img():
    md = '# R\n\n![a](https://evil.example/i.png)\n\n[b](https://evil.example/) `<img src=x>`'
    html = DailyReport.html('NORM', [{'level': 'WARNING', 'reason': 'see https://evil.example/r'}], md)
    assert '<img' not in html and 'href=' not in html and 'evil.example' not in html
