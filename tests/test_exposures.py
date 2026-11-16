from baskervillehall import exposures as ex


def test_sensitive_paths():
    assert ex.sensitive_path('/.env')[0] == 'high'
    assert ex.sensitive_path('/.env.production')[0] == 'high'
    assert ex.sensitive_path('/wp-config.php.bak')[0] == 'high'
    assert ex.sensitive_path('/backup.sql')[0] == 'high'
    assert ex.sensitive_path('/site.tar.gz')[0] == 'high'
    assert ex.sensitive_path('/wp-content/debug.log')[0] == 'medium'
    for url in ('/', '/index.php', '/wp-content/uploads/a.zip', '/news/2026/report.zip', '/.env/x', None):
        assert ex.sensitive_path(url) is None


def test_exposure_needs_the_file_not_a_page():
    assert ex.is_exposure('/.env', 200, 'text/plain; charset=utf-8', 162)
    assert ex.is_exposure('/wp-content/debug.log', '200', 'application/octet-stream', 106429824.0)
    # the site's home page for any unknown path
    assert not ex.is_exposure('/.env', 200, 'text/html; charset=utf-8', 7622)
    assert not ex.is_exposure('/.env', 404, 'text/plain', 10)
    assert not ex.is_exposure('/.env', 200, 'text/plain', 0)
    assert not ex.is_exposure('/robots.txt', 200, 'text/plain', 100)


def test_server_status_is_apache_page():
    assert ex.is_exposure('/server-status', 200, 'text/html; charset=ISO-8859-1', 6967)
    assert not ex.is_exposure('/server-status', 200, 'text/html; charset=utf-8', 6967)


def test_status_strings_and_edge_ips_are_not_files():
    assert not ex.is_exposure('/.env', 200, 'text/plain', 2)              # "OK"
    assert not ex.is_exposure('/.env', 200, 'text/plain', 162, host='51.68.103.214')
    assert ex.is_exposure('/.env', 200, 'text/plain', 162, host='example.org')


def test_too_big_for_the_file_is_a_page():
    assert not ex.is_exposure('/.git/HEAD', 200, 'text/plain', 63310, host='example.org')
    assert not ex.is_exposure('/.env', 200, 'text/plain', 205722, host='example.org')
    assert not ex.is_exposure('/.DS_Store', 200, 'text/plain', 205722, host='example.org')
    assert ex.is_exposure('/.git/HEAD', 200, 'text/plain', 23, host='example.org')
    assert ex.is_exposure('/wp-content/debug.log', 200, 'application/octet-stream', 106429824, host='example.org')
