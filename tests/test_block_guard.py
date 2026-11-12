from baskervillehall import block_guard


class FakeReader:
    def __init__(self, data):
        self.data = data
        self.hosts = set()

    def get(self):
        return self.data, True


def _with_config(monkeypatch, data):
    monkeypatch.setenv('DEFLECT_CONFIG_URL', 'https://example.invalid/config_v3')
    monkeypatch.setattr(block_guard, '_opt_out_reader', FakeReader(data))
    monkeypatch.setattr(block_guard, 'HOST_WHITELIST', ())


def test_opted_out_host_is_whitelisted(monkeypatch):
    _with_config(monkeypatch, {'optout.example': {'allowlist_domain': True},
                               'normal.example': {'allowlist_domain': False}})
    assert block_guard.is_host_whitelisted('optout.example')
    assert block_guard.is_host_whitelisted('www.optout.example')
    assert not block_guard.is_host_whitelisted('normal.example')
    # banjax matches the exact host, so a subdomain is not opted out
    assert not block_guard.is_host_whitelisted('sub.optout.example')


def test_failed_refresh_keeps_previous_list(monkeypatch):
    _with_config(monkeypatch, {'optout.example': {'allowlist_domain': True}})
    assert block_guard.is_host_opted_out('optout.example')
    block_guard._opt_out_reader.get = lambda: (None, True)
    assert block_guard.is_host_opted_out('optout.example')


def test_no_config_url_means_no_opt_outs(monkeypatch):
    monkeypatch.delenv('DEFLECT_CONFIG_URL', raising=False)
    monkeypatch.setattr(block_guard, 'HOST_WHITELIST', ())
    assert not block_guard.is_host_whitelisted('optout.example')
