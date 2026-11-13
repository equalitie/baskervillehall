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


def test_finalize_command_altcha_off(monkeypatch):
    monkeypatch.setattr(block_guard, 'ALTCHA_ENABLED', False)
    cmd = {'Name': 'challenge_ip', 'Value': '1.2.3.4', 'meta': 'x [challenge_ip]'}
    assert block_guard.finalize_command(dict(cmd)) == cmd


def test_finalize_command_altcha_on(monkeypatch):
    monkeypatch.setattr(block_guard, 'ALTCHA_ENABLED', True)
    monkeypatch.setattr(block_guard, 'ALTCHA_MAX_NUMBER', 1500)
    cmd = block_guard.finalize_command(
        {'Name': 'challenge_all', 'host': 'a.example', 'meta': 'first_responder [challenge_all] r',
         'source': 'first_responder'})
    assert cmd['Name'] == 'altcha_challenge_all'
    assert cmd['altcha_max_number'] == 1500
    assert cmd['meta'] == 'first_responder [altcha_challenge_all] r'
    # an explicit difficulty wins; blocks and clear_rules are left alone
    assert block_guard.finalize_command({'Name': 'challenge_ua', 'altcha_max_number': 20000})[
        'altcha_max_number'] == 20000
    for name in ('block_ip', 'block_ua', 'clear_rules', 'altcha_challenge_ip'):
        assert block_guard.finalize_command({'Name': name}) == {'Name': name}


def test_kill_switch_then_altcha(monkeypatch):
    monkeypatch.setattr(block_guard, 'BLOCKS_DISABLED', True)
    monkeypatch.setattr(block_guard, 'ALTCHA_ENABLED', True)
    cmd = {'Name': block_guard.guard_command_name('block_ip'), 'Value': '1.2.3.4'}
    assert block_guard.finalize_command(cmd)['Name'] == 'altcha_challenge_ip'
