import pytest

from baskervillehall import block_guard, manual_command
from baskervillehall.manual_command import CommandError, build_command

BASE = dict(reason='test', operator='tester')


def test_challenge_ip():
    msg, warnings = build_command('challenge_ip', 'Example.org', value='1.2.3.4', ttl=600, **BASE)
    assert msg == {'Name': 'challenge_ip', 'source': 'manual [tester] test', 'meta': 'manual [tester] test',
                   'ttl': 600, 'Value': '1.2.3.4', 'host': 'example.org'}
    assert any('every site' in w for w in warnings)


def test_ttl_limit():
    with pytest.raises(CommandError):
        build_command('challenge_all', 'example.org', ttl=24 * 3600 + 1, **BASE)
    with pytest.raises(CommandError):
        build_command('challenge_all', 'example.org', **BASE)


def test_reason_required():
    with pytest.raises(CommandError):
        build_command('challenge_all', 'example.org', ttl=60, reason=' ', operator='tester')


def test_bad_values():
    with pytest.raises(CommandError):
        build_command('challenge_ip', 'example.org', value='1.2.3', ttl=60, **BASE)
    with pytest.raises(CommandError):
        build_command('challenge_subnet', 'example.org', value='1.0.0.0/8', ttl=60, **BASE)
    with pytest.raises(CommandError):
        build_command('challenge_ua', 'example.org', ua='', ttl=60, **BASE)
    with pytest.raises(CommandError):
        build_command('drop_table', 'example.org', ttl=60, **BASE)


def test_subnet_normalized():
    msg, _ = build_command('challenge_subnet', 'example.org', value='1.2.3.77/24', ttl=60, **BASE)
    assert msg['Value'] == '1.2.3.0/24'


def test_block_sent_as_challenge_when_disabled(monkeypatch):
    monkeypatch.setattr(block_guard, 'BLOCKS_DISABLED', True)
    msg, warnings = build_command('block_ip', 'example.org', value='1.2.3.4', ttl=60, **BASE)
    assert msg['Name'] == 'challenge_ip'
    assert any('sent as challenge_ip' in w for w in warnings)


def test_whitelisted_host(monkeypatch):
    monkeypatch.setattr(block_guard, 'HOST_WHITELIST', ('kavkaz-uzel.eu',))
    with pytest.raises(CommandError):
        build_command('challenge_all', 'www.kavkaz-uzel.eu', ttl=60, **BASE)


def test_clear_ip_has_no_host():
    # host in clear_rules would also clear the host's challenge_all
    msg, _ = build_command('clear_rules', 'example.org', value='1.2.3.4', **BASE)
    assert msg['Value'] == '1.2.3.4' and 'host' not in msg and 'ttl' not in msg


def test_clear_challenge_all():
    msg, _ = build_command('clear_rules', 'example.org', clear_host=True, **BASE)
    assert msg['host'] == 'example.org' and 'Value' not in msg


def test_clear_ua_on_host_warns():
    msg, warnings = build_command('clear_rules', 'example.org', ua='Mozilla/5.0 X', clear_host=True, **BASE)
    assert msg['ua'] == 'Mozilla/5.0 X' and msg['host'] == 'example.org'
    assert any('challenge_all' in w for w in warnings)


def test_clear_needs_target():
    with pytest.raises(CommandError):
        build_command('clear_rules', 'example.org', **BASE)
