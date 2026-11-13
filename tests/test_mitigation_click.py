from baskervillehall import mitigation


def test_altcha_click_events():
    assert mitigation.altcha_click_event('ExpiringUAAltChaClickChallenge') == 'shown'
    assert mitigation.altcha_click_event('ExpiringAltChaClickChallenge') == 'shown'
    assert mitigation.altcha_click_event('SiteWideAltChaClickChallenge') == 'shown'
    assert mitigation.altcha_click_event('AltChaClickChallengePassed') == 'passed'
    for d in ('TooManyFailedAltChaClickChallenges', 'ExpiringAltChaChallenge', 'AltChaChallengePassed', '-', None):
        assert mitigation.altcha_click_event(d) is None


def test_click_pass_counts_as_passed():
    assert mitigation.classify('AltChaClickChallengePassed', 200) == 'passed'
    assert mitigation.classify('ExpiringUAAltChaClickChallenge', 429) == 'challenged'
