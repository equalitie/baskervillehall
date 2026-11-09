# -*- coding: utf-8 -*-
"""What banjax did with a request, from the nginx log fields banjax_decision and the status.

Traffic counts include requests answered with a challenge page, so a spike that continues
under challenge_all says nothing about whether the challenge works. These buckets do.
"""

PASSED_DECISIONS = frozenset({
    'ShaChallengePassed',
    'AltChaChallengePassed',
    'ShaChallengePassedByAltCha',
})

BUCKETS = ('served', 'passed', 'challenged', 'blocked', 'rate_limited')


def classify(banjax_decision, status):
    """Return one of BUCKETS.

    served        content served, no challenge involved (includes cache hits)
    passed        content served after the client passed a challenge (valid cookie)
    challenged    a challenge page was served instead of the content
    blocked       banjax block page or ban
    rate_limited  429 from a rate-limit rule
    """
    d = banjax_decision or ''
    if d in PASSED_DECISIONS:
        return 'passed'
    if 'Block' in d or d.startswith('TooManyFailed'):
        return 'blocked'
    if d.endswith('Exception'):
        return 'served'
    if 'Challenge' in d or d == 'PasswordProtectedPath':
        return 'challenged'
    try:
        code = int(status or 0)
    except (TypeError, ValueError):
        code = 0
    # A 403/429 with NoMention or *AccessGranted comes from the origin, not from banjax.
    if code == 429 and d in ('', '-'):
        # Before the deflect dev deploy of 2026-11-06 challenge pages were logged with '-'.
        return 'challenged'
    if code == 429 and d != 'NoMention' and 'AccessGranted' not in d:
        return 'rate_limited'
    return 'served'


def is_passed(banjax_decision):
    return (banjax_decision or '') in PASSED_DECISIONS
