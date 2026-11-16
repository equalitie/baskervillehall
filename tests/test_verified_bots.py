from baskervillehall import verified_bots as vb


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeSession:
    def __init__(self, answers):
        self.answers, self.headers = answers, {}

    def get(self, url, **kw):
        a = self.answers[url]
        if isinstance(a, Exception):
            raise a
        return FakeResponse(a)


SOURCES = (
    ('OpenAI', 'training', ['GPTBot'], 'https://gpt', 1),
    ('OpenAI', 'assistance', ['ChatGPT-User'], 'https://chat', 1),
    ('Google', 'fetcher', ['FeedFetcher-Google'], 'https://f1', 1),
    ('Google', 'fetcher', [], 'https://f2', 1),
)
UA_ONLY = (('Anthropic', 'training', ['ClaudeBot']),)


def _session(**over):
    answers = {
        'https://gpt': {'prefixes': [{'ipv4Prefix': '20.171.206.0/24'}]},
        'https://chat': {'prefixes': [{'ipv4Prefix': '23.98.142.176/28'}, {'ipv6Prefix': '2a01:111::/32'}]},
        'https://f1': {'prefixes': [{'ipv4Prefix': '10.0.0.0/24'}]},
        'https://f2': {'prefixes': [{'ipv4Prefix': '10.0.1.0/24'}]},
    }
    answers.update(over)
    return FakeSession(answers)


def test_shape_per_operator_and_category():
    doc, failed = vb.build(_session(), sources=SOURCES, ua_only=UA_ONLY)
    assert failed == []
    oa = doc['operators']['OpenAI']
    assert oa['training']['ua_tokens'] == ['GPTBot'] and oa['training']['ip_ranges'] == ['20.171.206.0/24']
    assert oa['assistance']['ip_ranges'] == ['23.98.142.176/28', '2a01:111::/32']
    # two Google fetcher lists merge into one category
    assert doc['operators']['Google']['fetcher']['ip_ranges'] == ['10.0.0.0/24', '10.0.1.0/24']
    assert doc['operators']['Anthropic']['training']['ip_ranges'] is None


def test_failed_source_keeps_previous_ranges():
    prev, _ = vb.build(_session(), sources=SOURCES, ua_only=UA_ONLY)
    doc, failed = vb.build(_session(**{'https://gpt': RuntimeError('down')}), previous=prev,
                           sources=SOURCES, ua_only=UA_ONLY)
    assert failed == ['https://gpt']
    assert doc['operators']['OpenAI']['training'] == prev['operators']['OpenAI']['training']


def test_failed_source_without_previous_is_left_out():
    doc, _ = vb.build(_session(**{'https://gpt': RuntimeError('down')}), sources=SOURCES, ua_only=UA_ONLY)
    assert 'training' not in doc['operators']['OpenAI']


def test_match():
    doc, _ = vb.build(_session(), sources=SOURCES, ua_only=UA_ONLY)
    gpt = 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)'
    assert vb.match(doc, gpt, '20.171.206.5') == ('OpenAI', 'training', True)
    assert vb.match(doc, gpt, '1.2.3.4') == ('OpenAI', 'training', False)   # spoofer
    assert vb.match(doc, 'Mozilla/5.0 (compatible; ClaudeBot/1.0)', '1.2.3.4') == ('Anthropic', 'training', None)
    assert vb.match(doc, 'Mozilla/5.0 (Windows NT 10.0) Chrome/157.0', '1.2.3.4') is None
