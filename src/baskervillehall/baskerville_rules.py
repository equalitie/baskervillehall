import re
from enum import Enum

# Define patterns for known scraping tools and bots
SCRAPER_PATTERNS = {
    "scrapy":           r"\bScrapy/\d+\.\d+(?:\.\d+)?",
    "python-requests":  r"\bpython-requests/\d+\.\d+(?:\.\d+)?",
    "urllib":           r"\burllib(?:3)?/\d+\.\d+(?:\.\d+)?",
    "aiohttp":          r"\baiohttp/\d+\.\d+(?:\.\d+)?",
    "httpx":            r"\bhttpx/\d+\.\d+(?:\.\d+)?",
    "curl":             r"\bcurl/\d+\.\d+(?:\.\d+)?",
    "wget":             r"\bwget/\d+\.\d+(?:\.\d+)?",
    "java":             r"\bJava/\d+\.\d+(?:\.\d+)?",
    "go-http-client":   r"\bGo-http-client/\d+\.\d+(?:\.\d+)?",
    "okhttp":           r"\bokhttp/\d+\.\d+(?:\.\d+)?",
    "lynx":             r"\bLynx/\d+\.\d+(?:\.\d+)?",
    "libwww-perl":      r"\blibwww-perl/\d+\.\d+(?:\.\d+)?",
    "mechanize":        r"\b(WWW::Mechanize|mechanize)",
    "apache-http":      r"\bApache-HttpClient/\d+\.\d+(?:\.\d+)?",
    "node-fetch":       r"\bnode-fetch",
    "axios":            r"\baxios/\d+\.\d+(?:\.\d+)?",
    "winhttp":          r"\bWinHttp",
    "wininet":          r"\bWinINet",
    "puppeteer":        r"\bPuppeteer",
    "playwright":       r"\bPlaywright",
    "headless-chrome":  r"\bHeadlessChrome",
}

# Compile all regexes once
compiled_patterns = {
    name: re.compile(pattern, re.IGNORECASE)
    for name, pattern in SCRAPER_PATTERNS.items()
}

class ModelType(Enum):
    HUMAN = 'human'
    BOT = 'bot'
    GENERIC = 'generic'


def count_accepted_languages(header: str) -> int:
    """
    Count distinct language codes in an Accept-Language HTTP header.
    Ignores q-values.
    """
    if not header:
        return 0

    languages = set()
    for lang_entry in header.split(","):
        lang_code = lang_entry.split(";")[0].strip().lower()
        if lang_code:
            languages.add(lang_code)
    return len(languages)


def is_asset_only_session(session):
    requests = session['requests']
    asset_mime_types = {'image/', 'text/css', 'application/javascript'}
    non_asset_types = {'text/html', 'application/json'}

    seen_asset = False
    for r in requests:
        ct = r.get('type', '')
        if any(ct.startswith(asset) for asset in asset_mime_types):
            seen_asset = True
        elif any(ct.startswith(nt) for nt in non_asset_types):
            return False  # this is a real content request

    return seen_asset


def is_weak_cipher(cipher):
    if 'RSA' in cipher and 'PFS' not in cipher:
        return True
    if 'CBC' in cipher or '3DES' in cipher or 'MD5' in cipher or 'RC4' in cipher:
        return True
    return False


def is_valid_browser_ciphers(ciphers):
    """
    Check whether the provided cipher list resembles that of a real browser.
    This function uses modern TLS 1.3 ciphers and widely used TLS 1.2 ciphers
    observed in Chrome, Firefox, and Safari.

    Args:
        ciphers (list of str): List of cipher suite names.

    Returns:
        bool: True if the cipher list looks like it comes from a browser.
    """
    if not isinstance(ciphers, (list, tuple)):
        return False

    if len(ciphers) < 5:
        return False  # Too few ciphers is a strong bot indicator

    # Common modern TLS 1.3 ciphers
    tls13_ciphers = {
        'TLS_AES_128_GCM_SHA256',
        'TLS_AES_256_GCM_SHA384',
        'TLS_CHACHA20_POLY1305_SHA256'
    }

    # Popular TLS 1.2 ciphers used by browsers
    popular_browser_ciphers = {
        'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256',
        'TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384',
        'TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256',
        'TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384',
        'TLS_DHE_RSA_WITH_AES_128_GCM_SHA256',
        'TLS_DHE_RSA_WITH_AES_256_GCM_SHA384',
    }

    # Must have at least one strong TLS 1.3 or TLS 1.2 browser cipher
    if not any(c in tls13_ciphers or c in popular_browser_ciphers for c in ciphers):
        return False

    # Forward secrecy indicator: must have ECDHE or DHE
    if not any('ECDHE' in c or 'DHE' in c for c in ciphers):
        return False

    return True


def is_short_user_agent(user_agent):
    return user_agent is None or len(user_agent.strip()) < 50


def is_bot_user_agent(user_agent):
    ua = user_agent
    if isinstance(user_agent, dict):
        ua = user_agent.get('name', '')
    ua = ua.lower()

    known_crawlers = [
        # Generic catch-all patterns (important!)
        r'bot', 
        r'spider',
        r'crawl',
        r'slurp',

        # Known legit crawlers
        r'googlebot',
        r'bingbot',
        r'baiduspider',
        r'yandexbot',
        r'duckduckbot',
        r'sogou',
        r'exabot',
        r'seznambot',
        r'petalbot',
        r'applebot',
        r'facebookexternalhit',
        r'facebookcatalog',
        r'twitterbot',
        r'linkedinbot',
        r'pinterestbot',
        r'whatsapp',
        r'telegrambot',
        r'slackbot',
        r'discordbot',
        r'ahrefsbot',
        r'semrushbot',
        r'mj12bot',
        r'dotbot',
        r'uptimerobot',
        r'structured-data',
    ]

    return any(re.search(pattern, ua) for pattern in known_crawlers)


def is_scraper(user_agent):
    return detect_scraper(user_agent) is not None


def detect_scraper(user_agent):
    """
    Returns the scraper/bot name if matched, or None if no known scraper is found.
    """
    for name, pattern in compiled_patterns.items():
        if pattern.search(user_agent):
            return name
    return None


def is_headless_ua(ua):
    headless_keywords = [
        "headless", "puppeteer", "playwright", "selenium", "phantomjs"
    ]
    return any(kw in ua for kw in headless_keywords)


def ua_score(user_agent: str) -> float:
    if not user_agent:
        return 1.0  # Empty UA is fully suspicious

    ua = user_agent.strip().lower()
    score = 0
    max_score = 7  # Total possible penalties

    # Very short UAs
    if len(ua) < 10:
        score += 3  # highly suspicious
    elif len(ua) < 30:
        score += 1  # somewhat suspicious

    # Known bot/tool indicators
    bot_keywords = [
        "curl", "wget", "python", "httpclient", "libwww", "perl", "okhttp",
        "aiohttp", "http.rb", "scrapy", "go-http-client", "java", "fetchlib",
        "phantomjs", "mechanize", "httprequest", "axios", "powershell"
    ]
    if any(kw in ua for kw in bot_keywords):
        score += 2

    if is_headless_ua(ua):
        score += 2

    # Missing browser identifiers
    browser_keywords = ["mozilla", "chrome", "safari", "firefox", "edge", "gecko", "applewebkit"]
    if not any(b in ua for b in browser_keywords):
        score += 1

    # Normalize to [0, 1]
    return min(score / max_score, 1.0)


def is_human(session):
    if session['ua_score'] > 0.6:
        return False
    # if session['datacenter_asn'] and not session['vpn']:
    #     return False
    if session['verified_bot']:
        return False
    if session['primary_session']:
        return False
    if session.get('is_scraper', is_scraper(session['ua'])):
        return False
    if session['num_languages'] == 0:
        return False
    if session.get('headless_ua', False):
        return False
    if session['bot_ua']:
        return False
    if session['short_ua']:
        return False
    if session['ai_bot_ua']:
        return False
    # if session['asset_only']:
    #     return False
    # if not session['valid_browser_ciphers']:
    #     return False
    if session['weak_cipher']:
        return False
    if is_ai_bot_user_agent(session['ua']):
        return False
    return True


def is_ai_bot_user_agent(user_agent: str) -> bool:
    """
    Returns True if the user-agent matches a known AI crawler or training-related bot.
    """
    if not user_agent:
        return False

    ua = user_agent.lower()

    known_ai_crawlers = [
        r"gptbot",  # OpenAI
        r"openai.*crawler",  # OpenAI legacy
        r"openai-httplib",  # Python OpenAI lib
        r"chatgpt",  # Any generic ChatGPT client

        r"anthropic",  # Claude / Anthropic
        r"claudebot",  # ClaudeBot

        r"google-extended",  # Google's opt-out agent
        r"ai crawler",  # Generic

        r"bytespider",  # ByteDance
        r"yisouspider",  # Baidu affiliate
        r"youdao",  # NetEase AI

        r"ccbot",  # Common Crawl (training source)
        r"petalbot",  # Huawei

        r"facebookbot",  # Facebook/Meta AI research
        r"facebot",  # Meta
        r"amazonbot",  # Amazon AI research
        r"yandexbot",  # Russia's search/LLM training
        r"cohere",  # Cohere.ai
        r"ai\scrawler",  # catch-all
        r"meta-externalagent",  # ← facebook training
    ]

    return any(re.search(pattern, ua) for pattern in known_ai_crawlers)


def is_bad_bot(session):
    if session['verified_bot']:
        return False

    if not session['primary_session']:
        return False

    if not session['bot_ua'] or not session['ai_bot_ua']:
        return True

    # a legit bot does not change its user agent
    if 'requests' in session:
        uas = set()
        for r in session['requests']:
            ua = r.get('ua', '')
            if len(ua) < 5:
                return True
            uas.add(ua)
        if len(uas) > 1:
            return True

    return False