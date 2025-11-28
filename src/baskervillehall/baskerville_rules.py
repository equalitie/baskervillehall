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
        r'googlebot', r'bingbot', r'baiduspider', r'yandexbot', r'duckduckbot',
        r'sogou', r'exabot', r'seznambot', r'petalbot', r'applebot',
        r'facebookexternalhit', r'facebookcatalog', r'twitterbot', r'linkedinbot',
        r'pinterestbot', r'whatsapp', r'telegrambot', r'slackbot', r'discordbot',
        r'ahrefsbot', r'semrushbot', r'mj12bot', r'dotbot', r'uptimerobot',
        r'structured-data'
        'curl', 'wget', 'python-requests', 'aiohttp', 'urllib', 'httpie',
        'go-http-client', 'okhttp', 'java', 'libcurl', 'node-fetch',
        'axios', 'postmanruntime', 'insomnia', 'restsharp', 'powershell',

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
        'bot', 'spider', 'crawl', 'slurp',
        'googlebot', 'bingbot', 'baiduspider', 'yandexbot', 'duckduckbot',
        'sogou', 'exabot', 'seznambot', 'petalbot', 'applebot',
        'facebookexternalhit', 'facebookcatalog', 'twitterbot', 'linkedinbot',
        'pinterestbot', 'whatsapp', 'telegrambot', 'slackbot', 'discordbot',
        'ahrefsbot', 'semrushbot', 'mj12bot', 'dotbot', 'uptimerobot',
        'structured-data'
        'curl', 'wget', 'python-requests', 'aiohttp', 'urllib', 'httpie',
        'go-http-client', 'okhttp', 'java', 'libcurl', 'node-fetch',
        'axios', 'postmanruntime', 'insomnia', 'restsharp', 'powershell',
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


def is_human(session, verbose: bool = False, logger=None):
    """
    Rule-based human detection with optional debugging.

    Parameters:
        session (dict): Parsed session object.
        verbose (bool): If True, logs detailed rule evaluation.
        logger: Optional logger. If None, messages are printed.

    Returns:
        bool: True if the session is considered human, False otherwise.
    """

    def dbg(message: str):
        """Emit debug output if verbose mode is enabled."""
        if verbose:
            if logger:
                logger.info(message)
            else:
                print(message)

    # ----------------------------------------------------------------------
    # 1. Language detection (critical rule)
    # ----------------------------------------------------------------------
    # Start with existing field if available.
    num_languages = session.get("num_languages", 0)

    # If num_languages is zero, attempt to recover Accept-Language
    # from multiple possible locations inside the session object.
    if num_languages == 0:
        raw_accept_lang = (
            session.get("accept_language") or
            session.get("Accept-Language")
        )

        if not raw_accept_lang:
            headers = session.get("headers", {})
            headers_lower = {k.lower(): v for k, v in headers.items()}
            raw_accept_lang = headers_lower.get("accept-language")

        if not raw_accept_lang and session.get("requests"):
            req0 = session["requests"][0]

            raw_accept_lang = (
                req0.get("accept_language") or
                req0.get("Accept-Language")
            )

            if not raw_accept_lang:
                h = req0.get("headers", {})
                h_lower = {k.lower(): v for k, v in h.items()}
                raw_accept_lang = h_lower.get("accept-language")

        # If we found Accept-Language, compute num_languages
        if raw_accept_lang:
            num_languages = count_accepted_languages(raw_accept_lang)
            dbg(f"[is_human] Recomputed num_languages={num_languages} from Accept-Language={raw_accept_lang!r}")
        else:
            dbg("[is_human] No Accept-Language found; num_languages remains 0")

    if num_languages == 0:
        dbg("[HUMAN FALSE] num_languages=0")
        return False

    # ----------------------------------------------------------------------
    # 2. HTTP Protocol version check (NEW!)
    # ----------------------------------------------------------------------
    http_protocol = session.get('http_protocol', '')
    if http_protocol:
        # HTTP/1.0 is very suspicious - almost certainly a bot
        if http_protocol == 'HTTP/1.0':
            dbg(f"[HUMAN FALSE] http_protocol=HTTP/1.0 (very suspicious)")
            return False
        # HTTP/1.1 with other bot indicators is suspicious
        # Modern browsers use HTTP/2 or HTTP/3
        elif http_protocol == 'HTTP/1.1':
            # Combine with UA score - if UA is also suspicious, likely a bot
            ua_score_val = session.get('ua_score', 0)
            if ua_score_val > 0.3:
                dbg(f"[HUMAN FALSE] http_protocol=HTTP/1.1 + ua_score={ua_score_val:.2f} (bot pattern)")
                return False
            # Also check for known scrapers
            if session.get('is_scraper', False):
                dbg(f"[HUMAN FALSE] http_protocol=HTTP/1.1 + is_scraper=True")
                return False
            # Log as warning but don't block yet (some legitimate old clients exist)
            dbg(f"[WARNING] http_protocol=HTTP/1.1 (suspicious for modern browser)")
    else:
        # No http_protocol data - log but don't block (backwards compatibility)
        dbg("[INFO] http_protocol not available in session data")

    # ----------------------------------------------------------------------
    # 3. UA score
    # ----------------------------------------------------------------------
    if session['ua_score'] > 0.6:
        dbg(f"[HUMAN FALSE] ua_score={session['ua_score']} > 0.6")
        return False

    # ----------------------------------------------------------------------
    # 4. Verified bot
    # ----------------------------------------------------------------------
    if session['verified_bot']:
        dbg("[HUMAN FALSE] verified_bot=True")
        return False

    # ----------------------------------------------------------------------
    # 5. Primary session (bot behavior)
    # ----------------------------------------------------------------------
    if session['primary_session']:
        dbg("[HUMAN FALSE] primary_session=True")
        return False

    # ----------------------------------------------------------------------
    # 6. Scraper detection
    # ----------------------------------------------------------------------
    if session.get('is_scraper', is_scraper(session['ua'])):
        dbg("[HUMAN FALSE] scraper detected")
        return False

    # ----------------------------------------------------------------------
    # 7. Headless browser
    # ----------------------------------------------------------------------
    if session.get('headless_ua', False):
        dbg("[HUMAN FALSE] headless_ua=True")
        return False

    # ----------------------------------------------------------------------
    # 8. User-Agent based rules
    # ----------------------------------------------------------------------
    if session['bot_ua']:
        dbg("[HUMAN FALSE] bot_ua=True")
        return False

    if session['short_ua']:
        dbg("[HUMAN FALSE] short_ua=True")
        return False

    if session['ai_bot_ua']:
        dbg("[HUMAN FALSE] ai_bot_ua=True")
        return False

    # ----------------------------------------------------------------------
    # 9. Weak TLS cipher
    # ----------------------------------------------------------------------
    if session['weak_cipher']:
        dbg("[HUMAN FALSE] weak_cipher=True")
        return False

    # ----------------------------------------------------------------------
    # 10. AI crawler UA patterns
    # ----------------------------------------------------------------------
    if is_ai_bot_user_agent(session['ua']):
        dbg("[HUMAN FALSE] AI crawler UA pattern detected")
        return False

    # ----------------------------------------------------------------------
    # Passed all rules
    # ----------------------------------------------------------------------
    dbg(f"[HUMAN TRUE] Passed all checks (num_languages={num_languages})")
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