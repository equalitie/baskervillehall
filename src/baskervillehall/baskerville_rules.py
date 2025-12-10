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


def normalize_cipher(cipher):
    """
    Normalize cipher to cipher family for better generalization.

    Groups specific ciphers into families to avoid frequency encoding issues
    with rare but valid ciphers (e.g., HTTP/3 QUIC ciphers).

    Strategy: Group by encryption strength and algorithm, not by TLS version.
    This allows HTTP/3 AEAD ciphers to be grouped with TLS 1.3 equivalents.

    Args:
        cipher (str): Cipher suite name

    Returns:
        str: Normalized cipher family name
    """
    if not cipher:
        return 'none'

    cipher = str(cipher).upper()

    # GREASE values from Chrome (random hex values for extensibility)
    if cipher.startswith('0X'):
        return 'grease'

    # Weak/deprecated ciphers (check first!)
    if any(weak in cipher for weak in ['3DES', 'RC4', 'MD5', 'DES']):
        return 'weak'

    # Modern AES128-GCM (TLS 1.3 + HTTP/3 QUIC)
    # Group AEAD-AES128-GCM-* with TLS_AES_128_GCM_* and ECDHE-*-AES128-GCM-*
    if ('AES128' in cipher or 'AES_128' in cipher or 'AES-128' in cipher) and 'GCM' in cipher:
        return 'modern_aes128_gcm'

    # Modern AES256-GCM (TLS 1.3 + HTTP/3 QUIC)
    if ('AES256' in cipher or 'AES_256' in cipher or 'AES-256' in cipher) and 'GCM' in cipher:
        return 'modern_aes256_gcm'

    # ChaCha20 (TLS 1.3 + HTTP/3 QUIC + ECDHE)
    if 'CHACHA20' in cipher:
        return 'modern_chacha20'

    # Legacy ECDHE with CBC (older but still valid)
    if 'ECDHE' in cipher and 'CBC' in cipher:
        return 'ecdhe_cbc'

    # Legacy RSA (no PFS, weaker)
    if 'RSA' in cipher and 'ECDHE' not in cipher:
        return 'rsa_legacy'
    if 'AES' in cipher and 'ECDHE' not in cipher and not cipher.startswith('TLS_') and not cipher.startswith('AEAD-'):
        return 'rsa_legacy'

    return 'other'


def is_valid_browser_ciphers(ciphers):
    """
    Check whether the provided cipher list resembles that of a real browser.
    This function uses modern TLS 1.3 ciphers and widely used TLS 1.2 ciphers
    observed in Chrome, Firefox, and Safari.

    Note: Cloudflare may provide either a full cipher list from Client Hello,
    or just the negotiated cipher. We handle both cases.

    Args:
        ciphers (list of str or str): List of cipher suite names, or a single cipher string.

    Returns:
        bool: True if the cipher(s) look like they come from a browser.
    """
    if not ciphers:
        return False

    # Handle single cipher string (from Cloudflare tlsCipher field)
    if isinstance(ciphers, str):
        ciphers = [ciphers]

    if not isinstance(ciphers, (list, tuple)):
        return False

    # Common modern TLS 1.3 ciphers (including Cloudflare format)
    tls13_ciphers = {
        'TLS_AES_128_GCM_SHA256',
        'TLS_AES_256_GCM_SHA384',
        'TLS_CHACHA20_POLY1305_SHA256',
        'AEAD-AES128-GCM-SHA256',      # Cloudflare format for TLS 1.3
        'AEAD-AES256-GCM-SHA384',      # Cloudflare format for TLS 1.3
        'AEAD-CHACHA20-POLY1305-SHA256' # Cloudflare format for TLS 1.3
    }

    # Popular TLS 1.2 ciphers used by browsers
    popular_browser_ciphers = {
        'TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256',
        'TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384',
        'TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256',
        'TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384',
        'TLS_DHE_RSA_WITH_AES_128_GCM_SHA256',
        'TLS_DHE_RSA_WITH_AES_256_GCM_SHA384',
        'ECDHE-RSA-AES128-GCM-SHA256',  # Cloudflare format
        'ECDHE-RSA-AES256-GCM-SHA384',  # Cloudflare format
        'ECDHE-ECDSA-AES128-GCM-SHA256', # Cloudflare format
        'ECDHE-ECDSA-AES256-GCM-SHA384', # Cloudflare format
    }

    # Weak/old ciphers that should NOT be used by modern browsers
    weak_ciphers = {
        'RC4', 'MD5', 'DES', '3DES', 'NULL', 'EXPORT', 'anon'
    }

    # If we have a full cipher list (5+), check it thoroughly
    if len(ciphers) >= 5:
        # Must have at least one strong cipher
        if not any(c in tls13_ciphers or c in popular_browser_ciphers for c in ciphers):
            return False

        # Forward secrecy indicator: must have ECDHE or DHE
        if not any('ECDHE' in c or 'DHE' in c for c in ciphers):
            return False

        # Should not contain weak ciphers
        if any(any(weak in c for weak in weak_ciphers) for c in ciphers):
            return False

        return True

    # If we have a single negotiated cipher (Cloudflare case)
    # Just check if it's a modern, secure cipher
    cipher = ciphers[0]

    # Check if it's a known good cipher
    if cipher in tls13_ciphers or cipher in popular_browser_ciphers:
        return True

    # Check for weak cipher patterns
    if any(weak in cipher for weak in weak_ciphers):
        return False

    # For TLS 1.3 AEAD ciphers with GCM or CHACHA20
    if 'AEAD' in cipher and ('GCM' in cipher or 'CHACHA20' in cipher):
        return True

    # For ECDHE ciphers with modern encryption
    if 'ECDHE' in cipher and ('AES' in cipher or 'CHACHA20' in cipher):
        return True

    # Unknown single cipher - be conservative
    return False


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
        tuple: (bool, int) where:
            - bool: True if the session is considered human (score > 30), False otherwise
            - int: human_score from 1 to 99 (1 = automated, 99 = human, similar to Cloudflare bot score)
    """

    def dbg(message: str):
        """Emit debug output if verbose mode is enabled."""
        if verbose:
            if logger:
                logger.info(message)
            else:
                print(message)

    # Start with maximum human score (99 = definitely human, 1 = definitely bot)
    human_score = 99

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
        # Critical indicator - almost certainly a bot
        human_score = 1
        dbg(f"[SCORE] num_languages=0 → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 2. HTTP Protocol version check
    # ----------------------------------------------------------------------
    http_protocol = session.get('http_protocol', '')
    if http_protocol:
        # HTTP/1.0 is very suspicious - almost certainly a bot
        if http_protocol == 'HTTP/1.0':
            human_score = min(human_score, 5)
            dbg(f"[SCORE] http_protocol=HTTP/1.0 (very suspicious) → human_score={human_score}")
        # HTTP/1.1 with other bot indicators is suspicious
        # Modern browsers use HTTP/2 or HTTP/3
        elif http_protocol == 'HTTP/1.1':
            # Combine with UA score - if UA is also suspicious, likely a bot
            ua_score_val = session.get('ua_score', 0)
            if ua_score_val > 0.3:
                human_score = min(human_score, 10)
                dbg(f"[SCORE] http_protocol=HTTP/1.1 + ua_score={ua_score_val:.2f} (bot pattern) → human_score={human_score}")
            # Also check for known scrapers
            elif session.get('is_scraper', False):
                human_score = min(human_score, 10)
                dbg(f"[SCORE] http_protocol=HTTP/1.1 + is_scraper=True → human_score={human_score}")
            else:
                # Slight penalty for HTTP/1.1 even if no other indicators
                human_score -= 5
                dbg(f"[SCORE] http_protocol=HTTP/1.1 (suspicious) → human_score={human_score}")
    else:
        # No http_protocol data - log but don't penalize (backwards compatibility)
        dbg("[INFO] http_protocol not available in session data")

    # ----------------------------------------------------------------------
    # 3. UA score (0-1 range, higher = more suspicious)
    # ----------------------------------------------------------------------
    ua_score_val = session.get('ua_score', 0)
    if ua_score_val > 0.6:
        # Very high UA score - strong bot indicator
        human_score = min(human_score, 15)
        dbg(f"[SCORE] ua_score={ua_score_val} > 0.6 (strong bot indicator) → human_score={human_score}")
    elif ua_score_val > 0.3:
        # Moderate penalty
        penalty = int(ua_score_val * 40)
        human_score -= penalty
        dbg(f"[SCORE] ua_score={ua_score_val} → penalty={penalty} → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 4. Verified bot (legitimate crawlers like Googlebot, Bingbot)
    # ----------------------------------------------------------------------
    if session.get('verified_bot', False):
        # Legitimate bot, but still automated
        human_score = min(human_score, 15)
        dbg(f"[SCORE] verified_bot=True (legitimate crawler) → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 5. Primary session (bot behavior - no session cookies)
    # ----------------------------------------------------------------------
    if session.get('primary_session', False):
        human_score = min(human_score, 20)
        dbg(f"[SCORE] primary_session=True (no cookies, bot-like) → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 6. Scraper detection
    # ----------------------------------------------------------------------
    if session.get('is_scraper', is_scraper(session.get('ua', ''))):
        human_score = min(human_score, 10)
        dbg(f"[SCORE] scraper detected → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 7. Headless browser
    # ----------------------------------------------------------------------
    if session.get('headless_ua', False):
        human_score = min(human_score, 10)
        dbg(f"[SCORE] headless_ua=True (automation tool) → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 8. User-Agent based rules
    # ----------------------------------------------------------------------
    if session.get('bot_ua', False):
        human_score = min(human_score, 20)
        dbg(f"[SCORE] bot_ua=True → human_score={human_score}")

    if session.get('short_ua', False):
        human_score -= 20
        dbg(f"[SCORE] short_ua=True → human_score={human_score}")

    if session.get('ai_bot_ua', False):
        human_score = min(human_score, 8)
        dbg(f"[SCORE] ai_bot_ua=True (AI crawler) → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 9. Weak TLS cipher
    # ----------------------------------------------------------------------
    if session.get('weak_cipher', False):
        human_score -= 15
        dbg(f"[SCORE] weak_cipher=True (outdated/suspicious) → human_score={human_score}")

    # ----------------------------------------------------------------------
    # 10. AI crawler UA patterns
    # ----------------------------------------------------------------------
    if is_ai_bot_user_agent(session.get('ua', '')):
        human_score = min(human_score, 8)
        dbg(f"[SCORE] AI crawler UA pattern detected → human_score={human_score}")

    # ----------------------------------------------------------------------
    # Ensure score is in valid range [1, 99]
    # ----------------------------------------------------------------------
    human_score = max(1, min(99, human_score))

    # ----------------------------------------------------------------------
    # Determine if human based on threshold (>30 = human)
    # ----------------------------------------------------------------------
    is_human_result = human_score > 30

    if is_human_result:
        dbg(f"[HUMAN TRUE] human_score={human_score} > 30 (num_languages={num_languages})")
    else:
        dbg(f"[HUMAN FALSE] human_score={human_score} <= 30")

    return (is_human_result, human_score)


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
        r"perplexity",  # Perplexity AI
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