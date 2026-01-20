import re
from enum import Enum

# Suspicious URL patterns typical for scanners/exploit attempts
SUSPICIOUS_PATH_PATTERNS = [
    r"/wp-admin/.*\.php$",
    r"/wp-admin/.*",
    r"/wp-includes/.*",
    r"/wp-content/.*",
    r"/xmlrpc\.php$",

    r"autoload_classmap\.php",
    r"/pwnd\.php",
    r"/pwnd/",
    r"/backups-dup-lite",
    r"/uploads/\d{4}/",
    r"/\.env",
    r"/config\.php",
]

compiled_path_patterns = [re.compile(p, re.IGNORECASE) for p in SUSPICIOUS_PATH_PATTERNS]

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

def compute_num_languages(session: dict, dbg=None) -> int:
    """
    Compute num_languages for a session, reusing Accept-Language extraction logic.

    - If session['num_languages'] is already set (including 0), returns it.
    - Otherwise tries to recover Accept-Language from various locations and count languages.
    - Uses optional dbg() callback if provided (for is_human verbose logging).
    """
    num_languages = session.get("num_languages", None)
    if num_languages is not None:
        return num_languages

    raw_accept_lang = (
        session.get("accept_language")
        or session.get("Accept-Language")
    )

    # Try top-level headers dict
    if not raw_accept_lang:
        headers = session.get("headers", {})
        headers_lower = {k.lower(): v for k, v in headers.items()}
        raw_accept_lang = headers_lower.get("accept-language")

    # Try first request headers if still not found
    if not raw_accept_lang and session.get("requests"):
        req0 = session["requests"][0]

        raw_accept_lang = (
            req0.get("accept_language")
            or req0.get("Accept-Language")
        )

        if not raw_accept_lang:
            h = req0.get("headers", {})
            h_lower = {k.lower(): v for k, v in h.items()}
            raw_accept_lang = h_lower.get("accept-language")

    if raw_accept_lang:
        num_languages = count_accepted_languages(raw_accept_lang)
        if dbg:
            dbg(f"[compute_num_languages] Recomputed num_languages={num_languages} from Accept-Language={raw_accept_lang!r}")
    else:
        if dbg:
            dbg("[compute_num_languages] No Accept-Language found; num_languages remains 0")

    return num_languages

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
        r'structured-data',
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
        'structured-data',
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

    if not session['bot_ua'] and not session['ai_bot_ua']:
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

def path_suspicion_score(url: str, status_code: int) -> float:
    """
    Returns a score between 0.0 and 1.0 representing how suspicious the URL looks.
    0.0 = not suspicious
    1.0 = highly suspicious (exploit/scan pattern)
    """
    if not url:
        return 0.0

    u = url.lower()
    score = 0.0

    # Check if this is a static resource (CSS, JS, images, fonts, etc.)
    static_extensions = ('.css', '.js', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp',
                         '.woff', '.woff2', '.ttf', '.eot', '.ico', '.mp4', '.webm', '.pdf')
    is_static = any(u.endswith(ext) for ext in static_extensions)

    # wp-content and wp-includes are suspicious ONLY if:
    # 1. They are .php files (exploit attempts)
    # 2. They return 404 (scanning for vulnerabilities)
    # 3. They are NOT static resources (legitimate site assets)
    is_wp_path = ('/wp-content/' in u or '/wp-includes/' in u)

    # Any .php request outside normal app flow is suspicious
    if ".php" in u:
        score += 0.3

    # Explicit known exploit/scan patterns
    for pat in compiled_path_patterns:
        if pat.search(u):
            # Don't penalize static resources from wp-content/wp-includes
            if is_wp_path and is_static:
                continue
            score += 0.5
            break

    # 404 on a suspicious path increases suspicion
    if status_code is not None and status_code == 404 and (".php" in u or "wp-" in u):
        score += 0.3

    return min(score, 1.0)


def cipher_suspicion_score(cipher_type: str) -> float:
    """
    Converts normalized cipher family into a suspicion score.
    0.0 = modern secure browser cipher
    1.0 = weak/legacy/bot-like cipher
    """
    if not cipher_type:
        return 0.3  # unknown -> slightly suspicious

    ct = cipher_type.lower()

    if ct in ("modern_aes128_gcm", "modern_aes256_gcm", "modern_chacha20"):
        return 0.0

    if ct == "ecdhe_cbc":
        return 0.2

    if ct == "rsa_legacy":
        return 0.6

    if ct == "weak":
        return 1.0

    # "none", "other", "grease" — mildly suspicious
    if ct in ("none", "other", "grease"):
        return 0.4

    return 0.4


def get_baskerville_score_1(session: dict) -> int:
    """
    Computes baskerville_score_1 for a full session,
    using ONLY the first request plus very cheap session-level signals.

    Output:
        1   = almost certainly a bot
        99  = almost certainly human
    """
    requests = session.get("requests", [])
    if not requests:
        return 50  # neutral fallback

    req = requests[0]  # use first request in the session

    # Extract core fields
    ua = req.get("ua", "") or req.get("user_agent", "")
    url = req.get("url", "") or req.get("path", "")
    status = req.get("code", None)

    # Suspicion based on UA (0..1 where 1 = suspicious)
    ua_susp = ua_score(ua)

    # Suspicion based on URL/path
    path_susp = path_suspicion_score(url, status_code=status)

    # Cipher suspicion (from session metadata)
    cipher_type = session.get("cipher_type")
    cipher = session.get("cipher")

    # If cipher_type missing but cipher exists, normalize it
    if cipher_type is None and cipher:
        cipher_type = normalize_cipher(cipher)

    cipher_susp = cipher_suspicion_score(cipher_type)

    # Language suspicion (reuse same logic as is_human, but without logging)
    num_languages = compute_num_languages(session, dbg=None)
    # 0 languages = highly suspicious, >=1 = not suspicious
    lang_susp = 1.0 if num_languages == 0 else 0.0

    # Weighted combination into bot suspicion raw score (0..1)
    # UA is most informative (curl, wget, python-requests), then language, path, cipher.
    # Increased ua_susp weight from 0.2 to 0.45 (45%) to better detect command-line tools
    # Increased lang_susp weight from 0.15 to 0.2 (20%) - missing Accept-Language is strong signal
    # Decreased path_susp weight from 0.5 to 0.2 (20%) - not all bots scan suspicious paths
    bot_susp_raw = (
        0.2 * path_susp +      # 20% - path patterns (scanners)
        0.45 * ua_susp +       # 45% - User-Agent (strongest signal for curl/wget/requests)
        0.15 * cipher_susp +   # 15% - cipher type
        0.2 * lang_susp        # 20% - Accept-Language presence
    )

    # Convert into baskerville human-like score (1..99)
    baskerville_score_1 = int(round(99 * (1.0 - bot_susp_raw)))
    baskerville_score_1 = max(1, min(99, baskerville_score_1))

    return baskerville_score_1

import math


def _entropy_from_counts(counts: dict[str, int]) -> float:
    """
    Same entropy formula used in FeatureExtractor.calculate_entropy().
    """
    total = sum(counts.values())
    if total == 0:
        return 0.0

    H = 0.0
    for c in counts.values():
        if c == 0:
            continue
        p = c / total
        H -= p * math.log(p, 2)
    return H


def get_baskerville_score_2(session: dict) -> int:
    """
    Computes baskerville_score_2 for a session with a small number of requests.

    Uses ONLY cheap, rule-based features:
      - aggregated path suspicion
      - UA suspicion
      - primary_session flag (no cookies)
      - cipher_type suspicion
      - entropy of URLs (important! bots have very low entropy)
      - num_languages (0 = suspicious)

    Output:
        1   = almost certainly a bot
        99  = almost certainly a human
    """
    requests = session.get("requests", [])
    if not requests:
        return 50

    # If session has only 1 request, score_0 should handle it.
    if len(requests) == 1:
        return 50

    # ----------------------------------------------------------------------
    # 1. Aggregate path suspicion across requests
    # ----------------------------------------------------------------------
    path_susps = []
    for r in requests:
        url = r.get("url", "") or r.get("path", "")
        status = r.get("code", None)
        path_susps.append(path_suspicion_score(url, status_code=status))

    avg_path_susp = sum(path_susps) / len(path_susps) if path_susps else 0.0

    # ----------------------------------------------------------------------
    # 2. UA suspicion (0..1)
    # ----------------------------------------------------------------------
    first_req = requests[0]
    ua = first_req.get("ua", "") or first_req.get("user_agent", "")
    ua_susp = ua_score(ua)

    # ----------------------------------------------------------------------
    # 3. Cipher suspicion
    # ----------------------------------------------------------------------
    cipher_type = session.get("cipher_type")
    cipher = session.get("cipher")
    if cipher_type is None and cipher:
        cipher_type = normalize_cipher(cipher)
    cipher_susp = cipher_suspicion_score(cipher_type)

    # ----------------------------------------------------------------------
    # 4. Primary session suspicion (no cookies)
    # ----------------------------------------------------------------------
    primary_susp = 1.0 if session.get("primary_session", False) else 0.0

    # ----------------------------------------------------------------------
    # 5. Language suspicion — IMPORTANT:
    #    Cloudflare gives only 0 or 1, never more.
    #    Therefore: 0 = highly suspicious (no Accept-Language).
    # ----------------------------------------------------------------------
    num_languages = session.get("num_languages", 0)
    lang_susp = 1.0 if num_languages == 0 else 0.0

    # ----------------------------------------------------------------------
    # 6. URL entropy — bots typically have extremely low entropy (<0.3).
    #    Humans click around → entropy >1.0.
    # ----------------------------------------------------------------------
    url_counts = {}
    for r in requests:
        u = r.get("url", "") or r.get("path", "")
        url_counts[u] = url_counts.get(u, 0) + 1

    entropy = _entropy_from_counts(url_counts)
    entropy_norm = min(entropy / 3.0, 1.0)  # normalize typical human range
    entropy_susp = 1.0 - entropy_norm       # low entropy = suspicious

    # ----------------------------------------------------------------------
    # 7. Weighted suspicion score (0..1)
    #
    # Tuned so that:
    #   ✔ scanners (/wp-admin + 404 + curl) → score < 30
    #   ✔ real users → score > 70
    #
    # ----------------------------------------------------------------------
    bot_susp_raw = (
        0.45 * avg_path_susp +
        0.15 * ua_susp +
        0.10 * primary_susp +
        0.05 * cipher_susp +
        0.15 * lang_susp +
        0.10 * entropy_susp
    )

    # Convert to human-like score (1..99)
    score = int(round(99 * (1.0 - bot_susp_raw)))
    score = max(1, min(99, score))

    return score

def get_baskerville_score_3(session: dict, verbose: bool = False, logger=None) -> int:
    """
    Computes baskerville_score_3 for a full session using all cheap rule-based signals.

    This is the "full context" rule-based score that replaces the old is_human human_score.

    Output:
        1   = almost certainly a bot
        99  = almost certainly a human
    """

    def dbg(message: str):
        """Emit debug output if verbose mode is enabled."""
        if verbose:
            if logger:
                logger.info(message)
            else:
                print(message)

    # Start with maximum human score (99 = definitely human, 1 = definitely bot)
    baskerville_score_3 = 99

    # ----------------------------------------------------------------------
    # 1. Language detection (critical rule)
    # ----------------------------------------------------------------------
    num_languages = compute_num_languages(session, dbg=dbg)

    if num_languages == 0:
        # Critical indicator - almost certainly a bot
        baskerville_score_3 = 1
        dbg(f"[SCORE] num_languages=0 → baskerville_score_3={baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 2. HTTP Protocol version check
    # ----------------------------------------------------------------------
    http_protocol = session.get('http_protocol', '')
    if http_protocol:
        # HTTP/1.0 is very suspicious - almost certainly a bot
        if http_protocol == 'HTTP/1.0':
            baskerville_score_3 = min(baskerville_score_3, 5)
            dbg(f"[SCORE] http_protocol=HTTP/1.0 (very suspicious) → baskerville_score_3={baskerville_score_3}")
        # HTTP/1.1 with other bot indicators is suspicious
        # Modern browsers use HTTP/2 or HTTP/3
        elif http_protocol == 'HTTP/1.1':
            # Combine with UA score - if UA is also suspicious, likely a bot
            ua_score_val = session.get('ua_score', 0)
            if ua_score_val > 0.3:
                prev = baskerville_score_3
                baskerville_score_3 = min(baskerville_score_3, 10)
                dbg(f"[SCORE] http_protocol=HTTP/1.1 + ua_score={ua_score_val:.2f} (bot pattern) → {prev} → {baskerville_score_3}")
            # Also check for known scrapers
            elif session.get('is_scraper', False):
                prev = baskerville_score_3
                baskerville_score_3 = min(baskerville_score_3, 10)
                dbg(f"[SCORE] http_protocol=HTTP/1.1 + is_scraper=True → {prev} → {baskerville_score_3}")
            else:
                # Slight penalty for HTTP/1.1 even if no other indicators
                baskerville_score_3 -= 5
                dbg(f"[SCORE] http_protocol=HTTP/1.1 (suspicious) → baskerville_score_3={baskerville_score_3}")
    else:
        # No http_protocol data - log but don't penalize (backwards compatibility)
        dbg("[INFO] http_protocol not available in session data")

    # ----------------------------------------------------------------------
    # 3. UA score (0-1 range, higher = more suspicious)
    # ----------------------------------------------------------------------
    ua_score_val = session.get('ua_score', 0)
    if ua_score_val > 0.6:
        # Very high UA score - strong bot indicator
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 15)
        dbg(f"[SCORE] ua_score={ua_score_val} > 0.6 (strong bot indicator) → {prev} → {baskerville_score_3}")
    elif ua_score_val > 0.3:
        # Moderate penalty
        penalty = int(ua_score_val * 40)
        baskerville_score_3 -= penalty
        dbg(f"[SCORE] ua_score={ua_score_val} → penalty={penalty} → baskerville_score_3={baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 4. Verified bot (legitimate crawlers like Googlebot, Bingbot)
    # ----------------------------------------------------------------------
    if session.get('verified_bot', False):
        # Legitimate bot, but still automated
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 15)
        dbg(f"[SCORE] verified_bot=True (legitimate crawler) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 5. Primary session (bot behavior - no session cookies)
    # ----------------------------------------------------------------------
    if session.get('primary_session', False):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 20)
        dbg(f"[SCORE] primary_session=True (no cookies, bot-like) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 6. Scraper detection
    # ----------------------------------------------------------------------
    if session.get('is_scraper', is_scraper(session.get('ua', ''))):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 10)
        dbg(f"[SCORE] scraper detected → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 7. Headless browser
    # ----------------------------------------------------------------------
    if session.get('headless_ua', False):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 10)
        dbg(f"[SCORE] headless_ua=True (automation tool) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 8. User-Agent based rules
    # ----------------------------------------------------------------------
    if session.get('bot_ua', False):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 20)
        dbg(f"[SCORE] bot_ua=True → {prev} → {baskerville_score_3}")

    if session.get('short_ua', False):
        prev = baskerville_score_3
        baskerville_score_3 -= 20
        dbg(f"[SCORE] short_ua=True → {prev} → {baskerville_score_3}")

    if session.get('ai_bot_ua', False):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 8)
        dbg(f"[SCORE] ai_bot_ua=True (AI crawler) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 9. Weak TLS cipher (precomputed flag)
    # ----------------------------------------------------------------------
    if session.get('weak_cipher', False):
        prev = baskerville_score_3
        baskerville_score_3 -= 15
        dbg(f"[SCORE] weak_cipher=True (outdated/suspicious) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 10. AI crawler UA patterns
    # ----------------------------------------------------------------------
    if is_ai_bot_user_agent(session.get('ua', '')):
        prev = baskerville_score_3
        baskerville_score_3 = min(baskerville_score_3, 8)
        dbg(f"[SCORE] AI crawler UA pattern detected → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # 11. Full-session URL features (path suspicion + entropy + primary_session)
    # ----------------------------------------------------------------------
    requests = session.get("requests") or []
    if requests:
        # 11.1 Aggregate path suspicion across all requests
        path_susps = []
        for r in requests:
            url = r.get("url", "") or r.get("path", "")
            status = r.get("code", None)
            path_susps.append(path_suspicion_score(url, status_code=status))

        avg_path_susp = sum(path_susps) / len(path_susps) if path_susps else 0.0
        dbg(f"[INFO] avg_path_susp={avg_path_susp:.3f} over {len(requests)} requests")

        if avg_path_susp > 0.7:
            prev = baskerville_score_3
            baskerville_score_3 = min(baskerville_score_3, 10)
            dbg(f"[SCORE] avg_path_susp={avg_path_susp:.2f} (high exploit/scan pattern) → {prev} → {baskerville_score_3}")
        elif avg_path_susp > 0.4:
            prev = baskerville_score_3
            baskerville_score_3 = min(baskerville_score_3, 25)
            dbg(f"[SCORE] avg_path_susp={avg_path_susp:.2f} (suspicious paths) → {prev} → {baskerville_score_3}")

        # 11.2 Strong rule: many requests + no cookies + suspicious paths
        if session.get("primary_session", False) and len(requests) >= 5 and avg_path_susp > 0.4:
            prev = baskerville_score_3
            baskerville_score_3 = min(baskerville_score_3, 8)
            dbg(
                f"[SCORE] primary_session=True + {len(requests)} requests + high path_susp "
                f"→ {prev} → {baskerville_score_3}"
            )

        # 11.3 Entropy of URLs
        if len(requests) >= 3:
            url_counts: dict[str, int] = {}
            for r in requests:
                u = r.get("url", "") or r.get("path", "")
                url_counts[u] = url_counts.get(u, 0) + 1

            entropy = _entropy_from_counts(url_counts)
            dbg(f"[INFO] entropy={entropy:.3f} for {len(url_counts)} distinct URLs")

            if entropy < 0.3:
                prev = baskerville_score_3
                baskerville_score_3 = min(baskerville_score_3, 10)
                dbg(f"[SCORE] entropy={entropy:.2f} (very low, repetitive URLs) → {prev} → {baskerville_score_3}")
            elif entropy < 0.8:
                prev = baskerville_score_3
                baskerville_score_3 = min(baskerville_score_3, 25)
                dbg(f"[SCORE] entropy={entropy:.2f} (low URL diversity) → {prev} → {baskerville_score_3}")

    # 11.4 Cipher family suspicion (normalized cipher_type)
    cipher_type = session.get("cipher_type")
    cipher = session.get("cipher")
    if cipher_type is None and cipher:
        cipher_type = normalize_cipher(cipher)

    if cipher_type is not None:
        ct_susp = cipher_suspicion_score(cipher_type)
        dbg(f"[INFO] cipher_type={cipher_type}, cipher_susp={ct_susp:.2f}")
        if ct_susp >= 0.8:
            prev = baskerville_score_3
            baskerville_score_3 = min(baskerville_score_3, 10)
            dbg(f"[SCORE] cipher_type={cipher_type} (very weak/legacy) → {prev} → {baskerville_score_3}")
        elif ct_susp >= 0.4:
            prev = baskerville_score_3
            baskerville_score_3 = max(1, baskerville_score_3 - 10)
            dbg(f"[SCORE] cipher_type={cipher_type} (legacy/mixed) → {prev} → {baskerville_score_3}")

    # ----------------------------------------------------------------------
    # Clamp into [1, 99]
    # ----------------------------------------------------------------------
    baskerville_score_3 = max(1, min(99, baskerville_score_3))
    dbg(f"[FINAL] baskerville_score_3={baskerville_score_3}")

    return baskerville_score_3

def get_score(session):
    if session.get('immature_session'):
        if len(session['requests']) == 1:
            score = session['baskerville_score_1']
        else:
            score = session['baskerville_score_2']
    else:
        score = session['baskerville_score_3']
    return score

def is_human(session):
    """
    Returns:
        (bool):
          - bool: True if score > 30 (considered human)
    """
    return get_score(session) > 30
