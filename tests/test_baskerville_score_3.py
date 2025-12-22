import pytest

from baskervillehall.baskerville_rules import get_baskerville_score_3, is_human


def test_scanner_low_score():
    """
    Classic scanner:
      - no Accept-Language (num_languages=0)
      - curl UA
      - WordPress paths + 404
      → должен давать низкий baskerville_score_3 (< 30)
    """
    session = {
        "requests": [
            {"ua": "curl/8.3.0", "url": "/wp-login.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/wp-admin/admin.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/xmlrpc.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/config.php", "code": 404, "method": "GET"},
        ],
        "cipher_type": "rsa_legacy",
        "num_languages": 0,          # нет Accept-Language → очень подозрительно
        "ua_score": 0.9,             # очень подозрительный UA
        "http_protocol": "HTTP/1.1",
        "primary_session": True,
        "verified_bot": False,
        "is_scraper": True,
        "headless_ua": False,
        "bot_ua": True,
        "ai_bot_ua": False,
        "weak_cipher": True,
        "ua": "curl/8.3.0",
    }

    score = get_baskerville_score_3(session)
    assert 1 <= score <= 99
    assert score < 30

    human_flag, score2 = is_human(session)
    assert human_flag is False
    assert score2 == score


def test_human_high_score():
    """
    Типичный браузерный пользователь:
      - нормальный UA
      - modern cipher
      - есть Accept-Language (num_languages=1)
      - HTTP/2
      - не primary_session
      → должен давать высокий baskerville_score_3 (> 70)
    """
    session = {
        "requests": [
            {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/131.0.0.0 Safari/537.36",
             "url": "/",
             "code": 200,
             "method": "GET"},
            {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/131.0.0.0 Safari/537.36",
             "url": "/about",
             "code": 200,
             "method": "GET"},
        ],
        "cipher_type": "modern_aes128_gcm",
        "num_languages": 1,
        "ua_score": 0.05,
        "http_protocol": "HTTP/2",
        "primary_session": False,
        "verified_bot": False,
        "is_scraper": False,
        "headless_ua": False,
        "bot_ua": False,
        "ai_bot_ua": False,
        "weak_cipher": False,
        "ua": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/131.0.0.0 Safari/537.36"),
    }

    score = get_baskerville_score_3(session)
    assert 1 <= score <= 99
    assert score > 70

    human_flag, score2 = is_human(session)
    assert human_flag is True
    assert score2 == score


def test_low_entropy_primary_session_scanner():
    """
    Сессия-сканер:
      - много одинаковых URL → низкая энтропия
      - primary_session=True
      - обычный HTTP/1.1
      → ждём низкий baskerville_score_3.
    """
    session = {
        "requests": [
            {"ua": "SomeScanner/1.0", "url": "/healthz", "code": 200, "method": "GET"},
            {"ua": "SomeScanner/1.0", "url": "/healthz", "code": 200, "method": "GET"},
            {"ua": "SomeScanner/1.0", "url": "/healthz", "code": 200, "method": "GET"},
            {"ua": "SomeScanner/1.0", "url": "/healthz", "code": 200, "method": "GET"},
            {"ua": "SomeScanner/1.0", "url": "/healthz", "code": 200, "method": "GET"},
        ],
        "cipher_type": "rsa_legacy",
        "num_languages": 0,
        "ua_score": 0.8,
        "http_protocol": "HTTP/1.1",
        "primary_session": True,
        "verified_bot": False,
        "is_scraper": True,
        "headless_ua": False,
        "bot_ua": True,
        "ai_bot_ua": False,
        "weak_cipher": True,
        "ua": "SomeScanner/1.0",
    }

    score = get_baskerville_score_3(session)
    assert 1 <= score <= 99
    assert score < 30

    human_flag, score2 = is_human(session)
    assert human_flag is False
    assert score2 == score
