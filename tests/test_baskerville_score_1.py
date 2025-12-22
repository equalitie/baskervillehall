import pytest

from baskervillehall.baskerville_rules import (
    get_baskerville_score_1,
    normalize_cipher,
)


def test_empty_session_returns_neutral_score():
    """
    If there are no requests in the session, get_baskerville_score_1()
    should return neutral score 50.
    """
    session = {
        "requests": [],
        "cipher_type": "modern_aes128_gcm",
        "cipher": "",
        "num_languages": 1,
    }

    score = get_baskerville_score_1(session)
    assert score == 50


def test_normal_browser_request_high_score():
    """
    A normal browser hit on '/' with 200 OK, modern cipher, and
    valid Accept-Language should give a high human score.
    """
    session = {
        "requests": [
            {
                "ts": "2025-12-11 10:00:00",
                "url": "/",
                "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/119.0.0.0 Safari/537.36",
                "code": 200,
                "type": "text/html",
                "method": "GET",
            }
        ],
        "cipher_type": "modern_aes128_gcm",
        "cipher": "",
        "accept_language": "en-US,en;q=0.9",
        "num_languages": 0,  # will be recomputed from accept_language
    }

    score = get_baskerville_score_1(session)
    # Expect a clearly "human" score
    assert 70 <= score <= 99


def test_wp_scanner_like_request_lower_score():
    """
    A suspicious WordPress scanning path (php + wp- + 404)
    should drive the score noticeably down.
    """
    session = {
        "requests": [
            {
                "ts": "2025-12-11 11:22:38",
                "url": "/wp-admin/phpmyadmin.php",
                "ua": "Mozilla/5.0 (Linux; Android 13; SM-S908E) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/116.0.0.0 Mobile Safari/537.36",
                "code": 404,
                "type": "",
                "method": "GET",
            }
        ],
        "cipher_type": "none",
        "cipher": "",
        "accept_language": "en-US",
        "num_languages": 0,  # will be recomputed to 1
    }

    score = get_baskerville_score_1(session)
    # Не обязаны точно знать число, но ожидаем, что оно значительно ниже, чем у нормального браузера
    assert 1 <= score <= 70


def test_missing_accept_language_penalizes_score():
    """
    If Accept-Language is missing, lang_susp should be 1.0, which
    lowers the final score relative to a similar session with languages.
    """
    base_session = {
        "requests": [
            {
                "ts": "2025-12-11 10:00:00",
                "url": "/",
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/119.0.0.0 Safari/537.36",
                "code": 200,
                "type": "text/html",
                "method": "GET",
            }
        ],
        "cipher_type": "modern_aes128_gcm",
        "cipher": "",
    }

    # With Accept-Language
    session_with_lang = {
        **base_session,
        "accept_language": "en-US,en;q=0.9",
        "num_languages": 0,
    }

    # Without Accept-Language
    session_without_lang = {
        **base_session,
        # no accept_language field
        "num_languages": 0,
    }

    score_with_lang = get_baskerville_score_1(session_with_lang)
    score_without_lang = get_baskerville_score_1(session_without_lang)

    # Without Accept-Language score must be strictly lower
    assert score_without_lang < score_with_lang


def test_cipher_normalization_used_when_cipher_type_missing():
    """
    If cipher_type is not provided, but cipher is, get_baskerville_score_1
    should normalize the cipher and use the resulting cipher_type
    in suspicion calculation.
    """
    # Cipher that should map to modern_aes128_gcm
    raw_cipher = "TLS_AES_128_GCM_SHA256"
    assert normalize_cipher(raw_cipher) == "modern_aes128_gcm"

    session = {
        "requests": [
            {
                "ts": "2025-12-11 10:00:00",
                "url": "/",
                "ua": "Mozilla/5.0 (X11; Linux x86_64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/119.0.0.0 Safari/537.36",
                "code": 200,
                "type": "text/html",
                "method": "GET",
            }
        ],
        "cipher_type": None,
        "cipher": raw_cipher,
        "accept_language": "en-US,en;q=0.9",
        "num_languages": 0,
    }

    score = get_baskerville_score_1(session)
    # Modern cipher, normal path, Accept-Language → тоже высокий human score
    assert 70 <= score <= 99
