from baskervillehall.baskerville_rules import get_baskerville_score_2

def test_single_request():
    session = {
        "requests": [
            {
                "ua": "Mozilla/5.0",
                "url": "/",
                "code": 200,
                "method": "GET"
            }
        ],
        "cipher_type": "modern_aes128_gcm",
        "num_languages": 1
    }

    score = get_baskerville_score_2(session)
    assert score == 50


def test_scanner_low_score():
    session = {
        "requests": [
            {"ua": "curl/8.3.0", "url": "/wp-login.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/wp-admin/admin.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/xmlrpc.php", "code": 404, "method": "GET"},
            {"ua": "curl/8.3.0", "url": "/config.php", "code": 404, "method": "GET"},
        ],
        "cipher_type": "rsa_legacy",
        "num_languages": 0
    }

    score = get_baskerville_score_2(session)
    assert score < 30


def test_human_high_score():
    session = {
        "requests": [
            {"ua": "Mozilla/5.0", "url": "/", "code": 200, "method": "GET"},
            {"ua": "Mozilla/5.0", "url": "/about", "code": 200, "method": "GET"},
            {"ua": "Mozilla/5.0", "url": "/static/app.js", "code": 200, "method": "GET"},
        ],
        "cipher_type": "modern_aes128_gcm",
        "num_languages": 2
    }

    score = get_baskerville_score_2(session)
    assert score > 60
