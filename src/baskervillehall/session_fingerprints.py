import hashlib


def _extract_tls_version(cipher):
    """Infer TLS version from cipher name when no explicit field is available."""
    if not cipher:
        return 'unknown'
    c = str(cipher).upper()
    if c.startswith('TLS_') or c.startswith('AEAD-'):
        return 'tls13'
    if 'ECDHE' in c or 'DHE' in c:
        return 'tls12'
    return 'legacy'


class SessionFingerprints(object):

    def __init__(self):
        super().__init__()

    @staticmethod
    def get_fingerprints(session):
        """
        Generates a fingerprint hash from device/tool characteristics.

        Captures: TLS cipher suite list (ordered — JA3-like), negotiated cipher,
        inferred TLS version, User-Agent, and Accept-Language.

        Country is intentionally excluded: it is a network property (IP geolocation),
        not a device characteristic. Including it would produce different hashes for
        the same bot tool routing through different countries.

        Cipher suite order is preserved (not sorted): the order in which a TLS client
        advertises ciphers is a strong fingerprint signal and differs between
        implementations (Chrome, curl, python-requests, etc.).
        """
        cipher = session.get('cipher')
        ciphers = session.get('ciphers') or []

        # accept_language order is not meaningful — sort for stability
        accept_language = sorted(session.get('accept_language') or [])

        params = [
            session.get('ua'),
            cipher,
            # ciphers joined in original order (not sorted) — order is the fingerprint
            ','.join(str(c) for c in ciphers),
            '|'.join(accept_language),
            _extract_tls_version(cipher),
        ]

        combined = '||'.join(p if p is not None else 'None' for p in params)
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()[:16]


# Example Usage:
if __name__ == '__main__':
    session_data_1 = {
        'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'country': 'US',
        'cipher': 'AEAD-AES128-GCM-SHA256',
        'ciphers': ['AEAD-AES128-GCM-SHA256', 'AEAD-CHACHA20-POLY1305-SHA256', 'AEAD-AES256-GCM-SHA384'],
        'accept_language': ['en-US', 'en;q=0.9']
    }

    # Same device, different country — must be same fingerprint
    session_data_2 = {
        'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'country': 'DE',
        'cipher': 'AEAD-AES128-GCM-SHA256',
        'ciphers': ['AEAD-AES128-GCM-SHA256', 'AEAD-CHACHA20-POLY1305-SHA256', 'AEAD-AES256-GCM-SHA384'],
        'accept_language': ['en-US', 'en;q=0.9']
    }

    # Same ciphers, different order — must be different fingerprint (different TLS implementation)
    session_data_3 = {
        'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'country': 'US',
        'cipher': 'AEAD-AES128-GCM-SHA256',
        'ciphers': ['AEAD-AES256-GCM-SHA384', 'AEAD-CHACHA20-POLY1305-SHA256', 'AEAD-AES128-GCM-SHA256'],
        'accept_language': ['en-US', 'en;q=0.9']
    }

    # Different browser, TLS 1.3
    session_data_4 = {
        'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'country': 'CA',
        'cipher': 'TLS_AES_256_GCM_SHA384',
        'ciphers': ['TLS_AES_256_GCM_SHA384', 'TLS_CHACHA20_POLY1305_SHA256'],
        'accept_language': ['en-CA', 'en;q=0.8']
    }

    # Missing cipher
    session_data_5 = {
        'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'country': 'CA',
        'cipher': None,
        'ciphers': ['TLS_AES_256_GCM_SHA384', 'TLS_CHACHA20_POLY1305_SHA256'],
        'accept_language': ['en-CA', 'en;q=0.8']
    }

    fp1 = SessionFingerprints.get_fingerprints(session_data_1)
    fp2 = SessionFingerprints.get_fingerprints(session_data_2)
    fp3 = SessionFingerprints.get_fingerprints(session_data_3)
    fp4 = SessionFingerprints.get_fingerprints(session_data_4)
    fp5 = SessionFingerprints.get_fingerprints(session_data_5)

    print(f"Fingerprint 1 (Chrome/US):           {fp1}")
    print(f"Fingerprint 2 (Chrome/DE same tool): {fp2}")
    print(f"Fingerprint 3 (Chrome diff order):   {fp3}")
    print(f"Fingerprint 4 (Firefox TLS1.3):      {fp4}")
    print(f"Fingerprint 5 (Firefox no cipher):   {fp5}")

    assert fp1 == fp2, "Same tool, different country must produce same fingerprint"
    assert fp1 != fp3, "Different cipher order must produce different fingerprint"
    assert fp1 != fp4, "Different browser must produce different fingerprint"
    assert fp4 != fp5, "Missing cipher must produce different fingerprint"
    print("\nAll assertions passed.")
