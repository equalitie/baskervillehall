import hashlib


class SessionFingerprints(object):

    def __init__(self):
        super().__init__()

    @staticmethod
    def get_fingerprints(session):
        """
        Collects specified parameters from a session and generates a hash.
        """
        params_to_hash = [
            session.get('ua'),  # User-Agent
            session.get('country'),
            session.get('cipher'),  # Specific cipher used for the connection
            session.get('ciphers'),  # List of ciphers supported by the client
            session.get('accept_language'),
        ]

        # Generate the hash from the collected parameters
        fingerprint_hash = SessionFingerprints.generate_hash(params_to_hash)
        return fingerprint_hash

    @staticmethod
    def generate_hash(params_list):
        """
        Generates a SHA256 hash from a list of parameters.
        """
        # Convert all parameters to strings and handle None values
        # Join them with a consistent separator to avoid ambiguities
        # (e.g., ['ab', 'c'] vs ['a', 'bc'])
        stringified_params = []
        for p in params_list:
            if p is None:
                stringified_params.append("None")  # Or "" if you prefer
            elif isinstance(p, list) or isinstance(p, tuple) or isinstance(p, set):
                # Sort lists/tuples/sets to ensure consistent order for hashing
                stringified_params.append(",".join(sorted([str(item) for item in p])))
            else:
                stringified_params.append(str(p))

        combined_string = "|".join(stringified_params)

        # Create a new SHA256 hash object
        sha256_hash = hashlib.sha256()

        # Update the hash object with the bytes of the combined string
        sha256_hash.update(combined_string.encode('utf-8'))

        # Get the hexadecimal representation of the hash
        return sha256_hash.hexdigest()[:16]


# Example Usage (assuming you have a session dictionary):
if __name__ == '__main__':
    # Example session data
    session_data_1 = {
        'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'country': 'US',
        'cipher': 'AEAD-AES128-GCM-SHA256',
        'ciphers': ['AEAD-AES128-GCM-SHA256', 'AEAD-CHACHA20-POLY1305-SHA256', 'AEAD-AES256-GCM-SHA384'],
        'accept_language': ['en-US', 'en;q=0.9']
    }

    session_data_2 = {
        'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
        'country': 'US',
        'cipher': 'AEAD-AES128-GCM-SHA256',
        'ciphers': ['AEAD-AES256-GCM-SHA384', 'AEAD-CHACHA20-POLY1305-SHA256', 'AEAD-AES128-GCM-SHA256'],
        # Order changed
        'accept_language': ['en-US', 'en;q=0.9']
    }

    session_data_3 = {
        'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'country': 'CA',
        'cipher': 'TLS_AES_256_GCM_SHA384',
        'ciphers': ['TLS_AES_256_GCM_SHA384', 'TLS_CHACHA20_POLY1305_SHA256'],
        'accept_language': ['en-CA', 'en;q=0.8']
    }

    session_data_4 = {  # Missing a parameter
        'ua': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0',
        'country': 'CA',
        'cipher': None,  # Cipher is None
        'ciphers': ['TLS_AES_256_GCM_SHA384', 'TLS_CHACHA20_POLY1305_SHA256'],
        'accept_language': ['en-CA', 'en;q=0.8']
    }

    fingerprint1 = SessionFingerprints.get_fingerprints(session_data_1)
    fingerprint2 = SessionFingerprints.get_fingerprints(session_data_2)  # Should be same as 1 due to sorting
    fingerprint3 = SessionFingerprints.get_fingerprints(session_data_3)
    fingerprint4 = SessionFingerprints.get_fingerprints(session_data_4)

    print(f"Fingerprint 1: {fingerprint1}")
    print(f"Fingerprint 2: {fingerprint2}")
    print(f"Fingerprint 3: {fingerprint3}")
    print(f"Fingerprint 4: {fingerprint4}")

    assert fingerprint1 == fingerprint2, "Fingerprints for session 1 and 2 should be the same due to sorting of 'ciphers' list."
    assert fingerprint1 != fingerprint3, "Fingerprints for session 1 and 3 should be different."
    assert fingerprint3 != fingerprint4, "Fingerprints for session 3 and 4 should be different."