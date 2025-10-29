import logging
from sklearn.ensemble import IsolationForest
from enum import Enum
import shap
import pandas as pd

from baskervillehall.feature_extractor import FeatureExtractor
from baskervillehall.baskerville_rules import (
    is_bot_user_agent, detect_scraper, ua_score, is_human, 
    is_ai_bot_user_agent, is_scraper
)

class ModelType(Enum):
    HUMAN = 'human'
    BOT = 'bot'
    GENERIC = 'generic'


class BaskervillehallIsolationForest(object):

    def __init__(
            self,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            features=None,
            categorical_features=None,
            pca_feature=False,
            datetime_format='%Y-%m-%d %H:%M:%S',
            max_features=1.0,
            bootstrap=False,
            n_jobs=-1,
            random_state=None,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.feature_extractor = FeatureExtractor(
            features=features,
            categorical_features=categorical_features,
            pca_feature=pca_feature,
            datetime_format=datetime_format,
            logger=self.logger
        )

        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = random_state
        self.isolation_forest = None
        self.shapley = None

    def clear_embeddings(self):
        self.feature_extractor.clear_embeddings()

    def set_n_estimators(self, n_estimators):
        self.n_estimators = n_estimators

    def set_contamination(self, contamination):
        self.contamination = contamination

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def is_weak_cipher(cipher):
        if 'RSA' in cipher and 'PFS' not in cipher:
            return True
        if 'CBC' in cipher or '3DES' in cipher or 'MD5' in cipher or 'RC4' in cipher:
            return True
        return False

    @staticmethod
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

    @staticmethod
    def is_short_user_agent(user_agent):
        return user_agent is None or len(user_agent.strip()) < 50
    


    @staticmethod
    def is_bad_bot(session):
        if session['verified_bot']:
            return False

        if not session['primary_session']:
            return False

        if not session['bot_ua'] or not session['ai_bot_ua']:
            return True

        # a legit bot does not change its user agent
        uas = set()
        for r in session['requests']:
            ua = r.get('ua', '')
            if len(ua) < 5:
                return True
            uas.add(ua)
        if len(uas) > 1:
            return True

        return False

    def get_all_features(self):
        return self.feature_extractor.get_all_features()

    def fit(
            self,
            sessions,
    ):
        self.shapley = None
        vectors = self.feature_extractor.fit_transform(sessions)

        self.isolation_forest = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=self.contamination,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            n_jobs=self.n_jobs,
            random_state=self.random_state
        )

        df = pd.DataFrame(data=vectors, columns=self.get_all_features())

        self.isolation_forest.fit(df)

    def get_shapley(self):
        if self.shapley:
            return self.shapley
        self.shapley = shap.TreeExplainer(self.isolation_forest)
        return self.shapley

    def transform(self, sessions, use_shapley=True):
        vectors = self.feature_extractor.transform(sessions)
        df_features = pd.DataFrame(data=vectors, columns=self.get_all_features())
        scores = self.isolation_forest.decision_function(df_features)

        shap_values = self.get_shapley()(vectors, check_additivity=False) if use_shapley else None
        return scores, shap_values, df_features
