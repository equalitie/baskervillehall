import logging
from sklearn.ensemble import IsolationForest
from enum import Enum
import shap
import pandas as pd
import re

from baskervillehall.feature_extractor import FeatureExtractor


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
            warmup_period=5,
            features=None,
            categorical_features=None,
            pca_feature=False,
            max_categories=3,
            min_category_frequency=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            max_features=1.0,
            bootstrap=False,
            n_jobs=None,
            random_state=None,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.feature_extractor = FeatureExtractor(
            warmup_period=warmup_period,
            features=features,
            categorical_features=categorical_features,
            pca_feature=pca_feature,
            max_categories=max_categories,
            min_category_frequency=min_category_frequency,
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
        if len(ciphers) < 5:
            return False
        if 'TLS_AES_128_GCM_SHA256' not in ciphers and \
                'TLS_AES_256_GCM_SHA384' not in ciphers and \
                'TLS_CHACHA20_POLY1305_SHA256' not in ciphers:
            return False

        ECDHE_exists = False
        for c in ciphers:
            if 'ECDHE' in c:
                ECDHE_exists = True

        if not ECDHE_exists:
            return False

        return True

    @staticmethod
    def is_short_user_agent(user_agent):
        return user_agent is None or len(user_agent.strip()) < 50
    
    @staticmethod
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


    @staticmethod
    def is_headless_ua(ua):
        return 'HeadlessChrome' in ua

    @staticmethod
    def is_human(session):
        if session['datacenter_asn'] and not session['vpn_asn']:
            return False
        if session['verified_bot']:
            return False
        if session['primary_session']:
            return False
        if session['num_languages'] == 0:
            return False
        if session['headless_ua']:
            return False
        if session['bot_ua']:
            return False
        if session['short_ua']:
            return False
        if session['ai_bot_ua']:
            return False
        if session['asset_only']:
            return False
        if not session['valid_browser_ciphers']:
            return False
        if session['weak_cipher']:
            return False
        return True


    @staticmethod
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
        df = pd.DataFrame(data=vectors, columns=self.get_all_features())
        scores = self.isolation_forest.decision_function(df)

        shap_values = self.get_shapley()(vectors, check_additivity=False) if use_shapley else None
        return scores, shap_values
