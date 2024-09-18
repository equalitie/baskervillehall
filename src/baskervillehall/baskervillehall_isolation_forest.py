import logging
from sklearn.ensemble import IsolationForest
from enum import Enum

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

    def clear_embeddings(self):
        self.feature_extractor.clear_embeddings()

    def set_n_estimators(self, n_estimators):
        self.n_estimators = n_estimators

    def set_contamination(self, contamination):
        self.contamination = contamination

    @staticmethod
    def is_bot_ua(ua):
        name = ua
        if isinstance(ua, dict):
            name = ua.get('name', '')
        name_lowercase = name.lower()
        return 'bot' in name_lowercase or 'spider' in name_lowercase or 'crawl' in name_lowercase

    @staticmethod
    def is_human(session):
        return not session.get('primary_session', False) and \
                not BaskervillehallIsolationForest.is_bot_ua(session['ua'])

    @staticmethod
    def is_bad_bot(session):
        if not session.get('primary_session', False):
            return False

        if not BaskervillehallIsolationForest.is_bot_ua(session):
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

    def fit(
            self,
            sessions,
    ):
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

        self.isolation_forest.fit(vectors)

    def transform(self, sessions):
        vectors = self.feature_extractor.transform(sessions)
        scores = self.isolation_forest.decision_function(vectors)
        return scores
