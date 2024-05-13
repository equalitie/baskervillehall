import logging
from sklearn.ensemble import IsolationForest

from baskervillehall.feature_extractor import FeatureExtractor


class BaskervillehallIsolationForest(object):

    def __init__(
            self,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            warmup_period=5,
            features=None,
            categorical_features=None,
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

    def set_n_estimators(self, n_estimators):
        self.n_estimators = n_estimators

    def set_contamination(self, contamination):
        self.contamination = contamination

    @staticmethod
    def is_bot_ua(ua):
        ua_lowercase = ua.lower()
        return 'bot' in ua_lowercase or 'spider' in ua_lowercase or 'crawl' in ua_lowercase

    @staticmethod
    def is_human(session):
        return (session['primary_session'] is False and
                not BaskervillehallIsolationForest.is_bot_ua(session['ua']))

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
