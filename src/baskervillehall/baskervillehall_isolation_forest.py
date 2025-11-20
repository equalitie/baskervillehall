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
