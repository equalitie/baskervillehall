import logging

import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import IsolationForest


class BaskervillehallIsolationForest(object):

    def __init__(
            self,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            max_features=1.0,
            bootstrap=False,
            n_jobs=None,
            random_state=None,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = random_state

        self.categorical_encoders = None
        self.feature_names = None
        self.categorical_feature_names = None
        self.mean = None
        self.std = None
        self.isolation_forest = None

    @staticmethod
    def get_vector_from_feature_map(features, features_map):
        vector = np.zeros(len(features))
        for i in range(len(features)):
            vector[i] = features_map.get(features[i], 0.0)
        return vector

    def _normalize(self, Y):
        return (Y - self.mean) / self.std

    def fit(
            self,
            features,
            feature_names,
            categorical_features=None,
            categorical_feature_names=None
    ):
        assert (features.shape[0] == len(categorical_features))

        self.categorical_encoders = []
        self.feature_names = feature_names
        self.categorical_feature_names = categorical_feature_names

        categorical_vectors = []
        self.categorical_encoders = []
        for i in range(len(categorical_features[0])):
            encoder = LabelEncoder()
            categorical_vectors.append(encoder.fit_transform(
                [categorical_features[k][i] for k in range(len(categorical_features))]))
            self.categorical_encoders.append(encoder)

        categorical_vectors = np.array(categorical_vectors).transpose()

        Y = np.concatenate((features, categorical_vectors), axis=1)

        self.mean = Y.mean(axis=0)
        self.std = Y.std(axis=0)
        Z = self._normalize(Y)

        self.isolation_forest = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=self.contamination,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            n_jobs=self.n_jobs,
            random_state=self.random_state
        )

        self.isolation_forest.fit(Z)

    def score(self, features, categorical_features):
        assert (features.shape[0] == len(categorical_features))
        assert(len(categorical_features[0]) == len(self.categorical_encoders))

        categorical_vectors = []
        for i in range(len(self.categorical_encoders)):
            categorical_vectors.append(self.categorical_encoders[i].transform(
                [categorical_features[k][i] for k in range(len(categorical_features))]
            ))

        categorical_vectors = np.array(categorical_vectors).transpose()

        Y = np.concatenate((features, categorical_vectors), axis=1)
        Z = self._normalize(Y)
        scores = self.isolation_forest.decision_function(Z)
        return scores
