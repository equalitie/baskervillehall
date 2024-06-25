import logging
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import numpy as np

from baskervillehall.feature_extractor import FeatureExtractor


class BaskervillehallAutoencoder(object):

    def __init__(
            self,
            contamination=0.01,
            warmup_period=5,
            features=None,
            categorical_features=None,
            max_categories=3,
            min_category_frequency=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
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

        self.contamination = contamination
        self.random_state = random_state
        self.autoencoder = None

    def fit(
            self,
            sessions,
    ):
        vectors = self.feature_extractor.fit_transform(sessions)

        input_dim = vectors.shape[1]

        self.autoencoder = Sequential([
            Dense(1024, activation='relu', input_shape=(input_dim,)),
            Dense(512, activation='relu'),
            Dense(1024, activation='relu'),
            Dense(input_dim, activation='tanh')  # Output layer size same as input
        ])

        self.autoencoder.compile(optimizer='adam', loss='mse')

        # Train the model on normal data only
        self.autoencoder.fit(vectors, vectors, epochs=200, batch_size=128)

    def transform(self, sessions):
        vectors = self.feature_extractor.transform(sessions)
        scores = self.autoencoder.decision_function(vectors)
        return scores
