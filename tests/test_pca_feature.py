import unittest
import random
import numpy as np
from baskervillehall.main import get_logger
from baskervillehall.pca_feature import PCAFeature, urls_to_text

logger = get_logger('Baskervillehall test')


class TestPCAFeature(unittest.TestCase):

    def test_url_to_text(self):
        self.assertTrue(urls_to_text(["/"]), 'home page')
        self.assertTrue(urls_to_text(['/articles/103346']), 'articles 103346')

    def test_training(self):
        random.seed(777)
        num_records = 100
        sessions = []
        urls = [f'{i}{i}{i}{i}.html' for i in range(20)]
        for session_id in range(num_records):
            sessions.append({
                'requests': [{
                    'url': urls[random.randrange(10)],
                } for i in range(10)]
            })
        pca = PCAFeature()
        scores_train = pca.fit_transform(sessions)

        sessions_anomaly = []
        for session_id in range(num_records):
            sessions_anomaly.append({
                'requests': [{
                    'url': urls[random.randrange(10, 20)],
                } for i in range(10)]
            })

        scores_anomaly = pca.transform(sessions_anomaly)
        average_train_score = np.average(scores_train)
        average_anomaly_score = np.average(scores_anomaly)
        self.assertTrue(average_anomaly_score > average_train_score)


