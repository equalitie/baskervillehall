import unittest
import random
from baskervillehall.behave_pca import BehavePCA
from collections import deque
from baskervillehall.main import get_logger

logger = get_logger('Behave test')


class TestBehaveModel(unittest.TestCase):

    def test_training(self):
        sessions = deque([])

        random.seed(777)
        num_urls = 20

        for ip in range(10000):
            sessions.append({
                'requests':
                    [{'url': f'/{random.randint(0, num_urls)}.html'} for i in range(random.randint(30, 40))]
            })

        model = BehavePCA(
            num_total_pca_components=num_urls,
            target_explained_variance=0.98,
            target_false_positive_rate=0.03,
            logger=logger
        )

        model.fit(sessions)

        scores = model.score(sessions)

        false_positive_rate = len(scores[scores > model.threshold]) * 1.0 / len(scores)
        logger.info(f'false_positive_rate = {false_positive_rate}, targetFPR={model.target_false_positive_rate}')
        self.assertTrue(false_positive_rate <= model.target_false_positive_rate)

        anomaly_sessions = []
        for ip in range(10000):
            anomaly_sessions.append({
                'requests': [{'url': '1.html'} for i in range(100)]
            })
        prediction = model.predict(anomaly_sessions)
        self.assertTrue(prediction[0])
