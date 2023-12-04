import unittest
import random
from baskervillehall.behave_pca import BehavePCA
from collections import deque
from baskervillehall.main import get_logger


logger = get_logger('Behave test')


class TestBehaveModel(unittest.TestCase):

    def test_training(self):
        dataset = deque([])

        random.seed(777)
        num_urls = 20

        # some IPs
        for ip in range(10000):
            dataset.append([f'/{random.randint(0, num_urls)}.html' for i in range(random.randint(30, 40))])

        model = BehavePCA(
            num_total_pca_components=num_urls,
            target_explained_variance=0.98,
            target_false_positive_rate=0.03,
            logger=logger
        )

        model.fit(dataset)

        scores = model.score(dataset)

        false_positive_rate = len(scores[scores > model.threshold]) * 1.0 / len(scores)
        logger.info(f'false_positive_rate = {false_positive_rate}, targetFPR={model.target_false_positive_rate}')
        self.assertTrue(false_positive_rate <= model.target_false_positive_rate)

        urls = ['1.html' for i in range(100)]
        prediction = model.predict([urls])
        self.assertTrue(prediction[0])

