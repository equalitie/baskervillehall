import unittest
import random
import numpy as np
import os
from datetime import datetime, timedelta

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.main import get_logger
from baskervillehall.model_io import ModelIO

logger = get_logger('Baskervillehall test')


class TestModel(unittest.TestCase):

    def test_basic_anomaly(self):
        random.seed(777)
        num_records = 1000

        num_features = 5

        categories = [
            ['a','b','c','d','e','f'],
            ['alfa', 'betta', 'gamma']
        ]

        features = []
        categorical_features = []

        for ip in range(num_records):
            features.append([np.random.normal(loc=float(i)) for i in range(num_features)])
            categorical_features.append([categories[i][random.randint(0, len(categories[i])-1)] for i in range(len(categories))])

        features = np.array(features)
        model = BaskervillehallIsolationForest()
        model.fit(features=features, feature_names=[], categorical_features=categorical_features)

        test_features = []
        test_categorical_features = []
        for ip in range(10):
            test_features.append([np.random.normal(loc=float(100)) for i in range(num_features)])
            test_categorical_features.append([categories[i][0] for i in range(len(categories))])
        test_features = np.array(test_features)

        scores = model.score(test_features, test_categorical_features)
        assert (len(scores[scores < 0]) == len(scores))

        logger.info('Testing "other" category...')
        test_features = []
        test_categorical_features = []
        test_features.append([np.random.normal(loc=float(100)) for i in range(num_features)])
        test_categorical_features.append(['xxx', 'yyy'])
        test_features = np.array(test_features)
        scores = model.score(test_features, test_categorical_features)
        assert (len(scores[scores < 0]) == len(scores))

    def test_features(self):
        duration = 60
        hit_rate = 20
        country = 'US'
        url = '/'
        session = {'duration': duration, 'country': country, 'fresh_sessions': False}
        requests = []
        num_hits = int(duration * hit_rate / 60)
        ts = datetime.now()
        time_increment = 60.0 / hit_rate
        countries = []

        for i in range(num_hits):
            requests.append({'ts': ts, 'url': url, 'query': '', 'code': 200, 'type': 'text/html', 'payload': 1000})
            ts += timedelta(seconds=time_increment)
            countries.append(country)
        session['requests'] = requests

        features = BaskervillehallIsolationForest.calculate_features(session, '')
        import pdb
        pdb.set_trace()
        assert(features['request_rate'] == float(hit_rate))
        assert(features['entropy'] < 30.0)
        assert(features['path_depth_average'] == 1.0)