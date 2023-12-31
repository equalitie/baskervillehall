import unittest
import random
import numpy as np
import os

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.main import get_logger
from baskervillehall.model_io import ModelIO

logger = get_logger('Baskervillehall test')


class TestModel(unittest.TestCase):

    def test_model(self):
        random.seed(777)
        num_records = 1000

        num_features = 10

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

        s3_storage = 'tests'
        host = 'host1'
        s3_connection = {
            's3_endpoint': os.environ.get('S3_ENDPOINT'),
            's3_access': os.environ.get('S3_ACCESS'),
            's3_secret': os.environ.get('S3_SECRET'),
            's3_region': os.environ.get('S3_REGION'),
        }
        io = ModelIO(**s3_connection)
        io.save(model, s3_storage, host)

        model1 = io.load(s3_storage, host)
        scores1 = model1.score(test_features, test_categorical_features)
        assert (len(scores1[scores1 < 0]) == len(scores1))