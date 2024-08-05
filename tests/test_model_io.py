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

    def test_model(self):
        random.seed(777)
        num_records = 10000
        sessions = []
        urls = [f'{i}.html' for i in range(30)]
        ts = datetime.now()
        for session_id in range(num_records):
            sessions.append({
                'session_id': session_id,
                'country': 'UK',
                'duration': 30,
                'primary_session': False,
                'requests': [{
                    'ts': ts + timedelta(seconds=i * 3),
                    'code': 200,
                    'url': urls[random.randrange(5)],
                    'query': f'{i}',
                    'type': 'text/html',
                    'payload': 100
                } for i in range(10)]
            })
        model = BaskervillehallIsolationForest(
            pca_feature=True
        )
        model.fit(sessions)
        scores = model.transform(sessions)

        s3_storage = 'tests'
        host = 'host1'
        s3_connection = {
            's3_endpoint': os.environ.get('S3_ENDPOINT'),
            's3_access': os.environ.get('S3_ACCESS'),
            's3_secret': os.environ.get('S3_SECRET'),
            's3_region': os.environ.get('S3_REGION'),
        }
        io = ModelIO(**s3_connection)
        model.clear_embeddings()
        io.save(model, s3_storage, host, human=True)

        model1 = io.load(s3_storage, host, human=True)
        scores1 = model1.transform(sessions)
        assert scores1[0] == scores[0]