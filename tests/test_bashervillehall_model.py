import unittest
import random
import numpy as np
import os
from datetime import datetime, timedelta

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.feature_extractor import FeatureExtractor
from baskervillehall.main import get_logger
from baskervillehall.model_io import ModelIO

logger = get_logger('Baskervillehall test')


class TestModel(unittest.TestCase):

    def test_session_anomaly(self):
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
            features=[
                'request_rate',
                'post_rate',
                'request_interval_average',
                'request_interval_std',
                'response4xx_to_request_ratio',
                'response5xx_to_request_ratio',
                'top_page_to_request_ratio',
                'unique_path_rate',
                'unique_path_to_request_ratio',
                'unique_query_rate',
                'unique_query_to_unique_path_ratio',
                'image_to_html_ratio',
                'js_to_html_ratio',
                'css_to_html_ratio',
                'path_depth_average',
                'path_depth_std',
                'payload_size_log_average',
                'entropy',
                'num_requests',
                'duration',
                'edge_entropy',
                'static_ratio',
                'ua_entropy',
            ]
        )
        model.fit(sessions)

        scores = model.transform(sessions)
        assert (len(scores[scores < 0]) < len(scores) * 0.4)

        anomaly_sessions = []
        ts = datetime.now()
        for ip in range(10):
            anomaly_sessions.append({
                'session_id': ip,
                'country': 'CA',
                'duration': 10,
                'primary_session': False,
                'requests': [{
                    'ts': ts + timedelta(seconds=i),
                    'code': 403,
                    'url': urls[random.randrange(3)],
                    'method': 'GET',
                    'query': f'{i}',
                    'type': 'text/html',
                    'payload': 10
                } for i in range(2 + random.randrange(200))]
            })

        anomaly_scores = model.transform(anomaly_sessions)
        assert (len(anomaly_scores[anomaly_scores < 0]) > len(anomaly_scores) * 0.7)

    def test_features(self):
        duration = 60
        hit_rate = 20
        country = 'US'
        urls = ['/', '/one', '/two', '/apple/gree.html', '/apple/red', '/orange', '/banana']
        session = {'duration': duration, 'country': country, 'primary_session': False}
        requests = []
        num_hits = int(duration * hit_rate / 60)
        ts = datetime.now()
        time_increment = 60.0 / hit_rate
        countries = []

        for i in range(num_hits):
            requests.append({'ts': ts,
                             'url': random.choice(urls),
                             'method': 'GET',
                             'query': '',
                             'code': 200,
                             'type': 'text/html',
                             'payload': 1000
                             })
            ts += timedelta(seconds=time_increment)
            countries.append(country)
        session['requests'] = requests

        fe = FeatureExtractor()
        features = fe.calculate_features_dict(session)

        assert features['request_rate'] == float(hit_rate)
        assert features['entropy'] < 30.0

        X = fe.fit_transform([session, session, session])
        assert np.average(X[:, -1]) > 0

