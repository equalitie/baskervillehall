import logging
from collections import defaultdict

import numpy as np
from baskervillehall.behave_pca import BehavePCA
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import IsolationForest
from datetime import datetime
import math


class BaskervillehallIsolationForest(object):

    def __init__(
            self,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            warmup_period=5,
            feature_names=None,
            use_pca=True,
            categorical_feature_names=None,
            datetime_format='%Y-%m-%d %H:%M:%S',
            max_features=1.0,
            bootstrap=False,
            n_jobs=None,
            random_state=None,
            logger=None
    ):
        super().__init__()
        if categorical_feature_names is None:
            categorical_feature_names = ['country']
        if feature_names is None:
            feature_names = ['request_rate', 'request_interval_average', 'request_interval_std',
                             'response4xx_to_request_ratio', 'top_page_to_request_ratio',
                             'unique_path_to_request_ratio', 'path_depth_average', 'fresh_session',
                             'entropy']
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.use_pca = use_pca
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = random_state
        self.warmup_period = warmup_period
        self.datetime_format = datetime_format

        self.categorical_encoders = None
        self.feature_names = feature_names
        self.categorical_feature_names = categorical_feature_names
        self.mean = None
        self.std = None
        self.isolation_forest = None

    def set_n_estimators(self, n_estimators):
        self.n_estimators = n_estimators

    def set_contamination(self, contamination):
        self.contamination = contamination

    def _normalize(self, Y):
        return (Y - self.mean) / self.std

    def get_features(self, session):
        features_dict = self.calculate_features_dict(session)
        return self.get_vector_from_features_dict(features_dict)

    def get_categorical_features(self, session):
        return [session[f] for f in self.categorical_feature_names]

    def get_vector_from_features_dict(self, features_map):
        vector = np.zeros(len(self.feature_names))
        for i in range(len(self.feature_names)):
            vector[i] = features_map.get(self.feature_names[i], 0.0)
        return vector

    def preprocess_session(self, session):
        result = {
            'session_id': session['session_id'],
            'country': session['country'],
            'duration': session['duration'],
            'requests': list()
        }

        requests_original = session.get('requests', [])
        if len(requests_original) == 0:
            return result

        parse_datetime = isinstance(requests_original[0]['ts'], str)
        for r in requests_original:
            if parse_datetime:
                r['ts'] = datetime.strptime(r['ts'], self.datetime_format)

        requests = result['requests']
        requests.append(requests_original[0])
        start = requests[0]['ts']
        for i in range(1, len(requests_original)):
            r = requests_original[i]
            if (r['ts'] - start).total_seconds() < self.warmup_period:
                continue
            requests.append(r)

        return result

    def calculate_features_dict(self, session):
        assert (len(session['requests']) > 0)

        features = {}
        requests = session['requests']

        hits = float(len(requests))
        intervals = []
        num_4xx = 0
        num_5xx = 0
        url_map = defaultdict(int)
        query_map = defaultdict(int)
        num_html = 0
        num_image = 0
        num_js = 0
        num_css = 0
        slash_counts = []
        payloads = []

        for i in range(len(requests)):
            r = requests[i]
            if i == 0:
                intervals.append(0)
            else:
                intervals.append((r['ts'] - requests[i - 1]['ts']).total_seconds())
            code = r['code']
            if code // 100 == 4:
                num_4xx += 1
            if code // 100 == 5:
                num_5xx += 1
            url = r['url']
            payloads.append(r['payload'] + 1.0)
            slash_counts.append(len(url.split('/')) - 1)
            url_map[url] += 1
            query_map[r['query']] += 1
            content_type = r['type']
            if content_type == 'text/html' or \
                    content_type == 'text/html; charset=UTF-8' or \
                    content_type == 'text/html; charset=utf-8':
                num_html += 1
            elif 'image' in content_type:
                num_image += 1
            elif 'javascript' in content_type:
                num_js += 1
            elif content_type == 'text/css' or \
                    content_type == 'text/css; charset=UTF-8' or \
                    content_type == 'text/css; charset=utf-8':
                num_css += 1

        intervals = np.array(intervals)
        unique_path = float(len(url_map.keys()))
        unique_query = float(len(query_map.keys()))
        session_duration = float(session['duration'])
        if session_duration == 0.0:
            session_duration = 1.0

        entropy = 0.0
        for url, count in url_map.items():
            px = float(count) / len(url_map.keys())
            entropy += - px * math.log(px, 2)

        features['request_rate'] = hits / session_duration * 60
        mean_intervals = np.mean(intervals)
        features['request_interval_average'] = mean_intervals
        features['request_interval_std'] = np.sqrt(np.mean((intervals - mean_intervals) ** 2))
        features['response4xx_to_request_ratio'] = num_4xx / hits
        features['response5xx_to_request_ratio'] = num_5xx / hits
        features['top_page_to_request_ratio'] = max(url_map.values()) / hits
        features['unique_path_rate'] = unique_path / session_duration * 60
        features['unique_path_to_request_ratio'] = unique_path / hits
        features['unique_query_rate'] = unique_query / session_duration * 60
        features['unique_query_to_unique_path_ratio'] = unique_query / unique_path
        features['image_to_html_ratio'] = float(num_image) / num_html if num_html > 0 else 10.0
        features['js_to_html_ratio'] = float(num_js) / num_html if num_html > 0 else 10.0
        features['css_to_html_ratio'] = float(num_css) / num_html if num_html > 0 else 10.0
        mean_depth = np.mean(slash_counts)
        features['path_depth_average'] = mean_depth
        features['path_depth_std'] = np.sqrt(np.mean((slash_counts - mean_depth) ** 2))
        features['payload_size_log_average'] = np.mean(np.log(payloads))
        features['fresh_session'] = 1 if session.get('fresh_sessions', False) else 0
        features['entropy'] = entropy

        return features

    def fit(
            self,
            features,
            categorical_features=None
    ):
        assert (features.shape[0] == len(categorical_features))

        self.categorical_encoders = []

        categorical_vectors = []
        self.categorical_encoders = []
        for i in range(len(categorical_features[0])):
            encoder = LabelEncoder()
            classes = [categorical_features[k][i] for k in range(len(categorical_features))]
            classes.append('_other')
            encoder.fit(classes)
            categorical_vectors.append(encoder.transform(
                [categorical_features[k][i] for k in range(len(categorical_features))]))
            self.categorical_encoders.append(encoder)

        categorical_vectors = np.array(categorical_vectors).transpose()
        Y = np.concatenate((features, categorical_vectors), axis=1)

        self.mean = Y.mean(axis=0)
        self.std = Y.std(axis=0)
        self.std[self.std == 0] = 1
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

    def fit_sessions(self, sessions):
        sessions =[self.preprocess_session(session) for session in sessions]
        features = list()
        categorical_features = list()
        for session in sessions:
            features.append(self.get_features(session))
            categorical_features.append(self.get_categorical_features(session))
        features = np.array(features)
        if self.use_pca:
            self.pca_model = BehavePCA()
            scores = self.pca_model.fit(sessions)
            features = np.concatenate((features, np.array([[s] for s in scores])), axis=1)
        return self.fit(features, categorical_features)

    def score(self, features, categorical_features):
        assert (features.shape[0] == len(categorical_features))
        assert (len(categorical_features[0]) == len(self.categorical_encoders))

        categorical_vectors = []
        for i in range(len(self.categorical_encoders)):
            categorical_vectors.append(self.categorical_encoders[i].transform(
                [categorical_features[k][i] if categorical_features[k][i] in self.categorical_encoders[i].classes_ else
                 '_other' for k in range(len(categorical_features))]
            ))

        categorical_vectors = np.array(categorical_vectors).transpose()

        Y = np.concatenate((features, categorical_vectors), axis=1)
        Z = self._normalize(Y)
        scores = self.isolation_forest.decision_function(Z)
        return scores

    def score_sessions(self, sessions):
        categorical_features = []
        features = []
        sessions = [self.preprocess_session(session) for session in sessions]

        for i in range(len(sessions)):
            categorical_features.append(self.get_categorical_features(sessions[i]))
            features.append(self.get_features(sessions[i]))

        features = np.array(features)

        if self.use_pca:
            scores = self.pca_model.score(sessions)
            features = np.concatenate((features, np.array([[s] for s in scores])), axis=1)

        return self.score(features, categorical_features)
