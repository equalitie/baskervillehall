import logging
import math
from sklearn.preprocessing import OneHotEncoder
from datetime import datetime
from collections import defaultdict
import numpy as np


class FeatureExtractor(object):
    def __init__(
            self,
            warmup_period=5,
            features=None,
            categorical_features=None,
            max_categories=3,
            min_category_frequency=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            logger=None
    ):
        super().__init__()
        self.warmup_period = 5
        self.max_categories = max_categories
        self.min_category_frequency = min_category_frequency
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.warmup_period = warmup_period
        self.datetime_format = datetime_format
        supported_features = [
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
            'duration'
        ]
        if features is None:
            features = supported_features
        self.features = features
        not_supported_features = set(self.features) - set(supported_features)
        if len(not_supported_features) > 0:
            raise RuntimeError(f'Feature(s) {not_supported_features} not supported.')

        supported_categorical_features = ['country', 'primary_session']
        if categorical_features is None:
            categorical_features = supported_categorical_features
        self.categorical_features = categorical_features
        not_supported_categorical_features = set(self.categorical_features) - \
                                             set(supported_categorical_features)
        if len(not_supported_categorical_features) > 0:
            raise RuntimeError(f'Categorical feature(s) {not_supported_features} not supported.')

        self.encoder = None
        self.mean = None
        self.std = None

    def preprocess_requests(self, requests):
        if len(requests) == 0:
            return requests

        parse_datetime = isinstance(requests[0]['ts'], str)
        timestamps = {}
        for r in requests:
            if parse_datetime:
                r['ts'] = datetime.strptime(r['ts'], self.datetime_format)
            timestamps[r['ts']] = r
        ordered_ts = [(ts, r) for ts, r in timestamps.items()]
        ordered_ts = sorted(ordered_ts, key=lambda x: x[0])
        grouped_requests = [x[1] for x in ordered_ts]

        result = []
        result.append(grouped_requests[0])
        start = grouped_requests[0]['ts']
        for i in range(1, len(grouped_requests)):
            r = requests[i]
            if (r['ts'] - start).total_seconds() < self.warmup_period:
                continue
            result.append(r)

        return result

    def calculate_features_dict(self, session):
        assert (len(session['requests']) > 0)

        features = {}
        requests = self.preprocess_requests(session['requests'])

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
        num_post = 0

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
            if r['method'] == 'POST':
                num_post += 1

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
        features['num_requests'] = len(requests)
        features['duration'] = session_duration
        features['request_rate'] = hits / session_duration * 60
        features['post_rate'] = num_post / hits
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
        features['entropy'] = entropy

        return features

    def get_categorical_vectors(self, sessions):
        data = []
        for s in sessions:
            data.append([s[f] for f in self.categorical_features])
        return np.array(data)

    def get_vectors(self, sessions):
        vectors = []
        for s in sessions:
            features_dict = self.calculate_features_dict(s)
            vector = np.zeros(len(self.features))
            for i in range(len(self.features)):
                vector[i] = features_dict.get(self.features[i], 0.0)
            vectors.append(vector)
        return np.array(vectors)

    def _normalize(self, X):
        return (X - self.mean) / self.std

    def fit_transform(self, sessions):
        X = self.get_vectors(sessions)
        self.mean = X.mean(axis=0)
        self.std = X.std(axis=0)
        self.std[self.std == 0] = 0.01
        X = self._normalize(X)
        if len(self.categorical_features) > 0:
            cat_vectors = self.get_categorical_vectors(sessions)

            self.encoder = OneHotEncoder(sparse_output=False,
                                         handle_unknown='ignore',
                                         min_frequency=self.min_category_frequency,
                                         max_categories=self.max_categories)
            self.encoder.fit(cat_vectors)

            X_cat = self.encoder.transform(cat_vectors)
            X = np.concatenate((X, X_cat), axis=1)
        return X

    def transform(self, sessions):
        X = self.get_vectors(sessions)
        X = self._normalize(X)
        if len(self.categorical_features) > 0:
            X_cat = self.encoder.transform(self.get_categorical_vectors(sessions))
            X = np.concatenate((X, X_cat), axis=1)
        return X

