import logging
import math
import copy
from sklearn.preprocessing import OrdinalEncoder
from datetime import datetime
from collections import defaultdict
import numpy as np
import pytz

from baskervillehall.pca_feature import PCAFeature


class FeatureExtractor(object):
    def __init__(
            self,
            warmup_period=5,
            features=None,
            pca_feature=False,
            categorical_features=None,
            max_categories=3,
            min_category_frequency=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            normalize=False,
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
            'duration',
            'edge_count',
            'static_ratio',
            'ua_count',
            'api_ratio',
            'num_ciphers',
            'num_languages',
            'ua_score',
            'hour_bucket',
            'odd_hour',
            'fingerprints_score'
        ]
        self.pca_feature = pca_feature
        if features is None:
            features = supported_features
        self.features = features
        not_supported_features = set(self.features) - set(supported_features)
        if len(not_supported_features) > 0:
            raise RuntimeError(f'Feature(s) {not_supported_features} not supported.')

        supported_categorical_features = [
            'country',
            'primary_session',
            'bad_bot',
            'human',
            'cipher',
            'valid_browser_ciphers',
            'weak_cipher',
            'headless_ua',
            'bot_ua',
            'ai_bot_ua',
            'verified_bot',
            'datacenter_asn',
            'short_ua',
            'asset_only',
            'timezone'
        ]
        if categorical_features is None:
            categorical_features = supported_categorical_features
        self.categorical_features = categorical_features
        not_supported_categorical_features = set(self.categorical_features) - \
                                          set(supported_categorical_features)
        if len(not_supported_categorical_features) > 0:
            raise RuntimeError(f'Categorical feature(s) {not_supported_categorical_features} not supported.')

        if self.pca_feature:
            self.pca = PCAFeature(logger=self.logger)
        else:
            self.pca = None

        self.normalize = normalize
        self.encoder = None
        self.mean = None
        self.std = None

    def clear_embeddings(self):
        if self.pca_feature:
            self.pca.clear_embeddings()

    def preprocess_requests(self, requests):
        if len(requests) == 0:
            return requests

        parse_datetime = isinstance(requests[0]['ts'], str)
        result = []
        for r in requests:
            rf = copy.deepcopy(r)
            if parse_datetime:
                rf['ts'] = datetime.strptime(rf['ts'], self.datetime_format)
            result.append(rf)

        return sorted(result, key=lambda x: x['ts'])

    def calculate_entropy(self, counts):
        entropy = 0.0
        for v, count in counts.items():
            px = float(count) / len(counts.keys())
            entropy += - px * math.log(px, 2)
        return entropy

    def is_api_request(self, request):
        ctype = request.get('type', 'text/html')
        if ctype == 'application/json' or \
           ctype == 'application/xml' or \
           ctype == 'application/graphql' or \
           ctype == 'application/ld+json' or \
           ctype == 'multipart/form-data' or \
           ctype == 'application/x-www-form-urlencoded':
            return True

        url = request.get('url', '/')
        if '/api/' in url or '/v1/' in url or '/rest/' in url or \
           '/graphql/' in url or '/gql/' in url or \
           '/auth/' in url or '/oauth/' in url or '/token/' in url or \
           '/webhook/' in url or '/callback/' in url or \
           '/payment/' in url or '/checkout/' in url or '/orders/' in url or \
           '/system/' in url or '/monitoring/' in url:
            return True

    @staticmethod
    def hour_bucket(hour):
        if 2 <= hour < 5:
            return 0  # "odd"
        elif 5 <= hour < 10:
            return 1  # "morning"
        elif 10 <= hour < 17:
            return 2  # "work_hours"
        elif 17 <= hour < 22:
            return 3  # "evening"
        else:
            return 4  # "late_night"

    def get_hour(self, session):
        if isinstance(session['start'], str):
            ts_utc = datetime.strptime(session['start'], self.datetime_format)
        else:
            ts_utc = session['start']

        tz = session.get('timezone', '')
        if len(tz) == 0:
            return 0
        user_tz = pytz.timezone(tz)
        user_local_time = ts_utc.replace(tzinfo=pytz.utc).astimezone(user_tz)
        return user_local_time.hour

    def is_odd_hour(self, hour):
        return hour >= 2 and hour < 6

    def calculate_features_dict(self, session):
        # assert (len(session['requests']) > 0)

        features = {}
        requests = self.preprocess_requests(session['requests'])

        hits = float(len(requests))
        intervals = []
        num_4xx = 0
        num_5xx = 0
        url_map = defaultdict(int)
        edge_map = defaultdict(int)
        ua_map = defaultdict(int)
        query_map = defaultdict(int)
        num_html = 0
        num_image = 0
        num_js = 0
        num_css = 0
        slash_counts = []
        payloads = []
        num_post = 0
        num_static = 0
        api_count = 0

        for i in range(len(requests)):
            r = requests[i]
            if i == 0:
                intervals.append(0)
            else:
                intervals.append((r['ts'] - requests[i - 1]['ts']).total_seconds())
            code = int(r.get('code', 200))
            if code // 100 == 4:
                num_4xx += 1
            if code // 100 == 5:
                num_5xx += 1
            url = r.get('url', '/')
            payloads.append(int(r.get('payload', 0)) + 1.0)
            slash_counts.append(len(url.split('/')) - 1)
            url_map[url] += 1
            edge_map[r.get('edge', '')] += 1
            ua_map[r.get('ua', '')] += 1
            query_map[r.get('query', '')] += 1
            content_type = r.get('type', 'text/html')
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
            if r.get('method', 'GET') == 'POST':
                num_post += 1
            if r.get('static', False):
                num_static += 1
            if self.is_api_request(r):
                api_count += 1

        intervals = np.array(intervals)
        unique_path = float(len(url_map.keys()))
        unique_query = float(len(query_map.keys()))
        session_duration = float(session['duration'])
        if session_duration == 0.0:
            session_duration = 1.0

        features['num_requests'] = len(requests)
        features['duration'] = session_duration
        features['request_rate'] = hits / session_duration * 60
        features['post_rate'] = num_post / hits
        mean_intervals = np.mean(intervals)
        features['request_interval_average'] = mean_intervals
        features['request_interval_std'] = np.std(intervals) if len(intervals) > 1 else 0  
        features['response4xx_to_request_ratio'] = num_4xx / hits
        features['response5xx_to_request_ratio'] = num_5xx / hits
        features['top_page_to_request_ratio'] = max(url_map.values()) / hits
        features['unique_path_rate'] = unique_path / session_duration * 60
        features['unique_path_to_request_ratio'] = unique_path / hits
        features['unique_query_rate'] = unique_query / session_duration * 60
        features['unique_query_to_unique_path_ratio'] = unique_query / unique_path if unique_path > 0 else 0 
        features['image_to_html_ratio'] = float(num_image) / num_html if num_html > 0 else 10.0
        features['js_to_html_ratio'] = float(num_js) / num_html if num_html > 0 else 10.0
        features['css_to_html_ratio'] = float(num_css) / num_html if num_html > 0 else 10.0
        mean_depth = np.mean(slash_counts)
        features['path_depth_average'] = mean_depth
        features['path_depth_std'] = np.std(slash_counts) if len(slash_counts) > 1 else 0  
        features['payload_size_log_average'] = np.mean(np.log(payloads))
        features['entropy'] = self.calculate_entropy(url_map)
        features['edge_count'] = len(edge_map.keys())
        features['ua_count'] = len(ua_map.keys())
        features['static_ratio'] = float(num_static) / hits
        features['api_ratio'] = float(api_count) / hits
        features['num_ciphers'] = len(session.get('ciphers', []))
        features['num_languages'] = session['num_languages']
        features['ua_score'] = float(session.get('ua_score', 0.0))

        hour = self.get_hour(session)
        features['hour_bucket'] = self.hour_bucket(hour)
        features['odd_hour'] = self.is_odd_hour(hour)
        features['fingerprints_score'] = float(session.get('fingerprints_score', 0.0))
        return features

    def get_categorical_vectors(self, sessions):
        data = []
        for s in sessions:
            data.append([s.get(f, False) for f in self.categorical_features])
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

    def get_all_features(self):
        res = []
        res += self.features
        if self.pca_feature:
            res.append('pca')
        res += self.categorical_features
        return res

    def fit_transform(self, sessions):
        X = self.get_vectors(sessions)
        self.mean = X.mean(axis=0)
        self.std = X.std(axis=0)
        self.std[self.std == 0] = 0.01

        if self.pca_feature:
            X_pca = self.pca.fit_transform(sessions)
            X_pca = np.reshape(X_pca, (X_pca.shape[0], 1))
            X = np.concatenate((X, X_pca), axis=1)

        if self.normalize:
            X = self._normalize(X)

        if len(self.categorical_features) > 0:
            cat_vectors = self.get_categorical_vectors(sessions)

            self.encoder = OrdinalEncoder(
                handle_unknown='use_encoded_value',
                unknown_value=10000
            )
            self.encoder.fit(cat_vectors)
            X_cat = self.encoder.transform(cat_vectors)
            X = np.concatenate((X, X_cat), axis=1)

        return X

    def transform(self, sessions):
        X = self.get_vectors(sessions)

        if self.pca_feature:
            assert self.pca
            X_pca = self.pca.transform(sessions)
            X_pca = np.reshape(X_pca, (X_pca.shape[0], 1))
            X = np.concatenate((X, X_pca), axis=1)

        if self.normalize:
            X = self._normalize(X)

        if len(self.categorical_features) > 0:
            X_cat = self.encoder.transform(self.get_categorical_vectors(sessions))
            X = np.concatenate((X, X_cat), axis=1)

        return X
