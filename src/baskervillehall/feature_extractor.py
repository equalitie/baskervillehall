import logging
import math
import copy
from datetime import datetime
from collections import defaultdict, Counter
import numpy as np
import pytz

from baskervillehall.pca_feature import PCAFeature
from sklearn.preprocessing import OneHotEncoder


class FeatureExtractor(object):
    def __init__(
            self,
            features=None,
            pca_feature=False,
            categorical_features=None,
            datetime_format='%Y-%m-%d %H:%M:%S',
            normalize=False,
            use_onehot_encoding=False,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.datetime_format = datetime_format
        supported_features = [
            'request_rate', 'post_rate', 'request_interval_average',
            'request_interval_std', 'response4xx_to_request_ratio',
            'response5xx_to_request_ratio', 'top_page_to_request_ratio',
            'unique_path_rate', 'unique_path_to_request_ratio',
            'unique_query_rate', 'unique_query_to_unique_path_ratio',
            'image_to_html_ratio', 'js_to_html_ratio', 'css_to_html_ratio',
            'path_depth_average', 'path_depth_std', 'payload_size_log_average',
            'entropy', 'num_requests', 'duration', 'edge_count', 'static_ratio',
            'ua_count', 'api_ratio', 'num_ciphers', 'num_languages',
            'ua_score', 'hour_bucket', 'odd_hour', 'fingerprints_score',
            'interval_cv','interval_consistency'
        ]
        self.pca_feature = pca_feature
        if features is None:
            features = supported_features
        self.features = features
        not_supported = set(self.features) - set(supported_features)
        if not_supported:
            raise RuntimeError(f'Feature(s) {not_supported} not supported.')

        supported_cats = [
            'country', 'primary_session', 'bad_bot', 'human', 'cipher',
            'valid_browser_ciphers', 'weak_cipher', 'headless_ua', 'bot_ua',
            'ai_bot_ua', 'verified_bot', 'datacenter_asn', 'short_ua',
            'asset_only', 'timezone'
        ]
        if categorical_features is None:
            categorical_features = supported_cats
        self.categorical_features = categorical_features
        not_sup_cat = set(self.categorical_features) - set(supported_cats)
        if not_sup_cat:
            raise RuntimeError(f'Categorical feature(s) {not_sup_cat} not supported.')

        self.pca = PCAFeature(logger=self.logger) if self.pca_feature else None
        self.normalize = normalize
        self.use_onehot = use_onehot_encoding
        self.mean = None
        self.std = None
        # for freq encoding
        self.freq_maps = {}
        # for one-hot
        self.onehot_encoder = None

    def clear_embeddings(self):
        if self.pca_feature:
            self.pca.clear_embeddings()

    def preprocess_requests(self, requests):
        if not requests:
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
        for count in counts.values():
            px = count / len(counts)
            entropy += -px * math.log(px, 2)
        return entropy

    def is_api_request(self, request):
        ctype = request.get('type', 'text/html')
        api_ctypes = {
            'application/json', 'application/xml', 'application/graphql',
            'application/ld+json', 'multipart/form-data',
            'application/x-www-form-urlencoded'
        }
        if ctype in api_ctypes:
            return True
        url = request.get('url', '/')
        api_paths = ['/api/', '/v1/', '/rest/', '/graphql/', '/gql/',
                     '/auth/', '/oauth/', '/token/', '/webhook/',
                     '/callback/', '/payment/', '/checkout/',
                     '/orders/', '/system/', '/monitoring/']
        return any(p in url for p in api_paths)

    @staticmethod
    def hour_bucket(hour):
        if 2 <= hour < 5:
            return 0
        if 5 <= hour < 10:
            return 1
        if 10 <= hour < 17:
            return 2
        if 17 <= hour < 22:
            return 3
        return 4

    def get_hour(self, session):
        ts = session['start']
        if isinstance(ts, str):
            ts = datetime.strptime(ts, self.datetime_format)
        tz = session.get('timezone') or 'UTC'
        user_local = ts.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tz))
        return user_local.hour

    def is_odd_hour(self, hour):
        return 2 <= hour < 6

    def calculate_features_dict(self, session):
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

        intervals = np.array(intervals[1:])
        mean_iv = intervals.mean() if intervals.size else 0.0
        std_iv = intervals.std() if intervals.size else 0.0

        unique_path = float(len(url_map.keys()))
        unique_query = float(len(query_map.keys()))
        session_duration = float(session['duration'])
        if session_duration == 0.0:
            session_duration = 1.0

        features['num_requests'] = len(requests)
        features['duration'] = session_duration
        features['request_rate'] = hits / session_duration * 60
        features['post_rate'] = num_post / hits
        features['request_interval_average'] = float(mean_iv)
        features['request_interval_std'] = std_iv if len(intervals) > 1 else 0
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
        features['interval_cv'] = float(std_iv / mean_iv if mean_iv > 0 else 0.0)

        # Interval consistency around the mode
        #   - round intervals to nearest integer second
        #   - find the most common rounded value (mode)
        #   - count how many actual intervals fall within ±eps seconds of that mode
        rounded = np.round(intervals).astype(int) if intervals.size else np.array([])
        if rounded.size:
            mode_val, mode_count = Counter(rounded).most_common(1)[0]
            eps = 0.5  # half-second window around the mode
            close_mask = np.abs(intervals - mode_val) <= eps
            features['interval_consistency'] = float(close_mask.sum()) / len(intervals)
        else:
            features['interval_consistency'] = 0.0
        return features


    def get_vectors(self, sessions):
        return np.vstack([
            np.array([self.calculate_features_dict(s).get(f, 0.0) for f in self.features], dtype=float)
            for s in sessions
        ])

    def get_all_features(self):
        res = []
        res += self.features
        if self.pca_feature:
            res.append('pca')
        res += self.categorical_features
        return res

    def fit_transform(self, sessions):
        # Numeric features
        X = self.get_vectors(sessions)
        if self.pca_feature:
            X_p = self.pca.fit_transform(sessions).reshape(-1, 1)
            X = np.concatenate([X, X_p], axis=1)
        if self.normalize:
            self.mean = X.mean(axis=0)
            self.std = X.std(axis=0)
            self.std[self.std == 0] = 1.0
            X = (X - self.mean) / self.std
        # Categorical
        if self.use_onehot:
            # One-hot encoding
            cat_data = np.array([[s.get(f, None) for f in self.categorical_features]
                                  for s in sessions])
            self.onehot_encoder = OneHotEncoder(handle_unknown='ignore', sparse=False)
            X_cat = self.onehot_encoder.fit_transform(cat_data)
        else:
            # Frequency encoding
            N = len(sessions)
            X_cat = np.zeros((N, len(self.categorical_features)))
            for j, feat in enumerate(self.categorical_features):
                vals = [s.get(feat, None) for s in sessions]
                counts = Counter(vals)
                freq_map = {k: v / N for k, v in counts.items()}
                self.freq_maps[feat] = freq_map
                X_cat[:, j] = [freq_map.get(v, 0.0) for v in vals]
        return np.concatenate([X, X_cat], axis=1)

    def transform(self, sessions):
        X = self.get_vectors(sessions)
        if self.pca_feature:
            X_p = self.pca.transform(sessions).reshape(-1, 1)
            X = np.concatenate([X, X_p], axis=1)
        if self.normalize:
            X = (X - self.mean) / self.std
        if self.use_onehot:
            cat_data = np.array([[s.get(f, None) for f in self.categorical_features]
                                  for s in sessions])
            X_cat = self.onehot_encoder.transform(cat_data)
        else:
            N = len(sessions)
            X_cat = np.zeros((N, len(self.categorical_features)))
            for j, feat in enumerate(self.categorical_features):
                fmap = self.freq_maps.get(feat, {})
                vals = [s.get(feat, None) for s in sessions]
                X_cat[:, j] = [fmap.get(v, 0.0) for v in vals]
        return np.concatenate([X, X_cat], axis=1)
