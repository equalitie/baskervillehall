import logging
from collections import defaultdict

import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import IsolationForest


class BaskervillehallIsolationForest(object):

    def __init__(
            self,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            max_features=1.0,
            bootstrap=False,
            n_jobs=None,
            random_state=None,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = random_state

        self.categorical_encoders = None
        self.feature_names = None
        self.categorical_feature_names = None
        self.mean = None
        self.std = None
        self.isolation_forest = None

    @staticmethod
    def get_vector_from_feature_map(features, features_map):
        vector = np.zeros(len(features))
        for i in range(len(features)):
            vector[i] = features_map.get(features[i], 0.0)
        return vector

    def _normalize(self, Y):
        return (Y - self.mean) / self.std

    @staticmethod
    def get_features(session):
        requests = session['requests']
        features = {}

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
            request = requests[i]
            if i < len(requests) - 1:
                intervals.append((requests[i+1]['ts'] - request['ts']).total_seconds())
            code = request['code']
            if code // 100 == 4:
                num_4xx += 1
            if code // 100 == 5:
                num_5xx += 1
            url = request['url']
            payloads.append(request['payload'] + 1.0)
            slash_counts.append(len(url.split('/'))-1)
            url_map[url] += 1
            query_map[request['query']] += 1
            content_type = request['type']
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
        session_duration = session['duration']

        features['request_rate'] = hits / session_duration * 60
        mean_intervals = np.mean(intervals)
        features['request_interval_average'] = mean_intervals
        features['request_interval_std'] = np.sqrt(np.mean((intervals-mean_intervals)**2))
        features['response4xx_to_request_ratio'] = num_4xx / hits
        features['response5xx_to_request_ratio'] = num_5xx / hits
        features['top_page_to_request_ratio'] = max(url_map.values()) / hits
        features['unique_path_rate'] = unique_path / session_duration * 60
        features['unique_path_to_request_ratio'] = unique_path / hits
        features['unique_query_rate'] = unique_query  / session_duration * 60
        features['unique_query_to_unique_path_ratio'] = unique_query / unique_path
        features['image_to_html_ratio'] = float(num_image) / num_html if num_html > 0 else 10.0
        features['js_to_html_ratio'] = float(num_js) / num_html if num_html > 0 else 10.0
        features['css_to_html_ratio'] = float(num_css) / num_html if num_html > 0 else 10.0
        mean_depth = np.mean(slash_counts)
        features['path_depth_average'] = mean_depth
        features['path_depth_std'] = np.sqrt(np.mean((slash_counts-mean_depth)**2))
        features['payload_size_log_average'] = np.mean(np.log(payloads))

        return features

    def fit(
            self,
            features,
            feature_names,
            categorical_features=None,
            categorical_feature_names=None
    ):
        assert (features.shape[0] == len(categorical_features))

        self.categorical_encoders = []
        self.feature_names = feature_names
        self.categorical_feature_names = categorical_feature_names

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
