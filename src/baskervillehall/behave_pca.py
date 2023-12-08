import logging
from collections import defaultdict

import numpy as np
import json
from sklearn.decomposition import PCA


class BehavePCA(object):

    def __init__(
            self,
            num_urls=300,
            min_hits_for_url=20,
            not_existing_penalty=10,
            num_total_pca_components=500,
            target_explained_variance=0.98,
            target_false_positive_rate=0.02,
            logger=None
    ):
        super().__init__()

        self.url_index = None
        self.url_dict = None
        self.mean = None
        self.std = None
        self.components = None
        self.threshold = None
        self.projector = None

        self.target_explained_variance = target_explained_variance
        self.num_total_pca_components = num_total_pca_components
        self.num_urls = num_urls
        self.min_hits_for_url = min_hits_for_url
        self.not_existing_penalty = not_existing_penalty
        self.num_total_pca_components = num_total_pca_components
        self.target_false_positive_rate = target_false_positive_rate

        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

    def _create_projector(self, components):
        PPT = np.dot(components, components.transpose())
        return np.identity(self.components.shape[0]) - PPT

    def create_sample(self, urls):
        sample = np.zeros(len(self.url_index))
        for url in urls:
            if url in self.url_index:
                sample[self.url_index[url]] += 1
            elif url in self.url_dict:
                sample[self.url_index['tail']] += 1
            else:
                sample[self.url_index['not_existing']] += self.not_existing_penalty

        return sample

    def fit(self, sessions):
        self.url_dict = defaultdict(int)

        for session in sessions:
            for url in urls:
                self.url_dict[url] += 1

        # build url-index dictionary from top hits
        self.logger.info(f'Total {len(dataset)} sessions, {len(self.url_dict.items())} urls')
        all_urls = sorted([(k, v) for k, v in self.url_dict.items()], key=lambda v: v[1], reverse=True)
        actual_num_urls = min(self.num_urls, len(all_urls))
        for i in range(actual_num_urls):
            if all_urls[i][1] < self.min_hits_for_url:
                actual_num_urls = i
                break
        self.url_index = {all_urls[i][0]: i for i in range(actual_num_urls)}
        self.url_index['tail'] = len(self.url_index)
        self.url_index['not_existing'] = len(self.url_index)

        if len(dataset) < self.num_total_pca_components:
            self.logger.info(f'Number of sessions ({len(dataset)} is less than the '
                             f'number of PCA components({self.num_total_pca_components})')

        self.logger.info('Creating samples from sessions...')
        Y = []
        for urls in dataset:
            Y.append(self.create_sample(urls=urls))
        Y = np.array(Y)

        self.logger.info('Normalizing...')
        self.mean = Y.mean(axis=0)
        self.std = Y.std(axis=0)

        self.logger.info(f'Zero urls in training dataset: {self.std[self.std == 0].shape}')
        self.std[self.std == 0] = 1

        Z = (Y - self.mean) / self.std
        Z[np.isnan(Z)] = 0

        total_pca_components = min(self.num_total_pca_components, len(self.url_index), len(dataset))
        pca = PCA(n_components=total_pca_components)
        self.logger.info(
            f'Fitting PCA with {total_pca_components} components for {len(Z)} samples of size {len(self.url_index)}')
        pca.fit(Z)

        explained_variance = pca.explained_variance_ratio_.cumsum()

        # find the number of pca components
        num_components = total_pca_components
        for i in range(len(explained_variance)):
            # self.logger.info(f'{i}, {explained_variance[i]}')
            if explained_variance[i] >= self.target_explained_variance:
                num_components = i
                break
        self.logger.info(f'Number of components = {num_components}')

        self.components = pca.components_[:num_components, :].transpose()

        self.projector = self._create_projector(self.components)

        score_train = self._score_vectors(Z)

        # find the threshold
        self.logger.info('Finding threshold...')
        self.threshold = 0
        false_positive_rate = 0.0
        while self.threshold < 10.0:
            false_positive_rate = len(score_train[score_train > self.threshold]) / len(score_train)
            # self.logger.info(f'threshold={self.threshold}, false_positive_rate={false_positive_rate}')
            if false_positive_rate < self.target_false_positive_rate:
                break
            self.threshold += 0.01
        self.logger.info(f'Threshold={self.threshold}, false_positive_rate={false_positive_rate}')

        return self

    def _score_vectors(self, Z):
        Ya = np.matmul(self.projector, Z.transpose())
        Z[np.isnan(Z)] = 0
        return (Ya ** 2).mean(axis=0)

    def score(self, sessions):
        Y = []
        for urls in sessions:
            Y.append(self.create_sample(urls=urls))

        Y = np.array(Y)

        Z = (Y - self.mean) / self.std
        Z[np.isnan(Z)] = 0

        return self._score_vectors(Z)

    def predict(self, sessions):
        score = self.score(sessions)
        return score > self.threshold



