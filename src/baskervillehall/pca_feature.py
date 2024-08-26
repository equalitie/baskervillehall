import logging
import numpy as np
import re
import tensorflow_hub as hub
import tensorflow as tf
import tensorflow_text as text
from sklearn.decomposition import PCA
from urllib.parse import urlparse, unquote
from collections import OrderedDict
import logging
import numpy as np
import re

import tensorflow as tf
import tensorflow_text as text
from sklearn.decomposition import PCA
from urllib.parse import urlparse, unquote
from collections import OrderedDict

from baskervillehall.ebmedding_model import EmbeddingModel


def urls_to_text(urls):
    texts = []
    for url in urls:
        words = []
        if url == '/':
            words.append('home page')
        else:
            # Decode URL to convert %20, etc. into real characters
            url = unquote(url)

            # Parse the URL to isolate different parts
            parsed_url = urlparse(url)

            # Extract the subdomain, path, and query string
            # Assuming top-level domain and second-level domain are not 'real words'
            subdomain_path_query = parsed_url.netloc.split('.')[:-2] + parsed_url.path.split(
                '/') + parsed_url.query.split('&')

            # Join these components into a single string, replacing delimiters with a space
            combined = ' '.join(subdomain_path_query).replace('-', ' ').replace('_', ' ')

            # Regular expression to extract words (alphanumeric characters)
            word_pattern = re.compile(r'\b[a-zA-Z0-9]+\b')

            # Find all words in the combined string
            words += word_pattern.findall(combined)

        texts.append(' '.join([w for w in words if len(w) > 2]))
    return texts


class PCAFeature(object):

    def __init__(
            self,
            target_explained_variance=0.95,
            bert_model_name='small_bert/bert_en_uncased_L-2_H-128_A-2',
            logger=None
    ):
        super().__init__()
        self.mean = None
        self.std = None
        self.components = None
        self.threshold = None
        self.projector = None
        self.url_index = OrderedDict()
        self.url_vectors = None
        self.bert_model_name = bert_model_name

        self.target_explained_variance = target_explained_variance
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.bert_preprocess_model = None
        self.bert_model = None

        self.create_embeddings()

    def clear_embeddings(self):
        self.bert_preprocess_model = None
        self.bert_model = None

    def create_embeddings(self):
        self.bert_preprocess_model, self.bert_model = EmbeddingModel(self.bert_model_name, self.logger).get_models()

    def texts_to_vec(self, texts):
        self.logger.info('Tokenizing...')
        text_preprocessed = self.bert_preprocess_model(texts)
        self.logger.info('Embedding...')
        bert_results = self.bert_model(text_preprocessed)
        self.logger.info('Embedding done.')
        return bert_results["pooled_output"]

    def urls_to_vec(self, urls):
        return self.texts_to_vec(urls_to_text(urls))

    def embed_urls(self, sessions):
        index = 0
        extra_urls = []
        url_index_extra = OrderedDict()
        for s in sessions:
            for r in s['requests']:
                url = r['url']
                if url not in self.url_index:
                    extra_urls.append(url)
                    url_index_extra[url] = index
                    index += 1

        url_vectors_extra = self.urls_to_vec(extra_urls)
        return url_index_extra, url_vectors_extra

    def get_vector(self, url, url_index_extra, url_vectors_extra):
        if url in self.url_index:
            return self.url_vectors[self.url_index[url]]
        else:
            assert url in url_index_extra
            return url_vectors_extra[url_index_extra[url]]

    def create_dataset(self, sessions):
        url_index_extra, url_vectors_extra = self.embed_urls(sessions)
        X = []
        for s in sessions:
            vector_average = []
            for r in s['requests']:
                vector_average.append(self.get_vector(r['url'], url_index_extra, url_vectors_extra))
            X.append(np.average(vector_average, axis=0))
        return np.array(X)

    def _create_projector(self, components):
        PPT = np.dot(components, components.transpose())
        return np.identity(self.components.shape[0]) - PPT

    def normalize(self, Y):
        Z = (Y - self.mean) / self.std
        Z[np.isnan(Z)] = 0
        return Z

    def fit_transform(self, sessions):
        self.create_embeddings()
        Y = self.create_dataset(sessions)

        self.logger.info('Normalizing...')
        self.mean = Y.mean(axis=0)
        self.std = Y.std(axis=0)

        self.logger.info(f'Zero urls in training dataset: {self.std[self.std == 0].shape}')
        self.std[self.std == 0] = 1

        Z = self.normalize(Y)

        total_pca_components = min(int(self.url_vectors.shape[1] * 3 / 4), len(sessions))
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

        return self._score_vectors(Z)

    def _score_vectors(self, Z):
        Ya = np.matmul(self.projector, Z.transpose())
        Z[np.isnan(Z)] = 0
        return (Ya ** 2).mean(axis=0)

    def transform(self, sessions):
        self.create_embeddings()
        Y = self.create_dataset(sessions)
        Z = self.normalize(Y)
        return self._score_vectors(Z)




