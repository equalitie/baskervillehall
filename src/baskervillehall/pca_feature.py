import logging
import numpy as np
import re
import tensorflow_hub as hub
import tensorflow as tf
import tensorflow_text as text
from sklearn.decomposition import PCA
from urllib.parse import urlparse, unquote
from collections import OrderedDict


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

        self.target_explained_variance = target_explained_variance
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        map_name_to_handle = {
            'bert_en_uncased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_L-12_H-768_A-12/3',
            'bert_en_cased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_cased_L-12_H-768_A-12/3',
            'bert_multi_cased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_multi_cased_L-12_H-768_A-12/3',
            'small_bert/bert_en_uncased_L-2_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-2_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-2_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-2_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-768_A-12/1',
            'small_bert/bert_en_uncased_L-4_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-4_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-4_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-4_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-4_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-4_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-4_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-4_H-768_A-12/1',
            'small_bert/bert_en_uncased_L-6_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-6_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-6_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-6_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-6_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-6_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-6_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-6_H-768_A-12/1',
            'small_bert/bert_en_uncased_L-8_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-8_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-8_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-8_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-8_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-8_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-8_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-8_H-768_A-12/1',
            'small_bert/bert_en_uncased_L-10_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-10_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-10_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-10_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-10_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-10_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-10_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-10_H-768_A-12/1',
            'small_bert/bert_en_uncased_L-12_H-128_A-2':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-12_H-128_A-2/1',
            'small_bert/bert_en_uncased_L-12_H-256_A-4':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-12_H-256_A-4/1',
            'small_bert/bert_en_uncased_L-12_H-512_A-8':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-12_H-512_A-8/1',
            'small_bert/bert_en_uncased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-12_H-768_A-12/1',
            'albert_en_base':
                'https://tfhub.dev/tensorflow/albert_en_base/2',
            'electra_small':
                'https://tfhub.dev/google/electra_small/2',
            'electra_base':
                'https://tfhub.dev/google/electra_base/2',
            'experts_pubmed':
                'https://tfhub.dev/google/experts/bert/pubmed/2',
            'experts_wiki_books':
                'https://tfhub.dev/google/experts/bert/wiki_books/2',
            'talking-heads_base':
                'https://tfhub.dev/tensorflow/talkheads_ggelu_bert_en_base/1',
        }

        map_model_to_preprocess = {
            'bert_en_uncased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'bert_en_cased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_cased_preprocess/3',
            'small_bert/bert_en_uncased_L-2_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-2_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-2_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-2_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-4_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-4_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-4_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-4_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-6_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-6_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-6_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-6_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-8_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-8_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-8_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-8_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-10_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-10_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-10_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-10_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-12_H-128_A-2':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-12_H-256_A-4':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-12_H-512_A-8':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'small_bert/bert_en_uncased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'bert_multi_cased_L-12_H-768_A-12':
                'https://tfhub.dev/tensorflow/bert_multi_cased_preprocess/3',
            'albert_en_base':
                'https://tfhub.dev/tensorflow/albert_en_preprocess/3',
            'electra_small':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'electra_base':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'experts_pubmed':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'experts_wiki_books':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
            'talking-heads_base':
                'https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3',
        }

        tfhub_handle_encoder = map_name_to_handle[bert_model_name]
        tfhub_handle_preprocess = map_model_to_preprocess[bert_model_name]

        self.bert_preprocess_model = hub.KerasLayer(tfhub_handle_preprocess)
        self.bert_model = hub.KerasLayer(tfhub_handle_encoder)

    def texts_to_vec(self, texts):
        self.logger.info('Tokenizing...')
        text_preprocessed = self.bert_preprocess_model(texts)
        self.logger.info('Embedding...')
        bert_results = self.bert_model(text_preprocessed)
        return bert_results["pooled_output"]

    def urls_to_vec(self, urls):
        return self.texts_to_vec(urls_to_text(urls))

    def embed_urls(self, sessions):
        index = 0
        extra_urls = []
        for s in sessions:
            for r in s['requests']:
                url = r['url']
                if url not in self.url_index:
                    extra_urls.append(url)
                    self.url_index[url] = index
                    index += 1

        urls_extra_vectors = self.urls_to_vec(extra_urls)

        if self.url_vectors is None:
            self.url_vectors = urls_extra_vectors
        else:
            self.url_vectors = tf.concat([self.url_vectors, urls_extra_vectors], 0)

    def get_vector(self, url):
        return self.url_vectors[self.url_index[url]]

    def create_dataset(self, sessions):
        self.embed_urls(sessions)
        X = []
        for s in sessions:
            vector_average = []
            for r in s['requests']:
                vector_average.append(self.get_vector(r['url']))
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
        Y = self.create_dataset(sessions)
        Z = self.normalize(Y)
        return self._score_vectors(Z)




