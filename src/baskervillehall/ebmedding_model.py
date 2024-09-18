from baskervillehall.singleton import Singleton
import tensorflow_hub as hub
import tensorflow_text as text


class EmbeddingModel(metaclass=Singleton):

    def __init__(self,
                 model_name='small_bert/bert_en_uncased_L-2_H-128_A-2',
                 logger=None):
        super().__init__()
        self.logger = logger
        self.model_name = model_name
        self.bert_preprocess_model = None
        self.bert_model = None

    def get_models(self):
        if self.bert_model is None or self.bert_preprocess_model is None:
            self.logger.info('Embedding singleton: loading embeddings models...')
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

            tfhub_handle_encoder = map_name_to_handle[self.model_name]
            tfhub_handle_preprocess = map_model_to_preprocess[self.model_name]

            self.bert_preprocess_model = hub.KerasLayer(tfhub_handle_preprocess)
            self.bert_model = hub.KerasLayer(tfhub_handle_encoder)

        return self.bert_preprocess_model, self.bert_model

