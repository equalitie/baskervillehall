import logging
import time
import json
from collections import deque
import numpy as np

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest, ModelType
from baskervillehall.host_selector import HostSelector
from baskervillehall.model_io import ModelIO
from kafka import KafkaConsumer, TopicPartition


class BaskervillehallTrainer(object):

    def __init__(
            self,
            warmup_period=5,
            features=None,
            categorical_features=None,
            pca_feature=False,
            max_categories=3,
            min_category_frequency=10,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            num_sessions=10000,
            min_session_duration=20,
            min_number_of_requests=2,
            accepted_contamination=0.1,
            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            max_features=1.0,
            bootstrap=True,
            n_jobs=None,
            random_state=None,
            datetime_format='%Y-%m-%d %H:%M:%S',

            train_batch_size=5,
            model_ttl_in_minutes=120,
            dataset_delay_from_now_in_minutes=60,
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            min_dataset_size=100,
            small_dataset_size=500,
            kafka_timeout_ms=1000,
            kafka_max_size=200,
            wait_time_minutes=5,
            logger=None
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}

        self.max_categories = max_categories
        self.min_category_frequency = min_category_frequency
        self.warmup_period = warmup_period
        self.features = features
        self.categorical_features = categorical_features
        self.pca_feature = pca_feature
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_sessions = num_sessions
        self.min_session_duration = min_session_duration
        self.min_number_of_requests = min_number_of_requests
        self.datetime_format = datetime_format

        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = random_state
        self.wait_time_minutes = wait_time_minutes

        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.model_ttl_in_minutes = model_ttl_in_minutes
        self.train_batch_size = train_batch_size
        self.kafka_connection = kafka_connection
        self.kafka_timeout_ms = kafka_timeout_ms
        self.kafka_max_size = kafka_max_size
        self.s3_connection = s3_connection
        self.s3_path = s3_path
        self.min_dataset_size = min_dataset_size
        self.small_dataset_size = small_dataset_size
        self.dataset_delay_from_now_in_minutes = dataset_delay_from_now_in_minutes
        self.accepted_contamination = accepted_contamination

    def train_and_save_model(
            self,
            sessions,
            host,
            model_type
    ):
        if len(sessions) == 0:
            return False
        self.logger.info(f'Training model for {host}, {model_type.value}')
        model_io = ModelIO(**self.s3_connection, logger=self.logger)
        categorical_features = self.categorical_features
        if model_type == ModelType.GENERIC:
            if 'human' not in categorical_features:
                categorical_features.append('human')
            if 'bad_bot' not in categorical_features:
                categorical_features.append('bad_bot')
        model = BaskervillehallIsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=self.contamination,
            max_features=self.max_features,
            warmup_period=self.warmup_period,
            features=self.features,
            categorical_features=categorical_features,
            pca_feature=self.pca_feature,
            max_categories=self.max_categories,
            min_category_frequency=self.min_category_frequency,
            datetime_format=self.datetime_format,
            bootstrap=self.bootstrap,
            n_jobs=self.n_jobs,
            random_state=self.random_state,
            logger=self.logger,
        )

        old_model = model_io.load(self.s3_path, host, model_type)
        if old_model:
            scores, _ = old_model.transform(sessions)
            contamination = float(len(scores[scores < 0])) / len(scores)
            if contamination > self.accepted_contamination:
                self.logger.info(f'Skipping training. High contamination: {contamination:.2f}. '
                                 f'Host = {host}. {scores.shape[0]} records, type={model_type.value}')
                return False

        self.logger.info(f'Training host {host}, dataset size {len(sessions)}, type={model_type.value}')

        if len(sessions) <= self.min_dataset_size:
            self.logger.info(f'Skipping training. Too few sessions: {len(sessions)}. Host = {host}.'
                             f'The minimum is {self.min_dataset_size}, type={model_type.value}')
            return False

        if len(sessions) <= self.small_dataset_size:
            model.set_n_estimators(len(self.features))
            new_contamination = self.contamination * 2
            if new_contamination > 0.5:
                new_contamination = 0.5
            model.set_contamination(new_contamination)

        model.fit(sessions)

        self.logger.info(f'@@@ Saving model for {host}, type={model_type.value}...')
        model.clear_embeddings()
        model_io.save(model, self.s3_path, host, model_type=model_type)
        return True

    def run(self):
        try:
            consumer = KafkaConsumer(
                **self.kafka_connection
            )

            host_selector = HostSelector(
                ttl_in_minutes=self.model_ttl_in_minutes,
                logger=self.logger
            )
            self.logger.info(self.kafka_connection['bootstrap_servers'])
            self.logger.info(f'Starting training on topic {self.topic_sessions}')

            consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
            consumer.seek_to_beginning()
            while True:
                self.logger.info(f'Getting next hosts... batch={self.train_batch_size}')
                time_now = int(time.time())
                hosts = host_selector.get_next_hosts(consumer, self.train_batch_size)
                if len(hosts) == 0:
                    self.logger.info(f'All models have been trained. Waiting {self.wait_time_minutes} minutes...')
                    time.sleep(self.wait_time_minutes * 60)
                    continue

                self.logger.info(f'Batch: {hosts}')
                self.logger.info('Reading sessions ...')
                consumer.seek_to_beginning()
                batch_complete = False
                batch = {
                    host: {
                        'bot': [],
                        'human': []
                    } for host in hosts
                }

                while not batch_complete:
                    raw_messages = consumer.poll(timeout_ms=self.kafka_timeout_ms, max_records=self.kafka_max_size)
                    for topic_partition, messages in raw_messages.items():
                        if batch_complete:
                            break
                        for message in messages:
                            # prevent from getting messages too close to the current time
                            if (time_now - message.timestamp / 1000) / 60 < self.dataset_delay_from_now_in_minutes:
                                self.logger.info('Topic offset reached the current time...')
                                batch_complete = True
                                break

                            if not message.value:
                                continue

                            session = json.loads(message.value.decode("utf-8"))
                            host = message.key.decode("utf-8")
                            if host not in batch:
                                continue
                            if len(batch[host]['bot']) >= self.num_sessions and \
                                    len(batch[host]['human']) >= self.num_sessions:
                                batch_complete = True
                                for _, v in batch.items():
                                    if len(v['bot']) > self.num_sessions or \
                                            len(v['human']) < self.num_sessions:
                                        batch_complete = False
                                        break
                                if batch_complete:
                                    self.logger.info('x1')
                                    break
                                else:
                                    continue

                            if BaskervillehallIsolationForest.is_human(session):
                                if len(batch[host]['human']) < self.num_sessions:
                                    batch[host]['human'].append(session)
                            else:
                                if not BaskervillehallIsolationForest.is_bad_bot(session) and \
                                        not session['verified_bot']:
                                    if len(batch[host]['bot']) < self.num_sessions:
                                        batch[host]['bot'].append(session)

                self.logger.info('The new batch:')
                for host, v in batch.items():
                    self.logger.info(f'---{host}: humans={len(v["human"])}, bots={len(v["bot"])}')
                for host, v in batch.items():
                    if not self.train_and_save_model(v['human'], host, ModelType.HUMAN) or \
                    not self.train_and_save_model(v['bot'], host, ModelType.BOT):
                        self.train_and_save_model(v['bot']+v['human'], host, ModelType.GENERIC)


        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')
