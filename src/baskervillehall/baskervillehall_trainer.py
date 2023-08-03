import logging
import time
import json
from collections import deque
import numpy as np

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.host_selector import HostSelector
from kafka import KafkaConsumer, TopicPartition


class BaskervillehallTrainer(object):

    def __init__(
            self,
            feature_names=None,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            kafka_group_id='baskervillehall_trainer',
            num_sessions=10000,
            min_session_duration=20,
            min_number_of_queries=2,

            n_estimators=500,
            max_samples="auto",
            contamination="auto",
            max_features=1.0,
            bootstrap=False,
            n_jobs=None,
            random_state=None,

            host_waiting_sleep_time_in_seconds=5,
            model_ttl_in_minutes=120,
            dataset_delay_from_now_in_minutes=60,
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            min_dataset_size=500,
            logger=None
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}

        if feature_names is None:
            feature_names = [
                'request_rate',
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
                'payload_size_log_average'
            ]

        self.feature_names = feature_names
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_sessions = num_sessions
        self.min_session_duration = min_session_duration
        self.min_number_of_queries = min_number_of_queries

        self.n_estimators = n_estimators
        self.max_samples = max()
        self.contamination = contamination
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.n_jobs = n_jobs
        self.random_state = None,

        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.model_ttl_in_minutes = model_ttl_in_minutes
        self.kafka_connection = kafka_connection
        self.kafka_group_id = kafka_group_id
        self.s3_connection = s3_connection
        self.s3_path = s3_path
        self.host_waiting_sleep_time_in_seconds = host_waiting_sleep_time_in_seconds
        self.min_dataset_size = min_dataset_size
        self.dataset_delay_from_now_in_minutes = dataset_delay_from_now_in_minutes

    def run(self):
        # model_io = ModelIO(**self.s3_connection, logger=self.logger)

        consumer = KafkaConsumer(
            **self.kafka_connection,
            auto_offset_reset='earliest',
            group_id=self.kafka_group_id
        )

        host_selector = HostSelector(
            ttl_in_minutes=self.model_ttl_in_minutes
        )
        self.logger.info(self.kafka_connection['bootstrap_servers'])
        self.logger.info(f'Starting training on topic {self.topic_sessions}')

        try:
            consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
            while True:
                self.logger.info('Getting next host...')
                time_now = int(time.time())
                host = host_selector.get_next_host(consumer)
                if host is None:
                    self.logger.info(
                        f'No next host available, waiting {self.host_waiting_sleep_time_in_seconds} seconds...')
                    time.sleep(self.host_waiting_sleep_time_in_seconds)
                    consumer.seek_to_beginning()
                    continue

                self.logger.info(f'-------- host: {host}')

                self.logger.info('Reading sessions ...')
                features = deque([])
                categorical_features = deque([])
                consumer.seek_to_beginning()
                host_complete = False
                while not host_complete:
                    raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
                    for topic_partition, messages in raw_messages.items():
                        if host_complete:
                            break
                        for message in messages:
                            # prevent from getting messages too close to the current time
                            if (time_now - message.timestamp / 1000) / 60 < self.dataset_delay_from_now_in_minutes:
                                self.logger.info('Topic offset reached the current time...')
                                host_complete = True
                                break

                            if not message.value:
                                continue

                            value = json.loads(message.value.decode("utf-8"))

                            if message.key.decode("utf-8") != host:
                                continue
                            if value['duration'] < self.min_session_duration:
                                continue
                            if len(value['queries']) < self.min_number_of_queries:
                                continue

                            ip = value['ip']
                            vector = BaskervillehallIsolationForest.get_vector_from_feature_map(self.feature_names,
                                                                                                value['features'])

                            features.append(vector)
                            categorical_features.append([value['country']])

                            if len(features) >= self.num_sessions:
                                features.popleft()
                                categorical_features.popleft()

                if len(features) <= self.min_dataset_size:
                    self.logger.info(f'Skipping training. Too few sessions: {len(features)}. '
                                     f'The minimum is {self.min_dataset_size}')
                    continue

                model = BaskervillehallIsolationForest(
                    n_estimators=self.n_estimators,
                    max_samples=self.max_samples,
                    contamination=self.contamination,
                    max_features=self.max_features,
                    bootstrap=self.bootstrap,
                    n_jobs=self.n_jobs,
                    random_state=self.random_state,
                    logger=self.logger,
                )

                features = np.array(features)
                model.fit(
                    features=features,
                    feature_names=self.feature_names,
                    categorical_features=categorical_features,
                    categorical_feature_names=['country']
                )
                del features
                del categorical_features

        #         if model is not None:
        #             self.logger.info(
        #                 f'@@@@@@@@@@@@@@@@@@@@@@@@ Saving model for {host} ... @@@@@@@@@@@@@@@@@@@@@@@@@')
        #             model_io.save(model, host, self.s3_path)
        # except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
