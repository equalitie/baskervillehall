import logging
import time
import json
from collections import deque
import numpy as np

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.host_selector import HostSelector
from baskervillehall.model_io import ModelIO
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
            datetime_format='%Y-%m-%d %H:%M:%S',

            train_batch_size = 5,
            model_ttl_in_minutes=120,
            dataset_delay_from_now_in_minutes=60,
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            min_dataset_size=100,
            small_dataset_size=500,
            kafka_timeout_ms=1000,
            kafka_max_size=5000,
            wait_time_minutes=5,
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
                'payload_size_log_average',
                'fresh_session'
            ]

        self.feature_names = feature_names
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_sessions = num_sessions
        self.min_session_duration = min_session_duration
        self.min_number_of_queries = min_number_of_queries
        self.date_time_format = datetime_format

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
        self.kafka_group_id = kafka_group_id
        self.kafka_timeout_ms = kafka_timeout_ms
        self.kafka_max_size = kafka_max_size
        self.s3_connection = s3_connection
        self.s3_path = s3_path
        self.min_dataset_size = min_dataset_size
        self.small_dataset_size = small_dataset_size
        self.dataset_delay_from_now_in_minutes = dataset_delay_from_now_in_minutes

    def run(self):
        model_io = ModelIO(**self.s3_connection, logger=self.logger)

        consumer = KafkaConsumer(
            **self.kafka_connection,
            auto_offset_reset='earliest',
            group_id=self.kafka_group_id
        )

        host_selector = HostSelector(
            ttl_in_minutes=self.model_ttl_in_minutes,
            logger=self.logger
        )
        self.logger.info(self.kafka_connection['bootstrap_servers'])
        self.logger.info(f'Starting training on topic {self.topic_sessions}')

        try:
            consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
            while True:
                self.logger.info('Getting next hosts...')
                time_now = int(time.time())
                hosts = host_selector.get_next_hosts(consumer, self.train_batch_size)
                if len(hosts) == 0:
                    self.logger.info(f'All models have been trained. Waiting {self.wait_time_minutes} minutes...')
                    time.sleep(self.wait_time_minutes*60)
                    continue

                self.logger.info(f'Batch: {hosts}')
                self.logger.info('Reading sessions ...')
                consumer.seek_to_beginning()
                batch_complete = False
                batch = {
                    host: {
                        'features': [],
                        'categorical_features': []
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

                            if session['ip'] == '58.20.77.156':
                                self.logger.info(session)

                            if host not in batch:
                                continue
                            if session['duration'] < self.min_session_duration:
                                continue
                            if len(session.get('requests', session.get('queries'))) < self.min_number_of_queries:
                                continue



                            # if session['session_id'] == '-' or session['session_id'] == '':
                            #     continue

                            if len(batch[host]['features']) >= self.num_sessions:
                                batch_complete = True
                                for _, dataset in batch.items():
                                    if len(dataset['features']) < self.num_sessions:
                                        batch_complete = False
                                        break
                                if batch_complete:
                                    break
                                else:
                                    continue

                            features = BaskervillehallIsolationForest.calculate_features(session, self.date_time_format)
                            vector = BaskervillehallIsolationForest.get_vector_from_feature_map(self.feature_names,
                                                                                                features)

                            batch[host]['features'].append(vector)
                            batch[host]['categorical_features'].append([session['country']])

                for host, dataset in batch.items():
                    features = dataset['features']
                    categorical_features = dataset['categorical_features']
                    self.logger.info(f'Training host {host}, dataset size {len(features)}')

                    if len(features) <= self.min_dataset_size:
                        self.logger.info(f'Skipping training. Too few sessions: {len(features)}. Host = {host}.'
                                         f'The minimum is {self.min_dataset_size}')
                        continue

                    small_dataset = len(features) <= self.small_dataset_size
                    model = BaskervillehallIsolationForest(
                        n_estimators=len(features) if small_dataset else self.n_estimators,
                        max_samples=self.max_samples,
                        contamination=self.contamination * 2 if small_dataset else self.contamination,
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

                    self.logger.info(f'@@@@@@@@@@@@@@@@ Saving model for {host} ... @@@@@@@@@@@@@@@@@@@@@@@@@')
                    model_io.save(model, self.s3_path, host)
        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
