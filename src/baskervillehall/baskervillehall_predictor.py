import logging
from collections import defaultdict

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.ip_storage import IPStorage
from baskervillehall.model_storage import ModelStorage
from baskervillehall.whitelist_ip import WhitelistIP
import json
import numpy as np
from datetime import datetime


class BaskervillehallPredictor(object):
    def __init__(
            self,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            num_partitions=3,
            topic_commands='banjax_command_topic',
            topic_reports='banjax_report_topic',
            kafka_group_id='baskervillehall_predictor',
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            s3_connection={},
            s3_path='/',
            datetime_format='%Y-%m-%d %H:%M:%S',
            white_list_refresh_in_minutes=5,
            model_reload_in_minutes=10,
            max_models=10000,
            min_session_duration=20,
            min_number_of_queries=2,
            batch_size=500,
            pending_challenge_ttl_in_minutes=30,
            maxsize_pending_challenge=10000000,
            passed_challenge_ttl_in_minutes=600,
            maxsize_passed_challenge=10000000,
            logger=None,
            whitelist_ip=None
    ):
        super().__init__()

        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_partitions = num_partitions
        self.topic_commands = topic_commands
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.s3_connection = s3_connection
        self.s3_path = s3_path
        self.min_session_duration = min_session_duration
        self.min_number_of_queries = min_number_of_queries
        self.white_list_refresh_in_minutes = white_list_refresh_in_minutes
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.whitelist_ip = whitelist_ip
        self.model_reload_in_minutes = model_reload_in_minutes
        self.max_models = max_models
        self.pending_challenge_ttl_in_minutes = pending_challenge_ttl_in_minutes
        self.topic_reports = topic_reports
        self.passed_challenge_ttl_in_minutes = passed_challenge_ttl_in_minutes
        self.maxsize_pending_challenge = maxsize_pending_challenge
        self.maxsize_passed_challenge = maxsize_passed_challenge
        self.batch_size = batch_size
        self.date_time_format = datetime_format

    @staticmethod
    def _is_debug_enabled(value):
        return value['ua'] == 'Baskervillehall'

    def run(self):
        model_storage = ModelStorage(
            self.s3_connection,
            self.s3_path,
            reload_in_minutes=self.model_reload_in_minutes ,
            logger=self.logger)
        model_storage.start()

        pending_challenge_ips = TTLCache(
            maxsize=self.maxsize_pending_challenge,
            ttl=self.pending_challenge_ttl_in_minutes * 60
        )

        ip_storage = IPStorage(
            topic_reports=self.topic_reports,
            num_partitions=self.num_partitions,
            kafka_group_id=f'{self.kafka_group_id}-{self.partition}',
            passed_challenge_ttl_in_minutes=self.passed_challenge_ttl_in_minutes,
            maxsize_passed_challenge=self.maxsize_passed_challenge,
            kafka_connection=self.kafka_connection,
            logger=self.logger
        )
        ip_storage.start()

        consumer = KafkaConsumer(
            **self.kafka_connection,
            group_id=self.kafka_group_id
        )

        producer = KafkaProducer(**self.kafka_connection)

        self.logger.info(f'Starting predicting on topic {self.topic_sessions}, partition {self.partition}')

        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_in_minutes)

        try:
            consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                for topic_partition, messages in raw_messages.items():
                    batch = defaultdict(list)
                    self.logger.info(f'Batch size {len(messages)}')
                    for message in messages:
                        if not message.value:
                            continue

                        session = json.loads(message.value.decode("utf-8"))
                        ip = session['ip']
                        host = message.key.decode("utf-8")

                        debug = self._is_debug_enabled(session)
                        if debug:
                            self.logger.info(session.get('ua'))

                        if whitelist_ip.is_in_whitelist(host, session['ip']):
                            if debug:
                                self.logger.info(f'ip {ip} whitelisted')
                            continue

                        if session['duration'] < self.min_session_duration:
                            if debug:
                                self.logger.info(f'ip {ip} value[duration] < self.min_session_duration')
                            continue
                        if len(session['requests']) < self.min_number_of_queries:
                            if debug:
                                self.logger.info(f'ip {ip} < min_number_of_queries')
                            continue

                        batch[host].append(session)

                    for host, sessions in batch.items():
                        model = model_storage.get_model(host)
                        if model is None:
                            continue
                        categorical_features = []
                        features = []
                        for i in range(len(sessions)):
                            categorical_features.append([sessions[i]['country']])
                            features.append(
                                BaskervillehallIsolationForest.get_vector_from_feature_map(
                                    model.feature_names,
                                    BaskervillehallIsolationForest.calculate_features(sessions[i],
                                                                                      self.date_time_format)
                                )
                            )

                        features = np.array(features)

                        ts = datetime.now()
                        scores = model.score(features, categorical_features)
                        self.logger.info(f'score() time = {(datetime.now() - ts).total_seconds()} sec, host {host}, '
                                         f'{features.shape[0]} items')

                        for i in range(scores.shape[0]):
                            score = scores[i]
                            prediction = score < 0
                            session = sessions[i]
                            debug = self._is_debug_enabled(session)
                            ip = session['ip']
                            end = session['end']

                            if debug:
                                ua = session['ua']
                                self.logger.info(f'777 ip={ip}, prediction = {prediction}, score = {score}, ua={ua}, end={end}')
                            if prediction:
                                session_id = session['session_id']
                                if ip_storage.is_challenge_passed(session_id):
                                    if debug:
                                        self.logger.info(f'ip = {ip} is in challenged_passed_storage')
                                    continue

                                if (ip, session_id) in pending_challenge_ips:
                                    if debug:
                                        self.logger.info(f'ip = {ip} is in challenged_pending_storage')
                                    continue
                                pending_challenge_ips[(ip, session_id)] = True

                                self.logger.info(f'Challenging ip={ip}, '
                                                 f'session_id={session_id}, host={host}, end={end}.')
                                message = json.dumps(
                                    {
                                        'Name': 'challenge_ip',
                                        'Value': f'{ip}',
                                        'session_id': session_id,
                                        'host': host,
                                        'source': 'baskervillehall',
                                        'start': session['start'],
                                        'end': session['end'],
                                        'duration': session['duration']
                                    }
                                ).encode('utf-8')
                                producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))
                    producer.flush()

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()