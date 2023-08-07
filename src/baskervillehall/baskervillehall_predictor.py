import logging

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.ip_storage import IPStorage
from baskervillehall.model_storage import ModelStorage
from baskervillehall.whitelist_ip import WhitelistIP
import json
import numpy as np


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
            white_list_refresh_in_minutes=5,
            model_reload_in_minutes=10,
            max_models=10000,
            min_session_duration=20,
            min_number_of_queries=2,
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

    def run(self):
        model_storage = ModelStorage(
            self.s3_connection,
            self.s3_path,
            reload_in_minutes=self.model_reload_in_minutes ,
            max_models=self.max_models,
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

        self.logger.info(f'Starting predicting on topic {self.topic_sessions}')

        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_in_minutes)

        try:
            consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:

                        if not message.value:
                            continue

                        value = json.loads(message.value.decode("utf-8"))
                        ip = value['ip']
                        host = message.key.decode("utf-8")

                        if whitelist_ip.is_in_whitelist(host, ip):
                            continue

                        session_id = value['session_id']

                        model = model_storage.get_model(host)
                        if model is None:
                            continue

                        if value['duration'] < self.min_session_duration:
                            continue
                        if len(value['queries']) < self.min_number_of_queries:
                            continue

                        vector = BaskervillehallIsolationForest.get_vector_from_feature_map(model.feature_names,
                                                                                            value['features'])

                        features = np.array([vector])
                        categorical_features = [[value['country']]]

                        prediction = model.score(features, categorical_features)[0] < 0

                        if prediction:
                            if ip_storage.is_challenge_passed(ip):
                                continue

                            if (ip, session_id) in pending_challenge_ips:
                                continue
                            pending_challenge_ips[(ip, session_id)] = True

                            self.logger.info(f'Challenging ip={ip}, session_id={session_id}, host={host}.')
                            message = json.dumps(
                                {
                                    'Name': 'challenge_ip',
                                    'Value': f'{ip}_testing',
                                    'session_id': session_id,
                                    'host': host,
                                    'source': 'baskervillehall',
                                    'start': value['start'],
                                    'end': value['end'],
                                    'duration': value['duration']
                                }
                            ).encode('utf-8')
                            producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))
                            producer.flush()

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
