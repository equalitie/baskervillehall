import logging
from collections import defaultdict

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.model_storage import ModelStorage
from baskervillehall.whitelist_ip import WhitelistIP
import json
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
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            datetime_format='%Y-%m-%d %H:%M:%S',
            white_list_refresh_in_minutes=5,
            model_reload_in_minutes=10,
            max_models=10000,
            min_session_duration=20,
            min_number_of_requests=2,
            max_offences_before_blocking=3,
            batch_size=500,
            pending_ttl=30,
            maxsize_pending=10000000,
            n_jobs_predict=10,
            logger=None,
            whitelist_ip=None,
            debug_ip=None
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_partitions = num_partitions
        self.topic_commands = topic_commands
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.s3_connection = s3_connection
        self.s3_path = s3_path
        self.min_session_duration = min_session_duration
        self.min_number_of_requests = min_number_of_requests
        self.white_list_refresh_in_minutes = white_list_refresh_in_minutes
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.whitelist_ip = whitelist_ip
        self.model_reload_in_minutes = model_reload_in_minutes
        self.max_models = max_models
        self.pending_ttl = pending_ttl
        self.topic_reports = topic_reports
        self.maxsize_pending = maxsize_pending
        self.batch_size = batch_size
        self.date_time_format = datetime_format
        self.debug_ip = debug_ip
        self.n_jobs_predict = n_jobs_predict
        self.max_offences_before_blocking = max_offences_before_blocking

    def _is_debug_enabled(self, value):
        return (self.debug_ip and value['ip'] == self.debug_ip) or value['ua'] == 'Baskervillehall'

    def run(self):
        model_storage_human = ModelStorage(
            self.s3_connection,
            self.s3_path,
            human=True,
            reload_in_minutes=self.model_reload_in_minutes,
            logger=self.logger)
        model_storage_human.start()

        model_storage_bot = ModelStorage(
            self.s3_connection,
            self.s3_path,
            human=False,
            reload_in_minutes=self.model_reload_in_minutes,
            logger=self.logger)
        model_storage_bot.start()

        pending_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_session = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

        offences = TTLCache(
            maxsize=10000,
            ttl=60 * 60
        )

        # ip_storage = IPStorage(
        #     topic_reports=self.topic_reports,
        #     num_partitions=self.num_partitions,
        #     kafka_group_id=f'{self.kafka_group_id}-{self.partition}',
        #     passed_challenge_ttl_in_minutes=self.passed_challenge_ttl_in_minutes,
        #     maxsize_passed_challenge=self.maxsize_passed_challenge,
        #     kafka_connection=self.kafka_connection,
        #     logger=self.logger
        # )
        # ip_storage.start()

        consumer = KafkaConsumer(
            **self.kafka_connection,
            group_id=self.kafka_group_id,
            max_poll_records=self.batch_size,
            fetch_max_bytes=52428800 * 5,
            max_partition_fetch_bytes=1048576 * 10,
            api_version=(0, 11, 5),
        )

        producer = KafkaProducer(
            **self.kafka_connection,
            api_version=(0, 11, 5),
        )

        self.logger.info(f'Starting predicting on topic {self.topic_sessions}, partition {self.partition}')
        self.logger.info(f'debug_ip={self.debug_ip}')
        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_in_minutes)

        consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
        consumer.seek_to_end()
        ts_lag_report = datetime.now()
        while True:
            raw_messages = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
            for topic_partition, messages in raw_messages.items():
                batch = defaultdict(list)
                self.logger.info(f'Batch size {len(messages)}')
                predicting_total = 0
                ip_whitelisted = 0
                for message in messages:
                    if (datetime.now() - ts_lag_report).total_seconds() > 5:
                        highwater = consumer.highwater(topic_partition)
                        lag = (highwater - 1) - message.offset
                        self.logger.info(f'Lag = {lag}')
                        ts_lag_report = datetime.now()

                    if not message.value:
                        continue

                    session = json.loads(message.value.decode("utf-8"))
                    human = BaskervillehallIsolationForest.is_human(session)

                    ip = session['ip']

                    host = message.key.decode("utf-8")

                    debug = self._is_debug_enabled(session)
                    if debug:
                        self.logger.info(ip)

                    if whitelist_ip.is_in_whitelist(host, session['ip']):
                        if debug:
                            self.logger.info(f'ip {ip} whitelisted')
                        ip_whitelisted += 1
                        continue

                    batch[(host, human)].append(session)
                    predicting_total += 1

                predicted = 0
                for (host, human), sessions in batch.items():
                    model = model_storage_human.get_model(host) if human else\
                        model_storage_bot.get_model(host)
                    if model is None:
                        model = model_storage_bot.get_model(host) if human else \
                            model_storage_human.get_model(host)

                    if model is None:
                        continue

                    ts = datetime.now()
                    scores = model.transform(sessions)
                    predicted += scores.shape[0]
                    self.logger.info(f'score() time = {(datetime.now() - ts).total_seconds()} sec, host {host}, '
                                     f'{scores.shape[0]} items')

                    for i in range(scores.shape[0]):
                        score = scores[i]
                        prediction = score < 0
                        session = sessions[i]
                        debug = self._is_debug_enabled(session)
                        ip = session['ip']
                        end = session['end']

                        if debug:
                            ua = session['ua']
                            self.logger.info(f'777 ip={ip}, prediction = {prediction}, score = {score}, ua={ua}, '
                                             f'end={end}')
                        if prediction:
                            session_id = session['session_id']

                            # if ip_storage.is_challenge_passed(session_id):
                            #     if debug:
                            #         self.logger.info(f'ip = {ip} is in challenged_passed_storage')
                            #     continue
                            #

                            primary_session = session['primary_session']
                            session_id = '-'

                            if primary_session:
                                if ip in pending_ip:
                                    continue
                                pending_ip[ip] = True
                            else:
                                if (ip, session_id) in pending_session:
                                    continue
                                pending_session[(ip, session_id)] = True

                            if ip not in offences:
                                offences[ip] = {}
                            offences[ip][session_id] = offences[ip].get(session_id, 0) + 1

                            if offences[ip][session_id]  > self.max_offences_before_blocking:
                                self.logger.info(f'Blocking multiple offences ip = {ip}, session = {session_id} '
                                                 f' offences = {offences[ip][session_id]} '
                                                 f'host = {host}')
                                command = 'block_ip' if primary_session else 'block_session'
                            else:
                                command = 'challenge_ip' if primary_session else 'challenge_session'



                            self.logger.info(f'Challenging for ip={ip}, '
                                             f'session_id={session_id}, host={host}, end={end}, score={score}.')
                            message = json.dumps(
                                {
                                    'Name': command,
                                    'Value': f'{ip}',
                                    'session_id': session_id,
                                    'host': host,
                                    'source': 'baskervillehall',
                                    'start': session['start'],
                                    'end': session['end'],
                                    'duration': session['duration'],
                                    'score': score,
                                    'num_requests': len(session['requests'])
                                }
                            ).encode('utf-8')
                            producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))

                            # for backward compatibility with  production
                            if not primary_session:
                                if host != 'palestinechronicle.com':
                                    message = json.dumps(
                                        {
                                            'Name': 'challenge_ip',
                                            'Value': f'{ip}',
                                            'session_id': '-',
                                            'forwarded': True,
                                            'host': host,
                                            'source': 'baskervillehall',
                                            'start': session['start'],
                                            'end': session['end'],
                                            'duration': session['duration'],
                                            'score': score,
                                            'num_requests': len(session['requests'])
                                        }
                                    ).encode('utf-8')
                                    producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))

                self.logger.info(f'batch={len(messages)}, predicting_total = {predicting_total}, '
                                 f'predicted = {predicted}, whitelisted = {ip_whitelisted}')
                producer.flush()
