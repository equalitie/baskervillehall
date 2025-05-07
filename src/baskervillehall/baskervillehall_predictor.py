import logging
from collections import defaultdict

from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest, ModelType
from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.settings_deflect_api import SettingsDeflectAPI
from baskervillehall.model_storage import ModelStorage
from baskervillehall.settings_postgres import SettingsPostgres
from baskervillehall.whitelist_ip import WhitelistIP
import json
from datetime import datetime


def is_static_session(session):
    for r in session['requests']:
        if not r.get('static', False):
            return False
    return True


class BaskervillehallPredictor(object):
    def __init__(
            self,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            num_partitions=3,
            topic_commands='banjax_command_topic',
            topic_reports='banjax_report_topic',
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            datetime_format='%Y-%m-%d %H:%M:%S',
            white_list_refresh_in_minutes=5,
            model_reload_in_minutes=10,
            max_models=10000,
            min_session_duration=20,
            min_number_of_requests=2,
            num_offences_for_difficult_challenge=3,
            batch_size=100,
            pending_ttl=30,
            maxsize_pending=10000000,
            n_jobs_predict=10,
            logger=None,
            whitelist_ip=None,
            deflect_config_url=None,
            white_list_refresh_period=5,
            bad_bot_challenge=True,
            debug_ip=None,
            use_shapley=True,
            postgres_connection = None,
            postgres_refresh_period_in_seconds=180,
            sensitivity_factor = 0.05,
            max_sessions_for_ip = 10,
            maz_size_ip_sessions = 100000,
            ip_sessions_ttl_in_minutes=30,
            max_requests_in_command=20,
            single_model=False
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if postgres_connection is None:
            postgres_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_partitions = num_partitions
        self.topic_commands = topic_commands
        self.kafka_connection = kafka_connection
        self.s3_connection = s3_connection
        self.postgres_connection = postgres_connection
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
        self.num_offences_for_difficult_challenge = num_offences_for_difficult_challenge
        self.deflect_config_url = deflect_config_url
        self.white_list_refresh_period = white_list_refresh_period
        self.bad_bot_challenge = bad_bot_challenge
        self.use_shapley = use_shapley
        self.max_sessions_for_ip = max_sessions_for_ip
        self.maxsize_ip_sessions = maz_size_ip_sessions
        self.ip_sessions_ttl_in_minutes = ip_sessions_ttl_in_minutes
        self.max_requests_in_command = max_requests_in_command
        self.single_model = single_model

        if deflect_config_url is None or len(deflect_config_url) == 0:
            self.settings = SettingsPostgres(refresh_period_in_seconds=postgres_refresh_period_in_seconds,
                                             postgres_connection=postgres_connection)
        else:
            self.settings = SettingsDeflectAPI(url=self.deflect_config_url,
                                               logger=self.logger,
                                               refresh_period_in_seconds=60 * self.white_list_refresh_period)
        self.sensitivity_factor = sensitivity_factor

    def _is_debug_enabled(self, value):
        return (self.debug_ip and value['ip'] == self.debug_ip) or value['ua'] == 'Baskervillehall'

    def run(self):
        if self.single_model:
            model_storage_human = None
            model_storage_bot = None
        else:
            model_storage_human = ModelStorage(
                self.s3_connection,
                self.s3_path,
                model_type=ModelType.HUMAN,
                reload_in_minutes=self.model_reload_in_minutes,
                logger=self.logger)
            model_storage_human.start()

            model_storage_bot = ModelStorage(
                self.s3_connection,
                self.s3_path,
                model_type=ModelType.BOT,
                reload_in_minutes=self.model_reload_in_minutes,
                logger=self.logger)
            model_storage_bot.start()

        model_storage_generic = ModelStorage(
            self.s3_connection,
            self.s3_path,
            model_type=ModelType.GENERIC,
            reload_in_minutes=self.model_reload_in_minutes,
            logger=self.logger)
        model_storage_generic.start()

        pending_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        host_ip_sessions = dict()
        pending_session = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

        offences = TTLCache(
            maxsize=10000,
            ttl=60 * 60
        )

        ip_with_sessions = TTLCache(
            maxsize=100000,
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
                    human = session.get('human', False)

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

                    if session.get('deflect_password', False):
                        continue

                    if not session.get('primary_session', False):
                        ip_with_sessions[session['ip']] = True

                    batch[(host, human)].append(session)
                    predicting_total += 1

                predicted = 0
                for (host, human), sessions in batch.items():
                    model = None
                    if self.single_model:
                        model = model_storage_generic.get_model(host)
                    else:
                        model = model_storage_human.get_model(host) if human else\
                            model_storage_bot.get_model(host)
                        if model is None:
                            model = model_storage_generic.get_model(host)

                    ts = datetime.now()
                    scores, shap_values = None, None
                    if model:
                        scores, shap_values = model.transform(sessions, use_shapley=self.use_shapley)
                        predicted += scores.shape[0]
                        self.logger.info(f'score() time = {(datetime.now() - ts).total_seconds()} sec, host {host}, '
                                         f'{scores.shape[0]} items')

                    for i in range(len(sessions)):
                        ip = session['ip']
                        if scores is not None:
                            score = scores[i]
                            sensitivity_shift = self.settings.get_sensitivity(host) * self.sensitivity_factor
                            score -= sensitivity_shift
                            prediction = score < 0
                        else:
                            score = 0.0
                            prediction = False
                        session = sessions[i]
                        meta = ''
                        if (self.bad_bot_challenge
                                and session['bad_bot'] \
                                and ip not in ip_with_sessions.keys()):
                            prediction = True
                            meta += 'Bad bot rule'

                        debug = self._is_debug_enabled(session)

                        end = session['end']

                        if debug:
                            ua = session['ua']
                            self.logger.info(f'777 ip={ip}, prediction = {prediction}, score = {score}, ua={ua}, '
                                             f'end={end}')

                        session_id = session['session_id']
                        primary_session = session['primary_session']
                        verified_bot = session.get('verified_bot', False)

                        if verified_bot:
                            continue
                        if session['asset_only']:
                            continue

                        hits = len(session['requests'])
                        session['requests'] = session['requests'][0:self.max_requests_in_command]

                        rule = None
                        if session.get('malicious_asn', False):
                            rule = 'malicious_asn'
                        elif session.get('weak_cipher', False):
                            rule = 'weak_cipher'

                        if rule:
                            if ip in pending_ip:
                                continue
                            pending_ip[ip] = True

                            meta = rule
                            self.logger.info(f'Rule {rule} for ip '
                                             f'{ip}, host {host}')
                            message = json.dumps(
                                {
                                    'Name': 'challenge_ip',
                                    'difficulty': 2,
                                    'Value': f'{ip}',
                                    'country': session.get('country', ''),
                                    'continent': session.get('continent', ''),
                                    'datacenter_code': session.get('datacenter_code', ''),
                                    'session_id': session_id,
                                    'host': host,
                                    'source': meta,
                                    'shapley': '',
                                    'meta': meta,
                                    'shapley_feature': '',
                                    'start': session['start'],
                                    'end': session['end'],
                                    'duration': session['duration'],
                                    'score': score,
                                    'num_requests': hits,
                                    'user_agent': session.get('ua'),
                                    'human': session.get('human', ''),
                                    'datacenter_asn': session.get('datacenter_asn', ''),
                                    'session': session
                                }
                            ).encode('utf-8')
                            producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))
                            continue

                        if not primary_session:
                            too_many_sessions = False
                            if host not in host_ip_sessions:
                                host_ip_sessions[host] = TTLCache(maxsize=self.maxsize_ip_sessions,
                                                                  ttl=120*60)
                            if ip not in host_ip_sessions[host]:
                                host_ip_sessions[host][ip] = TTLCache(maxsize=self.maxsize_ip_sessions,
                                                                  ttl=self.ip_sessions_ttl_in_minutes*60)
                            host_ip_sessions[host][ip][session_id] = True
                            if len(host_ip_sessions[host][ip]) >= self.max_sessions_for_ip:
                                if ip in pending_ip:
                                    continue
                                pending_ip[ip] = True

                                too_many_sessions = True
                                meta = 'Too many sessions.'
                                self.logger.info(f'Too many sessions ({len(host_ip_sessions[host][ip])}) for ip '
                                                 f'{ip}, host {host}')
                                message = json.dumps(
                                    {
                                        'Name': 'challenge_ip',
                                        'difficulty': 2,
                                        'Value': f'{ip}',
                                        'country': session.get('country', ''),
                                        'continent': session.get('continent', ''),
                                        'datacenter_code': session.get('datacenter_code', ''),
                                        'session_id': session_id,
                                        'host': host,
                                        'source': meta,
                                        'shapley': '',
                                        'meta': meta,
                                        'shapley_feature': '',
                                        'start': session['start'],
                                        'end': session['end'],
                                        'duration': session['duration'],
                                        'score': score,
                                        'num_requests': hits,
                                        'user_agent': session.get('ua'),
                                        'human': session.get('human', ''),
                                        'datacenter_asn': session.get('datacenter_asn', ''),
                                        'session': session
                                    }
                                ).encode('utf-8')
                                producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))
                                continue

                        if prediction:
                            if primary_session:
                                if ip in pending_ip:
                                    continue
                                pending_ip[ip] = True
                            else:
                                if ip not in pending_session:
                                    pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

                                if session_id in pending_session[ip]:
                                    continue
                                pending_session[ip][session_id] = True
                            difficulty = 1
                            if session['passed_challenge']:
                                if ip not in offences:
                                    offences[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
                                offences[ip][session_id] = offences[ip].get(session_id, 0) + 1

                                if offences[ip][session_id] >= self.num_offences_for_difficult_challenge:
                                    self.logger.info(f'Multiple offences(show difficult challenge)'
                                                     f' ip = {ip}, session = {session_id} '
                                                     f' offences = {offences[ip][session_id]} '
                                                     f'host = {host}')
                                    meta += f'Multiple offences {offences[ip][session_id]}'
                                    difficulty = 2
                                    # if primary_session:
                                    #     # if is_static_session(session):
                                    #     #     command = 'block_ip_table'
                                    #     # else:
                                    #     #     command = 'block_ip'
                                    #     command = 'block_ip'
                                    # else:
                                    #     if not whitelist_url.is_host_whitelisted_block_session(host):
                                    #         command = 'block_session'

                            if primary_session or too_many_sessions:
                                command = 'challenge_ip'
                            else:
                                command = 'challenge_session'

                            shapley = []
                            shapley_feature = ''
                            api_ratio = 0.0
                            if shap_values:
                                shap_value = shap_values[i]
                                min_shapley = 0
                                for k in range(len(shap_value.values)):
                                    feature = model.get_all_features()[k]
                                    if feature == 'api_ratio':
                                        api_ratio = round(shap_value.data[k], 2)
                                    if shap_value.values[k] < 0:
                                        if min_shapley > shap_value.values[k]:
                                            min_shapley = shap_value.values[k]
                                            shapley_feature = feature
                                        shapley.append({
                                            'name': feature,
                                            'values': {
                                                'shapley': round(shap_value.values[k], 2),
                                                'feature': round(shap_value.data[k], 2)
                                            }
                                        })

                            if api_ratio == 1.0:
                                self.logger.info(f'Skipping challenge for ip={ip}, host={host} since api_ratio is 1.0')
                                continue

                            self.logger.info(f'Challenging for ip={ip}, '
                                             f'session_id={session_id}, host={host}, end={end}, score={score}.'
                                             f'meta = {meta}')
                            message = json.dumps(
                                {
                                    'Name': command,
                                    'difficulty': difficulty,
                                    'Value': f'{ip}',
                                    'country': session.get('country', ''),
                                    'continent': session.get('continent', ''),
                                    'datacenter_code': session.get('datacenter_code', ''),
                                    'session_id': session_id,
                                    'host': host,
                                    'source': f'{shapley_feature},{meta}',
                                    'shapley': shapley,
                                    'meta': meta,
                                    'shapley_feature': shapley_feature,
                                    'start': session['start'],
                                    'end': session['end'],
                                    'duration': session['duration'],
                                    'score': score,
                                    'num_requests': hits,
                                    'user_agent': session.get('ua'),
                                    'human': session.get('human', ''),
                                    'datacenter_asn': session.get('datacenter_asn', ''),
                                    'session': session
                                }
                            ).encode('utf-8')
                            producer.send(self.topic_commands, message, key=bytearray(host, encoding='utf8'))

                self.logger.info(f'batch={len(messages)}, predicting_total = {predicting_total}, '
                                 f'predicted = {predicted}, whitelisted = {ip_whitelisted}')
                producer.flush()
