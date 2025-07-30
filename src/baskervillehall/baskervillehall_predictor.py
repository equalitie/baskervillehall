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
            postgres_connection=None,
            postgres_refresh_period_in_seconds=180,
            sensitivity_factor=0.05,
            max_sessions_for_ip=10,
            maz_size_ip_sessions=100000,
            ip_sessions_ttl_in_minutes=30,
            max_requests_in_command=20,
            bot_score_threshold=0.5,
            challenge_scrapers=True
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
        self.bot_score_threshold = bot_score_threshold
        self.challenge_scrapers = challenge_scrapers

        if deflect_config_url is None or len(deflect_config_url) == 0:
            self.settings = SettingsPostgres(refresh_period_in_seconds=postgres_refresh_period_in_seconds,
                                             postgres_connection=postgres_connection)
        else:
            self.settings = SettingsDeflectAPI(url=self.deflect_config_url,
                                               logger=self.logger,
                                               refresh_period_in_seconds=60 * self.white_list_refresh_period)
        self.sensitivity_factor = sensitivity_factor

    def get_shapley_report(self, shap_value, feature_names):
        """
        Generates a report of negative Shapley values per feature and returns the most impactful feature
        along with a sorted list of feature contributions by descending importance.

        :param shap_value: SHAP values object with attributes .values and .data
        :param feature_names: list of feature names corresponding to shap_value.values
        :return: tuple (top_feature, shapley_report_sorted)
        """
        # Initialize report list and track the most negative Shapley value
        shapley_report = []
        min_shapley = 0
        shapley_feature = None

        # Collect negative contributions
        for k, feature in enumerate(feature_names):
            value = shap_value.values[k]
            data_val = shap_value.data[k]
            if value < 0:
                # Update most impactful (most negative) feature
                if value < min_shapley:
                    min_shapley = value
                    shapley_feature = feature

                # Append to report
                shapley_report.append({
                    'name': feature,
                    'values': {
                        'shapley': round(value, 2),
                        'feature': round(data_val, 2)
                    }
                })

        # Sort report by descending importance (absolute Shapley value)
        shapley_report_sorted = sorted(
            shapley_report,
            key=lambda x: abs(x['values']['shapley']),
            reverse=True
        )

        return shapley_feature, shapley_report_sorted

    def create_command(self,
                       command_name,
                       session,
                       meta,
                       prediction_if,
                       score_if,
                       shapley_if,
                       shapley_feature_if,
                       prediction_ae,
                       score_ae,
                       shapley_ae,
                       shapley_feature_ae,
                       difficulty,
                       scraper_name,
                       threshold_ae
                       ):
        dict = {
                'Name': command_name,
                'difficulty': difficulty,
                'Value': session["ip"],
                'country': session.get('country', ''),
                'continent': session.get('continent', ''),
                'datacenter_code': session.get('datacenter_code', ''),
                'session_id': session['session_id'],
                'host': session['host'],
                'source': meta,
                'shapley': shapley_if,
                'shapley_if': shapley_if,
                'shapley_ae': shapley_ae,
                'meta': meta,
                'prediction_if': int(prediction_if),
                'prediction_ae': int(prediction_ae),
                'shapley_feature': shapley_feature_if if len(shapley_feature_if) > 0 else shapley_feature_ae,
                'shapley_feature_if': shapley_feature_if,
                'shapley_feature_ae': shapley_feature_ae,
                'start': session['start'],
                'end': session['end'],
                'duration': session['duration'],
                'score': float(score_if),
                'score_if': float(score_if),
                'score_ae': float(score_ae),
                'bot_score': session['bot_score'],
                'bot_score_top_factor': session.get('bot_score_top_factor', ''),
                'num_requests': len(session['requests']),
                'user_agent': session.get('ua'),
                'human': session.get('human', ''),
                'datacenter_asn': session.get('datacenter_asn', ''),
                'session': session,
                'scraper_name': scraper_name,
                'threshold_ae': float(threshold_ae)
            }
        return json.dumps(
            dict
        ).encode('utf-8')

    def run(self):

        models_isolation_forest = ModelStorage(
            self.s3_connection,
            self.s3_path,
            reload_in_minutes=self.model_reload_in_minutes,
            logger=self.logger
        )
        models_autoencoder = ModelStorage(
            self.s3_connection,
            f'{self.s3_path}_autoencoder3',
            reload_in_minutes=self.model_reload_in_minutes,
            logger=self.logger
        )

        pending_challenge_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_interactive_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_block_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
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

                    if whitelist_ip.is_in_whitelist(host, session['ip']):
                        ip_whitelisted += 1
                        continue

                    if session.get('deflect_password', False):
                        continue

                    if not session.get('primary_session', False):
                        ip_with_sessions[session['ip']] = True

                    batch[(host, human)].append(session)
                    session = None
                    predicting_total += 1

                for (host, human), sessions in batch.items():
                    model_if = models_isolation_forest.get_model(host,
                                                                 ModelType.HUMAN if human else ModelType.BOT)
                    model_ae = models_autoencoder.get_model(host,
                                                            ModelType.HUMAN if human else ModelType.BOT)

                    ts = datetime.now()
                    scores_if, shap_values_if = None, None
                    if model_if:
                        scores_if, shap_values_if = model_if.transform(sessions, use_shapley=self.use_shapley)
                        self.logger.info(
                            f'score() time isolation forest = {(datetime.now() - ts).total_seconds()} sec, host {host}, '
                            f'{scores_if.shape[0]} items')

                    scores_ae, shap_values_ae = None, None
                    if model_ae:
                        scores_ae, shap_values_ae = model_ae.transform(sessions, use_shapley=self.use_shapley)
                        self.logger.info(
                            f'score() time isolation forest = {(datetime.now() - ts).total_seconds()} sec, host {host}, '
                            f'{scores_ae.shape[0]} items')

                    for i in range(len(sessions)):
                        session = sessions[i]
                        ip = session['ip']
                        human = session.get('human', True)
                        scraper_name = \
                            session.get('scraper_name',
                                        BaskervillehallIsolationForest.detect_scraper(session['ua']))

                        session_id = session['session_id']
                        primary_session = session['primary_session']
                        verified_bot = session.get('verified_bot', False)

                        if verified_bot:
                            continue
                        if session['asset_only']:
                            continue

                        if scores_if is not None:
                            score_if = scores_if[i]
                            sensitivity_shift = self.settings.get_sensitivity(host) * self.sensitivity_factor
                            score_if -= sensitivity_shift
                            prediction_if = score_if < 0
                            shapley_feature_if, shapley_if = (
                                self.get_shapley_report(shap_values_if[i], model_if.get_all_features()))
                        else:
                            score_if = 0.0
                            prediction_if = False
                            shapley_feature_if, shapley_if = '', ''

                        if scores_ae is not None:
                            threshold_ae = model_ae.threshold
                            score_ae = scores_ae[i]
                            prediction_ae = score_ae > threshold_ae
                            shapley_feature_ae, shapley_ae = (
                                self.get_shapley_report(shap_values_ae[i], model_ae.get_all_features()))
                        else:
                            score_ae = 0.0
                            prediction_ae = False
                            shapley_feature_ae, shapley_ae = '', ''
                            threshold_ae = 0

                        session['requests'] = session['requests'][0:self.max_requests_in_command]
                        bot_score = session['bot_score']
                        bot_score_top_factor = session.get('bot_score_top_factor', '')

                        if (session['passed_challenge'] and \
                                bot_score > self.bot_score_threshold and \
                                bot_score_top_factor != 'no_payload'):
                            if ip in pending_block_ip:
                                continue
                            pending_block_ip[ip] = True
                            self.logger.info(f'High bot score = {bot_score}, human={human}, '
                                             f'top_factor = {bot_score_top_factor} for ip '
                                             f'{ip}, host {host}. Blocking.')

                            producer.send(self.topic_commands,
                                          self.create_command(
                                              command_name='block_ip_testing',
                                              session=session,
                                              meta='high_bot_score',
                                              prediction_if=prediction_if,
                                              score_if=score_if,
                                              shapley_if=shapley_if,
                                              shapley_feature_if=shapley_feature_if,
                                              prediction_ae=prediction_ae,
                                              score_ae=score_ae,
                                              shapley_ae=shapley_ae,
                                              shapley_feature_ae=shapley_feature_ae,
                                              difficulty=0,
                                              scraper_name=scraper_name,
                                              threshold_ae=threshold_ae,
                                          ),
                                          key=bytearray(host, encoding='utf8'))
                            continue

                        if self.bad_bot_challenge \
                                and session['bad_bot'] \
                                and ip not in ip_with_sessions.keys():
                            producer.send(self.topic_commands,
                                          self.create_command(
                                              command_name='challenge_ip',
                                              session=session,
                                              meta='Bad bot rule',
                                              prediction_if=prediction_if,
                                              score_if=score_if,
                                              shapley_if=shapley_if,
                                              shapley_feature_if=shapley_feature_if,
                                              prediction_ae=prediction_ae,
                                              score_ae=score_ae,
                                              shapley_ae=shapley_ae,
                                              shapley_feature_ae=shapley_feature_ae,
                                              difficulty=0,
                                              scraper_name=scraper_name,
                                              threshold_ae=threshold_ae,
                                          ),
                                          key=bytearray(host, encoding='utf8'))
                            continue

                        meta = None
                        if session.get('weak_cipher', False):
                            meta = 'weak_cipher'
                        elif len(scraper_name) > 0 and self.challenge_scrapers:
                            meta = 'scraper'

                        if meta:
                            if ip in pending_challenge_ip:
                                continue
                            pending_challenge_ip[ip] = True

                            producer.send(self.topic_commands,
                                          self.create_command(
                                              command_name='challenge_ip',
                                              session=session,
                                              meta=meta,
                                              prediction_if=prediction_if,
                                              score_if=score_if,
                                              shapley_if=shapley_if,
                                              shapley_feature_if=shapley_feature_if,
                                              prediction_ae=prediction_ae,
                                              score_ae=score_ae,
                                              shapley_ae=shapley_ae,
                                              shapley_feature_ae=shapley_feature_ae,
                                              difficulty=0,
                                              scraper_name=scraper_name,
                                              threshold_ae=threshold_ae,
                                          ),
                                          key=bytearray(host, encoding='utf8'))
                            continue

                        if not primary_session:
                            too_many_sessions = False
                            if host not in host_ip_sessions:
                                host_ip_sessions[host] = TTLCache(maxsize=self.maxsize_ip_sessions,
                                                                  ttl=120 * 60)
                            if ip not in host_ip_sessions[host]:
                                host_ip_sessions[host][ip] = TTLCache(maxsize=self.maxsize_ip_sessions,
                                                                      ttl=self.ip_sessions_ttl_in_minutes * 60)
                            host_ip_sessions[host][ip][session_id] = True
                            if len(host_ip_sessions[host][ip]) >= self.max_sessions_for_ip:
                                if ip in pending_challenge_ip:
                                    continue
                                pending_challenge_ip[ip] = True

                                too_many_sessions = True
                                meta = 'Too many sessions.'
                                self.logger.info(f'Too many sessions ({len(host_ip_sessions[host][ip])}) for ip '
                                                 f'{ip}, host {host}')
                                producer.send(self.topic_commands,
                                              self.create_command(
                                                  command_name='challenge_ip',
                                                  session=session,
                                                  meta=meta,
                                                  prediction_if=prediction_if,
                                                  score_if=score_if,
                                                  shapley_if=shapley_if,
                                                  shapley_feature_if=shapley_feature_if,
                                                  prediction_ae=prediction_ae,
                                                  score_ae=score_ae,
                                                  shapley_ae=shapley_ae,
                                                  shapley_feature_ae=shapley_feature_ae,
                                                  difficulty=0,
                                                  scraper_name=scraper_name,
                                                  threshold_ae=threshold_ae,
                                              ),
                                              key=bytearray(host, encoding='utf8'))
                                continue

                        if prediction_if or prediction_ae:
                            if primary_session:
                                if ip in pending_challenge_ip:
                                    continue
                                pending_challenge_ip[ip] = True
                            else:
                                if ip not in pending_session:
                                    pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

                                if session_id in pending_session[ip]:
                                    continue
                                pending_session[ip][session_id] = True

                            if primary_session:
                                command = 'challenge_ip'
                            else:
                                command = 'challenge_session'

                            api_ratio = 0.0
                            if shap_values_if:
                                shap_value = shap_values_if[i]
                                for k in range(len(shap_value.values)):
                                    feature = model_if.get_all_features()[k]
                                    if feature == 'api_ratio':
                                        api_ratio = round(shap_value.data[k], 2)

                            if api_ratio == 1.0:
                                self.logger.info(f'Skipping challenge for ip={ip}, host={host} since api_ratio is 1.0')
                                continue

                            self.logger.info(f'Challenging for ip={ip}, '
                                             f'session_id={session_id}, '
                                             f'host={host}, end={session["end"]}, score_if={score_if}.'
                                             f'score_ae={score_ae}.'
                                             )
                            producer.send(self.topic_commands,
                                          self.create_command(
                                              command_name=command,
                                              session=session,
                                              meta='',
                                              prediction_if=prediction_if,
                                              score_if=score_if,
                                              shapley_if=shapley_if,
                                              shapley_feature_if=shapley_feature_if,
                                              prediction_ae=prediction_ae,
                                              score_ae=score_ae,
                                              shapley_ae=shapley_ae,
                                              shapley_feature_ae=shapley_feature_ae,
                                              difficulty=0,
                                              scraper_name=scraper_name,
                                              threshold_ae=threshold_ae,
                                          ),
                                          key=bytearray(host, encoding='utf8'))
                            continue

                self.logger.info(f'batch={len(messages)}, predicting_total = {predicting_total}, '
                                 f'whitelisted = {ip_whitelisted}')
                producer.flush()
