import copy

from baskervillehall.whitelist_ip import WhitelistIP
from baskervillehall.whitelist_url import WhitelistURL
from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from datetime import datetime

class BaskervillehallSession(object):
    def __init__(
            self,
            topic_weblogs='BASKERVILLEHALL_WEBLOGS',
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            kafka_group_id='baskervillehall_session',
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            flush_window_seconds=60,
            reset_duration=5,
            session_inactivity=1,
            garbage_collection_period=30,
            whitelist_url=None,
            whitelist_url_default=[],
            whitelist_ip=None,
            white_list_refresh_period=5,
            max_fresh_sessions_per_ip=10000,
            fresh_session_ttl_minutes=3,
            ip_fresh_sessions_limit=10,
            fresh_session_grace_period=5,
            datetime_format='%Y-%m-%d %H:%M:%S',
            logger=None,
    ):
        super().__init__()
        self.topic_weblogs = topic_weblogs
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.session_inactivity = session_inactivity
        self.flush_window_seconds = flush_window_seconds
        self.garbage_collection_period = garbage_collection_period
        self.whitelist_url_default = whitelist_url_default,
        self.whitelist_ip = whitelist_ip
        self.whitelist_url = whitelist_url
        self.reset_duration = reset_duration
        self.white_list_refresh_period = white_list_refresh_period
        self.fresh_session_grace_period = fresh_session_grace_period

        self.max_fresh_sessions_per_ip = max_fresh_sessions_per_ip
        self.fresh_session_ttl_minutes = fresh_session_ttl_minutes
        self.ip_fresh_sessions_limit = ip_fresh_sessions_limit
        self.date_time_format = datetime_format

        self.logger = logger

    @staticmethod
    def get_timestamp_and_data(data):
        if 'datestamp' not in data:
            data = json.loads(data['message'].replace('000', '0'))
            timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S')
        else:
            timestamp = datetime.strptime(data['datestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return timestamp, data

    def flush_fresh_sessions(self, producer, fresh_session, ts):
        requests = []
        for session in fresh_session['sessions'].values():
            if (ts - session['start']).total_seconds() > self.fresh_session_grace_period:
                requests.append(session['requests'][0])
        if len(requests) < self.ip_fresh_sessions_limit:
            return

        requests_formatted = copy.deepcopy(requests)
        for q in requests_formatted:
            q['ts'] = q['ts'].strftime(self.date_time_format)

        message = {
            'host': fresh_session['host'],
            'ua': fresh_session['ua'],
            'country': fresh_session['country'],
            'session_id': fresh_session['session_id'],
            'ip': fresh_session['ip'],
            'start': fresh_session['start'].strftime(self.date_time_format),
            'end': fresh_session['end'].strftime(self.date_time_format),
            'duration': fresh_session['duration'],
            'fresh_sessions': True,
            'requests': requests_formatted,
        }
        producer.send(
            self.topic_sessions,
            key=bytearray(fresh_session['host'], encoding='utf8'),
            value=json.dumps(message).encode('utf-8')
        )

    def flush_session(self, producer, session):
        if session['duration'] == 0:
            return

        requests = session['requests']
        requests_formatted = copy.deepcopy(requests)
        for q in requests_formatted:
            q['ts'] = q['ts'].strftime(self.date_time_format)

        message = {
            'host': session['host'],
            'ua': session['ua'],
            'country': session['country'],
            'session_id': session['session_id'],
            'ip': session['ip'],
            'start': session['start'].strftime(self.date_time_format),
            'end': session['end'].strftime(self.date_time_format),
            'duration': session['duration'],
            'requests': requests_formatted,
        }
        producer.send(
            self.topic_sessions,
            key=bytearray(session['host'], encoding='utf8'),
            value=json.dumps(message).encode('utf-8')
        )

        if session['ua'] == 'Baskervillehall':
            self.logger.info(json.dumps(message, indent=2))

    @staticmethod
    def create_session(ua, host, country, ip, session_id, ts, request):
        return {
            'ua': ua,
            'host': host,
            'country': country,
            'ip': ip,
            'session_id': session_id,
            'start': ts,
            'end': ts,
            'duration': 0,
            'requests': [request] if request else []
        }

    @staticmethod
    def update_session(session, request, ts):
        session['end'] = ts
        session['duration'] = (session['end'] - session['start']).total_seconds()
        session['requests'].append(request)

    def is_session_expired(self, session, ts):
        if (ts - session['end']).total_seconds() > self.session_inactivity * 60:
            return True

        if session['duration'] > self.reset_duration * 60:
            return True

        return False

    def run(self):
        whitelist_url = WhitelistURL(url=self.whitelist_url,
                                     whitelist_default=self.whitelist_url_default,
                                     logger=self.logger,
                                     refresh_period_in_seconds=60 * self.white_list_refresh_period)

        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_period)

        consumer = KafkaConsumer(
            **self.kafka_connection,
            group_id=self.kafka_group_id
        )

        producer = KafkaProducer(**self.kafka_connection)
        self.logger.info(f'Starting Baskervillehall sessionizer on topic {self.topic_sessions}')
        confirmed_sessions = dict()
        fresh_sessions = dict()

        garbage_collection_time = datetime.now()

        try:
            consumer.assign([TopicPartition(self.topic_weblogs, self.partition)])
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if not message.value:
                            continue
                        host = message.key.decode('utf-8')
                        if whitelist_url.is_host_whitelisted(host):
                            continue
                        ts, data = self.get_timestamp_and_data(json.loads(message.value.decode('utf-8')))

                        ip = data['client_ip']
                        if whitelist_ip.is_in_whitelist(host, ip):
                            continue

                        url = data['client_url']
                        if whitelist_url.is_in_whitelist(url):
                            continue

                        ua = data.get('client_ua', {})
                        country = data.get('geoip', {}).get('country_code2', '')

                        session_id = data.get('deflect_session', '')

                        if session_id == '-' or session_id == '':
                            continue

                        fresh_session = data.get('deflect_session_new', 'false') == 'true'

                        request = {
                            'ts': ts,
                            'url': url,
                            'query': data['querystring'],
                            'code': data['http_response_code'],
                            'type': data['content_type'],
                            'payload': data['reply_length_bytes'],
                            'fresh_session': fresh_session
                        }

                        if fresh_session:
                            if ip in fresh_sessions and session_id in fresh_sessions[ip]['sessions']:
                                self.logger.info(
                                    f'Warning. Ignoring secondary fresh session ip {ip}, session = {session_id}.')
                                continue
                            elif ip in confirmed_sessions and session_id in confirmed_sessions[ip]:
                                session = confirmed_sessions[ip][session_id]
                                if self.is_session_expired(session, ts):
                                    session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                    confirmed_sessions[ip][session_id] = session
                                else:
                                    self.update_session(session, request, ts)
                            else:
                                session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                if ip not in fresh_sessions:

                                    fresh_sessions[ip] = self.create_session(ua, host, country, ip, '-', ts, None)
                                    fresh_sessions[ip]['sessions'] = TTLCache(self.max_fresh_sessions_per_ip,
                                                                              self.fresh_session_ttl_minutes * 60)
                                    fresh_sessions[ip]['flush_ts'] = ts

                                fresh_session = fresh_sessions[ip]
                                fresh_session['sessions'][session_id] = session

                                fresh_session['end'] = ts
                                fresh_session['duration'] = (ts - fresh_session['start']).total_seconds()
                                if len(fresh_session['sessions'].keys()) > self.ip_fresh_sessions_limit and \
                                        (ts - fresh_session[
                                            'flush_ts']).total_seconds() > self.flush_window_seconds:
                                    self.flush_fresh_sessions(producer, fresh_session, ts)
                                    producer.flush()
                                    fresh_session['flush_ts'] = ts

                        else:
                            if ip not in confirmed_sessions:
                                confirmed_sessions[ip] = {}

                            if ip in fresh_sessions and session_id in fresh_sessions[ip]['sessions'].keys():
                                session = fresh_sessions[ip]['sessions'][session_id]

                                confirmed_sessions[ip][session_id] = session
                                del fresh_sessions[ip]['sessions'][session_id]

                                self.update_session(session, request, ts)
                            else:
                                if session_id in confirmed_sessions[ip]:
                                    session = confirmed_sessions[ip][session_id]
                                    if self.is_session_expired(session, ts):
                                        session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                        confirmed_sessions[ip][session_id] = session
                                    else:
                                        self.update_session(session, request, ts)
                                else:
                                    session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                    confirmed_sessions[ip][session_id] = session

                        if session['duration'] > self.flush_window_seconds:
                            if 'flush_ts' not in session \
                                    or (ts - session['flush_ts']).total_seconds() > self.flush_window_seconds:
                                self.flush_session(producer, session)
                                producer.flush()
                                session['flush_ts'] = ts

                        time_now = datetime.now()
                        if (time_now - garbage_collection_time).total_seconds() > self.garbage_collection_period * 60:
                            garbage_collection_time = time_now

                            total_confirmed_sessions = 0
                            num_multi_ips = 0
                            for ip, sessions in confirmed_sessions.items():
                                if len(sessions) == 0:
                                    self.logger.info(f'error ip {ip} has zero sessions ')
                                total_confirmed_sessions += len(sessions.keys())
                                if len(sessions.keys()) > 1:
                                    num_multi_ips += 1

                            self.logger.info(f'Garbage collector. Total confirmed ips: {len(confirmed_sessions.keys())}, '
                                             f'total_confirmed_sessions: {total_confirmed_sessions},'
                                             f'num_multi_ips: {num_multi_ips},'
                                             f' fresh: {len(fresh_sessions.keys())} ')

                            self.logger.info('Garbage collector started...')
                            deleted = 0
                            deleted_ips = 0
                            for ip in list(confirmed_sessions.keys()):
                                sessions = confirmed_sessions[ip]
                                for session_id in list(sessions.keys()):
                                    session = sessions[session_id]
                                    if (time_now - session['end']).total_seconds() > self.session_inactivity * 60:
                                        self.flush_session(producer, session)
                                        del sessions[session_id]
                                        deleted += 1
                                        if len(sessions) == 0:
                                            del confirmed_sessions[ip]
                                            deleted_ips += 1
                                            break
                            producer.flush()

                            # for ip in fresh_sessions.keys():
                            #    not_confirmed_sessions = list()
                            #    for session_id, session in fresh_sessions[ip].items():
                            #        if (time_now - session['end']).total_seconds() > self.fresh_session_decision_delay:
                            #            not_confirmed_sessions.append(session)
                            #    if len(not_confirmed_sessions) > self.ip_fresh_sessions_limit:

                            total_confirmed_sessions = 0
                            num_multi_ips = 0
                            for ip, sessions in confirmed_sessions.items():
                                total_confirmed_sessions += len(sessions)
                                if len(sessions) > 1:
                                    num_multi_ips += 1
                                if len(sessions) == 0:
                                    self.logger.info(f'ip {ip} has no sessions!!!! error ')

                            total_fresh_sessions = 0
                            for ip, sessions in fresh_sessions.items():
                                total_fresh_sessions += len(sessions)

                            self.logger.info(f'Garbage collector. Total confirmed ips: {len(confirmed_sessions)}, '
                                             f'total_confirmed_sessions: {total_confirmed_sessions},'
                                             f'num_multi_ips: {num_multi_ips},'
                                             f' fresh ips: {len(fresh_sessions)} '
                                             f' fresh sessions: {total_fresh_sessions} '
                                             f' deleted {deleted_ips} ips and {deleted} sessions.')

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
