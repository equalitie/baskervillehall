import copy
import urllib.parse
import random
import string

from baskervillehall.whitelist_ip import WhitelistIP
from baskervillehall.whitelist_url import WhitelistURL
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from datetime import datetime


class BaskervillehallSession(object):
    def __init__(
            self,
            topic_weblogs='BASKERVILLEHALL_WEBLOGS',
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            flush_increment=10,
            min_session_duration=5,
            max_session_duration=60,
            reset_duration=5,
            session_inactivity=1,
            garbage_collection_period=2,
            whitelist_url=None,
            whitelist_url_default=[],
            whitelist_ip=None,
            white_list_refresh_period=5,
            max_primary_sessions_per_ip=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            read_from_beginning=False,
            min_number_of_requests=10,
            debug_ip=None,
            logger=None,
    ):
        super().__init__()
        self.topic_weblogs = topic_weblogs
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.kafka_connection = kafka_connection
        self.session_inactivity = session_inactivity
        self.flush_increment = flush_increment
        self.garbage_collection_period = garbage_collection_period
        self.whitelist_url_default = whitelist_url_default
        self.whitelist_ip = whitelist_ip
        self.whitelist_url = whitelist_url
        self.reset_duration = reset_duration
        self.white_list_refresh_period = white_list_refresh_period

        self.max_primary_sessions_per_ip = max_primary_sessions_per_ip
        self.date_time_format = datetime_format
        self.min_session_duration = min_session_duration
        self.max_session_duration = max_session_duration
        self.read_from_beginning = read_from_beginning

        self.logger = logger
        self.debug_ip = debug_ip

        self.num_flushed_primary_sessions = 0  # for debugging only
        self.min_number_of_requests = min_number_of_requests
        self.producer = None
        self.ips = dict()
        self.ips_primary = dict()
        self.flush_size_primary = dict()
        self.debugging = False

    @staticmethod
    def get_timestamp_and_data(data):
        if 'datestamp' not in data:
            data = json.loads(data['message'].replace('000', '0'))
            timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S')
        else:
            timestamp = datetime.strptime(data['datestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return timestamp, data

    def flush(self):
        self.producer.flush()

    def check_and_send_session(self, session, ts):
        if session['duration'] < self.max_session_duration:
            size = len(session['requests'])
            if session['duration'] > self.min_session_duration or \
                    size > self.min_number_of_requests:
                if 'flush_size' not in session or \
                        size - session.get('flush_size') > self.flush_increment:
                    self.send_session(session)
                    session['flush_size'] = size

    def send_session(self, session):
        requests = session['requests']
        requests_formatted = []
        passed_challenge = False
        deflect_password = False
        requests_sorted = sorted(requests, key=lambda x: x['ts'])
        duration = (requests_sorted[-1]['ts'] - requests_sorted[0]['ts']).total_seconds()

        for r in requests_sorted:
            rf = copy.deepcopy(r)
            rf['ts'] = r['ts'].strftime(self.date_time_format)
            if r['banjax_decision'] == 'ShaChallengePassed':
                passed_challenge = True
            if (r['banjax_decision'] in
                    ['PasswordChallengePassed', 'PasswordProtectedPriorityPass', 'PasswordChallengeRoamingPassed']):
                deflect_password = True
            requests_formatted.append(rf)

        message = {
            'host': session['host'],
            'ua': session['ua'],
            'country': session['country'],
            'session_id': session['session_id'],
            'ip': session['ip'],
            'start': requests_formatted[0]['ts'],
            'end': requests_formatted[-1]['ts'],
            'duration': duration,
            'primary_session': session.get('primary_session', False),
            'requests': requests_formatted,
            'passed_challenge': passed_challenge,
            'deflect_password': deflect_password
        }
        self.producer.send(
            self.topic_sessions,
            key=bytearray(session['host'], encoding='utf8'),
            value=json.dumps(message).encode('utf-8')
        )

        if self.debugging:
            self.logger.info(
                f'@@@@ DEBUG host={session["host"]}, ip={session["ip"]}, session_id={session["session_id"]}'
                f'end={session["end"]}, num_requests={len(session["requests"])}')

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
            'time_now': datetime.now(),
            'requests': [request] if request else []
        }

    @staticmethod
    def update_session(session, request):
        session['end'] = request['ts']
        session['duration'] = (session['end'] - session['start']).total_seconds()
        session['requests'].append(request)

    def is_session_expired(self, session, ts):
        if (ts - session['end']).total_seconds() > self.session_inactivity * 60:
            return True

        if session['duration'] > self.reset_duration * 60:
            return True

        return False

    def gc(self):
        time_now = datetime.now()
        total_sessions = 0
        for ip, sessions in self.ips.items():
            if len(sessions) == 0:
                self.logger.info(f'error ip {ip} has zero sessions ')
            total_sessions += len(sessions)
        total_primary_sessions = 0
        for ip, sessions in self.ips_primary.items():
            total_primary_sessions += len(sessions)
        total_ips = len(self.ips)

        deleted = 0
        deleted_ips = 0
        for ip in list(self.ips.keys()):
            sessions = self.ips[ip]
            for session_id in list(sessions):
                session = sessions[session_id]
                if (time_now - session['time_now']).total_seconds() > self.session_inactivity * 60:
                    del sessions[session_id]
                    deleted += 1
            if len(sessions) == 0:
                del self.ips[ip]
                deleted_ips += 1

        deleted_primary = 0
        for ip in list(self.ips_primary.keys()):
            sessions = self.ips_primary[ip]
            for session_id in list(sessions):
                session = sessions[session_id]
                if (time_now - session['time_now']).total_seconds() > self.session_inactivity * 60:
                    del sessions[session_id]
                    deleted_primary += 1
            if len(sessions) == 0:
                del self.ips_primary[ip]

        self.flush_size_primary = dict()

        self.logger.info(
            f'Garbage collector. \nips: {total_ips - deleted_ips} - {deleted_ips} deleted, \n'
            f'sessions: {total_sessions - deleted} -  {deleted} deleted, \n'
            f'primary: {total_primary_sessions - deleted_primary} - {deleted_primary} deleted \n')

    def collect_primary_session(self, ip):
        primary_sessions = self.ips_primary[ip]
        size = len(primary_sessions)
        if size > self.max_primary_sessions_per_ip:
            if ip in self.flush_size_primary:
                increment = size - self.flush_size_primary[ip]
                if increment < self.flush_increment:
                    if self.debugging:
                        self.logger.info(
                            f'session size increment {increment} < {self.flush_increment}, skipping flushing...')
                    return
            self.flush_size_primary[ip] = size

            hosts = {}
            for session_id in list(primary_sessions.keys()):
                s = primary_sessions[session_id]
                host = s['host']
                if host not in hosts:
                    hosts[host] = []
                hosts[host].append(s)

            for host, sessions in hosts.items():
                requests = [s['requests'][0] for s in sessions]
                requests = sorted(requests, key=lambda x: x['ts'])
                session = {
                    'ua': sessions[0]['ua'],
                    'host': host,
                    'country': sessions[0]['country'],
                    'ip': ip,
                    'session_id': '-',
                    'start': requests[0]['ts'],
                    'end': requests[-1]['ts'],
                    'duration': (requests[-1]['ts'] - requests[0]['ts']).total_seconds(),
                    'requests': requests,
                    'primary_session': True
                }

                if session['duration'] < self.max_session_duration:
                    if self.debugging:
                        self.logger.info('flushing primary session')
                        self.logger.info(f'ip={ip}, hits={len(session["requests"])} host={host}')
                    self.send_session(session)
                else:
                    if self.debugging:
                        self.logger.info(f'primary session is too long {session["duration"]} >= '
                                         f'{self.max_session_duration}')
                        self.logger.info(f'ip={ip}, hits={len(session["requests"])} host={host}')

            self.flush()

    def is_debugging_mode(self, data):
        if self.debug_ip and data['client_ip'] == self.debug_ip:
            return True
        if 'Baskerville' in data.get('client_ua', ''):
            return True
        return False

    def run(self):
        whitelist_url = WhitelistURL(url=self.whitelist_url,
                                     whitelist_default=self.whitelist_url_default,
                                     logger=self.logger,
                                     refresh_period_in_seconds=60 * self.white_list_refresh_period)

        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_period)

        try:
            consumer = KafkaConsumer(
                **self.kafka_connection
            )

            self.producer = KafkaProducer(**self.kafka_connection)
            self.logger.info(f'Starting Baskervillehall sessionizer on topic {self.topic_sessions}, '
                             f'partition {self.partition}')

            ts_gc = datetime.now()

            consumer.assign([TopicPartition(self.topic_weblogs, self.partition)])
            if self.read_from_beginning:
                consumer.seek_to_beginning()
            else:
                consumer.seek_to_end()
            ts_lag_report = datetime.now()
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=200, update_offsets=False)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if (datetime.now() - ts_lag_report).total_seconds() > 5:
                            highwater = consumer.highwater(topic_partition)
                            lag = (highwater - 1) - message.offset
                            self.logger.info(f'Lag = {lag}')
                            ts_lag_report = datetime.now()
                        if not message.value:
                            continue
                        ts, data = self.get_timestamp_and_data(json.loads(message.value.decode('utf-8')))

                        ip = data['client_ip']

                        self.debugging = self.is_debugging_mode(data)

                        host = message.key.decode('utf-8')
                        if whitelist_url.is_host_whitelisted(host):
                            if self.debugging:
                                self.logger.info(f'host {host} is whitelisted')
                            continue

                        if whitelist_ip.is_in_whitelist(host, ip):
                            if self.debugging:
                                self.logger.info(f'ip {ip} is whitelisted')
                            continue

                        url = data['client_url']
                        if whitelist_url.is_in_whitelist(url):
                            if self.debugging:
                                self.logger.info(f'host {host} url {url} is whitelisted')
                            continue

                        if 'client_ua' in data:
                            ua = data['client_ua']
                        else:
                            ua = data.get('client_user_agent', '')

                        country = data.get('geoip', {}).get('country_code2', '')

                        session_id = ''
                        if 'deflect_session' in data:
                            session_id = data.get('deflect_session', '')
                        else:
                            if 'cookies' in data:
                                session_id = data['cookies'].get('sessionCookie', '')

                        if len(session_id) < 5:
                            session_id = '-' + ''.join(random.choice(string.ascii_uppercase + string.digits)
                                                       for _ in range(7))

                        if self.debugging:
                            deflect_session = data['deflect_session']
                            client_url = data['client_url']
                            self.logger.info(f'@@@@ {deflect_session}, {client_url}')

                        request = {
                            'ts': ts,
                            'url': url,
                            'ua': ua,
                            'query': data['querystring'],
                            'code': data['http_response_code'],
                            'type': data['content_type'],
                            'payload': int(data['reply_length_bytes']),
                            'method': data['client_request_method'],
                            'edge': data.get('edge', ''),
                            'static': data.get('loc_in', '') == 'static_file',
                            'banjax_decision': data.get('banjax_decision', '')
                        }

                        if ip in self.ips and session_id in self.ips[ip]:
                            # existing session
                            if self.debugging:
                                self.logger.info('session exists')
                            session = self.ips[ip][session_id]
                            if self.is_session_expired(session, ts):
                                if self.debugging:
                                    self.logger.info('session is expired')
                                session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                self.ips[ip][session_id] = session
                            else:
                                if self.debugging:
                                    self.logger.info('updating session')
                                self.update_session(session, request)
                            self.check_and_send_session(session, ts)
                            self.flush()
                        elif ip in self.ips_primary and session_id in self.ips_primary[ip]:
                            # maturing session from primary
                            if self.debugging:
                                self.logger.info('maturing primary session')
                            session = self.ips_primary[ip][session_id]
                            self.update_session(session, request)
                            if ip not in self.ips:
                                self.ips[ip] = {}
                            self.ips[ip][session_id] = session
                            del self.ips_primary[ip][session_id]
                            self.check_and_send_session(session, ts)
                            self.flush()
                        else:
                            # primary session
                            session = self.create_session(ua, host, country, ip, session_id, ts, request)
                            if ip not in self.ips_primary:
                                self.ips_primary[ip] = {}
                            self.ips_primary[ip][session_id] = session
                            self.collect_primary_session(ip)

                        time_now = datetime.now()
                        if (time_now - ts_gc).total_seconds() > self.garbage_collection_period * 60:
                            ts_gc = time_now
                            self.gc()

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')
