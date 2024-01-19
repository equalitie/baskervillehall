import copy
import urllib.parse

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
            kafka_group_id='baskervillehall_session',
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            flush_window_seconds=60,
            min_session_duration=10,
            reset_duration=5,
            session_inactivity=1,
            garbage_collection_period=2,
            single_session_period_seconds=30,
            whitelist_url=None,
            whitelist_url_default=[],
            whitelist_ip=None,
            white_list_refresh_period=5,
            max_fresh_sessions_per_ip=10000,
            fresh_session_ttl_minutes=3,
            fresh_session_grace_period=5,
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
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.session_inactivity = session_inactivity
        self.flush_window_seconds = flush_window_seconds
        self.garbage_collection_period = garbage_collection_period
        self.whitelist_url_default = whitelist_url_default
        self.whitelist_ip = whitelist_ip
        self.whitelist_url = whitelist_url
        self.reset_duration = reset_duration
        self.white_list_refresh_period = white_list_refresh_period
        self.fresh_session_grace_period = fresh_session_grace_period
        self.single_session_period_seconds = single_session_period_seconds

        self.max_fresh_sessions_per_ip = max_fresh_sessions_per_ip
        self.fresh_session_ttl_minutes = fresh_session_ttl_minutes
        self.date_time_format = datetime_format
        self.min_session_duration = min_session_duration
        self.read_from_beginning = read_from_beginning

        self.logger = logger
        self.debug_ip = debug_ip

        self.num_flushed_fresh_sessions = 0  # for debugging only
        self.min_number_of_requests = min_number_of_requests

    @staticmethod
    def get_timestamp_and_data(data):
        if 'datestamp' not in data:
            data = json.loads(data['message'].replace('000', '0'))
            timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S')
        else:
            timestamp = datetime.strptime(data['datestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return timestamp, data

    def flush_session(self, producer, session):
        requests = session['requests']
        requests_formatted = []
        for r in requests:
            rf = copy.deepcopy(r)
            rf['ts'] = r['ts'].strftime(self.date_time_format)
            requests_formatted.append(rf)

        message = {
            'host': session['host'],
            'ua': session['ua'],
            'country': session['country'],
            'session_id': session['session_id'],
            'ip': session['ip'],
            'start': session['start'].strftime(self.date_time_format),
            'end': session['end'].strftime(self.date_time_format),
            'duration': session['duration'],
            'fresh_sessions': session.get('fresh_sessions', False),
            'requests': requests_formatted,
        }
        producer.send(
            self.topic_sessions,
            key=bytearray(session['host'], encoding='utf8'),
            value=json.dumps(message).encode('utf-8')
        )

        if session['ua'] == 'Baskervillehall':
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

    def run(self):
        whitelist_url = WhitelistURL(url=self.whitelist_url,
                                     whitelist_default=self.whitelist_url_default,
                                     logger=self.logger,
                                     refresh_period_in_seconds=60 * self.white_list_refresh_period)

        whitelist_ip = WhitelistIP(self.whitelist_ip, logger=self.logger,
                                   refresh_period_in_seconds=60 * self.white_list_refresh_period)

        try:
            consumer = KafkaConsumer(
                **self.kafka_connection,
                group_id=self.kafka_group_id
            )

            producer = KafkaProducer(**self.kafka_connection)
            self.logger.info(f'Starting Baskervillehall sessionizer on topic {self.topic_sessions}, '
                             f'partition {self.partition}')
            ip_sessions = dict()

            garbage_collection_time = datetime.now()
            single_session_time = datetime.now()

            consumer.assign([TopicPartition(self.topic_weblogs, self.partition)])
            if self.read_from_beginning:
                consumer.seek_to_beginning()

            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if not message.value:
                            continue
                        ts, data = self.get_timestamp_and_data(json.loads(message.value.decode('utf-8')))

                        ip = data['client_ip']

                        debugging = self.debug_ip and ip == self.debug_ip

                        host = message.key.decode('utf-8')
                        if whitelist_url.is_host_whitelisted(host):
                            if debugging:
                                self.logger.info(f'host {host} is whitelisted')
                            continue

                        if whitelist_ip.is_in_whitelist(host, ip):
                            if debugging:
                                self.logger.info(f'ip {ip} is whitelisted')
                            continue

                        url = data['client_url']
                        if whitelist_url.is_in_whitelist(url):
                            if debugging:
                                self.logger.info(f'host {host} url {url} is whitelisted')
                            continue

                        ua = data.get('client_ua', {})
                        country = data.get('geoip', {}).get('country_code2', '')

                        session_id = data.get('deflect_session', '')

                        origin_session_id = session_id
                        if len(session_id) > 5:
                            if session_id[-2:] == '==':
                                session_id = urllib.parse.quote_plus(session_id)
                            elif session_id[-5:] =='%253D':
                                session_id = urllib.parse.unquote(session_id)

                        if debugging:
                            deflect_session_new = data['deflect_session_new']
                            deflect_session = data['deflect_session']
                            client_url = data['client_url']
                            self.logger.info(f'@@@@ {deflect_session}, {deflect_session_new}, {client_url}')

                        request = {
                            'ts': ts,
                            'url': url,
                            'ua': ua,
                            'query': data['querystring'],
                            'code': data['http_response_code'],
                            'type': data['content_type'],
                            'payload': data['reply_length_bytes'],
                        }

                        if ip not in ip_sessions:
                            ip_sessions[ip] = {}

                        if session_id in ip_sessions[ip]:
                            if debugging:
                                self.logger.info('session exists')
                            session = ip_sessions[ip][session_id]
                            if self.is_session_expired(session, ts):
                                if debugging:
                                    self.logger.info('session is expired')
                                session = self.create_session(ua, host, country, ip, session_id, ts, request)
                                ip_sessions[ip][session_id] = session
                            else:
                                if debugging:
                                    self.logger.info('updating session')
                            self.update_session(session, request)
                        else:
                            if debugging:
                                self.logger.info('creating new  session')
                            session = self.create_session(ua, host, country, ip, session_id, ts, request)
                            ip_sessions[ip][session_id] = session

                        if 'flush_ts' not in session or \
                                (ts - session.get('flush_ts', ts)).total_seconds() > self.flush_window_seconds:

                            if session['duration'] > self.min_session_duration or \
                                    len(session['requests']) > self.min_number_of_requests:
                                if debugging:
                                    self.logger.info('flushing session')
                                    duration = session['duration']
                                    length = len(session['requests'])
                                    self.logger.info(f'duration={duration}, requests={length}')
                                self.flush_session(producer, session)
                                producer.flush()
                                session['flush_ts'] = ts

                        time_now = datetime.now()

                        if (time_now - single_session_time).total_seconds() > self.single_session_period_seconds:
                            single_session_time = time_now

                            for ip, sessions in ip_sessions.items():
                                single_session_count = 0
                                for _, session in sessions.items():
                                    if len(session['requests']) == 1 and \
                                            (ts - session['requests'][0]['ts']).total_seconds() > \
                                            self.fresh_session_grace_period:
                                        single_session_count += 1
                                        if single_session_count > self.min_number_of_requests:
                                            break
                                if single_session_count > self.min_number_of_requests:
                                    host_session = {}
                                    for _, session in sessions.items():
                                        if len(session['requests']) == 1 and \
                                                (ts - session['requests'][0]['ts']).total_seconds() > \
                                                self.fresh_session_grace_period:
                                            request = session['requests'][0]
                                            if session['host'] not in host_session:
                                                combined_session = self.create_session(
                                                    session['ua'],
                                                    session['host'],
                                                    session['country'],
                                                    ip,
                                                    '-',
                                                    session['start'],
                                                    request)
                                                combined_session['fresh_sessions'] = True
                                                host_session[session['host']] = combined_session
                                            else:
                                                self.update_session(host_session[session['host']], request)

                                    for host, session in host_session.items():
                                        if len(session['requests']) > self.min_number_of_requests and \
                                                session['duration'] > self.min_session_duration:
                                            self.flush_session(producer, session)
                            producer.flush()

                        if (time_now - garbage_collection_time).total_seconds() > self.garbage_collection_period * 60:
                            garbage_collection_time = time_now

                            total_sessions = 0
                            num_multi_session_ips = 0
                            for ip, sessions in ip_sessions.items():
                                if len(sessions) == 0:
                                    self.logger.info(f'error ip {ip} has zero sessions ')
                                total_sessions += len(sessions.keys())
                                if len(sessions.keys()) > 1:
                                    num_multi_session_ips += 1

                            self.logger.info(
                                    f'Garbage collector - START. Total ips: {len(ip_sessions.keys())}, '
                                f'total_sessions: {total_sessions},'
                                f'num_multi_session_ips: {num_multi_session_ips}')
                            deleted = 0
                            deleted_ips = 0
                            for ip in list(ip_sessions.keys()):
                                sessions = ip_sessions[ip]
                                for session_id in list(sessions.keys()):
                                    session = sessions[session_id]
                                    if (time_now - session['end']).total_seconds() > self.session_inactivity * 60:
                                        if debugging:
                                            self.logger.info('garbage flush  session')
                                            self.logger.info(session)
                                        if session['duration'] > self.min_session_duration or \
                                                len(session['requests']) > self.min_number_of_requests:
                                            self.flush_session(producer, session)
                                        del sessions[session_id]
                                        deleted += 1
                                if len(sessions.keys()) == 0:
                                    del ip_sessions[ip]
                                    deleted_ips += 1

                            producer.flush()

                            total_sessions = 0
                            num_multi_session_ips = 0
                            for ip, sessions in ip_sessions.items():
                                total_sessions += len(sessions)
                                if len(sessions) > 1:
                                    num_multi_session_ips += 1

                            total_fresh_sessions = 0
                            for ip, sessions in ip_sessions.items():
                                total_fresh_sessions += len(sessions)

                            self.logger.info(f'Garbage collector - FINISH. Total  ips: {len(ip_sessions)}, '
                                             f'total_sessions: {total_sessions},'
                                             f'num_multi_session_ips: {num_multi_session_ips},'
                                             f' deleted {deleted_ips} ips and {deleted} sessions.'
                                             )

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

