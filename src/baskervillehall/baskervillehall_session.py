import copy
from collections import defaultdict

from baskervillehall.whitelist_ip import WhitelistIP
from baskervillehall.whitelist_url import WhitelistURL
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from datetime import datetime, timedelta
import numpy as np


class BaskervillehallSession(object):
    def __init__(
            self,
            topic_weblogs='BASKERVILLEHALL_WEBLOGS',
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            kafka_group_id='baskervillehall_session',
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            window_duration=1,
            reset_duration=5,
            session_inactivity=1,
            secondary_validation_period=2,
            datetime_format='%Y-%m-%d %H:%M:%S',
            garbage_collection_period=30,
            whitelist_url=None,
            whitelist_url_default=[],
            whitelist_ip=None,
            white_list_refresh_period=5,
            logger=None,
    ):
        super().__init__()
        self.topic_weblogs = topic_weblogs
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.session_inactivity = session_inactivity
        self.secondary_validation_period = secondary_validation_period
        self.window_duration = window_duration
        self.date_time_format = datetime_format
        self.garbage_collection_period = garbage_collection_period
        self.whitelist_url_default = whitelist_url_default,
        self.whitelist_ip = whitelist_ip
        self.whitelist_url = whitelist_url
        self.reset_duration = reset_duration
        self.white_list_refresh_period = white_list_refresh_period
        self.logger = logger

    @staticmethod
    def get_timestamp_and_data(data):
        if 'datestamp' not in data:
            data = json.loads(data['message'].replace('000', '0'))
            timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S')
        else:
            timestamp = datetime.strptime(data['datestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return timestamp, data

    def get_features(self, session):
        requests = session['requests']
        features = {}

        hits = float(len(requests))
        intervals = []
        num_4xx = 0
        num_5xx = 0
        url_map = defaultdict(int)
        query_map = defaultdict(int)
        num_html = 0
        num_image = 0
        num_js = 0
        num_css = 0
        slash_counts = []
        payloads = []
        for i in range(len(requests)):
            request = requests[i]
            if i < len(requests) - 1:
                intervals.append((requests[i+1]['ts'] - request['ts']).total_seconds())
            code = request['code']
            if code // 100 == 4:
                num_4xx += 1
            if code // 100 == 5:
                num_5xx += 1
            url = request['url']
            payloads.append(request['payload'] + 1.0)
            slash_counts.append(len(url.split('/'))-1)
            url_map[url] += 1
            query_map[request['query']] += 1
            content_type = request['type']
            if content_type == 'text/html' or \
                    content_type == 'text/html; charset=UTF-8' or \
                    content_type == 'text/html; charset=utf-8':
                num_html += 1
            elif 'image' in content_type:
                num_image += 1
            elif 'javascript' in content_type:
                num_js += 1
            elif content_type == 'text/css' or \
                    content_type == 'text/css; charset=UTF-8' or \
                    content_type == 'text/css; charset=utf-8':
                num_css += 1

        intervals = np.array(intervals)
        unique_path = float(len(url_map.keys()))
        unique_query = float(len(query_map.keys()))
        session_duration = session['duration']

        features['request_rate'] = hits / session_duration * 60
        mean_intervals = np.mean(intervals)
        features['request_interval_average'] = mean_intervals
        features['request_interval_std'] = np.sqrt(np.mean((intervals-mean_intervals)**2))
        features['response4xx_to_request_ratio'] = num_4xx / hits
        features['response5xx_to_request_ratio'] = num_5xx / hits
        features['top_page_to_request_ratio'] = max(url_map.values()) / hits
        features['unique_path_rate'] = unique_path / session_duration * 60
        features['unique_path_to_request_ratio'] = unique_path / hits
        features['unique_query_rate'] = unique_query  / session_duration * 60
        features['unique_query_to_unique_path_ratio'] = unique_query / unique_path
        features['image_to_html_ratio'] = float(num_image) / num_html if num_html > 0 else 10.0
        features['js_to_html_ratio'] = float(num_js) / num_html if num_html > 0 else 10.0
        features['css_to_html_ratio'] = float(num_css) / num_html if num_html > 0 else 10.0
        mean_depth = np.mean(slash_counts)
        features['path_depth_average'] = mean_depth
        features['path_depth_std'] = np.sqrt(np.mean((slash_counts-mean_depth)**2))
        features['payload_size_log_average'] = np.mean(np.log(payloads))

        return features

    def flush_session(self, producer, session_id, session):
        if session['duration'] == 0:
            return

        requests = session['requests']
        requests_formatted = copy.deepcopy(requests)
        for q in requests_formatted:
            q['ts'] = q['ts'].strftime(self.date_time_format)

        message = {
            'host': session['host'],
            'country': session['country'],
            'session_id': session_id,
            'ip': session['ip'],
            'start': session['start'].strftime(self.date_time_format),
            'end': session['end'].strftime(self.date_time_format),
            'duration': session['duration'],
            'queries': requests_formatted,
            'features': self.get_features(session)
        }
        producer.send(
            self.topic_sessions,
            key=bytearray(session['host'], encoding='utf8'),
            value=json.dumps(message).encode('utf-8')
        )

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
        sessions = {}

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

                        session_id = data['client_ip']  # using IP for now

                        request = {
                            'ts': ts,
                            'url': url,
                            'query': data['querystring'],
                            'code': data['http_response_code'],
                            'type': data['content_type'],
                            'payload': data['reply_length_bytes']
                        }

                        create_new_session = False
                        if session_id not in sessions:
                            create_new_session = True
                        else:
                            session = sessions[session_id]
                            if (ts - session['end']).total_seconds() > self.session_inactivity * 60:
                                create_new_session = True

                            if session['duration'] > self.reset_duration * 60:
                                create_new_session = True

                        if create_new_session:
                            sessions[session_id] = {
                                'host': host,
                                'country': data.get('geoip', {}).get('country_code2', ''),
                                'ip': ip,
                                'session_id': session_id,
                                'start': ts,
                                'end': ts,
                                'duration': 0,
                                'requests': [request]
                            }
                        else:
                            session['end'] = ts
                            duration = (session['end'] - session['start']).total_seconds()
                            session['duration'] = duration
                            session['requests'].append(request)

                            if duration > self.window_duration * 60:
                                if 'flush_ts' not in session:
                                    self.flush_session(producer, session_id, session)
                                elif (ts - session['flush_ts']).total_seconds() > self.window_duration * 60:
                                    self.flush_session(producer, session_id, session)
                                session['flush_ts'] = ts

                        time_now = datetime.now()
                        if (time_now - garbage_collection_time).total_seconds() > self.garbage_collection_period * 60:
                            garbage_collection_time = time_now
                            self.logger.info('Garbage collector started...')
                            deleted = 0
                            for session_id in list(sessions.keys()):
                                session = sessions[session_id]
                                if (time_now - session['end']).total_seconds() > self.session_inactivity * 60:
                                    self.flush_session(producer, session_id, session)
                                    del sessions[session_id]
                                    deleted += 1
                            self.logger.info(f'Garbage collector. Total: {len(sessions)}, sessions, deleted {deleted} sessions.')

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
