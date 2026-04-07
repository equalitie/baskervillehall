# -*- coding: utf-8 -*-
import random
import string
import re
import psutil
import time as time_module
from collections import OrderedDict
from datetime import datetime, time, timezone

from baskervillehall.baskerville_rules import (is_human,
                                               get_baskerville_score_1,
                                               get_baskerville_score_2,
                                               get_baskerville_score_3)
from baskervillehall.frequency_analyzer import FrequencyAnalyzer
from baskervillehall.session_fingerprints import SessionFingerprints
from baskervillehall.vpn_detector import VpnDetector
from baskervillehall.asn_database import ASNDatabase
from baskervillehall.asn_database2 import ASNDatabase2
from baskervillehall import baskerville_rules
from baskervillehall.bot_verificator import BotVerificator
from baskervillehall.country_blocker import CountryBlocker
from baskervillehall.settings_deflect_api import SettingsDeflectAPI
from baskervillehall.tor_exit_scanner import TorExitScanner
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import NoBrokersAvailable
from kafka.consumer.subscription_state import ConsumerRebalanceListener

# Fast JSON / ISO date parsing
try:
    import orjson as _json


    def _loads(b):
        return _json.loads(b)


    def _dumps(o):
        return _json.dumps(o)  # -> bytes
except Exception:  # pragma: no cover
    import json as _json


    def _loads(b):
        if isinstance(b, (bytes, bytearray)):
            return _json.loads(b.decode('utf-8', errors='replace'))
        return _json.loads(b)


    def _dumps(o):
        return _json.dumps(o).encode('utf-8')

try:
    import ciso8601 as _ciso


    def _iso(s):
        return _ciso.parse_datetime(s)
except Exception:  # pragma: no cover
    def _iso(s):
        if s.endswith('Z'):
            s = s[:-1]
        try:
            # Python 3.11 supports microseconds without %f
            return datetime.fromisoformat(s)
        except Exception:
            # Fallback: "YYYY-MM-DDTHH:MM:SS.mmm"
            return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f')

APACHE_MONTH = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

VIDEO_REGEX = re.compile(
    r'^[^ ]+\.(mp4|webm|mov|avi|mkv|flv|wmv|mpeg|mpg|m3u8|mpd)(\?.*)?$',
    re.IGNORECASE
)

class RebalanceLogger(ConsumerRebalanceListener):
    def __init__(self, logger, name="consumer"):
        self.logger = logger
        self.name = name

    def on_partitions_revoked(self, revoked):
        self.logger.warning("[%s] REVOKED: %s", self.name, sorted(revoked, key=lambda tp:(tp.topic,tp.partition)))

    def on_partitions_assigned(self, assigned):
        self.logger.warning("[%s] ASSIGNED: %s", self.name, sorted(assigned, key=lambda tp:(tp.topic,tp.partition)))


def log_partition_assignment(consumer, logger):
    """Logs current partitions, offsets and lag owned by this consumer."""
    try:
        assignment = consumer.assignment()  # set[TopicPartition]
        subs = consumer.subscription()  # set[str] or None
        if not assignment:
            logger.info("Assignment: ∅ (no partitions yet) | subscription=%s", list(subs) if subs else "None")
            return

        tps = sorted(list(assignment), key=lambda tp: (tp.topic, tp.partition))
        # Offsets
        ends = consumer.end_offsets(tps)  # {TopicPartition: int}
        begins = consumer.beginning_offsets(tps)  # {TopicPartition: int}
        positions = {tp: consumer.position(tp) for tp in tps}  # {TopicPartition: int or None}

        lines = []
        for tp in tps:
            pos = positions.get(tp) or 0
            end = ends.get(tp) or 0
            beg = begins.get(tp) or 0
            lag = max(0, end - pos)
            lines.append(f"{tp.topic}[{tp.partition}] pos={pos} begin={beg} end={end} lag={lag}")

        logger.info("Assignment (%d): %s | subscription=%s",
                    len(tps), "; ".join(lines), list(subs) if subs else "None")
    except Exception as e:
        logger.warning(f"Assignment logging failed: {e!r}")


def _parse_apache_ts(s: str) -> datetime:
    """
    Fast parser for "10/Mar/2025:12:34:56 ..." format -> naive-UTC (ignores timezone)
    """
    main = s.split(' ')[0]
    DD = int(main[0:2])
    Mon = APACHE_MONTH[main[3:6]]
    YYYY = int(main[7:11])
    HH = int(main[12:14])
    MM = int(main[15:17])
    SS = int(main[18:20])
    return datetime(YYYY, Mon, DD, HH, MM, SS)


def _to_naive_utc(dt: datetime) -> datetime:
    """Convert to naive-UTC (without tzinfo)."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class BaskervillehallSession(object):
    def __init__(
            self,
            topic_weblogs='BASKERVILLEHALL_WEBLOGS',
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            group_id='session_pipeline',
            kafka_connection=None,
            kafka_connection_output=None,
            flush_increment=10,
            min_session_duration=5,
            max_session_duration=60,
            reset_duration=5,
            session_inactivity=1,
            garbage_collection_period=2,
            deflect_config_url=None,
            deflect_config_auth=None,
            whitelist_url_default=None,
            white_list_refresh_period=2,
            max_primary_sessions_per_ip=10,
            primary_session_expiration=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            read_from_beginning=False,
            min_number_of_requests=10,
            debug_ip=None,
            logger=None,
            asn_database_path='',
            asn_database2_path='',
            postgres_connection=None,
            max_session_age_hours=24,
            lag_high_threshold=10000,
            lag_moderate_threshold=5000,
            lag_emergency_threshold=20000,
            lag_critical_threshold=15000,
            enable_emergency_partition_seek=True,
            score_2_num_requests=5,
            hostname='localhost',
            topic_commands=None,
            dnet_partition_map=None
    ):
        super().__init__()
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        if kafka_connection_output is None:
            kafka_connection_output = {'bootstrap_servers': 'localhost:9092'}
        self.kafka_connection_output = kafka_connection_output
        if whitelist_url_default is None:
            whitelist_url_default = []

        self.hostname = hostname
        self.skip_expensive_op = False
        self.topic_weblogs = topic_weblogs
        self.topic_sessions = topic_sessions
        self.group_id = group_id
        self.kafka_connection = kafka_connection
        self.session_inactivity = session_inactivity
        self.flush_increment = flush_increment
        self.base_flush_increment = flush_increment
        self.garbage_collection_period = garbage_collection_period
        self.whitelist_url_default = whitelist_url_default
        self.deflect_config_url = deflect_config_url
        self.deflect_config_auth = deflect_config_auth
        self.reset_duration = reset_duration
        self.white_list_refresh_period = white_list_refresh_period
        self.max_primary_sessions_per_ip = max_primary_sessions_per_ip
        self.date_time_format = datetime_format
        self.min_session_duration = min_session_duration
        self.max_session_duration = max_session_duration
        self.read_from_beginning = read_from_beginning
        self.logger = logger
        self.dnet_partition_map = dnet_partition_map
        self.debug_ip = debug_ip
        self.primary_session_expiration = primary_session_expiration
        self.min_number_of_requests = min_number_of_requests
        self.max_session_age_hours = max_session_age_hours

        # Lag metrics
        self.current_lag = 0
        self.base_max_records = 2000
        self.lag_high_threshold = lag_high_threshold
        self.lag_moderate_threshold = lag_moderate_threshold
        self.lag_emergency_threshold = lag_emergency_threshold
        self.lag_critical_threshold = lag_critical_threshold
        self.last_lag_check = datetime.utcnow()
        self.messages_dropped = 0
        self.last_emergency_cleanup = datetime.utcnow()
        self.emergency_cleanup_interval = 30  # sec
        self.last_partition_seek = None
        self.partition_seek_interval = 60  # sec - minimum time between partition seeks
        self.enable_emergency_partition_seek = enable_emergency_partition_seek

        # --- state ---
        self.producer = None
        self.ips = dict()
        self.ips_primary = dict()
        self.flush_size_primary = dict()
        self.debugging = False

        self.bot_verificator = BotVerificator(logger=self.logger)
        self.asn_database = ASNDatabase(asn_database_path)
        self.asn_database2 = ASNDatabase2(asn_database2_path)
        self.tor_exit_scnaner = TorExitScanner()
        self.vpn_detector = VpnDetector(
            asn_db=self.asn_database2,
            tor_exit_scanner=self.tor_exit_scnaner,
            logger=self.logger,
            db_config=postgres_connection
        )
        self.fingerprints_analyzer = FrequencyAnalyzer()

        self.settings = SettingsDeflectAPI(
            url=self.deflect_config_url,
            auth=self.deflect_config_auth,
            whitelist_default=self.whitelist_url_default,
            logger=self.logger,
            refresh_period_in_seconds=60 * self.white_list_refresh_period
        )

        self.country_blocker = CountryBlocker(
            kafka_connection=kafka_connection,
            kafka_connection_output=kafka_connection_output,
            topic_commands=topic_commands or 'banjax_command_topic',
            dnet_partition_map=self.dnet_partition_map,
            deflect_api_setting=self.settings,
            logger=self.logger,
        )

        self.primary_collect_cooldown_sec = 2.0
        self._last_primary_collect = {}

        self._last_sess_key = None
        self._last_sess_obj = None
        self._last_sess_where = None

        self._wl_cache = OrderedDict()
        self._wl_cache_ttl = 60
        self._wl_cache_cap = 20000

        self.profiling_enabled = True
        self.profile_stats = {
            'message_processing': 0.0,
            'session_creation': 0.0,
            'session_update': 0.0,
            'session_send': 0.0,
            'primary_collection': 0.0,
            'gc_time': 0.0,
            'rule_processing': 0.0,
            'json_parsing': 0.0,
            'kafka_operations': 0.0,
            'whitelist_checks': 0.0,
            'data_extraction': 0.0,
            'session_lookup': 0.0,
            'request_building': 0.0,
            'bot_verification': 0.0,
            'asn_processing': 0.0,
            'dictionary_ops': 0.0,
            'string_processing': 0.0,
            'msg_decode': 0.0,
            'msg_ts_parse': 0.0,
            'msg_build_request': 0.0,
            'msg_cookie_ua_geo': 0.0,
            'msg_session_lookup': 0.0,
            'msg_bot_verif': 0.0,
            'msg_lag_housekeeping': 0.0,
            'send_format_requests': 0.0,
            'send_serialize': 0.0,
            'send_producer_send': 0.0,
            'primary_collection_send': 0.0,
            'message_count': 0.0,
        }
        self.last_profile_report = datetime.utcnow()
        self.score_2_num_requests = score_2_num_requests

    def _t(self):
        return time_module.time()

    def _acc(self, key, t0):
        self.profile_stats[key] += (time_module.time() - t0)

    def _wl_get(self, key):
        item = self._wl_cache.get(key)
        if not item:
            return None
        ts, val = item
        if time_module.time() - ts > self._wl_cache_ttl:
            self._wl_cache.pop(key, None)
            return None
        self._wl_cache.move_to_end(key)
        return val

    def _wl_put(self, key, val):
        self._wl_cache[key] = (time_module.time(), val)
        self._wl_cache.move_to_end(key)
        if len(self._wl_cache) > self._wl_cache_cap:
            self._wl_cache.popitem(last=False)

    def report_performance_profile(self):
        if self.profile_stats['message_count'] == 0:
            return
        total_time = sum(v for k, v in self.profile_stats.items() if k != 'message_count')
        avg_msg_time = total_time / self.profile_stats['message_count'] if self.profile_stats['message_count'] else 0
        sorted_stats = sorted(
            [(k, v, (v / total_time * 100.0 if total_time > 0 else 0.0))
             for k, v in self.profile_stats.items() if k != 'message_count'],
            key=lambda x: x[1], reverse=True
        )
        self.logger.warning(
            f"🔍 PERFORMANCE PROFILE (last {self.profile_stats['message_count']} messages, {avg_msg_time:.4f}s avg/msg):")
        for name, time_spent, percentage in sorted_stats[:14]:
            if time_spent > 0:
                self.logger.warning(f"  {name}: {time_spent:.3f}s ({percentage:.1f}%)")
        for k in list(self.profile_stats.keys()):
            self.profile_stats[k] = 0.0

    def create_kafka_connections(self, max_retries=10, initial_delay=5):
        consumer = None
        producer = None

        try:
            import orjson as _j
            def _val_ser(v):  # Allow int/float/bool/None keys, converting to strings
                return _j.dumps(v, option=_j.OPT_NON_STR_KEYS)
        except Exception:
            import json as _j
            def _val_ser(v):
                # stdlib json also handles int/float/bool/None keys; default=str as fallback
                return _j.dumps(v, default=str).encode('utf-8')

        kafka_producer_opts = dict(self.kafka_connection)
        kafka_producer_opts.update({
            'linger_ms': 50,
            'batch_size': 1024 * 1024,  # 1MB (was 512k)
            'compression_type': 'gzip',
            'max_in_flight_requests_per_connection': 5,
            'retries': 0,
            'value_serializer': _val_ser,
        })

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")

                consumer_opts = dict(self.kafka_connection)
                consumer_opts.update({
                    # pull large chunks per fetch
                    'fetch_max_bytes': 64 * 1024 * 1024,  # 64MB across partitions
                    'max_partition_fetch_bytes': 32 * 1024 * 1024,  # 32MB per partition
                    'fetch_min_bytes': 4 * 1024 * 1024,  # coalesce small messages into 1MB+
                    'fetch_max_wait_ms': 500,  # wait a bit to batch
                    # socket buffers to avoid throttling
                    'receive_buffer_bytes': 8 * 1024 * 1024,
                    'send_buffer_bytes': 8 * 1024 * 1024,
                    # we assign partitions manually; commits don’t matter
                    'enable_auto_commit': True,
                    # longer timeouts under burst
                    'request_timeout_ms': 60000,
                    'session_timeout_ms': 30000,
                    'max_poll_interval_ms': 20*60*1000,
                    'heartbeat_interval_ms': 5000,
                    'group_id': self.group_id,
                    'auto_offset_reset': 'earliest' if self.read_from_beginning else 'latest',
                    'client_id': f"session-pipeline-{self.hostname}",
                })
                consumer = KafkaConsumer(**consumer_opts)

                consumer.subscribe([self.topic_weblogs])
                listener = RebalanceLogger(self.logger, name=self.hostname)
                consumer.subscribe([self.topic_weblogs], listener=listener)

                # wait (up to ~30s) for a real assignment
                start = time_module.time()
                while not consumer.assignment():
                    consumer.poll(timeout_ms=1000)
                    if time_module.time() - start > 30:
                        break
                self.logger.info(f"Assigned: {consumer.assignment()}")

                producer = KafkaProducer(**kafka_producer_opts)
                self.logger.info("Successfully connected to Kafka brokers")
                return consumer, producer
            except NoBrokersAvailable as e:
                delay = initial_delay * (2 ** attempt)
                self.logger.warning(
                    f"No Kafka brokers available (attempt {attempt + 1}/{max_retries}). Retrying in {delay}s... Error: {e}")
                time_module.sleep(delay)
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                if producer:
                    try:
                        producer.close()
                    except:
                        pass
            except Exception as e:
                delay = initial_delay * (2 ** attempt)
                self.logger.error(f"Unexpected Kafka error (attempt {attempt + 1}/{max_retries}): {e}")
                time_module.sleep(delay)
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                if producer:
                    try:
                        producer.close()
                    except:
                        pass
        raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

    def update_lag_metrics(self, lag):
        self.current_lag = lag
        self.last_lag_check = datetime.utcnow()
        # IMPORTANT: increase flush threshold with high lag to reduce serialization
        if lag > self.lag_emergency_threshold:
            self.flush_increment = max(self.base_flush_increment * 4, 50)
        elif lag > self.lag_high_threshold:
            self.flush_increment = max(self.base_flush_increment * 3, 40)
        elif lag > self.lag_moderate_threshold:
            self.flush_increment = max(self.base_flush_increment * 2, 30)
        else:
            self.flush_increment = self.base_flush_increment

    def get_adaptive_max_records(self):
        if self.current_lag > self.lag_high_threshold:
            return min(10000, max(self.base_max_records, self.current_lag // 5))
        elif self.current_lag > self.lag_moderate_threshold:
            return int(self.base_max_records * 1.5)  # 7500
        else:
            return self.base_max_records

    def get_adaptive_gc_period(self):
        base_period = self.garbage_collection_period * 60
        if self.current_lag > self.lag_emergency_threshold:
            return max(15, base_period // 8)
        elif self.current_lag > self.lag_high_threshold:
            return max(30, base_period // 4)
        elif self.current_lag > self.lag_moderate_threshold:
            return max(60, base_period // 2)
        else:
            return base_period

    def get_timestamp_and_data(self, raw_obj):
        """
        raw_obj: dict already after _loads(); recognizes nested JSON in 'message' field.
        Returns naive-UTC timestamp and dict.
        """
        t0 = self._t()
        try:
            data = raw_obj
            msg = data.get('message')
            if isinstance(msg, str) and msg and msg[0] in '{[':
                msg = msg.replace('"banjax_bot_score": ,', '"banjax_bot_score": ""')
                msg = msg.replace('000', '0')
                data = _loads(msg)

            # timestamp
            if 'time_local' in data:
                ts = _parse_apache_ts(data['time_local'])
            else:
                ts = _iso(data['datestamp'])
            ts = _to_naive_utc(ts)
            self._acc('msg_ts_parse', t0)
            return ts, data
        except Exception as e:
            self.logger.error(f"Error parsing timestamp and data: {e!r}, {raw_obj}")
            return None, None

    @staticmethod
    def create_session(ua, host, country, continent, datacenter_code, ip, session_id, verified_bot, ts,
                       cipher, ciphers, request, asn, asn_name, num_languages, accept_language, timezone_str,
                       dnet, cloudflare_score):
        return {
            'ua': ua,
            'host': host,
            'dnet': dnet,
            'verified_bot': verified_bot,
            'country': country,
            'continent': continent,
            'datacenter_code': datacenter_code,
            'ip': ip,
            'session_id': session_id,
            'start': ts,
            'end': ts,
            'duration': 0,
            'time_now': datetime.utcnow(),
            'cipher': cipher,
            'ciphers': ciphers,
            'requests': [request] if request and not request.get('static', False) else [],
            'static_count': 1 if request and request.get('static', False) else 0,
            'asn': asn,
            'asn_name': asn_name,
            'num_languages': num_languages,
            'accept_language': accept_language,
            'timezone': timezone_str,
            'is_monotonic': True,
            'last_ts': ts,
            'cloudflare_score': cloudflare_score,
            'http_protocol': request.get('http_protocol', '') if request else ''
        }

    def update_session(self, session, request):
        if (len(session['requests']) > 0 and
                session['requests'][-1]['url'] == request['url'] and
                VIDEO_REGEX.match(request['url']) and request['code'] == 206):
            return
        prev = session['last_ts']
        cur = request['ts']
        if prev is not None and cur < prev:
            session['is_monotonic'] = False
        session['last_ts'] = cur

        session['end'] = cur
        session['duration'] = (session['end'] - session['start']).total_seconds()
        if request.get('static', False):
            session['static_count'] = session.get('static_count', 0) + 1
        else:
            session['requests'].append(request)

    def is_session_expired(self, session, ts):
        if (ts - session['end']).total_seconds() > self.session_inactivity * 60:
            return True
        if session['duration'] > self.reset_duration * 60:
            return True
        return False

    def flush(self):
        self.producer.flush()

    def check_and_send_session(self, session):
        if session['duration'] < self.max_session_duration:
            size = len(session['requests']) + session.get('static_count', 0)
            immature_session = self.score_2_num_requests <= size < self.min_number_of_requests
            session['immature_session'] = immature_session
            if (immature_session and not session.get('immature_session_flushed', False) ) or \
                    (session['duration'] > self.min_session_duration and size > self.min_number_of_requests):
                if 'flush_size' not in session or size - session.get('flush_size') > self.flush_increment:
                    if 'flush_end' not in session or (session['end'] - session['flush_end']).total_seconds() > 0:
                        t0 = self._t()
                        self.send_session(session)
                        self._acc('session_send', t0)
                        session['flush_size'] = size
                        session['flush_end'] = session['end']
                        session['immature_session_flushed'] = True

    def send_session(self, session):
        """
        Send session data in full format.
        """
        t_fmt = self._t()
        requests = session['requests']

        if session.get('is_monotonic', True):
            requests_sorted = requests
        else:
            requests_sorted = sorted(requests, key=lambda x: x['ts'])

        duration = (requests_sorted[-1]['ts'] - requests_sorted[0]['ts']).total_seconds() \
            if requests_sorted else session['duration']

        self._acc('send_format_requests', t_fmt)
        t_ser = self._t()
        requests_formatted = []
        passed_challenge = False
        bot_score = -1.0
        bot_score_top_factor = ''
        deflect_password = False

        # Compute baskerville scores only once (they're based on first request)
        if 'baskerville_score_1' not in session:
            session['baskerville_score_1'] = get_baskerville_score_1(session)
        if 'baskerville_score_2' not in session:
            session['baskerville_score_2'] = get_baskerville_score_2(session)

        for r in requests_sorted:
            rf = {
                'ts': r['ts'].strftime(self.date_time_format),
                'url': r['url'],
                'ua': r['ua'],
                'query': r['query'],
                'code': r['code'],
                'type': r['type'],
                'payload': r['payload'],
                'method': r['method'],
                'edge': r['edge'],
                'static': r['static'],
                'passed_challenge': r['passed_challenge'],
                'bot_score': r['bot_score'],
                'bot_score_top_factor': r['bot_score_top_factor'],
                'deflect_password': r['deflect_password']
            }
            if r['passed_challenge'] and not passed_challenge:
                passed_challenge = True
                bot_score = r['bot_score']
                bot_score_top_factor = r['bot_score_top_factor']
            if r['deflect_password']:
                deflect_password = True
            requests_formatted.append(rf)

        scraper_name = baskerville_rules.detect_scraper(session['ua'])
        session_final = {
            'host': session['host'],
            'dnet': session['dnet'],
            'ua': session['ua'],
            'country': session['country'],
            'continent': session['continent'],
            'datacenter_code': session['datacenter_code'],
            'session_id': session['session_id'],
            'ip': session['ip'],
            'start': requests_formatted[0]['ts'] if requests_formatted else session['start'].strftime(self.date_time_format),
            'end': requests_formatted[-1]['ts'] if requests_formatted else session['end'].strftime(self.date_time_format),
            'duration': duration,
            'primary_session': session.get('primary_session', False),
            'requests': requests_formatted,
            'passed_challenge': passed_challenge,
            'bot_score': bot_score,
            'bot_score_top_factor': bot_score_top_factor,
            'deflect_password': deflect_password,
            'verified_bot': session['verified_bot'],
            'cipher': session.get('cipher', ''),
            'ciphers': session.get('ciphers', ''),
            'cipher_type': baskerville_rules.normalize_cipher(session.get('cipher', '')),
            'valid_browser_ciphers': baskerville_rules.is_valid_browser_ciphers(session['ciphers']),
            'weak_cipher': baskerville_rules.is_weak_cipher(session.get('cipher', '')),
            'asn': session['asn'],
            'asn_name': session['asn_name'],
            'bot_ua': baskerville_rules.is_bot_user_agent(session['ua']),
            'ai_bot_ua': baskerville_rules.is_ai_bot_user_agent(session['ua']),
            'num_languages': session['num_languages'],
            'accept_language': session['accept_language'],
            'short_ua': baskerville_rules.is_short_user_agent(session['ua']),
            'asset_only': baskerville_rules.is_asset_only_session(session),
            'ua_score': baskerville_rules.ua_score(session['ua']),
            'headless_ua': baskerville_rules.is_headless_ua(session['ua']),
            'timezone': session['timezone'],
            'scraper_name': scraper_name if scraper_name else '',
            'is_scraper': scraper_name is not None,
            'cloudflare_score': session['cloudflare_score'],
            'http_protocol': session.get('http_protocol', ''),
            'immature_session': session.get('immature_session', False),
            'baskerville_score_1': session.get('baskerville_score_1', 50),
            'baskerville_score_2': session.get('baskerville_score_2', 50),
        }

        if self.current_lag > self.lag_critical_threshold:
            session_final['fingerprints'] = ''
            session_final['fingerprints_score'] = 0.0
        else:
            fingerprints = SessionFingerprints.get_fingerprints(session)
            self.fingerprints_analyzer.process(
                host=session['host'], key=fingerprints,
                timestamp=requests_sorted[0]['ts'] if requests_sorted else session['start'])
            session_final['fingerprints'] = fingerprints
            session_final['fingerprints_score'] = self.fingerprints_analyzer.get_key_zscore(
                host=session['host'], key=fingerprints)

        asn = session['asn']
        vps_asn = self.asn_database2.is_vps_asn(asn)
        malicious_asn = self.asn_database2.is_malicious_asn(asn)
        vpn_asn = self.asn_database2.is_vpn_asn(asn)
        session_final['datacenter_asn'] = self.asn_database.is_datacenter_asn(
            asn) or vps_asn or malicious_asn or vpn_asn
        session_final['vpn_asn'] = vpn_asn
        session_final['malicious_asn'] = malicious_asn
        session_final['vps_asn'] = vps_asn
        session_final['vpn'] = self.vpn_detector.is_vpn(session_final['ip'])
        session_final['tor'] = self.tor_exit_scnaner.is_tor(session_final['ip'])
        session_final['baskerville_score_3'] = get_baskerville_score_3(session_final)
        session_final['human'] = is_human(session_final)

        session_final['bad_bot'] = baskerville_rules.is_bad_bot(session_final)

        if session_final['human']:
            session_final['class'] = 'human'
        elif session_final['verified_bot']:
            session_final['class'] = 'verified_bot'
        elif session_final['ai_bot_ua']:
            session_final['class'] = 'ai_bot'
        elif session_final['bad_bot']:
            session_final['class'] = 'bad_bot'
        elif session_final['is_scraper']:
            session_final['class'] = 'scraper'
        else:
            session_final['class'] = 'bot'

        self._acc('send_serialize', t_ser)
        t_send = self._t()
        if not session_final.get('immature_session', False):
            self.producer.send(
                self.topic_sessions,
                key=bytearray(session['host'], encoding='utf8'),
                value=session_final
            )
        self._acc('send_producer_send', t_send)

    def collect_primary_session(self, ip):
        primary_sessions = self.ips_primary.get(ip)
        if not primary_sessions:
            return

        now_sec = time_module.time()
        last = self._last_primary_collect.get(ip, 0.0)
        if now_sec - last < self.primary_collect_cooldown_sec:
            return
        self._last_primary_collect[ip] = now_sec

        size = len(primary_sessions)
        flush_threshold = self.max_primary_sessions_per_ip
        min_flush_increment = self.flush_increment
        if self.current_lag > self.lag_high_threshold:
            flush_threshold = max(3, flush_threshold // 3)
            min_flush_increment = max(1, min_flush_increment // 4)
        elif self.current_lag > self.lag_moderate_threshold:
            flush_threshold = max(5, flush_threshold // 2)
            min_flush_increment = max(2, min_flush_increment // 2)

        prev_size = self.flush_size_primary.get(ip, 0)
        incr = size - prev_size
        if size != 1:
            if size <= flush_threshold or incr < min_flush_increment:
                return
        self.flush_size_primary[ip] = size

        hosts = {}
        for session_id, s in primary_sessions.items():
            host = s['host']
            h = hosts.get(host)
            if h is None:
                h = {
                    'reqs': [],
                    'static_count': 0,
                    'min_ts': s['start'],
                    'max_ts': s['end'],
                    'meta': s,
                }
                hosts[host] = h
            h['static_count'] += s.get('static_count', 0)
            if s['start'] < h['min_ts']:
                h['min_ts'] = s['start']
            if s['end'] > h['max_ts']:
                h['max_ts'] = s['end']
            if not s['requests']:
                continue
            r0 = s['requests'][0]
            h['reqs'].append(r0)

        now = datetime.utcnow()
        to_delete_by_host = {}

        t0 = self._t()
        for host, h in hosts.items():
            reqs = h['reqs']
            total_static = h.get('static_count', 0)
            # Skip if no requests at all (neither non-static nor static)
            if not reqs and total_static == 0:
                continue

            start_ts = h['min_ts']
            end_ts = h['max_ts']
            duration = (end_ts - start_ts).total_seconds()
            age = (now - start_ts).total_seconds()
            request_count = len(reqs) + total_static
            meta = h['meta']

            send_threshold = self.min_number_of_requests

            if duration > self.max_session_duration:
                if self.debugging:
                    self.logger.info(f'Deleting overlong primary for IP={ip}, host={host} (duration {duration:.1f}s)')
                to_delete = {sid for sid, s in primary_sessions.items() if s['host'] == host}
                to_delete_by_host[host] = to_delete
                continue
            if self.current_lag < self.lag_moderate_threshold:
                send_threshold = max(send_threshold, self.min_number_of_requests * 2)
            elif self.current_lag >= self.lag_high_threshold:
                send_threshold = max(send_threshold, int(self.min_number_of_requests * 1.5))

            immature_session = request_count == 1
            if immature_session or request_count >= send_threshold:
                reqs.sort(key=lambda x: x['ts'])
                session = {
                    'ua': meta['ua'],
                    'host': host,
                    'dnet': reqs[0]['dnet'] if reqs else meta.get('dnet', '-'),
                    'country': meta['country'],
                    'continent': meta['continent'],
                    'datacenter_code': meta['datacenter_code'],
                    'verified_bot': meta['verified_bot'],
                    'ip': ip,
                    'session_id': '-',
                    'start': start_ts,
                    'end': end_ts,
                    'cipher': meta['cipher'],
                    'ciphers': meta['ciphers'],
                    'duration': duration if duration > 0 else 1.0,
                    'requests': reqs,
                    'static_count': total_static,
                    'primary_session': True,
                    'asn': meta['asn'],
                    'asn_name': meta['asn_name'],
                    'num_languages': meta['num_languages'],
                    'accept_language': meta['accept_language'],
                    'timezone': meta['timezone'],
                    'is_monotonic': True,
                    'cloudflare_score': meta.get('cloudflare_score', 0),
                    'http_protocol': meta.get('http_protocol', ''),
                    'immature_session': immature_session,
                }
                t_send = self._t()
                self.send_session(session)
                self._acc('primary_collection_send', t_send)

                to_delete = {sid for sid, s in primary_sessions.items() if s['host'] == host}
                to_delete_by_host[host] = to_delete
            else:
                if age > 1800:
                    to_delete = {sid for sid, s in primary_sessions.items() if s['host'] == host}
                    to_delete_by_host[host] = to_delete

        for host, sids in to_delete_by_host.items():
            for sid in sids:
                primary_sessions.pop(sid, None)
        self._acc('primary_collection', t0)

    def gc(self, current_event_ts_horizon):
        if current_event_ts_horizon is None:
            self.logger.warning("GC called with None as current_event_ts_horizon. Skipping.")
            return

        start_gc = self._t()
        total_sessions = sum(len(sessions) for sessions in self.ips.values())
        total_primary_sessions_before_gc = sum(len(sessions) for sessions in self.ips_primary.values())
        total_ips = len(self.ips)

        deleted_main_sessions_count = 0
        deleted_main_ips_count = 0

        for ip in list(self.ips.keys()):
            sessions = self.ips[ip]
            session_ids_to_delete_main = []
            for session_id in list(sessions):
                session = sessions[session_id]
                session_age_hours = (current_event_ts_horizon - session['start']).total_seconds() / 3600
                if ((current_event_ts_horizon - session['end']).total_seconds() > self.session_inactivity * 60 or
                        session_age_hours > self.max_session_age_hours):
                    self.check_and_send_session(session)
                    session_ids_to_delete_main.append(session_id)
            for session_id in session_ids_to_delete_main:
                del sessions[session_id]
                deleted_main_sessions_count += 1
            if len(sessions) == 0:
                del self.ips[ip]
                deleted_main_ips_count += 1

        fast_gc = (self.current_lag >= self.lag_high_threshold)
        deleted_primary_sessions_count = 0

        if fast_gc:
            for ip in list(self.ips_primary.keys()):
                sessions_for_ip = self.ips_primary[ip]
                if not sessions_for_ip:
                    self.ips_primary.pop(ip, None)
                    continue
                oldest = min((s['start'] for s in sessions_for_ip.values()), default=None)
                if oldest and (
                        current_event_ts_horizon - oldest).total_seconds() > self.primary_session_expiration * 60:
                    deleted_primary_sessions_count += len(sessions_for_ip)
                    self.ips_primary.pop(ip, None)
                    self.flush_size_primary.pop(ip, None)
        else:
            for ip in list(self.ips_primary.keys()):
                sessions_for_ip = self.ips_primary[ip]
                if len(sessions_for_ip) > 5000:
                    continue

                hosts_data = {}
                for _, s_data in list(sessions_for_ip.items()):
                    host = s_data['host']
                    h = hosts_data.get(host)
                    if h is None:
                        h = {
                            'requests': [],
                            'static_count': 0,
                            'oldest_ts': datetime.max,
                            'latest_ts': datetime.min,
                            'first_session_data': s_data
                        }
                        hosts_data[host] = h
                    h['requests'].extend(s_data['requests'])
                    h['static_count'] += s_data.get('static_count', 0)
                    if s_data['start'] < h['oldest_ts']:
                        h['oldest_ts'] = s_data['start']
                    if s_data['end'] > h['latest_ts']:
                        h['latest_ts'] = s_data['end']

                for host, h in hosts_data.items():
                    reqs_sorted = sorted(h['requests'], key=lambda x: x['ts'])
                    total_static = h.get('static_count', 0)
                    total_count = len(reqs_sorted) + total_static
                    if total_count == 0:
                        continue
                    current_aggregated_duration = (h['latest_ts'] - h['oldest_ts']).total_seconds()
                    oldest_age = (current_event_ts_horizon - h['oldest_ts']).total_seconds()
                    should_flush = (current_aggregated_duration > self.max_session_duration) or (
                                oldest_age > self.primary_session_expiration * 60)
                    if should_flush and total_count >= self.min_number_of_requests:
                        meta = h['first_session_data']
                        start_ts = h['oldest_ts']
                        end_ts = h['latest_ts']
                        duration = (end_ts - start_ts).total_seconds()
                        summary = {
                            'ua': meta['ua'],
                            'host': host,
                            'dnet': reqs_sorted[0]['dnet'] if reqs_sorted else meta.get('dnet', '-'),
                            'country': meta['country'],
                            'continent': meta['continent'],
                            'datacenter_code': meta['datacenter_code'],
                            'verified_bot': meta['verified_bot'],
                            'ip': ip,
                            'session_id': '-',
                            'start': start_ts,
                            'end': end_ts,
                            'cipher': meta['cipher'],
                            'ciphers': meta['ciphers'],
                            'duration': duration if duration > 0 else 1.0,
                            'requests': reqs_sorted,
                            'static_count': total_static,
                            'primary_session': True,
                            'asn': meta['asn'],
                            'asn_name': meta['asn_name'],
                            'num_languages': meta['num_languages'],
                            'accept_language': meta['accept_language'],
                            'timezone': meta['timezone'],
                            'is_monotonic': True,
                            'cloudflare_score': meta.get('cloudflare_score', 0),
                            'http_protocol': meta.get('http_protocol', ''),
                        }
                        self.send_session(summary)

                if hosts_data:
                    oldest_any = min((v['oldest_ts'] for v in hosts_data.values() if v['oldest_ts'] != datetime.max),
                                     default=None)
                    if oldest_any and (
                            current_event_ts_horizon - oldest_any).total_seconds() > self.primary_session_expiration * 60:
                        deleted_primary_sessions_count += len(sessions_for_ip)
                        self.ips_primary[ip] = {}
                        self.flush_size_primary.pop(ip, None)
                        if ip in self.ips_primary and not self.ips_primary[ip]:
                            del self.ips_primary[ip]

        self.producer.flush()

        mem = psutil.Process().memory_info().rss / 1024 / 1024
        self.logger.info(
            f'Garbage collector. \n'
            f'Main IPs: {total_ips - deleted_main_ips_count} - {deleted_main_ips_count} deleted, \n'
            f'Main Sessions: {total_sessions - deleted_main_sessions_count} -  {deleted_main_sessions_count} deleted, \n'
            f'Primary Sessions: {total_primary_sessions_before_gc - deleted_primary_sessions_count} - {deleted_primary_sessions_count} deleted \n'
            f'GC took {time_module.time() - start_gc:.2f} seconds | RAM usage: {mem:.2f} MB'
        )
        self._acc('gc_time', start_gc)

    def is_debugging_mode(self, data):
        if data.get('client_request_host') == 'farmal.in':
            return True
        if self.debug_ip and data.get('client_ip') == self.debug_ip:
            return True
        ua = data.get('client_ua', data.get('client_user_agent', ''))
        if 'Baskerville' in ua:
            return True
        return False

    def get_session_cookie(self, data):
        if 'deflect_session' in data:
            return data.get('deflect_session', '')
        if 'cookies' in data:
            return data['cookies'].get('sessionCookie', '')
        return ''

    def run(self):

        try:
            consumer, self.producer = self.create_kafka_connections()
            self.logger.info(
                f'Starting Baskervillehall sessionizer from topic {self.topic_weblogs} to {self.topic_sessions}')

            ts_gc_trigger_wall_clock = datetime.utcnow()
            latest_event_ts_seen = None

            ts_lag_report = datetime.utcnow()
            ts_assign_report = datetime.utcnow()

            while True:

                if (datetime.utcnow() - ts_assign_report).total_seconds() > 30:
                    log_partition_assignment(consumer, self.logger)
                    ts_assign_report = datetime.utcnow()

                if int(time_module.time()) % 30 == 0:
                    mem = psutil.Process().memory_info().rss / 1024 / 1024
                    if int(time_module.time()) % 60 == 0:
                        self.logger.info(f'RAM usage: {mem:.2f} MB')
                    if mem > 4096:
                        self.logger.warning(f'High RAM usage ({mem:.2f} MB) - running emergency GC')
                        self.gc(latest_event_ts_seen if latest_event_ts_seen else datetime.utcnow())

                max_records = self.get_adaptive_max_records()
                t_poll = self._t()
                raw_messages = consumer.poll(timeout_ms=250, max_records=max_records
                                             # , update_offsets=False
                                             )
                poll_time = time_module.time() - t_poll
                self._acc('kafka_operations', t_poll)

                if not raw_messages:
                    if poll_time > 10.0:
                        self.logger.warning(f'Slow Kafka poll: {poll_time:.1f}s, no messages returned')
                        assignment = consumer.assignment()
                        self.logger.warning(f'Current assignment: {list(assignment)}')
                        for tp in assignment:
                            try:
                                pos = consumer.position(tp)
                                end = consumer.end_offsets([tp]).get(tp, 'unknown')
                                self.logger.warning(
                                    f'  {tp}: position={pos}, end={end}, lag={end - pos if end != "unknown" else "unknown"}')
                            except Exception as e:
                                self.logger.warning(f'  {tp}: error getting metrics - {e}')
                else:
                    msg_count = sum(len(msgs) for msgs in raw_messages.values())
                    if poll_time > 5.0:
                        self.logger.warning(f'Slow Kafka poll: {poll_time:.1f}s returned {msg_count} messages')

                for topic_partition, messages in raw_messages.items():
                    if (datetime.utcnow() - ts_lag_report).total_seconds() > 5:

                        try:
                            end = consumer.end_offsets([topic_partition]).get(
                                topic_partition)  # end offset (next to be written)
                        except Exception:
                            end = None

                        last_off = messages[-1].offset if messages else None
                        if end is not None and last_off is not None:
                            # messages remaining after the last one we just processed
                            lag = max(0, end - (last_off + 1))
                        else:
                            lag = 0

                        self.update_lag_metrics(lag)
                        self.logger.info(
                            f'Lag = {lag} (adaptive max_records: {max_records}, dropped: {self.messages_dropped})')
                        ts_lag_report = datetime.utcnow()
                        if self.profiling_enabled and (
                                datetime.utcnow() - self.last_profile_report).total_seconds() > 30:
                            self.report_performance_profile()
                            self.last_profile_report = datetime.utcnow()

                    for message in messages:
                        if not message.value:
                            continue
                        msg_start = self._t()

                        t_dec = self._t()
                        val_s = message.value  # bytes
                        try:
                            data_obj = _loads(val_s)
                        except Exception:
                            data_obj = _loads(val_s.decode('utf-8', errors='replace'))
                        self._acc('msg_decode', t_dec)

                        t_ts = self._t()
                        ts_event, data = self.get_timestamp_and_data(data_obj)
                        if data is None:
                            continue
                        self._acc('json_parsing', t_ts)

                        if latest_event_ts_seen is None or ts_event > latest_event_ts_seen:
                            latest_event_ts_seen = ts_event

                        self.skip_expensive_ops = self.current_lag > self.lag_critical_threshold
                        if self.current_lag > self.lag_emergency_threshold:
                            self.emergency_session_cleanup(
                                latest_event_ts_seen if latest_event_ts_seen else datetime.utcnow())
                            if self.enable_emergency_partition_seek:
                                self.emergency_partition_seek(consumer)

                        t_ext = self._t()
                        ip = data['client_ip']
                        if 'cloudflareProperties' in data:
                            prop = data['cloudflareProperties']
                            verified_bot = prop.get('botManagement', {}).get('verifiedBot', False)
                            continent = prop.get('continent', '')
                            datacenter_code = prop.get('cloudflare_datacenter_code', '')
                        else:
                            if data.get('banjax_decision') == 'GlobalAccessGranted':
                                verified_bot = True
                            else:
                                if self.skip_expensive_ops:
                                    verified_bot = False
                                else:
                                    t_bv = self._t()
                                    verified_bot = self.bot_verificator.is_verified_bot(ip)
                                    self._acc('msg_bot_verif', t_bv)
                            continent = ''
                            datacenter_code = ''
                        self.debugging = self.is_debugging_mode(data)
                        host = message.key.decode('utf-8', errors='replace')

                        asn = data.get('geoip_asn', {}).get('as', {}).get('number', '0')
                        asn_name = data.get('geoip_asn', {}).get('as', {}).get('organization', {}).get('name', '')
                        session_id = self.get_session_cookie(data)
                        self._acc('data_extraction', t_ext)

                        if len(session_id) > 5 and not self.skip_expensive_ops:
                            self.vpn_detector.log(
                                ip=ip, host=host, session_cookie=session_id,
                                asn_number=asn, asn_name=asn_name, timestamp=ts_event
                            )

                        ua = data.get('client_ua', data.get('client_user_agent', ''))
                        geoip = data.get('geoip', {})
                        country = geoip.get('country_code2', geoip.get('country_code', ''))
                        dnet = data.get('dnet', '-')
                        timezone_str = geoip.get('timezone', 'America/Los_Angeles')

                        if self.country_blocker.process(host, ip, country, dnet):
                            if host == 'farmal.in':
                                self.logger.info(f'{host} country {country} blocked ip{ip}')
                            self.profile_stats['message_processing'] += (time_module.time() - msg_start)
                            self.profile_stats['message_count'] += 1
                            continue

                        if host == 'farmal.in':
                            self.logger.info(f' {host} country {country} ip {ip} is not blocked')
                            self.logger.info(data)
                        if len(session_id) < 5:
                            session_id = '-' + ''.join(
                                random.choice(string.ascii_uppercase + string.digits) for _ in range(7))

                        # Whitelist with TTL cache
                        t_wl = self._t()
                        k1 = ('hip', host, ip)
                        cached = self._wl_get(k1)
                        if cached is None:
                            allowed = self.settings.is_host_whitelisted(host)
                            self._wl_put(k1, allowed)
                        else:
                            allowed = cached
                        if allowed:
                            self._acc('whitelist_checks', t_wl)
                            self.profile_stats['message_processing'] += (time_module.time() - msg_start)
                            self.profile_stats['message_count'] += 1
                            continue

                        url = data['client_url']
                        k2 = ('url', url)
                        cached = self._wl_get(k2)
                        if cached is None:
                            allowed2 = self.settings.is_in_whitelist(url)
                            self._wl_put(k2, allowed2)
                        else:
                            allowed2 = cached
                        if allowed2:
                            self._acc('whitelist_checks', t_wl)
                            self.profile_stats['message_processing'] += (time_module.time() - msg_start)
                            self.profile_stats['message_count'] += 1
                            continue
                        self._acc('whitelist_checks', t_wl)

                        passed_challenge = False
                        deflect_password = False
                        bot_score = data.get('banjax_bot_score', -1.0)
                        bot_score_top_factor = data.get('banjax_bot_score_top_factor', '')
                        cloudflare_score = 0
                        if 'banjax_decision' in data:
                            banjax_decision = data['banjax_decision']
                            if banjax_decision == 'ShaChallengePassed':
                                passed_challenge = True
                            if banjax_decision in ['PasswordChallengePassed', 'PasswordProtectedPriorityPass',
                                                   'PasswordChallengeRoamingPassed']:
                                deflect_password = True
                        elif 'cloudflareProperties' in data:
                            cloudflare_score = data['cloudflareProperties']['botManagement']['score']
                            passed_challenge = len(data.get('cookies', {}).get('challengePassedCookie', '')) > 0

                        t_build = self._t()
                        request = {
                            'ts': ts_event,
                            'dnet': dnet,
                            'url': url,
                            'ua': ua,
                            'query': data.get('querystring', ''),
                            'code': data.get('http_response_code', 0),
                            'type': data.get('content_type', ''),
                            'payload': int(data.get('reply_length_bytes', 0)),
                            'method': data.get('client_request_method', ''),
                            'edge': data.get('edge', ''),
                            'static': data.get('loc_in', '') == 'static_file',
                            'passed_challenge': passed_challenge,
                            'bot_score': bot_score,
                            'bot_score_top_factor': bot_score_top_factor,
                            'deflect_password': deflect_password,
                            'http_protocol': data.get('http_request_version', '')
                        }
                        # Get cipher from different possible locations
                        cipher = data.get('ssl_cipher', '')
                        cloudflare_props = data.get('cloudflareProperties', {})
                        if not cipher:
                            cipher = cloudflare_props.get('tlsCipher', '')
                        if not cipher:
                            # Fallback to top-level tlsCipher (legacy)
                            cipher = data.get('tlsCipher', '')

                        # Check if this is HTTP/3 without TLS info (QUIC)
                        http_protocol = cloudflare_props.get('httpProtocol', data.get('http_request_version', ''))
                        if not cipher and http_protocol == 'HTTP/3':
                            # HTTP/3 uses QUIC with built-in encryption
                            # Set a default strong cipher to indicate secure connection
                            cipher = 'AEAD-AES128-GCM-SHA256'  # Standard for QUIC

                        ciphers = data.get('ssl_ciphers', '')
                        ciphers = ciphers.split(':') if ciphers else []
                        # If no cipher list available, use the negotiated cipher
                        if not ciphers and cipher:
                            ciphers = [cipher]
                        accept_language = data.get('accept_language', '')
                        if len(accept_language) == 0:
                            accept_language = data.get('language', 'en-US')
                        num_languages = baskerville_rules.count_accepted_languages(accept_language)
                        self._acc('msg_build_request', t_build)

                        t_lookup = self._t()
                        key = (ip, session_id)
                        session = None
                        where = None
                        if self._last_sess_key == key:
                            session = self._last_sess_obj
                            where = self._last_sess_where
                        else:
                            if ip in self.ips and session_id in self.ips[ip]:
                                session = self.ips[ip][session_id]
                                where = 'main'
                            elif ip in self.ips_primary and session_id in self.ips_primary[ip]:
                                session = self.ips_primary[ip][session_id]
                                where = 'primary'

                        self._acc('msg_session_lookup', t_lookup)

                        if where == 'main':
                            if self.is_session_expired(session, ts_event):
                                t_cr = self._t()
                                session = self.create_session(ua, host, country, '', datacenter_code, ip, session_id,
                                                              verified_bot, ts_event, cipher, ciphers, request,
                                                              asn, asn_name, num_languages, accept_language,
                                                              timezone_str,
                                                              dnet, cloudflare_score)
                                self._acc('session_creation', t_cr)
                                self.ips[ip][session_id] = session
                            else:
                                t_upd = self._t()
                                self.update_session(session, request)
                                self._acc('session_update', t_upd)
                            t_send = self._t()
                            self.check_and_send_session(session)
                            self._acc('session_send', t_send)
                            self._last_sess_key = key
                            self._last_sess_obj = session
                            self._last_sess_where = 'main'

                        elif where == 'primary':
                            t_upd = self._t()
                            self.update_session(session, request)
                            self._acc('session_update', t_upd)

                            if ip not in self.ips:
                                self.ips[ip] = {}

                            self.ips[ip][session_id] = session
                            prim_map = self.ips_primary.get(ip)

                            if prim_map is not None:
                                prim_map.pop(session_id, None)  # idempotent
                                if not prim_map:
                                    self.ips_primary.pop(ip, None)

                            t_send = self._t()
                            self.check_and_send_session(session)
                            self._acc('session_send', t_send)
                            self._last_sess_key = (ip, session_id)
                            self._last_sess_obj = session
                            self._last_sess_where = 'main'

                        else:
                            t_cr = self._t()
                            session = self.create_session(ua, host, country, '', datacenter_code, ip, session_id,
                                                          verified_bot, ts_event, cipher, ciphers, request,
                                                          asn, asn_name, num_languages, accept_language, timezone_str,
                                                          dnet, cloudflare_score)
                            self._acc('session_creation', t_cr)
                            if ip not in self.ips_primary:
                                self.ips_primary[ip] = {}
                            self.ips_primary[ip][session_id] = session
                            t_col = self._t()
                            self.collect_primary_session(ip)
                            self._acc('primary_collection', t_col)
                            self._last_sess_key = key
                            self._last_sess_obj = session
                            self._last_sess_where = 'primary'

                        time_now = datetime.utcnow()
                        if (time_now - ts_gc_trigger_wall_clock).total_seconds() > self.get_adaptive_gc_period():
                            ts_gc_trigger_wall_clock = time_now
                            self.gc(latest_event_ts_seen if latest_event_ts_seen else time_now)

                        self.profile_stats['message_processing'] += (time_module.time() - msg_start)
                        self.profile_stats['message_count'] += 1

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

    def emergency_session_cleanup(self, current_ts):
        cleanup_start = time_module.time()
        if (datetime.utcnow() - self.last_emergency_cleanup).total_seconds() < self.emergency_cleanup_interval:
            return
        self.last_emergency_cleanup = datetime.utcnow()

        cleared_main_sessions = 0
        cleared_main_ips = 0
        for ip in list(self.ips.keys()):
            sessions = self.ips[ip]
            sessions_to_delete = []
            for session_id, session in list(sessions.items()):
                session_age_hours = (current_ts - session['start']).total_seconds() / 3600
                inactivity_minutes = (current_ts - session['end']).total_seconds() / 60
                if (session_age_hours > 1 or inactivity_minutes > max(1, self.session_inactivity // 2)):
                    if (session['duration'] > self.min_session_duration and len(
                            session['requests']) >= self.min_number_of_requests):
                        self.send_session(session)
                    sessions_to_delete.append(session_id)
                    cleared_main_sessions += 1
            for sid in sessions_to_delete:
                del sessions[sid]
            if len(sessions) == 0:
                del self.ips[ip]
                cleared_main_ips += 1

        cleared_primary_sessions = 0
        cleared_primary_ips = 0
        for ip in list(self.ips_primary.keys()):
            sessions = self.ips_primary[ip]
            oldest_session_age = 0
            for _, s in sessions.items():
                session_age_hours = (current_ts - s['start']).total_seconds() / 3600
                oldest_session_age = max(oldest_session_age, session_age_hours)
            if oldest_session_age > 1:
                total_requests = sum(len(s['requests']) for s in sessions.values())
                if total_requests >= self.min_number_of_requests:
                    first_session = next(iter(sessions.values()))
                    all_requests = []
                    for s in sessions.values():
                        all_requests.extend(s['requests'])
                    all_requests.sort(key=lambda x: x['ts'])
                    if all_requests:
                        emergency_flush_session = {
                            'ua': first_session['ua'],
                            'host': first_session['host'],
                            'dnet': first_session['dnet'],
                            'country': first_session['country'],
                            'continent': first_session['continent'],
                            'datacenter_code': first_session['datacenter_code'],
                            'verified_bot': first_session['verified_bot'],
                            'ip': ip,
                            'session_id': 'emergency_flush',
                            'start': all_requests[0]['ts'].strftime(self.date_time_format),
                            'end': all_requests[-1]['ts'].strftime(self.date_time_format),
                            'cipher': first_session['cipher'],
                            'ciphers': first_session['ciphers'],
                            'duration': (all_requests[-1]['ts'] - all_requests[0]['ts']).total_seconds(),
                            'requests': all_requests,
                            'primary_session': True,
                            'asn': first_session['asn'],
                            'asn_name': first_session['asn_name'],
                            'num_languages': first_session['num_languages'],
                            'accept_language': first_session['accept_language'],
                            'timezone': first_session['timezone'],
                            'is_monotonic': True,
                            'cloudflare_score': first_session.get('cloudflare_score', 0),
                            'http_protocol': first_session.get('http_protocol', ''),
                        }
                        self.send_session(emergency_flush_session)
                cleared_primary_sessions += len(sessions)
                del self.ips_primary[ip]
                if ip in self.flush_size_primary:
                    del self.flush_size_primary[ip]
                cleared_primary_ips += 1

        self.producer.flush()
        cleanup_duration = time_module.time() - cleanup_start
        mem_after = psutil.Process().memory_info().rss / 1024 / 1024
        self.logger.warning(
            f'EMERGENCY CLEANUP completed. Lag: {self.current_lag} | '
            f'Cleared {cleared_main_sessions} main sessions, {cleared_main_ips} main IPs | '
            f'Cleared {cleared_primary_sessions} primary sessions, {cleared_primary_ips} primary IPs | '
            f'Emergency cleanup took {cleanup_duration:.2f}s | RAM: {mem_after:.2f}MB'
        )

    def emergency_partition_seek(self, consumer):
        if (not self.last_partition_seek or
                (datetime.utcnow() - self.last_partition_seek).total_seconds() < self.partition_seek_interval):
            return
        self.last_partition_seek = datetime.utcnow()

        try:
            assignment = consumer.assignment()
            if not assignment:
                self.logger.warning("EMERGENCY PARTITION SEEK: No assigned partitions")
                return

            seek_start = time_module.time()
            partitions_seeked = 0
            total_messages_skipped = 0

            for partition in assignment:
                try:
                    current_position = consumer.position(partition)
                    end_offset = consumer.end_offsets([partition]).get(partition)

                    if end_offset is None or current_position is None:
                        continue

                    messages_to_skip = max(0, end_offset - current_position)

                    if messages_to_skip > self.lag_emergency_threshold:
                        consumer.seek_to_end(partition)
                        partitions_seeked += 1
                        total_messages_skipped += messages_to_skip
                        self.messages_dropped += messages_to_skip

                        self.logger.error(
                            f"EMERGENCY PARTITION SEEK: Jumped to end of {partition.topic}[{partition.partition}] | "
                            f"Skipped {messages_to_skip:,} messages | Position: {current_position} -> {end_offset}"
                        )
                except Exception as e:
                    self.logger.error(f"EMERGENCY PARTITION SEEK: Failed to seek partition {partition}: {e}")
                    continue

            seek_duration = time_module.time() - seek_start

            if partitions_seeked > 0:
                self.logger.critical(
                    f"EMERGENCY PARTITION SEEK COMPLETED | "
                    f"Partitions seeked: {partitions_seeked} | "
                    f"Total messages skipped: {total_messages_skipped:,} | "
                    f"Seek operation took: {seek_duration:.2f}s | "
                    f"Current lag after seek: {self.current_lag}"
                )

                self.current_lag = 0
                self.last_lag_check = datetime.utcnow()
            else:
                self.logger.info("EMERGENCY PARTITION SEEK: No partitions required seeking")

        except Exception as e:
            self.logger.error(f"EMERGENCY PARTITION SEEK: Unexpected error: {e}")
            import traceback
            self.logger.error(f"EMERGENCY PARTITION SEEK: Traceback: {traceback.format_exc()}")
