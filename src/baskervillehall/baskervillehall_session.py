import copy
import random
import string

from baskervillehall.session_fingerprints import SessionFingerprints
from baskervillehall.vpn_detector import VpnDetector
from baskervillehall.asn_database import ASNDatabase
from baskervillehall.asn_database2 import ASNDatabase2
from baskervillehall.baskervillehall_isolation_forest import BaskervillehallIsolationForest
from baskervillehall.bot_verificator import BotVerificator
from baskervillehall.settings_deflect_api import SettingsDeflectAPI
from baskervillehall.tor_exit_scanner import TorExitScanner
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
            deflect_config_url=None,
            whitelist_url_default=[],
            whitelist_ip=None,
            white_list_refresh_period=5,
            max_primary_sessions_per_ip=10,
            primary_session_expiration=10,
            datetime_format='%Y-%m-%d %H:%M:%S',
            read_from_beginning=False,
            min_number_of_requests=10,
            debug_ip=None,
            logger=None,
            asn_database_path='',
            asn_database2_path='',
            postgres_connection=None
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
        self.deflect_config_url = deflect_config_url
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
        self.primary_session_expiration = primary_session_expiration
        self.min_number_of_requests = min_number_of_requests
        self.producer = None
        self.ips = dict()
        self.ips_primary = dict()
        self.flush_size_primary = dict()
        self.debugging = False
        self.bot_verificator = BotVerificator()
        self.asn_database = ASNDatabase(asn_database_path)
        self.asn_database2 = ASNDatabase2(asn_database2_path)
        self.tor_exit_scnaner = TorExitScanner()
        self.vpn_detector = VpnDetector(
            asn_db=self.asn_database2,
            tor_exit_scanner=self.tor_exit_scnaner,
            logger=self.logger,
            db_config=postgres_connection
        )

    def get_timestamp_and_data(self, data):
        try:
            if 'message' in data:
                message = data['message']
                message = message.replace('"banjax_bot_score": ,', '"banjax_bot_score": "",')
                data = json.loads(message.replace('000', '0'))
                if '0000' in data['time_local']:
                    timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S %z')
                else:
                    timestamp = datetime.strptime(data['time_local'].split(' ')[0], '%d/%b/%Y:%H:%M:%S')
            else:
                timestamp = datetime.strptime(data['datestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            return timestamp, data
        except Exception as e:
            self.logger.error(f"Error parsing timestamp and data: {str(e)}, {data}")
            return None, None

    def flush(self):
        self.producer.flush()

    def check_and_send_session(self, session):
        if session['duration'] < self.max_session_duration:
            size = len(session['requests'])
            if session['duration'] > self.min_session_duration and \
                    size > self.min_number_of_requests:
                if 'flush_size' not in session or \
                        size - session.get('flush_size') > self.flush_increment:
                    if 'flush_end' not in session or \
                            (session['end'] - session['flush_end']).total_seconds() > 0:
                        self.send_session(session)
                        session['flush_size'] = size
                        session['flush_end'] = session['end']

    def send_session(self, session):
        requests = session['requests']

        if len(requests) == 1:
            self.logger.info(f'1 request session')
            self.logger.info(session)
            self.logger.info(self.ips_primary[session['ip']])

        requests_formatted = []
        passed_challenge = False
        bot_score = -1.0
        deflect_password = False
        requests_sorted = sorted(requests, key=lambda x: x['ts'])
        duration = (requests_sorted[-1]['ts'] - requests_sorted[0]['ts']).total_seconds()

        for r in requests_sorted:
            rf = copy.deepcopy(r)
            rf['ts'] = r['ts'].strftime(self.date_time_format)
            if r['passed_challenge']:
                passed_challenge = True
                bot_score = r['bot_score']
            if r['deflect_password']:
                deflect_password = True
            requests_formatted.append(rf)

        session_final = {
            'host': session['host'],
            'ua': session['ua'],
            'country': session['country'],
            'continent': session['continent'],
            'datacenter_code': session['datacenter_code'],
            'session_id': session['session_id'],
            'ip': session['ip'],
            'start': requests_formatted[0]['ts'],
            'end': requests_formatted[-1]['ts'],
            'duration': duration,
            'primary_session': session.get('primary_session', False),
            'requests': requests_formatted,
            'passed_challenge': passed_challenge,
            'bot_score': bot_score,
            'deflect_password': deflect_password,
            'verified_bot': session['verified_bot'],
            'cipher': session.get('cipher', ''),
            'ciphers': session.get('ciphers', ''),
            'valid_browser_ciphers': BaskervillehallIsolationForest.is_valid_browser_ciphers(session['ciphers']),
            'weak_cipher': BaskervillehallIsolationForest.is_weak_cipher(session.get('cipher', '')),
            'asn': session['asn'],
            'bot_ua': BaskervillehallIsolationForest.is_bot_user_agent(session['ua']),
            'ai_bot_ua': BaskervillehallIsolationForest.is_ai_bot_user_agent(session['ua']),
            'num_languages': session['num_languages'],
            'accept_language': session['accept_language'],
            'short_ua': BaskervillehallIsolationForest.is_short_user_agent(session['ua']),
            'asset_only': BaskervillehallIsolationForest.is_asset_only_session(session),
            'ua_score': BaskervillehallIsolationForest.ua_score(session['ua']),
            'headless_ua': BaskervillehallIsolationForest.is_headless_ua(session['ua']),
            'fingerprints': SessionFingerprints.get_fingerprints(session),
            'timezone': session['timezone']
        }

        asn = session['asn']
        vps_asn = self.asn_database2.is_vps_asn(asn)
        malicious_asn = self.asn_database2.is_malicious_asn(asn)
        vpn_asn = self.asn_database2.is_vpn_asn(asn)
        session_final['datacenter_asn'] = self.asn_database.is_datacenter_asn(asn) or \
                                          vps_asn or malicious_asn or vpn_asn
        session_final['vpn_asn'] = vpn_asn
        session_final['malicious_asn'] = malicious_asn
        session_final['vps_asn'] = vps_asn
        session_final['vpn'] = self.vpn_detector.is_vpn(session_final['ip'])

        session_final['tor'] = self.tor_exit_scnaner.is_tor(session_final['ip'])

        session_final['human'] = BaskervillehallIsolationForest.is_human(session_final)
        session_final['bad_bot'] = BaskervillehallIsolationForest.is_bad_bot(session_final)

        if session_final['human']:
            session_final['class'] = 'human'
        elif session_final['verified_bot']:
            session_final['class'] = 'verified_bot'
        elif session_final['ai_bot_ua']:
            session_final['class'] = 'ai_bot'
        elif session_final['bad_bot']:
            session_final['class'] = 'bad_bot'
        else:
            session_final['class'] = 'bot'

        self.producer.send(
            self.topic_sessions,
            key=bytearray(session['host'], encoding='utf8'),
            value=json.dumps(session_final).encode('utf-8')
        )

        if self.debugging:
            self.logger.info(
                f'@@@@ DEBUG host={session["host"]}, ip={session["ip"]}, session_id={session["session_id"]}'
                f'end={session["end"]}, num_requests={len(session["requests"])}')

    @staticmethod
    def create_session(ua, host, country, continent, datacenter_code, ip, session_id, verified_bot, ts,
                       cipher, ciphers, request, asn, num_languages, accept_language, timezone):
        return {
            'ua': ua,
            'host': host,
            'verified_bot': verified_bot,
            'country': country,
            'continent': continent,
            'datacenter_code': datacenter_code,
            'ip': ip,
            'session_id': session_id,
            'start': ts,
            'end': ts,
            'duration': 0,
            'time_now': datetime.now(),
            'cipher': cipher,
            'ciphers': ciphers,
            'requests': [request] if request else [],
            'asn': asn,
            'num_languages': num_languages,
            'accept_language': accept_language,
            'timezone': timezone
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

    def gc(self, current_event_ts_horizon):  # This is the latest event timestamp we've processed
        """
        Garbage collects expired sessions (main and primary), flushing them to Kafka,
        and flushes/resets long-lived primary sessions.
        """
        if current_event_ts_horizon is None:
            self.logger.warning("GC called with None as current_event_ts_horizon. Skipping inactivity checks.")
            return

        # Log current state before GC
        total_sessions = 0
        for ip, sessions in self.ips.items():
            if len(sessions) == 0:
                self.logger.info(f'error ip {ip} has zero sessions ')
            total_sessions += len(sessions)
        total_primary_sessions_before_gc = 0
        for ip, sessions in self.ips_primary.items():
            total_primary_sessions_before_gc += len(sessions)
        total_ips = len(self.ips)

        deleted_main_sessions_count = 0
        deleted_main_ips_count = 0

        # --- Process Main Sessions (self.ips) ---
        for ip in list(self.ips.keys()):  # Iterate over a copy of keys for safe deletion
            sessions = self.ips[ip]
            session_ids_to_delete_main = []
            for session_id in list(sessions):  # Iterate over a copy of session_ids
                session = sessions[session_id]

                # Check if main session has expired due to inactivity
                if (current_event_ts_horizon - session['end']).total_seconds() > self.session_inactivity * 60:
                    self.logger.info(
                        f"Flushing and deleting expired main session for IP: {ip}, Session ID: {session_id} "
                        f"due to inactivity.")
                    self.check_and_send_session(session)  # Flush expired main sessions
                    session_ids_to_delete_main.append(session_id)

            for session_id in session_ids_to_delete_main:
                del sessions[session_id]
                deleted_main_sessions_count += 1

            # If all sessions for this IP are deleted, remove the IP entry
            if len(sessions) == 0:
                del self.ips[ip]
                deleted_main_ips_count += 1

        # --- Process Primary Sessions (self.ips_primary) ---
        ips_to_process_for_primary_gc = list(self.ips_primary.keys())
        deleted_primary_sessions_count = 0

        for ip in ips_to_process_for_primary_gc:
            sessions_for_ip = self.ips_primary[ip]  # These are individual primary session entries for the IP

            # Aggregate primary session data for this IP by host
            hosts_data = {}
            # Flag to check if *any* primary session for this IP is still active enough to prevent overall inactivity deletion
            is_ip_still_active_by_event_time = False

            for session_id, s_data in list(sessions_for_ip.items()):
                current_host = s_data['host']
                if current_host not in hosts_data:
                    hosts_data[current_host] = {
                        'requests': [],
                        'oldest_ts': datetime.max,
                        'latest_ts': datetime.min,
                        'first_session_data': s_data
                    }

                hosts_data[current_host]['requests'].extend(s_data['requests'])
                if s_data['start'] < hosts_data[current_host]['oldest_ts']:
                    hosts_data[current_host]['oldest_ts'] = s_data['start']
                if s_data['end'] > hosts_data[current_host]['latest_ts']:
                    hosts_data[current_host]['latest_ts'] = s_data['end']

                # If any session for this IP is within the primary_session_expiration window, the IP is still active
                if (current_event_ts_horizon - s_data['end']).total_seconds() <= self.primary_session_expiration * 60:
                    is_ip_still_active_by_event_time = True

            # Flag to decide if the *entire IP's primary sessions* should be cleared from memory
            # This happens either if inactive, or if a segment was flushed by duration.
            should_clear_entire_ip_primary_sessions = False

            # Iterate through each host's aggregated primary session for this IP
            for host, host_data in hosts_data.items():
                all_requests_for_host_sorted = sorted(host_data['requests'], key=lambda x: x['ts'])

                if not all_requests_for_host_sorted:
                    continue

                current_aggregated_duration = (host_data['latest_ts'] - host_data['oldest_ts']).total_seconds()

                should_flush_this_host_segment = False
                flush_reason = ""

                # --- Condition A: Exceeded its maximum allowed lifetime (forced flush + reset) ---
                # This catches active, long-running, low-volume segments.
                if current_aggregated_duration > self.max_session_duration:
                    should_flush_this_host_segment = True
                    flush_reason = f"exceeded max session duration ({self.max_session_duration}s) (forced flush)"

                # --- Condition B: Inactivity (final flush before deletion) ---
                # This applies if the IP is overall inactive, covering all its primary sessions.
                elif not is_ip_still_active_by_event_time:
                    should_flush_this_host_segment = True
                    flush_reason = "inactivity expiration (final flush before deletion)"

                if should_flush_this_host_segment:
                    should_clear_entire_ip_primary_sessions = True  # Mark for clearing the whole IP's primary data

                    # Only flush if there are enough requests for the aggregated session
                    if len(all_requests_for_host_sorted) >= self.min_number_of_requests:
                        first_session_entry = host_data['first_session_data']
                        summary_session_for_flush = {
                            'ua': first_session_entry['ua'],
                            'host': host,
                            'country': first_session_entry['country'],
                            'continent': first_session_entry['continent'],
                            'datacenter_code': first_session_entry['datacenter_code'],
                            'verified_bot': first_session_entry['verified_bot'],
                            'ip': ip,
                            'session_id': '-',
                            'start': all_requests_for_host_sorted[0]['ts'],
                            'end': all_requests_for_host_sorted[-1]['ts'],
                            'cipher': first_session_entry['cipher'],
                            'ciphers': first_session_entry['ciphers'],
                            'duration': current_aggregated_duration,
                            'requests': all_requests_for_host_sorted,
                            'primary_session': True,
                            'asn': first_session_entry['asn'],
                            'num_languages': first_session_entry['num_languages'],
                            'accept_language': first_session_entry['accept_language'],
                            'timezone': first_session_entry['timezone']
                        }
                        self.logger.info(f"Flushing primary session IP: {ip}, Host: {host} due to {flush_reason} "
                                         f"with {len(summary_session_for_flush['requests'])} requests.")
                        self.send_session(summary_session_for_flush)
                    else:
                        self.logger.info(
                            f"Skipping flush for primary session IP: {ip}, Host: {host} due to insufficient requests "
                            f"({len(all_requests_for_host_sorted)} < {self.min_number_of_requests}) and {flush_reason}.")

            # After processing all hosts for this IP, perform clearing if necessary.
            if should_clear_entire_ip_primary_sessions:
                deleted_primary_sessions_count += len(sessions_for_ip)
                self.ips_primary[ip] = {}  # Clear all primary sessions for this IP
                if ip in self.flush_size_primary:
                    del self.flush_size_primary[ip]

                    # If the IP's primary sessions are now empty, remove the IP entry itself.
                if ip in self.ips_primary and not self.ips_primary[ip]:
                    del self.ips_primary[ip]

        # Ensure all pending Kafka messages are sent
        self.producer.flush()

        # Final logging summary of GC operation
        self.logger.info(
            f'Garbage collector. \n'
            f'Main IPs: {total_ips - deleted_main_ips_count} - {deleted_main_ips_count} deleted, \n'
            f'Main Sessions: {total_sessions - deleted_main_sessions_count} -  {deleted_main_sessions_count} deleted, \n'
            f'Primary Sessions: {total_primary_sessions_before_gc - deleted_primary_sessions_count} - {deleted_primary_sessions_count} deleted \n'
        )

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
                    'continent': sessions[0]['continent'],
                    'datacenter_code': sessions[0]['datacenter_code'],
                    'verified_bot': sessions[0]['verified_bot'],
                    'ip': ip,
                    'session_id': '-',
                    'start': requests[0]['ts'],
                    'end': requests[-1]['ts'],
                    'cipher': sessions[0]['cipher'],
                    'ciphers': sessions[0]['ciphers'],
                    'duration': (requests[-1]['ts'] - requests[0]['ts']).total_seconds(),
                    'requests': requests,
                    'primary_session': True,
                    'asn': sessions[0]['asn'],
                    'num_languages': sessions[0]['num_languages'],
                    'accept_language': sessions[0]['accept_language'],
                    'timezone': sessions[0]['timezone']
                }

                if session['duration'] < self.max_session_duration and \
                        len(session['requests']) > self.max_primary_sessions_per_ip:
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
        if data['client_request_host'] == 'farmal.in':
            return True
        if self.debug_ip and data['client_ip'] == self.debug_ip:
            return True
        if 'client_ua' in data:
            ua = data['client_ua']
        else:
            ua = data.get('client_user_agent', '')
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
        settings = SettingsDeflectAPI(url=self.deflect_config_url,
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

            ts_gc_trigger_wall_clock = datetime.now()
            latest_event_ts_seen = None  # To track the latest event timestamp for GC calculations

            consumer.assign([TopicPartition(self.topic_weblogs, self.partition)])
            if self.read_from_beginning:
                consumer.seek_to_beginning()
            else:
                consumer.seek_to_end()
            ts_lag_report = datetime.now()
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=200, update_offsets=False)
                if not raw_messages:
                    time_now = datetime.now()
                    if (time_now - ts_gc_trigger_wall_clock).total_seconds() > self.garbage_collection_period * 60:
                        ts_gc_trigger_wall_clock = time_now
                        # If no messages have been processed yet, use current wall clock as a safe fallback
                        gc_ts_to_use = latest_event_ts_seen if latest_event_ts_seen else time_now
                        self.gc(gc_ts_to_use)
                    continue  # No messages, so continue loop

                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if (datetime.now() - ts_lag_report).total_seconds() > 5:
                            highwater = consumer.highwater(topic_partition)
                            lag = (highwater - 1) - message.offset
                            self.logger.info(f'Lag = {lag}')
                            ts_lag_report = datetime.now()
                        if not message.value:
                            continue
                        ts_event, data = self.get_timestamp_and_data(json.loads(message.value.decode('utf-8')))
                        if data is None:
                            continue

                        # Update latest_event_ts_seen
                        if latest_event_ts_seen is None or ts_event > latest_event_ts_seen:
                            latest_event_ts_seen = ts_event

                        ip = data['client_ip']

                        if 'cloudflareProperties' in data:
                            prop = data['cloudflareProperties']
                            verified_bot = prop.get(
                                'botManagement', {}).get('verifiedBot', False)
                            continent = prop.get('continent', '')
                            datacenter_code = prop.get('cloudflare_datacenter_code', '')
                        else:
                            if data['banjax_decision'] == 'GlobalAccessGranted':
                                verified_bot = True
                            else:
                                verified_bot = self.bot_verificator.is_verified_bot(ip)
                            continent = ''
                            datacenter_code = ''

                        self.debugging = self.is_debugging_mode(data)

                        host = message.key.decode('utf-8')
                        asn = data.get('geoip_asn', {}).get('as', {}).get('number', '0')
                        asn_name = data.get('geoip_asn', {}).get('as', {}).get('organization', {}).get('name', '')
                        session_id = self.get_session_cookie(data)

                        if len(session_id) > 5:
                            self.vpn_detector.log(ip=ip,
                                                  host=host,
                                                  session_cookie=session_id,
                                                  asn_number=asn,
                                                  asn_name=asn_name,
                                                  timestamp=ts_event)

                        if whitelist_ip.is_in_whitelist(host, ip):
                            if self.debugging:
                                self.logger.info(f'ip {ip} is whitelisted')
                            continue

                        if settings.is_host_whitelisted(host):
                            if self.debugging:
                                self.logger.info(f'host {host} is whitelisted')
                            continue

                        url = data['client_url']
                        if settings.is_in_whitelist(url):
                            if self.debugging:
                                self.logger.info(f'host {host} url {url} is whitelisted')
                            continue
                        if 'client_ua' in data:
                            ua = data['client_ua']
                        else:
                            ua = data.get('client_user_agent', '')

                        geoip = data.get('geoip', {})
                        country = geoip.get('country_code2', geoip.get('country_code', ''))
                        timezone = geoip.get('timezone', 'America/Los_Angeles')

                        if len(session_id) < 5:
                            if data.get('loc_in', '') == 'static_file':
                                continue
                            session_id = '-' + ''.join(random.choice(string.ascii_uppercase + string.digits)
                                                       for _ in range(7))

                        if self.debugging:
                            client_url = data['client_url']
                            self.logger.info(f'@@@@ {self.get_session_cookie(data)}, {client_url}')

                        passed_challenge = False
                        deflect_password = False
                        bot_score = data.get('banjax_bot_score', -1.0)

                        if 'banjax_decision' in data:
                            banjax_decision = data['banjax_decision']
                            if banjax_decision == 'ShaChallengePassed':
                                passed_challenge = True
                            if (banjax_decision in
                                    ['PasswordChallengePassed', 'PasswordProtectedPriorityPass',
                                     'PasswordChallengeRoamingPassed']):
                                deflect_password = True
                        elif 'cloudflareProperties' in data:
                            passed_challenge = len(data.get('cookies', {}).get('challengePassedCookie', '')) > 0

                        request = {
                            'ts': ts_event,
                            'url': url,
                            'ua': ua,
                            'query': data['querystring'],
                            'code': data['http_response_code'],
                            'type': data['content_type'],
                            'payload': int(data['reply_length_bytes']),
                            'method': data['client_request_method'],
                            'edge': data.get('edge', ''),
                            'static': data.get('loc_in', '') == 'static_file',
                            'passed_challenge': passed_challenge,
                            'bot_score': bot_score,
                            'deflect_password': deflect_password
                        }

                        cipher = data.get('ssl_cipher', '')
                        ciphers = data.get('ssl_ciphers', '').split(':')
                        accept_language = data.get('accept_language', '')
                        num_languages = BaskervillehallIsolationForest.count_accepted_languages(
                            accept_language)

                        if ip in self.ips and session_id in self.ips[ip]:
                            # existing session
                            if self.debugging:
                                self.logger.info('session exists')
                            session = self.ips[ip][session_id]
                            if self.is_session_expired(session, ts_event):
                                if self.debugging:
                                    self.logger.info('session is expired')
                                session = self.create_session(ua, host, country, continent, datacenter_code,
                                                              ip, session_id,
                                                              verified_bot, ts_event, cipher, ciphers, request,
                                                              asn, num_languages, accept_language, timezone)
                                self.ips[ip][session_id] = session
                            else:
                                if self.debugging:
                                    self.logger.info('updating session')
                                self.update_session(session, request)
                            self.check_and_send_session(session)
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
                            self.check_and_send_session(session)
                            self.flush()
                        else:
                            # primary session
                            session = self.create_session(ua, host, country, continent, datacenter_code,
                                                          ip, session_id,
                                                          verified_bot, ts_event, cipher, ciphers, request,
                                                          asn, num_languages, accept_language, timezone)
                            if ip not in self.ips_primary:
                                self.ips_primary[ip] = {}
                            self.ips_primary[ip][session_id] = session
                            self.collect_primary_session(ip)

                        time_now = datetime.now()
                        if (time_now - ts_gc_trigger_wall_clock).total_seconds() > self.garbage_collection_period * 60:
                            ts_gc_trigger_wall_clock = time_now
                            self.gc(latest_event_ts_seen)

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')
