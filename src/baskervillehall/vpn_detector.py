import ipaddress
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import psycopg2
from sqlalchemy.testing.plugin.plugin_base import logging

from baskervillehall.asn_database2 import ASNDatabase2
from baskervillehall.tor_exit_scanner import TorExitScanner

class VpnDetector:
    def __init__(self, asn_db: ASNDatabase2,
                 tor_exit_scanner: TorExitScanner,
                 fast_switch_threshold_minutes=10,
                 session_timeout_minutes=60,
                 alert_timeout_minutes=60*24,
                 insert_interval_minutes=60,
                 db_config=None,
                 logger=None):
        self.sessions = defaultdict(list)
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
        self.alert_timeout = timedelta(minutes=alert_timeout_minutes)
        self.fast_switch_threshold = timedelta(minutes=fast_switch_threshold_minutes)
        self.insert_interval = timedelta(minutes=insert_interval_minutes)
        self.alerts = []
        self.asn_db = asn_db
        self.tor_exit_scanner = tor_exit_scanner
        self.db_config = db_config
        self.last_inserted = {}  # (ip, host) -> last insert time
        self.last_cleanup_time = None
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        if db_config:
            self.init_db()

    def init_db(self):
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS vpn (
                        ip TEXT,
                        host TEXT,
                        timestamp TIMESTAMP,
                        asn INTEGER,
                        asn_name TEXT,
                        type TEXT
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_vpn_ip_host ON vpn(ip, host)")
                conn.commit()

    def get_prefix(self, ip):
        try:
            return str(ipaddress.ip_network(ip + '/24', strict=False))
        except Exception:
            return "unknown"

    def log(self, ip, host, session_cookie, asn_number, asn_name, timestamp):
        timestamp = pd.to_datetime(timestamp)
        prefix = self.get_prefix(ip)
        entry = {
            'ip': ip,
            'host': host,
            'asn_number': asn_number,
            'asn_name': asn_name,
            'prefix': prefix,
            'timestamp': timestamp
        }
        self.cleanup(timestamp)
        self.sessions[session_cookie].append(entry)
        self.detect_anomalies(session_cookie)

        if self.asn_db and self.asn_db.is_vpn_asn(asn_number):
            self.record_alert(entry, "Known VPN ASN")
        if self.tor_exit_scanner and self.tor_exit_scanner.is_tor(ip):
            self.record_alert(entry, "Tor Exit Node")

    def cleanup_sessions(self, current_time):
        expired = [session for session, entries in self.sessions.items()
                   if entries and current_time - entries[-1]['timestamp'] > self.session_timeout]
        for session in expired:
            del self.sessions[session]

    def cleanup_alerts(self, current_time):
        cutoff_time = current_time - self.alert_timeout
        original_count = len(self.alerts)
        self.alerts = [alert for alert in self.alerts if alert['timestamp'] >= cutoff_time]
        removed = original_count - len(self.alerts)
        if removed > 0:
            self.logger.info(f"[cleanup_alerts] Removed {removed} old alerts.")

    def cleanup(self, current_time):
        if self.last_cleanup_time is None or current_time - self.last_cleanup_time >= timedelta(hours=1):
            self.cleanup_sessions(current_time)
            self.cleanup_alerts(current_time)
            self.last_cleanup_time = current_time

    def detect_anomalies(self, session_cookie):
        entries = sorted(self.sessions[session_cookie], key=lambda x: x['timestamp'])

        for i in range(1, len(entries)):
            prev = entries[i - 1]
            curr = entries[i]
            delta = curr['timestamp'] - prev['timestamp']

            same_prefix = prev['prefix'] == curr['prefix']
            same_asn = prev['asn_number'] == curr['asn_number']

            # Fast IP switch: only alert if different /24 or ASN
            if curr['ip'] != prev['ip'] and delta <= self.fast_switch_threshold:
                if same_prefix and same_asn:
                    self.record_alert(curr, 'Internal IP switch')
                else:
                    self.record_alert(curr, 'Fast IP switch')

            if not same_asn:
                self.record_alert(curr, 'ASN hopping')

            if same_prefix and curr['ip'] != prev['ip'] and not same_asn:
                self.record_alert(curr, 'Rotation inside /24')

    def record_alert(self, entry, alert_type):
        alert = {
            'ip': entry['ip'],
            'host': entry['host'],
            'timestamp': entry['timestamp'],
            'asn': entry['asn_number'],
            'asn_name': entry['asn_name'],
            'type': alert_type
        }

        key = (alert['ip'], alert['host'])
        now = alert['timestamp']
        last_time = self.last_inserted.get(key)

        if last_time is None or now - last_time >= self.insert_interval:
            self.logger.info(f'VPN alert:{alert}')
            self.alerts.append(alert)
            if self.db_config:
                self.save_to_db(alert)
            self.last_inserted[key] = now

    def save_to_db(self, alert):
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO vpn (ip, host, timestamp, asn, asn_name, type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (alert['ip'], alert['host'], alert['timestamp'],
                      alert['asn'], alert['asn_name'], alert['type']))
                conn.commit()

    def get_alerts(self):
        return pd.DataFrame(self.alerts)

    def is_vpn(self, ip):
        return any(alert['ip'] == ip for alert in self.alerts)