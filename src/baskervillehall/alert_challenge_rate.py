import logging
import threading
from time import sleep
from datetime import datetime, timedelta
import numpy as np
from cachetools import TTLCache

import psycopg2

from baskervillehall.alerts import send_webhook_alert


class AlertChallengeRate(object):
    def __init__(self,
                 postgres_connection=None,
                 aggregation_window_in_minutes=5,
                 dataset_in_hours=12,
                 zscore_hits=2.0,
                 zscore_challenge_rate=2.0,
                 pending_period_in_minutes=30,
                 min_num_sessions=10,
                 host_white_list=None,
                 threshold_challenge_rate=20,
                 webhook=None,
                 cc=None,
                 logger=None):
        super().__init__()
        if postgres_connection is None:
            postgres_connection = {}
        self.aggregation_window_in_minutes = aggregation_window_in_minutes
        self.postgres_connection = postgres_connection
        self.dataset_in_hours = dataset_in_hours
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.zscore_hits = zscore_hits
        self.zscore_challenge_rate = zscore_challenge_rate
        self.host_white_list = host_white_list if host_white_list else []
        self.pending_alerts = TTLCache(maxsize=10000, ttl=60 * pending_period_in_minutes)
        self.webhook = webhook
        self.min_num_sessions = min_num_sessions
        self.cc = cc if cc else ''
        self.threshold_challenge_rate = threshold_challenge_rate

    def start(self):
        t = threading.Thread(target=self.run)
        t.start()
        return t

    def get_sql(self, ts_from, ts_to):
        date_from = ts_from.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_to = ts_to.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"""
            select Z.time, Z.host, Z.challenge_rate, Y.hits  from
            (
    
            select A.time, A.host, num_challenged *100.0 / num_sessions as "challenge_rate" from
             (
            SELECT
              floor(extract(epoch from session_end)/300)*300 AS "time",
              count(distinct ip_cookie) AS "num_challenged",
              host_name as "host"
            FROM challenge_command_history cch
            WHERE
              session_end BETWEEN '{date_from}' AND '{date_to}' 
            GROUP BY 1,3
            ) as A inner join (
    
                SELECT
                  floor(extract(epoch from session_end)/300)*300 AS "time",
                  count(distinct ip_cookie) AS "num_sessions",
                  host_name as "host"
                FROM sessions
                WHERE
                  session_end BETWEEN '{date_from}' AND '{date_to}' 
                GROUP BY 1,3
    
            ) as B on A.time=B.time and A.host=B.host where num_sessions > {self.min_num_sessions}
    
    
            ) as Z inner join (
                    SELECT
                  floor(extract(epoch from session_end)/300)*300 AS "time",
                  sum(hits) AS "hits",
                  host_name as "host"
                FROM sessions
                WHERE
                  session_end BETWEEN '{date_from}' AND '{date_to}' 
                GROUP BY 1,3
            ) as Y on Z.time = Y.time and Y.host=Z.host order by host,time;
            """

    def zscore(self, vector, value):
        if len(vector) < 5:
            return 0.0

        v = np.array(vector)
        mean = float(np.mean(v))
        std = float(np.std(v))
        if std == 0:
            return 0.0
        return (float(value) - mean) / std

    def detect_outliers(self, hosts):
        for host, data in hosts.items():
            if host in self.pending_alerts:
                continue
            self.pending_alerts[host] = True
            last = data[-1]
            all = data[:-1]
            challenge_rate = last[1]
            hits = last[2]
            vector_rate = [d[1] for d in all]
            vector_hits = [d[2] for d in all]
            alert_timestamp = datetime.utcfromtimestamp(last[0]).strftime('%Y-%m-%d %H:%M:%S')
            zscore_challenge_rate = self.zscore(vector_rate, challenge_rate)
            zscore_hits = self.zscore(vector_hits, hits)
            if zscore_challenge_rate > self.zscore_challenge_rate and \
                    zscore_hits > self.zscore_hits and \
                    challenge_rate > self.threshold_challenge_rate:
                self.logger.info(f'ALERT: {host} rate = {challenge_rate}({zscore_challenge_rate}), '
                                 f'zscore hits = {hits}({zscore_hits})')
                send_webhook_alert(f'Challenge Rate Alert - `{host}`',
                                   f'\n**Challenge rate: `{challenge_rate:.1f}%`**.\n\n\n\n\n'
                                   f'Timestamp: `{alert_timestamp}`.\n'
                                   f'ZScore=`{zscore_challenge_rate:.1f}`\n'
                                   f'\ncc: {self.cc}\n', self.webhook, self.logger)

    def run(self):
        self.logger.info(f'ALERT:Starting challenge rate alert thread...')
        while True:
            conn = None
            try:
                self.logger.info('ALERT:Connecting...')
                conn = psycopg2.connect(**self.postgres_connection)
                cur = conn.cursor()
                sql = self.get_sql(datetime.now() - timedelta(hours=self.dataset_in_hours), datetime.now())
                self.logger.info('ALERT:Executing...')
                cur.execute(sql)

                hosts = dict()
                for r in cur.fetchall():
                    host = r[1]
                    if host not in hosts:
                        hosts[host] = []
                    hosts[host].append((r[0], r[2], r[3]))

                self.detect_outliers(hosts)

            except psycopg2.DatabaseError as error:
                if conn:
                    conn.rollback()
                self.logger.error(error)
            finally:
                if conn:
                    conn.close()

            self.logger.info('ALERT:Sleeping...')
            sleep(self.aggregation_window_in_minutes * 60)
