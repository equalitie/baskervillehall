import logging
import psycopg2

from kafka import KafkaConsumer, TopicPartition

import json
from datetime import datetime, timedelta


class SessionStorage(object):
    def __init__(
            self,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            partition=0,
            batch_size=100,
            kafka_connection=None,
            datetime_format='%Y-%m-%d %H:%M:%S',
            ttl_records_days=7,
            logger=None,
            postgres_connection=None,
    ):
        super().__init__()

        if postgres_connection is None:
            postgres_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        self.topic_sessions = topic_sessions
        self.partition = partition
        self.batch_size = batch_size
        self.kafka_connection = kafka_connection
        self.postgres_connection = postgres_connection
        self.datetime_format = datetime_format
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.host_id_timestamp = None
        self.delete_old_records_timestamp = None
        self.hostname_id = None
        self.ttl_records_days = ttl_records_days

    def get_host_id(self, host):
        if self.host_id_timestamp is None or \
                (datetime.now() - self.host_id_timestamp).total_seconds() > 60 * 10:
            self.host_id_timestamp = datetime.now()

            conn = None
            try:
                conn = psycopg2.connect(**self.postgres_connection)
                cur = conn.cursor()
                cur.execute('select hostname, hostname_id from hostname;')

                self.hostname_id = dict()
                for r in cur.fetchall():
                    self.hostname_id[r[0]] = r[1]

            except psycopg2.DatabaseError as error:
                if conn:
                    conn.rollback()
                self.logger.error(error)
            finally:
                if conn:
                    conn.close()

        return self.hostname_id.get(host, '')

    def get_delete_query(self, table, ts):
        return f"delete from {table} where created_at < \'{ts.strftime(self.datetime_format)}\';"

    def delete_old_records(self):
        if self.delete_old_records_timestamp is None or \
                (datetime.now() - self.delete_old_records_timestamp).total_seconds() > 60 * 60:
            self.delete_old_records_timestamp = datetime.now()

            conn = None
            ts_cutoff = (datetime.now() -
                         timedelta(days=self.ttl_records_days))
            try:
                conn = psycopg2.connect(**self.postgres_connection)
                cur = conn.cursor()
                self.logger.info(f'Deleting records older than {ts_cutoff}...')
                sql = self.get_delete_query('sessions', ts_cutoff)
                cur.execute(sql)
                conn.commit()
            except psycopg2.DatabaseError as error:
                if conn:
                    conn.rollback()
                self.logger.error(error)
            finally:
                if conn:
                    conn.close()

    def run(self):
        consumer = KafkaConsumer(
            **self.kafka_connection,
            max_poll_records=self.batch_size,
            fetch_max_bytes=52428800 * 5,
            max_partition_fetch_bytes=1048576 * 10,
            api_version=(0, 11, 5),
        )

        self.logger.info(f'Starting session storage on topic {self.topic_sessions}, partition {self.partition}')

        consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
        consumer.seek_to_end()
        ts_lag_report = datetime.now()
        while True:
            self.delete_old_records()
            raw_messages = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
            for topic_partition, messages in raw_messages.items():
                self.logger.info(f'Batch size {len(messages)}')
                sessions = []
                for message in messages:
                    if (datetime.now() - ts_lag_report).total_seconds() > 5:
                        highwater = consumer.highwater(topic_partition)
                        lag = (highwater - 1) - message.offset
                        self.logger.info(f'Lag = {lag}')
                        ts_lag_report = datetime.now()

                    if not message.value:
                        continue
                    sessions.append(json.loads(message.value.decode("utf-8")))

                conn = None
                try:
                    conn = psycopg2.connect(**self.postgres_connection)
                    cur = conn.cursor()

                    for s in sessions:
                        requests = []
                        start = datetime.strptime(s['start'], self.datetime_format)
                        for r in s['requests'][0:20]:
                            requests.append(
                                (
                                    (datetime.strptime(r['ts'], self.datetime_format) - start).total_seconds(),
                                    r['url']
                                )
                            )
                        requests = json.dumps(requests)
                        host = s["host"]
                        host_id = self.get_host_id(host)
                        if len(host_id) == 0:
                            continue
                        hits = len(s['requests'])
                        duration = s['duration']
                        num_ua = len(set([r['ua'] for r in s['requests']]))
                        cur.execute(f'insert into sessions (\n'
                                    f'hostname_id, host_name, ip, session_cookie, ip_cookie, '
                                    f'primary_session, user_agent, country, continent, '
                                    f'datacenter, hits, \n'
                                    f'hit_rate, num_user_agent,'
                                    f'duration, session_start, session_end, requests)\n'
                                    f'values (\'{host_id}\', \'{host}\', \'{s["ip"]}\', \'{s["session_id"]}\',\n'
                                    f'\'{s["ip"]}_{s["session_id"]}\',{int(s["primary_session"])},\n'
                                    f'\'{s["ua"]}\', \n \'{s["country"]}\', \'{s["continent"]}\', '
                                    f'\'{s["datacenter_code"]}\',\n'
                                    f'{hits}, {hits * 60.0 / duration:.1f}, {num_ua}, '
                                    f'{duration:.1f}, \'{s["start"]}\', \'{s["end"]}\',\n'
                                    f'\'{requests}\''
                                    f');')
                    conn.commit()
                    cur.close()
                except psycopg2.DatabaseError as error:
                    if conn:
                        conn.rollback()
                    self.logger.error(error)
                finally:
                    if conn:
                        conn.close()

                self.logger.info(f'batch={len(sessions)} saved')
