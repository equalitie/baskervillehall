

import logging
import threading

import psycopg2

from kafka import KafkaConsumer, TopicPartition

import json
from datetime import datetime, timedelta


class StorageBase(object):
    def __init__(
            self,
            topic='',
            partition=0,
            batch_size=100,
            kafka_connection=None,
            datetime_format='%Y-%m-%d %H:%M:%S',
            ttl_records_days=7,
            logger=None,
            postgres_connection=None,
            table=None,
            autocreate_hostname_id=True
    ):
        super().__init__()

        if postgres_connection is None:
            postgres_connection = {}
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        self.topic = topic
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
        self.table = table
        self.autocreate_hostname_id = autocreate_hostname_id

    def get_host_id(self, host):
        if self.host_id_timestamp is None or \
                host not in self.hostname_id or \
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

                if host not in self.hostname_id:
                    if self.autocreate_hostname_id:
                        sql = f'insert into public.hostname '\
                            f'(hostname, created_at, updated_at, updated_by) values '\
                            f'(\'{host}\', current_timestamp, current_timestamp, \'pipeline\') RETURNING hostname_id;'
                        cur.execute(sql)
                        for r in cur.fetchall():
                            self.hostname_id[host] = r[0]
                        conn.commit()
                        self.logger.info(f'New hostname_id added {host} = {self.hostname_id[host]}')

            except psycopg2.DatabaseError as error:
                if conn:
                    conn.rollback()
                self.logger.error(error)
            finally:
                if conn:
                    conn.close()

        return self.hostname_id.get(host, '')

    def get_delete_query(self, ts):
        return f"delete from {self.table} where created_at < \'{ts.strftime(self.datetime_format)}\';"

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
                sql = self.get_delete_query(ts_cutoff)
                cur.execute(sql)
                conn.commit()
            except psycopg2.DatabaseError as error:
                if conn:
                    conn.rollback()
                self.logger.error(error)
            finally:
                if conn:
                    conn.close()

    def get_sql(self, record):
        return None

    def get_session_requests(self, session, num_requests):
        start = datetime.strptime(session['start'], self.datetime_format)
        requests = []
        for r in session['requests'][0:num_requests]:
            requests.append(
                (
                    (datetime.strptime(r['ts'], self.datetime_format) - start).total_seconds(),
                    r['url'].replace('\'', '')
                )
            )
        return json.dumps(requests)

    def get_number_of_useragents(self, session):
        return len(set([r['ua'] for r in session['requests']]))

    def start(self):
        t = threading.Thread(target=self.run)
        t.start()
        return t

    def run(self):
        consumer = KafkaConsumer(
            **self.kafka_connection,
            max_poll_records=self.batch_size,
            fetch_max_bytes=52428800 * 5,
            max_partition_fetch_bytes=1048576 * 10,
            api_version=(0, 11, 5),
        )

        self.logger.info(f'Starting session storage on topic '
                         f'{self.topic}, partition {self.partition}, table {self.table}')

        consumer.assign([TopicPartition(self.topic, self.partition)])
        consumer.seek_to_end()
        ts_lag_report = datetime.now()
        while True:
            self.delete_old_records()
            raw_messages = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
            for topic_partition, messages in raw_messages.items():
                records = []
                for message in messages:
                    if (datetime.now() - ts_lag_report).total_seconds() > 5:
                        highwater = consumer.highwater(topic_partition)
                        lag = (highwater - 1) - message.offset
                        self.logger.info(f'Lag = {lag}')
                        ts_lag_report = datetime.now()

                    if not message.value:
                        continue
                    records.append(json.loads(message.value.decode("utf-8")))

                conn = None
                try:
                    conn = psycopg2.connect(**self.postgres_connection)
                    cur = conn.cursor()

                    for r in records:
                        sql = self.get_sql(r)
                        if sql:
                            cur.execute(sql)
                    conn.commit()
                    cur.close()
                except psycopg2.DatabaseError as error:
                    self.logger.info(sql)
                    if conn:
                        conn.rollback()
                    self.logger.error(error)
                finally:
                    if conn:
                        conn.close()