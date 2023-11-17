import json
from datetime import datetime
import time
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import requests
import concurrent.futures
import threading
from urllib.request import Request, urlopen


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))


class BaskervillePinger(object):

    def __init__(
            self,
            topic = 'deflect.log',
            topic_output = 'ping',
            partition=0,
            kafka_group_id='baskerville_pinger',
            kafka_connection=None,
            kafka_timeout_ms=1000,
            kafka_max_size=5000,
            num_workers=100,
            host_refresh_minutes=60,
            logger=None,
    ):
        self.topic = topic
        self.topic_output = topic_output
        self.partition = partition
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.kafka_timeout_ms = kafka_timeout_ms
        self.kafka_max_size = kafka_max_size
        self.num_workers = num_workers
        self.host_refresh_minutes = host_refresh_minutes
        self.logger = logger

    def ping(self, hosts, event):
        producer = KafkaProducer(**self.kafka_connection)

        while not event.is_set():
            # wait for the first half of minute
            while datetime.now().second > 30 and not event.is_set():
                time.sleep(1)

            for host in hosts:
                if event.is_set():
                    break
                time_ms = 10000
                tag = ''
                try:
                    ts1 = datetime.now()
                    self.logger.info(f'pinging {host}')

                    # response = requests.head(f'https://{host}', timeout=5, headers={
                    #     'User-Agent': 'Baskerville ping'
                    # })
                    # self.logger.info(f'elapsed.total_seconds={response.elapsed.total_seconds()}, timer = {(datetime.now()-ts1).total_seconds()}')

                    req = Request(f'https://{host}')
                    req.add_header('User-Agent', 'Baskerville ping')
                    with urlopen(req) as response:
                        response.read()

                    self.logger.info(f'host{host} timer = {(datetime.now()-ts1).total_seconds()}')
                except requests.exceptions.Timeout:
                    self.logger.info(f'Host {host} timeout.')
                    tag = 'timeout'
                except requests.exceptions.TooManyRedirects:
                    self.logger.info(f'Host {host} too many redirects.')
                    tag = 'too_many_redirects'
                except requests.exceptions.RequestException as e:
                    self.logger.info(f'Host {host} request exception {str(e)}')
                    tag = 'request_exception'

                message = {
                    'host': host,
                    'ping': time_ms,
                    'tag': tag
                }

                producer.send(
                    self.topic_output,
                    key=bytearray(host, encoding='utf8'),
                    value=json.dumps(message).encode('utf-8')
                )

            time.sleep(40)

    def get_hosts(self):
        consumer = KafkaConsumer(
            **self.kafka_connection,
            group_id=self.kafka_group_id
        )
        self.logger.info(f'Updating host list from topic {self.topic}')

        hosts = set()
        counter = 0
        try:
            consumer.assign([TopicPartition(self.topic, self.partition)])
            time_start = int(time.time())
            consumer.seek_to_beginning()
            while True:
                raw_messages = consumer.poll(timeout_ms=self.kafka_timeout_ms, max_records=self.kafka_max_size)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if (time_start - message.timestamp / 1000) / 60 < 2:
                            self.logger.info('Topic offset is too close to the current times...')
                            return list(hosts)
                        if not message.value:
                            continue
                        host = message.key.decode("utf-8")
                        hosts.add(host)
                        counter += 1
                        if counter > 100:
                            return list(hosts)


        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')
        finally:
            consumer.close()

        return hosts

    def run(self):

        self.logger.info('updating hosts...')
        hosts = self.get_hosts()
        self.logger.info(hosts)

        while True:
            event = threading.Event()
            num_workers = min(self.num_workers, len(hosts))

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                host_batches = split(hosts, num_workers)
                for host_batch in host_batches:
                    self.logger.info(f'batch {host_batch}')
                    executor.submit(self.ping, host_batch, event)

                self.logger.info(f'sleeping for {self.host_refresh_minutes} minutes...')
                time.sleep(60*self.host_refresh_minutes)

                self.logger.info('updating hosts...')
                hosts = self.get_hosts()
                self.logger.info(hosts)

                # wait for the second half of minute
                while datetime.now().second < 30:
                    time.sleep(1)
                event.set()
                executor.shutdown()


