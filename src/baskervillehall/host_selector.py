import logging
import re
import time


class HostSelector(object):

    def __init__(self,
                 ttl_in_minutes=120,
                 whitelist=[],
                 kafka_timeout_ms=1000,
                 kafka_max_size=5000,
                 logger=None):
        super().__init__()
        self.ttl_in_minutes = ttl_in_minutes
        self.hosts = {}
        self.whitelist = whitelist
        self.kafka_timeout_ms = kafka_timeout_ms
        self.kafka_max_size = kafka_max_size
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

    @staticmethod
    def is_ip(value):
        return re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', value) is not None

    def get_next_hosts(self, consumer, number_of_hosts=5):
        time_start = int(time.time())
        host_batch = []
        consumer.seek_to_beginning()
        while True:
            raw_messages = consumer.poll(timeout_ms=self.kafka_timeout_ms, max_records=self.kafka_max_size)
            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    if (time_start - message.timestamp / 1000) / 60 < 2:
                        self.logger.info('Topic offset is too close to the current times...')
                        return host_batch

                    if not message.value:
                        continue
                    host = message.key.decode("utf-8")

                    if self.is_ip(host):
                        continue

                    if host in self.whitelist:
                        continue

                    if (host not in self.hosts) or ((int(time.time()) - self.hosts[host]) / 60 > self.ttl_in_minutes):
                        self.hosts[host] = int(time.time())
                        host_batch.append(host)
                        if len(host_batch) == number_of_hosts:
                            return host_batch
