import logging
import re
import time


class HostSelector(object):

    def __init__(self,
                 ttl_in_minutes=120,
                 whitelist=[],
                 new_model_wildcard_period=2,
                 logger=None):
        super().__init__()
        self.ttl_in_minutes = ttl_in_minutes
        self.hosts = {}
        self.whitelist = whitelist
        self.new_model_wildcard_period = new_model_wildcard_period
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

    @staticmethod
    def is_ip(value):
        return re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', value) is not None

    def get_next_host(self, consumer):
        time_start = int(time.time())
        while True:
            raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    if (time_start - message.timestamp / 1000) / 60 < 2:
                        self.logger.info('Topic offset is too close to the current times...')
                        return None

                    if not message.value:
                        continue
                    host = message.key.decode("utf-8")

                    if self.is_ip(host):
                        continue

                    if host in self.whitelist:
                        continue

                    if int(time.time()) - time_start < self.new_model_wildcard_period * 60:
                        if (host not in self.hosts):
                            self.logger.info(f'HostSelector: Wildcard new host {host}')
                            self.hosts[host] = int(time.time())
                            return host
                        continue

                    if (host not in self.hosts) or ((int(time.time()) - self.hosts[host]) / 60 > self.ttl_in_minutes):
                        self.logger.info(f'HostSelector: Refreshing model for host {host}')
                        self.hosts[host] = int(time.time())
                        return host
