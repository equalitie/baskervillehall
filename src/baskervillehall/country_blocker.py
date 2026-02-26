# -*- coding: utf-8 -*-
import json
import logging

from cachetools import TTLCache
from kafka import KafkaProducer


class CountryBlocker:
    """
    Per-domain country-based IP blocking.
    When a request's country is in the blocked list for a host,
    sends a block_ip command to Kafka and signals the caller to skip the message.
    """

    # Whitelist mode: only these countries are allowed, everything else is blocked.
    # Keys: hostname, values: set of allowed ISO country codes.
    ALLOWED_COUNTRIES = {
        # 'antijob.net': {
        #     'RU',  # Russia
        #     'US',  # USA
        #     'DE',  # Germany
        #     'NL',  # Netherlands
        #     'UA',  # Ukraine
        #     'GE',  # Georgia
        #     'KG',  # Kyrgyzstan
        #     'BY',  # Belarus
        #     'FI',  # Finland
        #     'CA',  # Canada
        # },
    }

    def __init__(self, kafka_connection, topic_commands, dnet_partition_map, logger=None):
        self.topic_commands = topic_commands
        self.logger = logger or logging.getLogger(__name__)
        self.blocked_ips = TTLCache(maxsize=50000, ttl=300)
        self.producer = KafkaProducer(**kafka_connection)
        self.dnet_partition_map = dnet_partition_map

    def is_country_blocked(self, host, country):
        allowed = self.ALLOWED_COUNTRIES.get(host)
        if allowed is None:
            return False
        return country not in allowed

    def process(self, host, ip, country, dnet='-'):
        if not self.is_country_blocked(host, country):
            return False

        if ip in self.blocked_ips:
            return True

        self._send_block_command(host, ip, country, dnet)
        self.blocked_ips[ip] = True
        return True

    def _send_block_command(self, host, ip, country, dnet):
        command = {
            "Name": "block_ip",
            "Value": ip,
            "host": host,
            "country": country,
            "dnet": dnet,
            "source": "country_block",
            "meta": "country_block",
        }
        partition = self.dnet_partition_map.get(dnet, -1)
        if partition < 0:
            self.logger.warning(f"Dnet  {dnet} is not found in "
                                f"the dnet map {self.dnet_partition_map}.")
            self.producer.send(
                topic=self.topic_commands,
                value=json.dumps(command).encode("utf-8"),
                key=bytearray(host, encoding="utf8"),
            )
        else:
            self.producer.send(
                topic=self.topic_commands,
                value=json.dumps(command).encode("utf-8"),
                partition=partition,
            )

        self.logger.info(
            f'Country block: ip={ip} country={country} host={host}'
        )
