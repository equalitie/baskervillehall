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
    Country blocking rules are fetched from the Deflect API via SettingsDeflectAPI.
    """

    def __init__(self, deflect_api_setting,
                 kafka_connection_output, kafka_connection, topic_commands, dnet_partition_map,
                 logger=None):
        self.topic_commands = topic_commands
        self.logger = logger or logging.getLogger(__name__)
        self.blocked_ips = TTLCache(maxsize=50000, ttl=300)
        self.producer_output = KafkaProducer(**kafka_connection_output)
        if kafka_connection is not None:
            self.producer = KafkaProducer(**kafka_connection)
        else:
            self.producer = None
        self.dnet_partition_map = dnet_partition_map
        self.deflect_api_setting = deflect_api_setting

    def is_country_blocked(self, host, country):
        return self.deflect_api_setting.is_country_blocked(host, country)

    def process(self, host, ip, country, dnet='-'):
        if not self.is_country_blocked(host, country):
            return False

        if ip in self.blocked_ips:
            return True

        self._send_block_command(host, ip, country, dnet)
        self.blocked_ips[ip] = True
        return True

    def _send_block_command(self, host, ip, country, dnet):
        action_type = self.deflect_api_setting.get_country_action_type(host)
        ttl_ban_time = self.deflect_api_setting.get_ttl_ban_time(host)

        if action_type == 'challenge':
            command_name = 'challenge_ip'
        else:
            command_name = 'block_ip'

        command = {
            "Name": command_name,
            "Value": ip,
            "host": host,
            "country": country,
            "dnet": dnet,
            "source": "country_block",
            "meta": "country_block",
            "ttl": ttl_ban_time,
        }
        partition = self.dnet_partition_map.get(dnet, -1)
        value = json.dumps(command).encode("utf-8")
        if self.producer is not None:
            self.producer.send(
                topic=self.topic_commands,
                value=value,
                key=bytearray(host, encoding="utf8"),
            )
        if partition < 0:
            self.logger.warning(f"Dnet  {dnet} is not found in "
                                f"the dnet map {self.dnet_partition_map}.")
            self.producer_output.send(
                topic=self.topic_commands,
                value=value,
                key=bytearray(host, encoding="utf8"),
            )
        else:
            self.producer_output.send(
                topic=self.topic_commands,
                value=value,
                partition=partition,
            )

        self.producer.flush()

        self.logger.info(
            f'Country block: ip={ip} country={country} host={host} command={command_name} ttl={ttl_ban_time}'
        )
