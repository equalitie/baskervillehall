# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import logging
import threading
import json

from cachetools import TTLCache
from kafka import KafkaConsumer, TopicPartition


class IPStorage(object):

    def __init__(
            self,
            passed_challenge_ttl_in_minutes=600,
            maxsize_passed_challenge=10000000,
            topic_reports='banjax_report_topic',
            num_partitions=3,
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            kafka_group_id='IPStorage',
            logger=None):
        super().__init__()

        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.lock = threading.Lock()
        self.topic_reports = topic_reports
        self.kafka_connection = kafka_connection
        self.kafka_group_id = kafka_group_id
        self.num_partitions = num_partitions

        self.passed = TTLCache(maxsize=maxsize_passed_challenge, ttl=passed_challenge_ttl_in_minutes*60)

    def start(self):
        consumer_thread = threading.Thread(target=self.run)
        consumer_thread.start()

    def run(self):
        consumer = KafkaConsumer(
            group_id=self.kafka_group_id,
            auto_offset_reset='earliest',
            **self.kafka_connection
        )

        try:
            consumer.assign([TopicPartition(self.topic_reports, partition) for partition in range(self.num_partitions)])
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if len(message.value) > 0:
                            try:
                                s = message.value.decode("utf-8")
                            except UnicodeDecodeError:
                                self.logger.info("got bad utf-8 over the kafka channel")

                            try:
                                d = json.loads(s)
                            except json.JSONDecodeError:
                                self.logger.info(f"got bad json over the kafka channel: {s}")

                            if d.get("name") == "ip_failed_challenge":
                                pass
                            elif d.get("name") == "ip_passed_challenge":
                                self.consume_ip_passed_challenge(d['value_ip'])
                            elif d.get("name") == "ip_banned":
                                pass
        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()

    def is_challenge_passed(self, ip):
        with self.lock:
            return ip in self.passed.keys()

    def consume_ip_passed_challenge(self, ip):
        with self.lock:
            if ip in self.passed.keys():
                return
            if len(self.passed) > 0.98 * self.passed.maxsize:
                self.logger.warning('IP cache passed challenge is 98% full. ')

            self.passed[ip] = 1

