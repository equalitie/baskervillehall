import json
from collections import defaultdict

from kafka import KafkaConsumer, TopicPartition
from datetime import datetime


class BaskervillehallCounter(object):
    def __init__(
            self,
            topic='BASKERVILLEHALL_WEBLOGS',
            partition=0,
            kafka_group_id='baskervillehall_counter',
            kafka_connection={'bootstrap_servers': 'localhost:9092'},
            window=1,
            batch_size=1000,
            logger=None,
    ):
        super().__init__()
        self.topic = topic
        self.partition = partition
        self.kafka_group_id = kafka_group_id
        self.kafka_connection = kafka_connection
        self.window = window
        self.logger = logger
        self.batch_size = batch_size

    def run(self):
        consumer = KafkaConsumer(
            **self.kafka_connection,
            group_id=self.kafka_group_id
        )

        self.logger.info(f'Starting Baskervillehall counter on topic {self.topic}')

        ts = datetime.now()
        counter = 0
        edges = defaultdict(int)
        try:
            consumer.assign([TopicPartition(self.topic, self.partition)])
            while True:
                raw_messages = consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                for topic_partition, messages in raw_messages.items():
                    for message in messages:
                        if not message.value:
                            continue

                        value = json.loads(message.value.decode("utf-8"))
                        edges[value['host']['name']] += 1

                        counter += 1
                        if (datetime.now() - ts).total_seconds() >= self.window * 60:
                            self.logger.info(f'Number of records = {counter} per {self.window} minutes')
                            counter = 0
                            ts = datetime.now()
                            self.logger.info(edges)
                            edges = defaultdict(int)

        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        finally:
            consumer.close()
