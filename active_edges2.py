import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, KafkaError

conf = {
    "bootstrap.servers": "kafkab0.prod.deflect.network:9094,kafkab1.prod.deflect.network:9094,kafkab2.prod.deflect.network:9094",
    # "bootstrap.servers": "kafkadev0.prod.deflect.network:9094,kafkadev1.prod.deflect.network:9094,kafkadev2.prod.deflect.network:9094",
    "security.protocol": "SSL",
    "ssl.endpoint.identification.algorithm": "none",
    "ssl.ca.location": "/Users/almaz/equalitie/greenhost/caroot.pem",
    "ssl.certificate.location": "/Users/almaz/equalitie/greenhost/certificate.pem",
    "ssl.key.location": "/Users/almaz/equalitie/greenhost/key.pem",
    "enable.auto.commit": False,
    "group.id": "almaz-test-1",
    "socket.keepalive.enable": True,
    "reconnect.backoff.ms": 500,
    "reconnect.backoff.max.ms": 5000,
    "session.timeout.ms": 15000,
    "metadata.max.age.ms": 15000,
    "socket.timeout.ms": 30000,
    # "debug": "broker,protocol,security",
}

c = Consumer(conf)
c.subscribe(["logstash_deflect.log"])

try:
    ts = datetime.now()
    edges = set()
    while True:
        msg = c.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(msg.error())
            continue
        # print(msg.topic(), msg.partition(), msg.offset(), msg.value()[:200])
        v = msg.value().decode("utf-8")
        d = json.loads(v)
        # print(d['agent']['name'])
        # import pdb; pdb.set_trace()
        edges.add(d['agent']['name'])
        if (datetime.now()-ts).total_seconds() > 10:
            ts = datetime.now()
            for e in edges:
                print(e)
            print(f'\n\n################## total {len(edges)} edges.')

        c.commit(asynchronous=True)
finally:
    c.close()
