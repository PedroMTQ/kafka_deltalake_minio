from kafka import KafkaProducer

from kafka_deltalake_minio.core.utils import serializer
from kafka_deltalake_minio.settings import KAFKA_BROKER, KAFKA_TOPIC


class BaseProducer(KafkaProducer):
    def __init__(self):
        if not KAFKA_BROKER:
            raise Exception('Missing KAFKA_BROKER')
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                                      value_serializer=serializer)

    def __str__(self):
        return f'{self.__class__.__name__} is pushing to {KAFKA_TOPIC}'
