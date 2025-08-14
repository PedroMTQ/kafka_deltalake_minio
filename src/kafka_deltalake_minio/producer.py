from kafka import KafkaProducer
import json
import random
from kafka_deltalake_minio.settings import KAFKA_BROKER,KAFKA_TOPIC




class Producer():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def run(self):
        data = [
            {'name': 'Alice', 'age': random.randint(20,30)},
            {'name': 'Bob', 'age': random.randint(20,50)},
            {'name': 'Cathy', 'age': random.randint(45,70)},
                ]
        for msg in data:
            self.producer.send(KAFKA_TOPIC, msg)
        self.producer.flush()
        print(f'Wrote {len(data)} messages: {data}')

if __name__ == '__main__':
    test = Producer()
    test.run()