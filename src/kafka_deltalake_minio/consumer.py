from kafka import KafkaConsumer
import polars as pl
from deltalake import write_deltalake
import json
from kafka_deltalake_minio.settings import DELTA_TABLE, STORAGE_OPTIONS,KAFKA_BROKER,KAFKA_TOPIC,CONSUMER_TIMEOUT_MS,CONSUMER_BATCH_SIZE


class Consumer():
    def __init__(self):
        self.consumer = KafkaConsumer(KAFKA_TOPIC,bootstrap_servers=KAFKA_BROKER,consumer_timeout_ms=CONSUMER_TIMEOUT_MS)
        print('Consuming...')

    def write_batch(self):
        batch = []
        for msg in self.consumer:
            batch.append(json.loads(msg.value.decode("utf-8")))  # decode bytes to string
            if len(batch) >= CONSUMER_BATCH_SIZE:
                break
        if not batch:
            return
        df = pl.DataFrame(batch)
        write_deltalake(DELTA_TABLE, df, mode="overwrite",storage_options=STORAGE_OPTIONS)
        print(f"Wrote {len(batch)} records to Delta table")

    def run(self):
        while True:
            self.write_batch()

if __name__ == '__main__':
    test = Consumer()
    test.run()