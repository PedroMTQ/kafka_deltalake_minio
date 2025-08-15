from datetime import datetime

import polars
from deltalake import DeltaTable

from kafka_deltalake_minio.core.customer_data import CustomerData
from kafka_deltalake_minio.core.utils import batch_yielder
from kafka_deltalake_minio.io.client_mongo import ClientMongo
from kafka_deltalake_minio.io.logger import logger
from kafka_deltalake_minio.settings import (
    CUSTOMERS_COLLECTION_INDEXES,
    CUSTOMERS_COLLECTION_NAME,
    DELTA_TABLE_URI,
    MONGO_BATCH_SIZE,
    STORAGE_OPTIONS,
    UPSERT_KEY,
)


class MongoSyncer():
    def __init__(self):
        self.mongo_client = ClientMongo()

    def yield_data(self, version: int=None):
        source_df = polars.scan_delta(source=DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS, version=version).collect(engine='streaming')
        for row in source_df.rows(named=True):
            yield row

    def run(self, version: int=None, timestamp: datetime=None):
        if not DeltaTable.is_deltatable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS):
            logger.info(f'{DELTA_TABLE_URI} is not yet available, skipping syncing')
            return
        total = 0
        for batch in batch_yielder(self.yield_data(version=version or timestamp), batch_size=MONGO_BATCH_SIZE):
            total += self.mongo_client.batch_upsert(batch=batch,
                                                    collection_name=CUSTOMERS_COLLECTION_NAME,
                                                    collection_indexes=CUSTOMERS_COLLECTION_INDEXES,
                                                    upsert_keys=[UPSERT_KEY],
                                                    headers=CustomerData.fields())
            logger.debug(f"Wrote batch of {len(batch)} records to MongoDb")
        logger.info(f"Wrote {total} records to MongoDb")



if __name__ == '__main__':
    test = MongoSyncer()
    test.run()
