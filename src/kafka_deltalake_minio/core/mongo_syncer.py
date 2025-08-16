from datetime import datetime

import polars
from deltalake import DeltaTable
from typing import Iterable
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
    MONGO_WRITE_MODE,
    MONGO_WRITE_MODES,
)


class MongoSyncer():
    def __init__(self):
        self.mongo_client = ClientMongo()

    def yield_data(self, version: int=None):
        source_df = polars.scan_delta(source=DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS, version=version).collect(engine='streaming')
        for row in source_df.rows(named=True):
            yield row

    def __run_upsert(self, data_yielder: Iterable):
        for batch in data_yielder:
            self.mongo_client.batch_upsert(batch=batch,
                                           collection_name=CUSTOMERS_COLLECTION_NAME,
                                           upsert_keys=[UPSERT_KEY],
                                           headers=CustomerData.fields())

    def __run_insert(self, data_yielder: Iterable):
        temp_collection = f'temp_{CUSTOMERS_COLLECTION_NAME}'
        for batch in data_yielder:
           self.mongo_client.batch_insert(batch=batch,
                                          collection_name=temp_collection)
        self.mongo_client.replace_collection(old_collection_name=CUSTOMERS_COLLECTION_NAME,
                                             new_collection_name=temp_collection)

    def run(self, version: int=None, timestamp: datetime=None, write_mode: MONGO_WRITE_MODES = MONGO_WRITE_MODE):
        if not DeltaTable.is_deltatable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS):
            logger.info(f'{DELTA_TABLE_URI} is not yet available, skipping syncing')
            return
        total = 0
        self.mongo_client.create_indexes(collection_name=CUSTOMERS_COLLECTION_NAME,
                                         collection_indexes=CUSTOMERS_COLLECTION_INDEXES)
        data_yielder = batch_yielder(self.yield_data(version=version or timestamp), batch_size=MONGO_BATCH_SIZE)
        if write_mode == 'insert':
            self.__run_insert(data_yielder=data_yielder)
        elif write_mode == 'upsert':
            self.__run_upsert(data_yielder=data_yielder)
        logger.info(f"Wrote {total} records in {write_mode} mode to MongoDb")



if __name__ == '__main__':
    test = MongoSyncer()
    test.run()
