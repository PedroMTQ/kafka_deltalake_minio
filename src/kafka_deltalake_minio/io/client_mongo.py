
import os
from datetime import datetime
from time import perf_counter
from typing import Optional

import bson
import pymongo
import pymongo.errors
from pymongo.errors import ServerSelectionTimeoutError
from retry import retry

from kafka_deltalake_minio.core.utils import get_current_time
from kafka_deltalake_minio.io.logger import logger

MONGO_RETRY_EXCEPTIONS = (ServerSelectionTimeoutError)  # We only retry on "disconnection", not when authentication fails ofc


MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_PORT = os.getenv('MONGO_PORT')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')

MONGO_RETRIES = int(os.getenv('MONGO_RETRIES', '5'))
MONGO_RETRY_DELAY = int(os.getenv('MONGO_RETRY_DELAY', '1'))
MONGO_RETRY_BACKOFF = int(os.getenv('MONGO_RETRY_BACKOFF', '2'))


retry_mongo = retry(exceptions=MONGO_RETRY_EXCEPTIONS,
                    tries=MONGO_RETRIES,
                    delay=MONGO_RETRY_DELAY,
                    backoff=MONGO_RETRY_BACKOFF,
                    logger=logger)


class ClientMongo():
    def __init__(self,
                 user: str=MONGO_USER,
                 password: str=MONGO_PASSWORD,
                 host: str=MONGO_HOST,
                 port: str=MONGO_PORT,
                 dbname: str=MONGO_DATABASE,
                 uri: Optional[str]=None):
        super().__init__()
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        if uri:
            self.uri = uri
        else:
            self.uri = f'mongodb://{self.__user}:{self.__password}@{self.__host}:{self.__port}'
        self.client = pymongo.MongoClient(self.uri,
                                          document_class=dict,
                                          connect=True)
        self.database = self.client.get_database(name=self.__dbname,
                                                 codec_options=bson.codec_options.CodecOptions(uuid_representation=bson.binary.UUID_SUBTYPE))
        self.test_connection()
        logger.info(f'Connected successfully to Mongo DB:{self.__dbname} at {self.__host}:{self.__port}')

    def test_connection(self):
        try:
            self.client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logger.error(f"Impossible to connect to Mongo (url: {self.uri}, database: {self.__dbname})")
            raise e
        except pymongo.errors.OperationFailure as e:
            logger.error(f"Authentication failed when connecting to Mongo (url: {self.uri}, database: {self.__dbname})")
            raise e

    def flush_db(self):
        logger.warning(f'Flushing database mongo:{self.__dbname}')
        self.client.drop_database(self.__dbname)

    def get_collections(self):
        return self.database.list_collection_names()

    def replace_collection(self, old_collection_name: str, new_collection_name: str):
        self.database.drop_collection(old_collection_name)
        self.database.get_collection(new_collection_name).rename(old_collection_name)

    def get_indexes(self, collection_name: str):
        collection = self.database.get_collection(collection_name)
        res = set()
        for index_name in collection(collection_name).index_information():
            res.add(index_name)
        return res

    def count_documents(self, collection_name: str, query: Optional[dict] = None) -> int:
        if query is None:
            query = {}
        collection = self.database.get_collection(collection_name)
        count = collection.count_documents(query)
        return count

    def create_indexes(self, collection_name: str, collection_indexes: list[str]):
        collection = self.database.get_collection(collection_name)
        for idx in collection_indexes:
            collection.create_index(name=idx, keys=idx)

    @retry(Exception, tries=MONGO_RETRIES, delay=MONGO_RETRY_DELAY, backoff=MONGO_RETRY_BACKOFF, logger=logger)
    def batch_insert(self,
                     batch: list[dict],
                     collection_name: str):
        if not batch:
            return
        collection = self.database.get_collection(collection_name)
        logger.debug('Processing data batch')
        start = perf_counter()
        collection.insert_many(batch)
        end = perf_counter()
        logger.debug(f'Data insertion of {len(batch)} rows took {end-start} seconds')

    @retry(Exception, tries=MONGO_RETRIES, delay=MONGO_RETRY_DELAY, backoff=MONGO_RETRY_BACKOFF, logger=logger)
    def batch_upsert(self,
                     batch: list[dict],
                     collection_name: str,
                     upsert_keys: list[str],
                     headers: list[str]) -> int:
        processed_batch = []
        current_time = get_current_time()
        if not batch:
            return 0
        collection = self.database.get_collection(collection_name)
        logger.debug('Processing data batch')
        start = perf_counter()
        for row in batch:
            processed_row = self.batch_processing_per_row(row=row,
                                                          headers=headers,
                                                          upsert_keys=upsert_keys,
                                                          current_time=current_time)
            processed_batch.append(processed_row)
        end = perf_counter()
        logger.debug(f'Data processing of {len(processed_batch)} rows took {end-start} seconds')

        if processed_batch:
            logger.debug('Writing data')
            start = perf_counter()
            collection.bulk_write(processed_batch, ordered=False)
            end = perf_counter()
            logger.debug(f'Data writing took {end-start} seconds')
        return len(processed_batch)


    def batch_processing_per_row(self,
                                 row: dict,
                                 upsert_keys: list[str],
                                 headers: list[str],
                                 current_time: datetime):
        query = {k: row.get(k) for k in upsert_keys}
        update_pipeline = [
                {
                    "$set": {
                        "created_at": {
                            "$cond": {
                                "if": {"$eq": [{"$type": "$created_at"}, "missing"]},
                                "then": current_time,
                                "else": "$created_at",
                            }
                        },
                        "updated_at": {
                            "$cond": {
                               "if": {
                                    "$or": [{"$eq": [{"$type": "$updated_at"}, 'missing']}] + [
                                        {"$ne": [row.get(k), f"${k}"]} for k in headers if k not in upsert_keys
                                    ]
                                },
                                "then": current_time,
                                "else": "$updated_at",
                            }
                        },
                    }
                },
                {
                    "$set": row,
                },
            ]
        return pymongo.UpdateOne(filter=query, update=update_pipeline, upsert=True)


if __name__ == '__main__':
    db = ClientMongo()
    print(db.get_collections())
