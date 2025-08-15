from datetime import datetime, timezone

from kafka_deltalake_minio.core.mongo_syncer import MongoSyncer
from kafka_deltalake_minio.io.logger import logger
from deltalake import DeltaTable
from kafka_deltalake_minio.settings import DELTA_TABLE_URI, STORAGE_OPTIONS

class RollbackJob():
    def run(self, version: int=None, timestamp: datetime=None):
        if not version and not timestamp:
            raise Exception('Missing version and timestamp')
        mongo_syncer = MongoSyncer()
        try:
            mongo_syncer.run(version=version, timestamp=timestamp)
            logger.exception(f'Rolled back mongo to {version or timestamp}')
        except Exception as e:
            logger.exception(f'Failed to rollback mongo due to {e}')
        dt = DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
        dt.restore(target=version or timestamp)
        logger.exception(f'Rolled back DeltaTable {DELTA_TABLE_URI} to {version or timestamp}')


if __name__ == '__main__':
    test = RollbackJob()
    test.run(version=2)
    # test.run(timestamp=datetime(year=2025, month=8, day=15, hour=19, minute=49, second=36, tzinfo=timezone.utc))
