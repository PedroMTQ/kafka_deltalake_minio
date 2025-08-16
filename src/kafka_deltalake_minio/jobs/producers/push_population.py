from kafka_deltalake_minio.core.data_pusher import DataPusher
from kafka_deltalake_minio.io.logger import logger
from deltalake import DeltaTable
from kafka_deltalake_minio.settings import (
    DELTA_TABLE_URI,
    STORAGE_OPTIONS,
)

class PushPopulationJob():
    '''
    Populates mongo DB according to n_customers
    Note that the customer IDs are static since we are using a range and not an UUID
    This is intentional so that we can easily modify and read specific customers within a given range
    '''
    @staticmethod
    def run(n_customers: int):
        # in a real production environment you wouldn't delete the delta tables, I just did this so that we avoid pushing the rows with the same ID multiple times. In a prod environemnt the ID would be an UUID
        if DeltaTable.is_deltatable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS):
            dt = DeltaTable(DELTA_TABLE_URI, storage_options=STORAGE_OPTIONS)
            dt.delete()
        DataPusher.run(n_customers=n_customers)
        logger.info(f'Pushed {n_customers} customers data')

if __name__ == '__main__':
    # job = PushPopulationJob.run(n_customers=1_000_000_000)
    # job = PushPopulationJob.run(n_customers=1_000_000)
    job = PushPopulationJob.run(n_customers=100)
