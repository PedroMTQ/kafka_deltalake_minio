from kafka_deltalake_minio.core.data_pusher import DataPusher
from kafka_deltalake_minio.io.logger import logger


class PushPopulationJob():
    '''
    Populates mongo DB according to n_customers
    Note that the customer IDs are static since we are using a range and not an UUID
    This is intentional so that we can easily modify and read specific customers within a given range
    '''
    @staticmethod
    def run(n_customers: int):
        DataPusher.run(n_customers=n_customers)
        logger.info(f'Pushed {n_customers} customers data')

if __name__ == '__main__':
    # job = PopulateMongo.run(n_customers=1_000_000_000)
    # job = PushPopulationJob.run(n_customers=1_000_000)
    job = PushPopulationJob.run(n_customers=1)
