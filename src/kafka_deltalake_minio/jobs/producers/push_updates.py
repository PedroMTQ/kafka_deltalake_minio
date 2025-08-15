from kafka_deltalake_minio.core.data_pusher import DataPusher
import polars
from kafka_deltalake_minio.io.delta_table_reader import DeltaTableReader
from kafka_deltalake_minio.io.logger import logger
from kafka_deltalake_minio.settings import PARTITION_KEY, UPSERT_KEY


class PushUpdatesJob():
    '''
    Updates mongo DB according to list of customer IDs
    '''
    @staticmethod
    def run(list_ids: list[int]):
        data = []
        for customer_id in list_ids:
            customer_data = DeltaTableReader.get_customer_data(customer_id=customer_id)
            if not customer_data:
                logger.error(f'Customer {customer_id} not found')
                continue
            if len(data) > 1:
                logger.error(f'Found too many rows for {customer_id}')
            old_customer_data = customer_data[0]
            new_customer_data = DataPusher.generate_customer(**{UPSERT_KEY: old_customer_data.__getattribute__(UPSERT_KEY),
                                                                PARTITION_KEY: old_customer_data.__getattribute__(PARTITION_KEY)})
            logger.info(f'Updating {old_customer_data} to {new_customer_data}')
            data.append(new_customer_data)
        if data:
            DataPusher.run(data=data)


if __name__ == '__main__':
    list_ids = [1]
    job = PushUpdatesJob.run(list_ids=list_ids)
