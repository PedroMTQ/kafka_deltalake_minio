from datetime import datetime, timezone

from faker import Faker

from kafka_deltalake_minio.core.customer_data import CustomerData
from kafka_deltalake_minio.io.base_kafka_producer import BaseProducer
from kafka_deltalake_minio.io.logger import logger
from kafka_deltalake_minio.settings import KAFKA_PUSH_BATCH_SIZE, KAFKA_TOPIC, PARTITION_KEY


FAKE_DATA_GENERATOR = Faker()


class DataPusher():
    '''
    Populates mongo DB according to n_customers
    Note that the customer IDs are static since we are using a range and not an UUID
    This is intentional so that we can easily modify and read specific customers within a given range
    '''


    @staticmethod
    def generate_customer(**kwargs):
        return CustomerData(
                _id=kwargs['_id'],
                name=kwargs.get('name', FAKE_DATA_GENERATOR.name()),
                email=kwargs.get('email', FAKE_DATA_GENERATOR.email()),
                country=kwargs.get('country', FAKE_DATA_GENERATOR.country()),
                phone=kwargs.get('phone', FAKE_DATA_GENERATOR.phone_number()),
                signup_date=kwargs.get('signup_date', datetime.combine(FAKE_DATA_GENERATOR.date_between(start_date='-2y', end_date='today'),
                                                                       datetime.min.time(),
                                                                       tzinfo=timezone.utc))
                 )

    @staticmethod
    def yield_fake_data(n_customers: int) -> list[dict]:
        for customer_id in range(1, n_customers + 1):
            yield DataPusher.generate_customer(_id=customer_id)

    @staticmethod
    def run(data: list[CustomerData]=None, n_customers: int=None, push_frequency: int=KAFKA_PUSH_BATCH_SIZE):
        if not data and not n_customers:
            raise Exception('data and n_customers are None...')
        if n_customers:
            data = DataPusher.yield_fake_data(n_customers=n_customers)
        kafka_producer = BaseProducer()
        logger.info(kafka_producer)
        count = 0
        customer_data: CustomerData
        for customer_data in data:
            kafka_producer.producer.send(topic=KAFKA_TOPIC,
                                         key=customer_data.__getattribute__(PARTITION_KEY).encode('utf-8'),
                                         value=customer_data.to_dict())
            if count >= push_frequency:
                kafka_producer.producer.flush()
                count = 0
                logger.info(f'Pushed {count} messages to Kafka')
        kafka_producer.producer.flush()

if __name__ == '__main__':
    fake_data = DataPusher.yield_fake_data(n_customers=1)
    for i in fake_data:
        print(i)
    customer = DataPusher.generate_customer(_id=2, country='Portugal')
    print(customer)