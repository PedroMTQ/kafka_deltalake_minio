import os

SERVICE_NAME = 'kafka_deltalake_minio'
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
DATA = os.path.join(ROOT, 'data')


DEBUG = int(os.getenv('DEBUG', '0'))
LOGGER_PATH = os.getenv('LOGGER_PATH')

# we could create and use access keys, but for now we keep it like this
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')

MINIO_HOST = os.getenv('MINIO_HOST')
MINIO_S3_PORT = os.getenv('MINIO_S3_PORT')

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

DELTA_BUCKET = os.getenv('DELTA_BUCKET')
DELTA_TABLE = os.getenv('DELTA_TABLE')
DELTA_TABLE_URI = f's3a://{DELTA_BUCKET}/{DELTA_TABLE}'
STORAGE_OPTIONS = {
        "AWS_ACCESS_KEY_ID": MINIO_ROOT_USER,
        "AWS_SECRET_ACCESS_KEY": MINIO_ROOT_PASSWORD,
        "AWS_ENDPOINT_URL": f"http://{MINIO_HOST}:{MINIO_S3_PORT}",
        "AWS_ALLOW_HTTP": "true",
    }
# TIMEOUT TO RESTART READING MESSAGES
CONSUMER_TIMEOUT_MS = 1000
KAFKA_PUSH_BATCH_SIZE = 10_000
MONGO_BATCH_SIZE = 50_000
CONSUMER_BATCH_SIZE = 50_000
CONSUMER_BATCH_SIZE = int(os.getenv('CONSUMER_BATCH_SIZE'))


CUSTOMERS_COLLECTION_NAME = 'customers'
CUSTOMERS_COLLECTION_INDEXES = ['customer_id', 'country']
PARTITION_KEY = os.getenv('PARTITION_KEY')
UPSERT_KEY = os.getenv('UPSERT_KEY')

MONGO_SYNC_SLEEP_TIME = int(os.getenv('MONGO_SYNC_SLEEP_TIME'))
