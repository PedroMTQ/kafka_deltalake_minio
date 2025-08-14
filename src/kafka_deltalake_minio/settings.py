
DELTA_TABLE = 's3a://delta-lake/delta-table'
STORAGE_OPTIONS = {
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ALLOW_HTTP": "true",
    }
KAFKA_BROKER ='localhost:9092'
KAFKA_TOPIC = 'my-kafka-topic'
# TIMEOUT TO RESTART READING MESSAGES
CONSUMER_TIMEOUT_MS = 1000
CONSUMER_BATCH_SIZE = 500