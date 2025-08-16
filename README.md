# Description

This is a simple project to test Deltalake with Kafka, MinIO and MongoDb.


## Infrastructure Deployment

```bash
docker compose -f docker-compose-infra.yaml up
```
This will deploy Minio (with auto-creation of buckets - see MINIO_BUCKETS in the docker compose) and Kafka

## Workflow

The idea is simple:
1. A consumer is consuming from a Kafka topic (`KAFKA_TOPIC`)

```python ./src/kafka_deltalake_minio/jobs/consumers/delta_consumer.py```

2. N Customer records are pushed with to a Kafka topic (`KAFKA_TOPIC`) with `PARTITION_KEY` partitions

 ```python ./src/kafka_deltalake_minio/jobs/producers/push_population.py```

3. When the consumer above consumes new Kafka messages, it reads (or creates if non-existent) a DeltaTable (`DELTA_TABLE`) in MinIO (to `DELTA_BUCKET` bucket) and merges new or updated consumer data. This merge is done using a predicate with both `UPSERT_KEY` and `PARTITION_KEY`

4. A syncing process is reading from this DeltaTable and upserting the data into mongo using the upsert key `UPSERT_KEY`. This sync runs every `MONGO_SYNC_SLEEP_TIME` seconds

```python /home/pedroq/workspace/./src/kafka_deltalake_minio/jobs/database/sync_mongo.py```


### Checking data


-  When you want to read your table you can run:

```python ./src/kafka_deltalake_minio/jobs/delta/read.py```

- To check data changes:

```python ./src/kafka_deltalake_minio/jobs/delta/read_changes.py```


### Rolling back data

You can rollback to a specific version or timestamp with:

```python ./src/kafka_deltalake_minio/jobs/database/rollback.py```

You can rollback to a specific version or timestamp (as of) - since we are adding data in batches timestamps are more relevant since every batch will increment the _commit_version

## Workflow deployment

```bash
# build image first (we use one template for all services)
docker compose build
# deploy it then
docker compose up
```
This will spawm multiple consumer replicas, and one mongo syncer
