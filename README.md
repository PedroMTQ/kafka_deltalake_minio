To deploy:

```bash
docker compose up
```
This will deploy Minio (with auto-creation of buckets - see MINIO_BUCKETS in the docker compose) and Kafka

Workflow:
1. Run consumer `python deltalake_minio/src/deltalake_minio/consumer.py`
2. Send messages `python deltalake_minio/src/deltalake_minio/producer.py`
3. Check data `python deltalake_minio/src/deltalake_minio/read.py`

