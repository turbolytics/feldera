# Temper map for Feldera

This map checks Feldera's documented guarantees in the open-source edition,
0.383.0. The tests are black-box pytest in `testing/temper`.

## Setup

```sh
docker network create temper-feldera
docker network create temper-feldera-ext
docker run -d --name temper-feldera --network temper-feldera --memory 3g --cpus 4 \
  -p 26080:8080 images.feldera.com/feldera/pipeline-manager:0.383.0
docker run -d --name temper-feldera-kafka --network temper-feldera-ext --memory 1g -p 26092:29092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:26092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 apache/kafka:3.9.1
docker network connect --alias kafka temper-feldera temper-feldera-kafka
uv venv -p 3.12 .venv-temper
uv pip install -p .venv-temper/bin/python feldera==0.383.0 duckdb pytest requests confluent-kafka temper-python
temper clean && temper run --level oss -- .venv-temper/bin/pytest -q -p no:cacheprovider testing/temper
temper matrix --all-levels
```

Compiling each program takes about five minutes the first time.

## What the tests do

- `test_incremental.py`, `test_incremental_adv.py` and `test_lateness.py` send
  random inserts, deletes and updates. After each completion token, they
  compare every materialized view with DuckDB running the same query from
  scratch. Each view is read two ways: an ad-hoc SELECT, and the sum of its
  HTTP change stream.
- `test_transactions.py` checks that nothing leaves an open transaction, and
  that each commit reaches a view as one batch.
- `test_kafka.py` copies a Kafka topic through pauses, broker outages and
  network cuts, and checks that nothing is lost.

Crash recovery needs fault tolerance, which is Enterprise-only, so the map
marks those faults unsupported.
