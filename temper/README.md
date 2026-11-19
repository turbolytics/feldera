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
docker run -d --name temper-feldera-pg --network temper-feldera --memory 512m -p 26432:5432 \
  -e POSTGRES_PASSWORD=temper postgres:17
uv pip install -p .venv-temper/bin/python feldera==0.383.0 duckdb pytest requests confluent-kafka \
  'psycopg[binary]' temper-python
temper clean && temper run --level oss -- .venv-temper/bin/pytest -q -p no:cacheprovider testing/temper
temper matrix --all-levels
```

Compiling each program takes about five minutes the first time. The session
compiles all seven programs at once, and the stopped container keeps them, so
later runs skip compiling.

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
- `test_gap.py` covers the rest of the docs' claims: ad-hoc snapshot
  consistency, primary keys, recursion, `NOW()`, materialized views, malformed
  input, every SQL type over HTTP and Kafka, and the PostgreSQL sink.

## Speed

Work on different pipelines runs at the same time (`lanes.py`): the session
starts every lane, and each test waits for its own. Lanes that share a pipeline
run in order. A process pause targets only its own pipeline's process.

The Kafka faults are scaled down with the clocks they cross:

| Clock | Default | Here | Fault that crosses it |
|---|---|---|---|
| librdkafka socket and request timeout | 60 s | 10 s | long broker pause, long network cut (15 s) |
| short faults | 20 s | 6 s | broker pause, network cut, process pause |
| pause and resume | 10 s | 4 s | pause through the API |

Crash recovery needs fault tolerance, which is Enterprise-only, so the map
marks those faults unsupported.


## Claims audit

Every behavioral claim in the docs at a5dbb88 (`docs.feldera.com/docs`), and
where the map checks it. "Out of scope" names the reason.

| Page | Claim | Map |
|---|---|---|
| what-is-feldera.md | Views equal the batch query over the same input | `view.matches_batch` (45 views, plus the recursive view) |
| connectors/completion-tokens.md | A completed token means outputs reached every sink | `view.changes_add_up`, `pg.mirrors_view` |
| tutorials/time-series.md | LATENESS does not change output when inputs respect it | `view.lateness_preserves_output`, `late.changes_add_up` |
| pipelines/transactions.md | No output while a transaction is open; one batch per view on commit; views match all inputs after commit | `txn.no_output_while_open`, `txn.commit_is_one_batch`, `txn.matches_batch_after_commit` |
| sql/ad-hoc.md | Queries read a consistent snapshot of tables and views | `adhoc.consistent_snapshot` |
| sql/ad-hoc.md | Only materialized relations answer ad-hoc SELECTs | `adhoc.only_materialized` |
| sql/ad-hoc.md | A multi-statement request reads one snapshot taken at its start | not tested in this pass |
| connectors/unique_keys.md | Insert overwrites; delete of a missing key is ignored; update changes named columns or is ignored | `pk.insert_overwrites`, `pk.delete_missing_ignored`, `pk.delete_by_key`, `pk.update_changes_columns`, `pk.update_missing_ignored` |
| connectors/unique_keys.md | Deleting a missing record from a table without a key is undefined | out of scope: the docs call it undefined |
| sql/recursion.mdx | Recursive views reach the least fixed point, as DISTINCT | `view.matches_batch` and `view.changes_add_up` on `view.recursive` |
| sql/datetime.md | NOW() is the step time, in UTC, always increasing; a step runs every second without input; views over NOW() update | `now.increasing_utc`, `now.temporal_filter_advances` |
| formats/json.md | Out-of-range integers and invalid dates are rejected; longer strings are kept; NaN is not supported | `input.rejects_out_of_range`, `input.rejects_invalid_date`, `input.keeps_long_strings`, `input.rejects_nan`, `input.rejects_broken_json` |
| formats/json.md, sql/types.md | Each type's JSON encoding, CHAR(n) padding, timestamp sub-second precision, decimals as strings | `types.contract` and `nulls.contract` on HTTP and Kafka |
| connectors/sinks/kafka.md | Without fault tolerance, broker failures are retried without a delivery deadline | `delivery.no_loss`, `delivery.recovers` |
| connectors/sinks/kafka.md | Same-key messages go to one partition in order | out of scope: exactly-once fault tolerance only (Enterprise) |
| connectors/sources/kafka.md | `synchronize_partitions` reads in timestamp order across partitions | not tested in this pass; off by default, and the docs say the default can reorder |
| pipelines/lifecycle.md | A graceful stop checkpoints, and the start resumes | `faults.contract/stop_and_resume`, `delivery.no_duplicates` |
| pipelines/fault-tolerance.md | Crash recovery without loss or duplicates | out of scope: Enterprise-only (`faults.contract/process_kill` is unsupported, proven by `test_oss_limits.py`) |
| connectors/sinks/postgresql.md | Materialized mode keeps the table a snapshot of the view | `pg.mirrors_view` |
| sql/system.md | `ERROR_VIEW` holds late records | not tested in this pass |
| what-is-feldera.md | Datasets larger than RAM spill to disk | no runtime claim about disk faults, so none declared |
| connectors (Delta, Iceberg, S3, file, NATS, Redis, DynamoDB, Snowflake, Pub/Sub, S2, HTTP GET, Debezium, PostgreSQL CDC and input, Confluent JDBC, datagen) | Each connector's delivery | out of scope this pass: each needs its own service and reader; PostgreSQL output is the one covered |
