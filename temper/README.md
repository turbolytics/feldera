# Temper map for Feldera

This map checks Feldera's documented guarantees in the open-source edition,
0.383.0. Every check runs as a Temper scenario (`temper/scenarios`) against a
Docker Compose topology (`temper/compose.yml`): Feldera's pipeline manager,
Kafka, PostgreSQL and a one-shot oracle that drives Feldera's REST API.

The views, lateness, transaction and type-semantics scenarios run the original
black-box tests (`testing/temper`) unchanged: `temper/recipe/oracle/legacy/` is
a copy of them, and `oracle/legacy.py` calls each test and credits the claims
its pytest marks name. `temper/REBUILD.md` records how the map moved onto
scenarios and the cell-for-cell parity check against the original.

## Reproduce the findings

Each finding is a scenario in `temper/scenarios/repro.yml`. One command
reproduces it, with Docker and Temper 0.1.7 or later
(`brew install turbolytics/tap/temper`, or
`go install github.com/turbolytics/temper/cmd/temper@v0.1.7`):

```sh
temper scenarios run --scenario feldera.decimal_sum_overflow --build   # #8032
temper scenarios run --scenario feldera.grouping_empty --build         # #8033
```

Each starts the pipeline manager, compiles a fresh pipeline, prints its
verdict, and exits 1 when the bug reproduces. A run takes a few minutes, most
of it the compile; `--build` builds the oracle image the first time. The checks
are in `temper/recipe/oracle/feldera_check.py`, standard-library Python: the
grouping check computes SQL's answer for ROLLUP, CUBE and GROUPING SETS itself.
The full log is in `.temper/scenarios/<scenario>/<subject>/run.log`.

## Run the whole map

```sh
temper clean
temper run --level oss -- temper scenarios run --build
temper matrix --all-levels
```

The first run compiles every program, about an hour on a laptop. The compiler
cache lives on the external volume `temper-feldera-compiler`, which each
scenario creates and prunes (`compiler_prune` in `temper/commands.yml`), so
the Docker VM needs about 15 GB free. The original tests' lane results are kept
for six hours on the volume `temper-feldera-legacy`, so later subjects reuse a
lane computed by an earlier one; each run's log says when it does.

## What the tests do

- `test_incremental.py`, `test_incremental_adv.py` and `test_lateness.py` send
  random inserts, deletes and updates. After each completion token, they
  compare every materialized view with DuckDB running the same query from
  scratch. Each view is read two ways: an ad-hoc SELECT, and the sum of its
  HTTP change stream.
- `test_churn.py` does the same with `views_churn.py`, where deletes and
  updates are as common as inserts. A primary-key table gets upserts,
  deletes and updates of missing keys, and several writes to one key in a
  single request. A table without a key gets many identical rows, and deletes
  of each group's current MIN or MAX. A recursive view gets edges removed as
  often as added. Tables drain to empty, or are replaced in one request.
- `test_workers.py` runs the three random-input programs again at 1 worker
  and at 8, on the same pipelines after the default runs, with the same oracle. The default runs use
  4 workers. Worker count is a runtime setting, and the batch promise names no
  exception for it.
- `test_types_ops.py` puts every SQL type through the operators that keep
  state: GROUP BY, DISTINCT, an equi-join, ARG_MIN and ARG_MAX, a window
  ORDER BY, and SUM. The oracle is a model of each type's equality and order
  from the docs, not DuckDB: one zero and one NaN, NaN above Infinity, NULL
  elements smallest in arrays and maps, a zoned timestamp as an instant.
  Deletes often name a row by another spelling of its value, such as "1.50"
  for 1.5 or a UUID in upper case. Three seeds run at 1, 4 and 8 workers. Pairs
  the compiler refuses, such as SUM over a DATE, are proven refused one by one.
  A separate pipeline sends +X, +X, -X in one request to check that SUM's
  wider intermediate type absorbs the overflow.
- `test_transactions.py` checks that nothing leaves an open transaction, and
  that each commit reaches a view as one batch.
- `test_kafka.py` copies a Kafka topic through pauses, broker outages and
  network cuts, and checks that nothing is lost.
- `test_gap.py` covers the rest of the docs' claims: ad-hoc snapshot
  consistency, primary keys, recursion, `NOW()`, materialized views, malformed
  input, every SQL type over HTTP and Kafka, and the PostgreSQL sink.

`repro_rollup_empty.sh` reproduces the one consistency failure on its own,
with curl against any running Feldera: a ROLLUP view has no grand-total row
when its table is empty, where Postgres and DuckDB return (NULL, NULL, 0, NULL).
Run `FELDERA_URL=http://localhost:8080 testing/temper/repro_rollup_empty.sh`.

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
marks those faults unsupported. So is a graceful stop: the lifecycle page
says `stop?force=false` checkpoints, but the community edition answers
`501 EnterpriseFeature` and keeps running. (An earlier version of this map
ignored that response and counted a stop that never happened as a resume.
`harness.stop` now fails on any refused or unfinished stop.)


## Claims audit

Every behavioral claim in the docs at a5dbb88 (`docs.feldera.com/docs`), and
where the map checks it. "Out of scope" names the reason.

| Page | Claim | Map |
|---|---|---|
| what-is-feldera.md | Views equal the batch query over the same input | `view.matches_batch` (45 views, the recursive view, and 19 churn views) |
| pipelines/configuration.mdx | `workers` is a runtime setting | `workers.contract` (every view at 1 and 8 workers) |
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
| sql/comparisons.md, sql/float.md, sql/array.md, sql/map.md | Equality and order for every type, including NaN, -0, NULL elements and nested values | `types.operators` on six operators |
| sql/aggregates.md | SUM computes with a wider intermediate type | `operator.sum_partial_overflow` |
| formats/json.md, sql/types.md | Each type's JSON encoding, CHAR(n) padding, timestamp sub-second precision, decimals as strings | `types.contract` and `nulls.contract` on HTTP and Kafka |
| connectors/sinks/kafka.md | Without fault tolerance, broker failures are retried without a delivery deadline | `delivery.no_loss`, `delivery.recovers` |
| connectors/sinks/kafka.md | Same-key messages go to one partition in order | out of scope: exactly-once fault tolerance only (Enterprise) |
| connectors/sources/kafka.md | `synchronize_partitions` reads in timestamp order across partitions | not tested in this pass; off by default, and the docs say the default can reorder |
| pipelines/lifecycle.md | A graceful stop checkpoints, and the start resumes | out of scope: the community edition refuses `stop?force=false` with 501 (`faults.contract/stop_and_resume` unsupported and `delivery.no_duplicates` exempt, proven by `test_oss_limits.py`) |
| pipelines/fault-tolerance.md | Crash recovery without loss or duplicates | out of scope: Enterprise-only (`faults.contract/process_kill` is unsupported, proven by `test_oss_limits.py`) |
| connectors/sinks/postgresql.md | Materialized mode keeps the table a snapshot of the view | `pg.mirrors_view` |
| sql/system.md | `ERROR_VIEW` holds late records | not tested in this pass |
| what-is-feldera.md | Datasets larger than RAM spill to disk | no runtime claim about disk faults, so none declared |
| connectors (Delta, Iceberg, S3, file, NATS, Redis, DynamoDB, Snowflake, Pub/Sub, S2, HTTP GET, Debezium, PostgreSQL CDC and input, Confluent JDBC, datagen) | Each connector's delivery | out of scope this pass: each needs its own service and reader; PostgreSQL output is the one covered |
