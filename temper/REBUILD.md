# Rebuilding Feldera's map as fault scenarios

**Goal:** every claim in this map (22 features, 26 invariants, 34 subjects,
5 lattices) decided by `temper scenarios run`, with a freshly written oracle,
matching the original matrix (@68df7b1, made by the pytest harness in
`testing/temper`) cell for cell, including #8032 and #8033 failing where
they did. Written from Feldera's docs and the declarations, not ported from
`testing/temper`.

**Needs:** Temper with recipe packs (v0.1.6) and exemption proofs from
scenarios (temper `docs/superpowers/specs/2026-11-21-scenario-exemptions-design.md`).

## Topology (`temper/compose.yml`)

| Service | Image | Notes |
|---|---|---|
| `feldera` | `images.feldera.com/feldera/pipeline-manager:0.383.0` | `/home/ubuntu/.feldera/compiler` on the external volume `temper-feldera-compiler`, so a program compiles once across runs and subjects |
| `kafka` | `apache/kafka:3.9.1` | KRaft, one node, `kafka:9092` |
| `postgres` | `postgres:17` | the Postgres output connector's target |
| `oracle` | built from `temper/recipe/oracle/Dockerfile` | Python 3.12, `duckdb`, `confluent-kafka`, `psycopg[binary]` |

`temper.yml` includes packs: `postgres` (db `sink`), `service` as `kafka`
(target `kafka`), `service` as `feldera` (target `feldera`). A setup step
creates the external volume: `host: [docker, volume, create, temper-feldera-compiler]`.

## The oracle (`temper/recipe/oracle/`)

One program, `python -m oracle <area> <subject> [--seed N] [--workers W]`,
printing a results object keyed by reference, explanations on stderr.

- `api.py`: the REST calls (create, compile, start, ingress with
  completion tokens, ad-hoc query, egress change stream, stop, runtime
  config).
- `workload.py`: seeded random inserts, deletes and updates per table,
  including churn (drain to empty, replace in one request).
- `reference.py`: DuckDB running each view's query from scratch over the
  current tables; the grouping-sets evaluator from `feldera_check.py`.
- `stream.py`: sums a view's HTTP change stream into a bag, to check
  `view.changes_add_up`.
- `types.py`: a model of each SQL type's equality and order from the docs
  (one zero, NaN above Infinity, NULLs smallest in arrays and maps, zoned
  timestamps as instants) and its spellings ("1.50" for 1.5, upper-case
  UUIDs).
- One module per area below.

## Scenarios (`temper/scenarios/`)

| File | Scenario | Subjects | Claims |
|---|---|---|---|
| `views.yml` | `feldera.views` | the 14 `view.*` | `view.matches_batch`, `view.changes_add_up`, `workers.contract/w1`, `workers.contract/w8`, each subject's `sql.*` feature |
| `late.yml` | `feldera.lateness` | the 7 `late.*` | `view.lateness_preserves_output`, `late.changes_add_up`, `sql.lateness` |
| `txn.yml` | `feldera.transactions` | `txn.http` | `txn.no_output_while_open`, `txn.commit_is_one_batch`, `txn.matches_batch_after_commit`, `transactions` |
| `pk.yml` | `feldera.primary_key` | `table.primary_key` | the five `pk.*`, `sql.primary_key` |
| `adhoc.yml` | `feldera.adhoc` | `adhoc.datafusion` | `adhoc.consistent_snapshot`, `adhoc.only_materialized`, `sql.adhoc` |
| `now.yml` | `feldera.now` | `clock.now` | `now.increasing_utc`, `now.temporal_filter_advances`, `sql.now` |
| `input.yml` | `feldera.input` | `input.http_json` | the five `input.*`, `input.errors` |
| `types.yml` | `feldera.types` | `transport.http`, `transport.kafka` | `types.contract/*`, `nulls.contract/*`, `sql.types`, `connector.http` |
| `types.yml` | `feldera.type_semantics` | the six `op.*` | `types.operators/*` (covers or exempts by declaration), `operator.sum_partial_overflow` on `op.sum`, `sql.type_semantics` |
| `kafka.yml` | `feldera.kafka` | `path.kafka_kafka` | `delivery.no_loss`, `delivery.recovers`, `faults.contract/*` (the Enterprise-only members as exemption proofs), `connector.kafka` |
| `pg.yml` | `feldera.postgres` | `sink.postgres` | `pg.mirrors_view`, `connector.postgres` |
| `repro.yml` | the two repros | `op.sum`, `view.grouping` | as now |

Faults in `feldera.kafka` come from the packs: `kafka.pause`,
`kafka.disconnect`, `feldera.pause`, plus long variants declared locally
(`long_broker_pause`, `long_network_cut`) and `pause_api` (pausing the
pipeline through the API). Each `faults.contract` member is one `fault:`
step followed by checks, so its verdict lands on its own member.

Exemption proofs: each subject's `proven_by` changes from the pytest id to
the scenario's test id (`<scenario>/<subject>/<ref>`), and the oracle's
`refuses` checks confirm the refusal (the compiler rejects the program; the
community edition answers 501 to a graceful stop).

## Order of work

1. Temper: exemption proofs from scenarios (design above), released with
   recipe packs as v0.1.6.
2. Topology, compiler cache volume, the oracle skeleton (`api.py`), and
   `feldera.input` end to end, the smallest area, to prove the pipeline.
3. `views` and `late` (the bulk: workload, reference, stream).
4. `txn`, `pk`, `adhoc`, `now`.
5. `types` and `type_semantics`, with the exemption proofs.
6. `kafka` under faults, with its exemptions; `pg`.
7. Move the repros onto the packs; retire `testing/temper` from the matrix
   path (it stays in the history as the first implementation).
8. Acceptance: a script compares `temper/status/` from this rebuild with
   @68df7b1's, cell for cell; every difference fixed or explained in
   writing. Each repro fails 10 of 10.

## Ledger

Progress, with commits and every ruling, is kept below as work lands.
