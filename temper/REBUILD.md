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

- 2026-11-21: step 2 done. Topology (feldera, kafka, postgres, oracle, external compiler volume), oracle api.py, feldera.input: 6 of 6 pass, matching @68df7b1. Ruling: decides lists name each claim; only <lattice>/* is a pattern.
- 2026-11-21: step 3 written (views: 12 scenarios; lateness: 7) and step 4 written (txn, pk, adhoc, now). view.grouping reproduces #8033 independently: all 5 cells fail, only on empty-table steps. Ruling: transaction completion is detected by both reads matching all inputs (the commit API is asynchronous), and each unfamiliar API response is logged to stderr.
- 2026-11-21: steps 5-6 written: types (2 transports), type_semantics (6 operators, 21 exemption proofs incl. 2 cast proofs), kafka (6 faults + 3 exemption proofs), postgres sink. All 31 scenarios pass temper check; no declaration names testing/temper. Ruling: SUM over VARCHAR and CHAR is unsupported by casting, so its proof shows the sum arrives as a number; temper check caught the two stale proven_by.
- 2026-11-22: type_semantics fixes. (1) Disk: each run's per-pipeline crates filled the VM; `compiler_prune` (commands.yml) now runs after `compiler_cache` in every scenario, the repros included (they had no `compiler_cache` and would fail on a fresh clone). It always deletes pipeline binaries and `feldera_pipe_pipeline_*` crates, and keeps the hash-named `feldera_pipe_operator_*` crates, which later programs reuse, unless the volume passes 11 GB. Ruling: an earlier version also deleted operator crates; that forced a full recompile per subject. (2) Parity: the SUM views summed every value class, so 255+255 overflowed TINYINT UNSIGNED and the panic stopped every subject (every subject's program holds every operator). The original restricted SUM to small classes (views_types.py); `SUM_ONLY` now does the same, and signed integers gain a class `1`. Ruling: SUM overflow is its own claim, not an operator-semantics cell.
- 2026-11-22: kafka passes 13/13 with temper#44 (a reconnect restores network aliases); the kill proof restarts with dismiss_error. Committed at 6b9daf380.
- 2026-11-22: candidate finding, not filed: `WHERE v IN (0, 7)` over an INT UNSIGNED column panics the pipeline when a row holds 4294967295 ("Error converting 4294967295 to INTEGER", sqllib casts.rs:161): Feldera compares the column as a signed INTEGER. The SUM filter now casts its literals to the column's type. Probe a minimal repro (u32, u64) before deciding whether it is worth reporting.
- 2026-11-22: CHAR(4) cells failed in every operator because the oracle inserted 'ab' and deleted 'ab  '. Feldera's docs say "the runtime also expects data sources to provide correctly padded data" (sql/types.md), so unpadded input is outside the contract. Ruling: CHAR classes are padded, one spelling each, as in the original.
- 2026-11-22: **parity audit by reading (oracle vs testing/temper), before more runs.** The rebuild is much weaker in four areas, so its green cells are not parity:
  - views: original 3-4 seeds x 40 steps plus a churn program (3 seeds x 60 steps, deletes and updates as often as inserts, primary keys); rebuild 1 seed x 10 steps per worker count, no churn.
  - lateness: original 2 seeds x 100 steps; rebuild 12 steps x 3.
  - transactions: original 12 transactions over 6 views with torn-batch detection; rebuild 1 transaction over 2 views.
  - type semantics: the original's value classes (inf, NaN, NULLs inside arrays/maps/rows, empty and unicode strings, extreme dates) were replaced by ordinary values.
  Equivalent: types (HTTP, Kafka), kafka faults, pk, input, now, adhoc. Ruling: port, not rewrite: run the original modules in the oracle image behind harness.py pointed at the new topology; the earlier "356 of 356 decided" overstated parity. The @68df7b1 results (Discussion, issues, case study) are unaffected.
- 2026-11-22: **the port.** Views (with advanced views, churn and worker counts), lateness, transactions and operator type semantics now run the original harness unchanged: `temper/recipe/oracle/legacy/` is a byte-for-byte copy of testing/temper (harness.py's API address from FELDERA_API is the one change), and `oracle/legacy.py` calls each original test with its own parameters, counting a failed assertion against the references its pytest marks name; an exception is an error and decides nothing. Lane results are cached for 6 h on the temper-feldera-legacy volume, keyed by a hash of legacy/, so later subjects reuse a lane (the log says so). Decides lists come from the original marks, which restored connector.http, sql.primary_key, operator.sum_partial_overflow and test_gap's recursion test. op.sum moved to its own scenario (feldera.type_semantics_sum) because operator.sum_partial_overflow is op.sum's claim alone. Ruling: DuckDB 1.4.2 (the original pinned nothing). Ruling: the original tests' stdout goes to the log (pytest captured it).
- 2026-11-22: **parity, cell for cell.** All 334 invariant and lattice cells and all 22 features match the original's evidence (@68df7b1's .temper/evidence-pytest) regenerated with today's Temper. The original's committed matrix used an older rule and showed connector.http, runtime.workers and sql.type_semantics covered despite failing records; today's rule (a failing record makes the cell failing) gives 9 failing, as the rebuild does. Ruling: compare against the regenerated original.
- 2026-11-22: disk. compiler_prune also deletes cargo's top-level copies of the operator libraries (target/optimized/libfeldera_pipe_*, 5.9 GB after one parity run). Never run another Docker stack beside a Feldera run on this VM.
- 2026-11-22: **the repros broke at step 2 and nobody saw it.** feldera.decimal_sum_overflow and feldera.grouping_empty call /recipe/feldera_check.py, which the old compose mounted; the rebuilt oracle image did not contain it, so both errored. Their cells were failing anyway (other scenarios decide them), so the matrix hid it. Fixed: feldera_check.py moved into recipe/oracle and copied to /recipe/ in the image. Lesson: a repro must be run on its own after every topology change; a red cell is not proof the repro works. The issues cite repros at earlier commits (0320e94c2 onward), where they work.
- 2026-11-22: official recorded run (temper clean; temper run --level oss -- temper scenarios run): 325 covered, 22 exempt, 9 failing, the regenerated original's numbers.
