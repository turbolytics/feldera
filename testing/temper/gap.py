"""Programs for the gap pass: claims from Feldera's docs not covered before.

Three programs, compiled in parallel up front (each takes minutes):

- GAP: ad-hoc snapshot consistency across views, primary-key upserts,
  recursive views, NOW() and temporal filters, non-materialized views, and
  malformed input.
- TYPES: one table and one view per transport (HTTP and Kafka) holding every
  Feldera SQL type, for round trips.
- PG: a view with a unique key written to PostgreSQL in materialized mode.
"""

import json
import time

import harness

KAFKA_INSIDE = "kafka:9092"
PG_URI = "postgresql://postgres:temper@temper-feldera-pg:5432/postgres"

GAP = "temper_gap"
TYPES = "temper_types"
PG = "temper_pg"

GAP_SQL = """
-- Ad-hoc snapshot consistency: three relations over one table.
CREATE TABLE ledger (k INT NOT NULL, v BIGINT NOT NULL) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW c_total AS SELECT COUNT(*) AS n, SUM(v) AS s FROM ledger;
CREATE MATERIALIZED VIEW c_by_k AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM ledger GROUP BY k;
CREATE VIEW c_hidden AS SELECT k FROM ledger;

-- Primary keys.
CREATE TABLE acct (id INT NOT NULL PRIMARY KEY, owner VARCHAR, bal BIGINT NOT NULL);

-- Recursion: transitive closure, as in the docs.
CREATE TABLE edges (x INT, y INT) WITH ('materialized' = 'true');
DECLARE RECURSIVE VIEW closure(x INT, y INT);
CREATE LOCAL VIEW step AS SELECT e.x, closure.y FROM edges e JOIN closure ON e.y = closure.x;
CREATE MATERIALIZED VIEW closure AS (SELECT * FROM edges) UNION (SELECT * FROM step);

-- NOW() and a temporal filter.
CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW recent AS SELECT id FROM events WHERE ts >= NOW() - INTERVAL 5 SECONDS;
CREATE TABLE clock_seed (id INT NOT NULL) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW clock AS SELECT id, NOW() AS n FROM clock_seed;

-- Input checks.
CREATE TABLE strict (id INT NOT NULL, small TINYINT, d DATE, s VARCHAR(3), f DOUBLE) WITH ('materialized' = 'true');
"""

# name -> SQL type. The order is the column order.
TYPE_COLUMNS = {
    "c_bool": "BOOLEAN", "c_i8": "TINYINT", "c_i16": "SMALLINT", "c_i32": "INT", "c_i64": "BIGINT",
    "c_u8": "TINYINT UNSIGNED", "c_u16": "SMALLINT UNSIGNED", "c_u32": "INT UNSIGNED",
    "c_u64": "BIGINT UNSIGNED", "c_real": "REAL", "c_double": "DOUBLE", "c_dec": "DECIMAL(38, 10)",
    "c_varchar": "VARCHAR", "c_char": "CHAR(4)", "c_binary": "BINARY(4)", "c_varbinary": "VARBINARY",
    "c_date": "DATE", "c_time": "TIME", "c_ts": "TIMESTAMP", "c_tstz": "TIMESTAMP WITH TIME ZONE",
    "c_uuid": "UUID", "c_variant": "VARIANT", "c_arr_int": "INT ARRAY", "c_arr_str": "VARCHAR ARRAY",
    "c_arr_ts": "TIMESTAMP ARRAY", "c_arr_arr": "BIGINT ARRAY ARRAY", "c_row": "ROW(a INT NULL, b VARCHAR NULL)",
    "c_arr_row": "ROW(a INT NULL) ARRAY", "c_map": "MAP<VARCHAR, INT>",
}


def types_sql(columns=TYPE_COLUMNS):
    cols = ",\n  ".join(f"{c} {t}" for c, t in columns.items())

    def conn(name, topic, extra, fmt):
        return json.dumps([{"transport": {"name": name, "config": {
            "bootstrap.servers": KAFKA_INSIDE, "topic": topic, **extra}},
            "format": {"name": "json", "config": {"update_format": fmt, "array": False}}}])
    kin = conn("kafka_input", "ty-in", {"start_from": "earliest"}, "raw")
    kout = conn("kafka_output", "ty-out", {}, "insert_delete")
    return f"""
CREATE TABLE ty (id INT NOT NULL PRIMARY KEY,
  {cols});
CREATE MATERIALIZED VIEW ty_out AS SELECT * FROM ty;
CREATE TABLE tyk (id INT NOT NULL PRIMARY KEY,
  {cols}) WITH ('connectors' = '{kin}');
CREATE MATERIALIZED VIEW tyk_out WITH ('connectors' = '{kout}') AS SELECT * FROM tyk;
"""


def pg_sql():
    conn = json.dumps([{"index": "rich_idx", "transport": {"name": "postgres_output", "config": {
        "uri": PG_URI, "table": "rich"}}}])
    return f"""
CREATE TABLE accts (id INT NOT NULL PRIMARY KEY, owner VARCHAR, bal BIGINT NOT NULL);
CREATE MATERIALIZED VIEW rich WITH ('connectors' = '{conn}') AS SELECT id, owner, bal FROM accts WHERE bal > 50;
CREATE INDEX rich_idx ON rich(id);
"""


def compile_all(programs, timeout=1500):
    """PUTs every program at once, then waits for all of them. Returns {name: error or None}."""
    pending = {}
    for name, sql in programs.items():
        p = harness.pipeline(name)
        if p and p.get("program_code") == sql and p.get("program_status") == "Success":
            continue
        if p:
            harness.stop(name, force=True)
            harness.requests.post(f"{harness.API}/pipelines/{name}/clear", timeout=30)
            harness.wait_for(lambda n=name: (harness.pipeline(n) or {}).get("storage_status") in (None, "Cleared"), 120)
            harness.requests.delete(f"{harness.API}/pipelines/{name}", timeout=30)
            harness.wait_for(lambda n=name: harness.pipeline(n) is None, 60)
        body = {"name": name, "program_code": sql, "runtime_config": {"workers": 2}}
        r = harness.requests.put(f"{harness.API}/pipelines/{name}", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"create {name}: {r.status_code} {r.text}")
        pending[name] = sql
    errors = {n: None for n in programs}
    deadline = time.time() + timeout
    while pending and time.time() < deadline:
        for name in list(pending):
            p = harness.pipeline(name) or {}
            status = p.get("program_status")
            if status == "Success":
                del pending[name]
            elif status and ("Error" in status or status in ("SqlError", "RustError", "SystemError")):
                errors[name] = f"{status}: {json.dumps(p.get('program_error'))[:2000]}"
                del pending[name]
        time.sleep(3)
    for name in pending:
        errors[name] = "compile timed out"
    return errors
