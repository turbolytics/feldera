"""Claims from Feldera's docs that the first pass did not cover.

Every claim quotes a docs page at a5dbb88 (temper/README.md lists them).

- Ad-hoc snapshot consistency (sql/ad-hoc.md): queries read "a consistent
  snapshot of the pipeline's tables and views". A writer streams inserts and
  deletes while a reader compares a table with two views over it, in one query.
- Primary keys (connectors/unique_keys.md): insert overwrites, deleting a
  missing key is ignored, update changes some columns or is ignored.
- Recursion (sql/recursion.mdx): a transitive closure equals DuckDB's
  recursive query after every step, and its change stream adds up.
- NOW() (sql/datetime.md): constant within a step, increasing, UTC, and
  views that depend on it update as time advances (a step every second).
- Materialized (sql/ad-hoc.md): only materialized relations answer ad-hoc
  SELECTs.
- Input checks (formats/json.md): out-of-range integers and invalid dates
  are rejected on ingress; longer strings are kept untruncated; NaN is not
  supported.
- Types: every SQL type round-trips over HTTP and over Kafka (JSON).
- PostgreSQL (connectors/sinks/postgresql.md): the target table is "a
  materialized snapshot of the view".
"""

import datetime as dt
import json
import random
import struct
import threading
import time
import uuid
from decimal import Decimal

import duckdb
import psycopg
import pytest
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import gap
import harness

KAFKA = "localhost:26092"
PG_HOST = "localhost:26432"


def adhoc(name, sql):
    return harness.query(name, sql)


def post_raw(name, table, body, update_format="insert_delete"):
    return requests.post(f"{harness.API}/pipelines/{name}/ingress/{table}",
                         params={"format": "json", "update_format": update_format, "array": "true"},
                         data=body, timeout=30)


def utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


# --- GAP lane ----------------------------------------------------------------


def run_adhoc_snapshot(name):
    """A writer streams changes into `ledger` while a reader checks, in one ad-hoc
    query, that the table and two views over it agree."""
    rng = random.Random(5)
    stop = threading.Event()
    rows = []
    sent = [0]

    def writer():
        while not stop.is_set():
            batch = []
            for _ in range(rng.randint(1, 50)):
                if rows and rng.random() < 0.35:
                    batch.append({"delete": rows.pop(rng.randrange(len(rows)))})
                else:
                    r = {"k": rng.randint(1, 8), "v": rng.randint(-100, 100)}
                    rows.append(r)
                    batch.append({"insert": r})
            harness.ingest(name, "ledger", batch)
            sent[0] += len(batch)

    q = ("SELECT (SELECT n FROM c_total) AS a, (SELECT SUM(n) FROM c_by_k) AS b, (SELECT COUNT(*) FROM ledger) AS c, "
         "(SELECT s FROM c_total) AS sa, (SELECT SUM(s) FROM c_by_k) AS sb, (SELECT SUM(v) FROM ledger) AS sc")
    t = threading.Thread(target=writer, daemon=True)
    t.start()
    queries, bad, paused = 0, [], False
    end = time.time() + 10
    while time.time() < end:
        if not paused and time.time() > end - 5:  # pause halfway: ad-hoc queries work while paused
            requests.post(f"{harness.API}/pipelines/{name}/pause", timeout=30)
            paused = True
        r = adhoc(name, q)[0]
        z = {k: (v or 0) for k, v in r.items()}
        queries += 1
        if not (z["a"] == z["b"] == z["c"] and z["sa"] == z["sb"] == z["sc"]):
            bad.append(r)
    requests.post(f"{harness.API}/pipelines/{name}/resume", timeout=30)
    stop.set()
    t.join(30)
    return {"queries": queries, "inconsistent": bad[:5], "n_inconsistent": len(bad), "changes": sent[0]}


def run_primary_key(name):
    ing = lambda ups: harness.wait_complete(name, harness.ingest(name, "acct", ups))
    ing([{"insert": {"id": 1, "owner": "a", "bal": 10}}, {"insert": {"id": 3, "owner": "c", "bal": 30}}])
    ing([{"insert": {"id": 1, "owner": "b", "bal": 20}}])
    after_overwrite = sorted(map(json.dumps, adhoc(name, "SELECT * FROM acct WHERE id = 1")))
    ing([{"delete": {"id": 2, "owner": "x", "bal": 0}}])  # missing key
    after_missing_delete = adhoc(name, "SELECT COUNT(*) AS n FROM acct")[0]["n"]
    ing([{"update": {"id": 3, "bal": 35}}])  # partial update
    after_update = adhoc(name, "SELECT owner, bal FROM acct WHERE id = 3")
    ing([{"update": {"id": 4, "bal": 1}}])  # missing key
    after_missing_update = adhoc(name, "SELECT COUNT(*) AS n FROM acct WHERE id = 4")[0]["n"]
    ing([{"delete": {"id": 1, "owner": "zzz", "bal": 999}}])  # key matches, other columns do not
    after_key_delete = adhoc(name, "SELECT COUNT(*) AS n FROM acct WHERE id = 1")[0]["n"]
    return {"overwrite": after_overwrite, "missing_delete_rows": after_missing_delete, "update": after_update,
            "missing_update_rows": after_missing_update, "key_delete_rows": after_key_delete}


def run_recursion(name):
    rng = random.Random(9)
    edges = set()
    stream = harness.ChangeStream(name, "closure")
    stream.start()
    stream.ready.wait(30)
    db = duckdb.connect()
    content = changes = None
    try:
        for i in range(30):
            ups = []
            for _ in range(rng.choice([1, 2, 4])):
                if edges and rng.random() < 0.35:
                    e = rng.choice(sorted(edges))
                    edges.discard(e)
                    ups.append({"delete": {"x": e[0], "y": e[1]}})
                else:
                    e = (rng.randint(0, 9), rng.randint(0, 9))
                    if e in edges:
                        continue
                    edges.add(e)
                    ups.append({"insert": {"x": e[0], "y": e[1]}})
            if ups:
                harness.wait_complete(name, harness.ingest(name, "edges", ups))
            db.execute("CREATE OR REPLACE TABLE edges (x INT, y INT)")
            if edges:
                db.executemany("INSERT INTO edges VALUES (?, ?)", sorted(edges))
            want = harness.bag(dict(zip(("x", "y"), r)) for r in db.execute(
                "WITH RECURSIVE c(x, y) AS (SELECT x, y FROM edges UNION SELECT e.x, c.y FROM edges e JOIN c ON e.y = c.x) "
                "SELECT x, y FROM c").fetchall())
            if content is None:
                d = harness.diff(want, harness.bag(adhoc(name, "SELECT x, y FROM closure")))
                if d:
                    content = {"step": i, **d}
            if changes is None:
                d = harness.diff(want, stream.snapshot()) if not stream.error else {"error": repr(stream.error)}
                if d:
                    changes = {"step": i, **d}
    finally:
        stream.close()
    return {"content": content, "changes": changes}


def run_now(name):
    out = {}
    harness.wait_complete(name, harness.ingest(name, "clock_seed", [{"insert": {"id": 1}}]))
    samples = []
    for _ in range(8):
        r = adhoc(name, "SELECT n FROM clock")
        if r:
            samples.append((utc_now(), dt.datetime.fromisoformat(str(r[0]["n"]).replace("T", " ").rstrip("Z"))))
        time.sleep(0.5)
    values = [n for _, n in samples]
    out["clock_samples"] = len(samples)
    out["clock_nondecreasing"] = all(b >= a for a, b in zip(values, values[1:]))
    out["clock_advanced"] = len(set(values)) > 1
    out["clock_max_skew_s"] = max((abs((h - n).total_seconds()) for h, n in samples), default=None)
    # Temporal filter: a fresh event shows up, then leaves 5 s later.
    ts = utc_now().replace(microsecond=0)
    harness.wait_complete(name, harness.ingest(name, "events", [
        {"insert": {"id": 1, "ts": ts.strftime("%Y-%m-%d %H:%M:%S")}},
        {"insert": {"id": 2, "ts": (ts - dt.timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S")}}]))
    out["fresh_visible"] = {r["id"] for r in adhoc(name, "SELECT id FROM recent")}
    gone = harness.wait_for(lambda: not adhoc(name, "SELECT id FROM recent WHERE id = 1"), 12, every=0.25)
    out["fresh_left_after_s"] = round((utc_now() - ts).total_seconds(), 1) if gone else None
    return out


def run_materialized(name):
    try:
        adhoc(name, "SELECT * FROM c_hidden")
        return {"rejected": False, "error": None}
    except RuntimeError as e:
        return {"rejected": True, "error": str(e)[:200]}


def run_input_checks(name):
    def one(row):
        r = post_raw(name, "strict", json.dumps([{"insert": row}]))
        return r.status_code
    out = {}
    base = {"small": 1, "d": "2024-01-01", "s": "ab", "f": 1.0}
    out["out_of_range_status"] = one({"id": 1, **base, "small": 200})
    out["invalid_date_status"] = one({"id": 2, **base, "d": "1997-02-29"})
    out["long_string_status"] = one({"id": 3, **base, "s": "abcdef"})
    r = post_raw(name, "strict", '[{"insert": {"id": 4, "small": 1, "d": "2024-01-01", "s": "ab", "f": NaN}}]')
    out["nan_status"] = r.status_code
    r = post_raw(name, "strict", '[{"insert": {"id": 5, "small": 1,')
    out["broken_json_status"] = r.status_code
    # One bad record in a batch with good ones.
    r = post_raw(name, "strict", json.dumps([{"insert": {"id": 6, **base}}, {"insert": {"id": 7, **base, "small": 999}},
                                             {"insert": {"id": 8, **base}}]))
    out["mixed_batch_status"] = r.status_code
    time.sleep(1.5)
    out["stored"] = sorted((r["id"], r["s"]) for r in adhoc(name, "SELECT id, s FROM strict"))
    return out


def run_gap_lane():
    name = gap.GAP
    harness.reset(name)
    harness.start(name)
    results = {}
    for key, fn in [("pk", run_primary_key), ("recursion", run_recursion), ("materialized", run_materialized),
                    ("input", run_input_checks), ("now", run_now), ("adhoc", run_adhoc_snapshot)]:
        try:
            results[key] = fn(name)
        except Exception as e:  # recorded per scenario
            results[key] = e
    harness.stop(name, force=True)
    return results


def gap_result(key):
    r = harness.get("gap", run_gap_lane)[key]
    if isinstance(r, Exception):
        raise r
    print(f"\n[{key}] {json.dumps(r, default=str)[:600]}")
    return r


# --- TYPES lane --------------------------------------------------------------

VALUES = {
    "c_bool": True, "c_i8": -128, "c_i16": -32768, "c_i32": -2147483648, "c_i64": -9223372036854775808,
    "c_u8": 255, "c_u16": 65535, "c_u32": 4294967295, "c_u64": 18446744073709551615,
    "c_real": 0.1, "c_double": 1.7976931348623157e308, "c_dec": "1234567890123456789012345678.0123456789",
    "c_varchar": 'héllo "q" \\ \n\t \U0001F600 ∑', "c_char": "ab",
    "c_binary": [1, 2, 3, 4], "c_varbinary": [0, 255, 16],
    "c_date": "9999-12-31", "c_time": "23:59:59.123456789", "c_ts": "2024-02-25 12:12:33.123456",
    "c_tstz": "2024-02-25T12:12:33.123+02:00", "c_uuid": "123e4567-e89b-12d3-a456-426614174000",
    "c_variant": {"a": [1, 2, {"b": None}], "c": "x"}, "c_arr_int": [1, 2, 3], "c_arr_str": ["a", "é"],
    "c_arr_ts": ["2024-01-01 00:00:00"], "c_arr_arr": [[1, 2], [3]], "c_row": {"a": 1, "b": "x"},
    "c_arr_row": [{"a": 1}, {"a": 2}], "c_map": {"k": 1, "é": 2},
}
NESTED_NULLS = {"c_arr_int": [1, None, 3], "c_map": {"k": None}, "c_row": {"a": None, "b": "x"},
                "c_arr_row": [{"a": None}]}


def as_bytes(v):
    if isinstance(v, list):
        return bytes(v)
    if isinstance(v, str):
        import base64
        try:
            return bytes.fromhex(v)
        except ValueError:
            return base64.b64decode(v)
    return v


def ts(v):
    s = str(v).replace("T", " ").rstrip("Z")
    return dt.datetime.fromisoformat(s)


def instant(v):
    s = str(v).replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    d = dt.datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def tod(v):
    h, m, rest = str(v).split(":")
    sec, _, frac = rest.partition(".")
    return int(h), int(m), int(sec), int((frac + "000000000")[:9])


def f32(x):
    return struct.unpack("f", struct.pack("f", float(x)))[0]


def jsonish(v):
    return json.loads(v) if isinstance(v, str) else v


NORMAL = {
    "c_bool": bool, "c_i8": int, "c_i16": int, "c_i32": int, "c_i64": int, "c_u8": int, "c_u16": int, "c_u32": int,
    "c_u64": int, "c_real": f32, "c_double": float, "c_dec": lambda v: Decimal(str(v)), "c_varchar": str,
    "c_char": str, "c_binary": as_bytes, "c_varbinary": as_bytes, "c_date": lambda v: str(v)[:10], "c_time": tod,
    "c_ts": ts, "c_tstz": instant, "c_uuid": lambda v: str(v).lower(), "c_variant": jsonish,
    "c_arr_int": list, "c_arr_str": list, "c_arr_ts": lambda v: [ts(x) for x in v], "c_arr_arr": list,
    "c_row": jsonish, "c_arr_row": jsonish, "c_map": jsonish,
}
# What comes back where it may differ from what goes in. CHAR(n) does not pad
# on JSON ingest: the JSON format "does not currently enforce limits on the
# number of characters in a string" (formats/json.md), so 'ab' stays 'ab'.
EXPECT = {}


def loads(text):
    """Feldera writes DECIMAL as a full-precision JSON number; keep every digit."""
    return json.loads(text, parse_float=Decimal)


def compare(col, got):
    want = EXPECT.get(col, VALUES[col])
    try:
        ok = NORMAL[col](got) == NORMAL[col](want)
    except Exception as e:
        return False, f"could not read {got!r}: {e}"
    return ok, None if ok else f"sent {want!r}, got {got!r}"


COLUMN_ID = {c: 100 + i for i, c in enumerate(gap.TYPE_COLUMNS)}  # one row per type
NULL_ID, NESTED_ID = 2, 3


def rows_for(columns):
    """One row per type, so one bad encoding fails only its own member; then an
    all-NULL row and a row of NULLs nested inside arrays, maps and rows."""
    rows = [{"id": COLUMN_ID[c], c: VALUES[c]} for c in columns]
    rows.append({"id": NULL_ID, **{c: None for c in columns}})
    rows.append({"id": NESTED_ID, **{c: v for c, v in NESTED_NULLS.items() if c in columns}})
    return rows


def check_rows(columns, by_id, ingest_errors=None):
    """{member: (ok, detail)} for every type, plus the null members."""
    ingest_errors = ingest_errors or {}
    out = {}
    for c in columns:
        row = by_id.get(COLUMN_ID[c])
        if row is None:
            out[c[2:]] = (False, ingest_errors.get(COLUMN_ID[c], "the row never came back"))
        else:
            out[c[2:]] = compare(c, row.get(c))
    nulls = by_id.get(NULL_ID)
    bad_nulls = [c for c in columns if nulls is None or nulls.get(c) is not None]
    out["null:column"] = (not bad_nulls, None if not bad_nulls else f"not null: {bad_nulls[:5]}")
    nested = by_id.get(NESTED_ID) or {}
    for member, col, path in [("null:array_element", "c_arr_int", lambda v: jsonish(v)[1]),
                              ("null:map_value", "c_map", lambda v: jsonish(v)["k"]),
                              ("null:row_field", "c_row", lambda v: jsonish(v)["a"]),
                              ("null:array_of_rows_field", "c_arr_row", lambda v: jsonish(v)[0]["a"])]:
        if col not in columns:
            continue
        try:
            v = path(nested.get(col))
            out[member] = (v is None, None if v is None else f"got {nested.get(col)!r}")
        except Exception as e:
            out[member] = (False, f"could not read {nested.get(col)!r}: {e}")
    return out


def lower_keys(row):
    return {k.lower(): v for k, v in row.items()}


def run_types_lane(columns):
    name = gap.TYPES
    harness.reset(name)
    harness.start(name)
    rows = rows_for(columns)
    ids = {r["id"] for r in rows}
    out = {}
    try:
        # HTTP in, one request per row; read back through an ad-hoc query and HTTP egress.
        errors, tokens = {}, []
        for x in rows:
            r = post_raw(name, "ty", json.dumps([{"insert": x}]))
            if r.ok:
                tokens.append(r.json()["token"])
            else:
                errors[x["id"]] = f"HTTP {r.status_code}: {r.text[:200]}"
        for t in tokens:
            harness.wait_complete(name, t)
        out["http_ingest_errors"] = errors
        q = requests.get(f"{harness.API}/pipelines/{name}/query", params={"sql": "SELECT * FROM ty_out", "format": "json"},
                         timeout=60)
        q.raise_for_status()
        queried = {x["id"]: lower_keys(x) for x in (loads(l) for l in q.text.splitlines() if l.strip())}
        out["http_adhoc"] = check_rows(columns, queried, errors)
        out["http_egress"] = check_rows(columns, egress_rows(name, "ty_out", len(queried)), errors)
        # Kafka in, Kafka out.
        # Kafka in, Kafka out. Each run uses fresh ids: re-sending an identical
        # row to a primary-key table changes nothing, so nothing would come out.
        base = (int(time.time()) % 100000) * 1000
        start = end_offsets("ty-out")
        p = Producer({"bootstrap.servers": KAFKA})
        for x in rows:
            p.produce("ty-in", value=json.dumps({**x, "id": x["id"] + base}).encode())
        p.flush(30)
        got = kafka_rows("ty-out", {i + base for i in ids}, start)
        out["kafka"] = check_rows(columns, {k - base: v for k, v in got.items()})
    finally:
        harness.stop(name, force=True)
    return out


def egress_rows(name, view, want):
    """Reads a view's snapshot once over HTTP egress, keeping raw JSON values."""
    resp = requests.post(f"{harness.API}/pipelines/{name}/egress/{view}",
                         params={"format": "json", "array": "false", "send_snapshot": "true", "backpressure": "true"},
                         stream=True, timeout=(10, 15))
    rows = {}
    try:
        for line in resp.iter_lines():
            if not line:
                continue
            for ch in loads(line).get("json_data") or []:
                if "insert" in ch:
                    x = lower_keys(ch["insert"])
                    rows[x["id"]] = x
            if len(rows) >= want:
                break
    except requests.exceptions.RequestException:
        pass
    finally:
        resp.close()
    return rows


def end_offsets(topic):
    from confluent_kafka import TopicPartition
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"ty-{uuid.uuid4().hex}"})
    try:
        parts = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def kafka_rows(topic, ids, start, timeout=45):
    """Reads `topic` from the offsets in `start`, so older runs' output is skipped."""
    from confluent_kafka import TopicPartition
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"ty-{uuid.uuid4().hex}", "enable.auto.commit": False})
    c.assign([TopicPartition(topic, p, o) for p, o in start.items()])
    rows, deadline = {}, time.time() + timeout
    try:
        while time.time() < deadline and not ids <= set(rows):
            m = c.poll(0.5)
            if m is None or m.error():
                continue
            for line in m.value().splitlines():
                ch = loads(line)
                if "insert" in ch:
                    x = lower_keys(ch["insert"])
                    rows[x["id"]] = x
    finally:
        c.close()
    return rows


def ensure_topics(*names):
    admin = AdminClient({"bootstrap.servers": KAFKA})
    have = admin.list_topics(timeout=20).topics
    new = [NewTopic(t, num_partitions=1, replication_factor=1) for t in names if t not in have]
    for f in (admin.create_topics(new).values() if new else []):
        f.result(timeout=30)


def types_columns():
    return gap.TYPE_COLUMNS


def types_result():
    r = harness.get("types", lambda: run_types_lane(types_columns()))
    if isinstance(r, Exception):
        raise r
    return r


# --- PG lane -----------------------------------------------------------------


def pg_connect():
    return psycopg.connect(f"postgresql://postgres:temper@{PG_HOST}/postgres", autocommit=True)


def run_pg_lane():
    name = gap.PG
    with pg_connect() as c:
        c.execute("DROP TABLE IF EXISTS rich")
        c.execute("CREATE TABLE rich (id INT PRIMARY KEY, owner TEXT, bal BIGINT NOT NULL)")
    harness.reset(name)
    harness.start(name)
    rng = random.Random(3)
    accts, mismatch, steps = {}, None, 0
    try:
        for i in range(25):
            ups = []
            for _ in range(rng.choice([1, 3, 8])):
                k = rng.randint(1, 30)
                if k in accts and rng.random() < 0.3:
                    ups.append({"delete": {"id": k, "owner": accts[k][0], "bal": accts[k][1]}})
                    del accts[k]
                else:
                    v = (rng.choice(["a", "b", None]), rng.randint(0, 100))
                    accts[k] = v
                    ups.append({"insert": {"id": k, "owner": v[0], "bal": v[1]}})
            if i == 12:  # PostgreSQL pauses while updates arrive
                harness.sh("docker", "pause", "temper-feldera-pg")
                tok = harness.ingest(name, "accts", ups)
                time.sleep(6)
                harness.sh("docker", "unpause", "temper-feldera-pg")
                harness.wait_complete(name, tok, timeout=120)
            else:
                harness.wait_complete(name, harness.ingest(name, "accts", ups))
            steps += 1
            want = sorted((k, o, b) for k, (o, b) in accts.items() if b > 50)
            with pg_connect() as c:
                got = sorted(c.execute("SELECT id, owner, bal FROM rich").fetchall())
            if got != want and mismatch is None:
                mismatch = {"step": i, "missing": sorted(set(want) - set(got))[:5],
                            "extra": sorted(set(got) - set(want))[:5]}
    finally:
        harness.sh("docker", "unpause", "temper-feldera-pg")
        harness.stop(name, force=True)
    return {"steps": steps, "mismatch": mismatch}


def pg_result():
    r = harness.get("pg", run_pg_lane)
    print(f"\n[pg] {r}")
    return r


# --- tests -------------------------------------------------------------------


@pytest.mark.covers("adhoc.consistent_snapshot", subject="adhoc.datafusion")
@pytest.mark.covers("sql.adhoc")
def test_adhoc_reads_one_snapshot():
    r = gap_result("adhoc")
    assert r["queries"] > 20, f"only {r['queries']} queries ran"
    assert not r["n_inconsistent"], \
        f"{r['n_inconsistent']} of {r['queries']} queries saw the table and its views disagree, e.g. {r['inconsistent']}"


@pytest.mark.covers("adhoc.only_materialized", subject="adhoc.datafusion")
def test_non_materialized_view_is_not_queryable():
    r = gap_result("materialized")
    assert r["rejected"], "an ad-hoc SELECT read a view that is not materialized"


@pytest.mark.covers("pk.insert_overwrites", subject="table.primary_key")
@pytest.mark.covers("sql.primary_key")
def test_insert_overwrites_same_key():
    r = gap_result("pk")
    assert r["overwrite"] == [json.dumps({"id": 1, "owner": "b", "bal": 20})], r["overwrite"]


@pytest.mark.covers("pk.delete_missing_ignored", subject="table.primary_key")
def test_delete_of_missing_key_is_ignored():
    r = gap_result("pk")
    assert r["missing_delete_rows"] == 2, r


@pytest.mark.covers("pk.delete_by_key", subject="table.primary_key")
def test_delete_matches_on_key():
    r = gap_result("pk")
    assert r["key_delete_rows"] == 0, "a delete whose key exists but whose other columns differ left the row"


@pytest.mark.covers("pk.update_changes_columns", subject="table.primary_key")
def test_update_changes_named_columns():
    r = gap_result("pk")
    assert r["update"] == [{"owner": "c", "bal": 35}], r["update"]


@pytest.mark.covers("pk.update_missing_ignored", subject="table.primary_key")
def test_update_of_missing_key_is_ignored():
    r = gap_result("pk")
    assert r["missing_update_rows"] == 0, r


@pytest.mark.covers("view.matches_batch", subject="view.recursive")
@pytest.mark.covers("sql.recursion")
def test_recursive_view_matches_batch():
    r = gap_result("recursion")
    assert r["content"] is None, f"closure differs from DuckDB's recursive query: {r['content']}"


@pytest.mark.covers("view.changes_add_up", subject="view.recursive")
def test_recursive_changes_add_up():
    r = gap_result("recursion")
    assert r["changes"] is None, f"closure's change stream differs: {r['changes']}"


@pytest.mark.covers("now.increasing_utc", subject="clock.now")
@pytest.mark.covers("sql.now")
def test_now_increases_in_utc():
    r = gap_result("now")
    assert r["clock_samples"] >= 6, r
    assert r["clock_nondecreasing"] and r["clock_advanced"], r
    assert r["clock_max_skew_s"] is not None and r["clock_max_skew_s"] < 3, f"NOW() is {r['clock_max_skew_s']}s from UTC"


@pytest.mark.covers("now.temporal_filter_advances", subject="clock.now")
def test_temporal_filter_drops_expired_rows():
    r = gap_result("now")
    assert r["fresh_visible"] == {1}, f"recent held {r['fresh_visible']}, want only the fresh event"
    assert r["fresh_left_after_s"] is not None and r["fresh_left_after_s"] <= 8, \
        f"the event left recent after {r['fresh_left_after_s']}s (window 5 s, one step a second)"


@pytest.mark.covers("input.rejects_out_of_range", subject="input.http_json")
@pytest.mark.covers("input.errors")
def test_out_of_range_integer_is_rejected():
    r = gap_result("input")
    assert r["out_of_range_status"] >= 400, r
    assert 1 not in {i for i, _ in r["stored"]}, r["stored"]


@pytest.mark.covers("input.rejects_invalid_date", subject="input.http_json")
def test_invalid_date_is_rejected():
    r = gap_result("input")
    assert r["invalid_date_status"] >= 400, r
    assert 2 not in {i for i, _ in r["stored"]}, r["stored"]


@pytest.mark.covers("input.keeps_long_strings", subject="input.http_json")
def test_long_string_is_kept_untruncated():
    r = gap_result("input")
    assert (3, "abcdef") in r["stored"], r


@pytest.mark.covers("input.rejects_nan", subject="input.http_json")
def test_nan_is_rejected():
    r = gap_result("input")
    assert r["nan_status"] >= 400 and 4 not in {i for i, _ in r["stored"]}, r


@pytest.mark.covers("input.rejects_broken_json", subject="input.http_json")
def test_broken_json_is_rejected():
    r = gap_result("input")
    assert r["broken_json_status"] >= 400 and 5 not in {i for i, _ in r["stored"]}, r


def type_params():
    out = []
    for transport in ("http", "kafka"):
        for col in gap.TYPE_COLUMNS:
            out.append(pytest.param(transport, col[2:], id=f"{transport}-{col[2:]}", marks=[
                pytest.mark.covers(f"types.contract/{col[2:]}", subject=f"transport.{transport}"),
                pytest.mark.covers("sql.types")]))
        for m in ("column", "array_element", "map_value", "row_field", "array_of_rows_field"):
            out.append(pytest.param(transport, f"null:{m}", id=f"{transport}-null-{m}", marks=[
                pytest.mark.covers(f"nulls.contract/{m}", subject=f"transport.{transport}")]))
    return out


@pytest.mark.parametrize("transport,member", type_params())
def test_type_round_trips(transport, member):
    r = types_result()
    if transport == "http":
        ok, detail = r["http_adhoc"].get(member, (False, "not checked"))
        ok2, detail2 = r["http_egress"].get(member, (False, "not checked"))
        assert ok and ok2, f"ad-hoc: {detail}; egress: {detail2}"
    else:
        ok, detail = r["kafka"].get(member, (False, "not checked"))
        assert ok, detail


@pytest.mark.covers("pg.mirrors_view", subject="sink.postgres")
@pytest.mark.covers("connector.postgres")
def test_postgres_table_mirrors_view():
    r = pg_result()
    assert r["steps"] == 25 and r["mismatch"] is None, f"PostgreSQL differs from the view: {r['mismatch']}"
