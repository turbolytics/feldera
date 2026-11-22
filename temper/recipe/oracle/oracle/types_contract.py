"""transport.http and transport.kafka: every SQL type, and every place a
NULL can sit, comes back equal (SQL types; JSON format).

One wide table holds a column per type. Each row carries one edge-case
value per type; the last row carries a NULL in each place a NULL can sit.
HTTP reads it back by ad-hoc query and by the change stream; Kafka sends it
through Kafka JSON input and output connectors. Values are compared after
normalizing by the docs' equality: a zoned timestamp is an instant, REAL is
single precision, CHAR(4) is padded.
"""
import datetime
import decimal
import json
import struct
import time

from . import api
from .views import Stream

D = decimal.Decimal

# member -> (SQL type, values sent)
TYPES = {
    "bool": ("BOOLEAN", [True, False]),
    "i8": ("TINYINT", [-128, 127, 0]),
    "i16": ("SMALLINT", [-32768, 32767]),
    "i32": ("INT", [-2147483648, 2147483647]),
    "i64": ("BIGINT", [-9223372036854775808, 9223372036854775807]),
    "u8": ("TINYINT UNSIGNED", [0, 255]),
    "u16": ("SMALLINT UNSIGNED", [0, 65535]),
    "u32": ("INT UNSIGNED", [0, 4294967295]),
    "u64": ("BIGINT UNSIGNED", [0, 18446744073709551615]),
    "real": ("REAL", [1.5, 3.4028235e38, 1.17549435e-38, -2.5]),
    "double": ("DOUBLE", [1.7976931348623157e308, -2.2250738585072014e-308, 0.1]),
    "dec": ("DECIMAL(38, 10)", ["1234567890123456789012345678.1234567890", "-0.0000000001", "0"]),
    "varchar": ("VARCHAR", ["", "héllo wörld ✓", "quote \" backslash \\ newline \n tab \t", "\U0001F600"]),
    "char": ("CHAR(4)", ["ab", "abcd"]),
    "binary": ("BINARY(4)", [[0, 255, 1, 2], [9, 8, 7, 6]]),
    "varbinary": ("VARBINARY", [[], [0, 1, 2, 255]]),
    "date": ("DATE", ["1970-01-01", "9999-12-31", "0001-01-01"]),
    "time": ("TIME", ["00:00:00", "23:59:59.999999", "12:34:56.123456"]),
    "ts": ("TIMESTAMP", ["1970-01-01 00:00:00", "9999-12-31 23:59:59.999999", "2026-03-08 02:30:00.5"]),
    "tstz": ("TIMESTAMP WITH TIME ZONE", ["2026-01-01T12:00:00+02:00", "2026-06-30T23:59:59.5Z"]),
    "uuid": ("UUID", ["123e4567-e89b-12d3-a456-426614174000", "00000000-0000-0000-0000-000000000000"]),
    "variant": ("VARIANT", [{"a": [1, "x", None]}, 5, "s"]),
    "arr_int": ("INT ARRAY", [[1, 2, 3], []]),
    "arr_str": ("VARCHAR ARRAY", [["a", "ü"], []]),
    "arr_ts": ("TIMESTAMP ARRAY", [["2026-01-01 00:00:00", "2026-01-01 00:00:00.25"]]),
    "arr_arr": ("BIGINT ARRAY ARRAY", [[[1, 2], [3]], [[]]]),
    "row": ("ROW(a INT NULL, b VARCHAR NULL)", [{"a": 1, "b": "x"}]),
    "arr_row": ("ROW(a INT NULL) ARRAY", [[{"a": 1}, {"a": 2}]]),
    "map": ("MAP<VARCHAR, INT>", [{"x": 1, "y": 2}]),
}

# nulls.contract member -> (column, value carrying the NULL, expected)
NULLS = {
    "column": ("varchar", None),
    "array_element": ("arr_int", [1, None, 3]),
    "map_value": ("map", {"x": None}),
    "row_field": ("row", {"a": None, "b": None}),
    "array_of_rows_field": ("arr_row", [{"a": None}]),
}

COLS = ", ".join(f"c_{m} {t}" for m, (t, _) in TYPES.items())


def rows():
    n = max(len(v) for _, v in TYPES.values())
    out = []
    for i in range(n):
        r = {"id": i}
        for m, (_, vals) in TYPES.items():
            r[f"c_{m}"] = vals[i] if i < len(vals) else None
        out.append(r)
    nulls = {"id": 100}
    for _, (col, val) in NULLS.items():
        nulls[f"c_{col}"] = val
    out.append(nulls)
    return out


def f32(x):
    return struct.unpack("f", struct.pack("f", x))[0]


def instant(s):
    t = datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00").replace(" ", "T"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=datetime.timezone.utc)
    return t.astimezone(datetime.timezone.utc).isoformat()


def stamp(s):
    t = datetime.datetime.fromisoformat(str(s).replace("T", " "))
    return t.isoformat(sep=" ")


def norm(member, v):
    """A value in the docs' equality, so equal values compare equal."""
    if v is None:
        return None
    if member == "real":
        return f32(float(v))
    if member == "double":
        return float(v)
    if member == "dec":
        return str(D(str(v)).normalize())
    if member == "char":
        return str(v).ljust(4)
    if member in ("binary", "varbinary"):
        if isinstance(v, str):  # a hex or base64 spelling
            try:
                return list(bytes.fromhex(v))
            except ValueError:
                import base64
                return list(base64.b64decode(v))
        return list(v)
    if member == "time":
        return datetime.time.fromisoformat(str(v)).isoformat()
    if member == "ts":
        return stamp(v)
    if member == "tstz":
        return instant(v)
    if member == "uuid":
        return str(v).lower()
    if member == "variant":
        return json.dumps(json.loads(v) if isinstance(v, str) and v[:1] in "[{\"0123456789-tfn" else v, sort_keys=True)
    if member == "arr_ts":
        return [stamp(x) if x is not None else None for x in v]
    if member == "map":
        return sorted((k, x) for k, x in (v.items() if isinstance(v, dict) else v))
    return v


def compare(label, got_rows):
    """Violations per member: values that did not come back equal."""
    by_id = {r.get("id"): r for r in got_rows}
    types = {m: 0 for m in TYPES}
    nulls = {m: 0 for m in NULLS}
    for sent in rows():
        got = by_id.get(sent["id"])
        if sent["id"] == 100:
            for m, (col, val) in NULLS.items():
                g = None if got is None else got.get(f"c_{col}")
                if got is None or norm(col, g) != norm(col, val):
                    nulls[m] += 1
                    api.log(f"{label}: NULL in {m} came back as {g!r}")
            continue
        for m in TYPES:
            want = sent[f"c_{m}"]
            if want is None:
                continue
            g = None if got is None else got.get(f"c_{m}")
            if got is None or norm(m, g) != norm(m, want):
                types[m] += 1
                api.log(f"{label}: {m} sent {want!r}, came back {g!r}")
    return types, nulls


def http(subject):
    name = "types_http"
    api.pipeline(name, f"CREATE TABLE wide (id INT NOT NULL, {COLS}) WITH ('materialized' = 'true');\n"
                       "CREATE MATERIALIZED VIEW wide_v AS SELECT * FROM wide;")
    stream = Stream("wide_v", name, normalize=False)
    stream.start()
    time.sleep(1)
    api.ingest(name, "wide", [{"insert": r} for r in rows()])
    time.sleep(2)
    t1, n1 = compare("ad-hoc", api.query(name, "SELECT * FROM wide"))
    streamed = [json.loads(k, parse_float=decimal.Decimal) for k in stream.bag()[0]]
    t2, n2 = compare("change stream", streamed)
    return {m: t1[m] + t2[m] for m in TYPES}, {m: n1[m] + n2[m] for m in NULLS}


def kafka(subject):
    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    name = "types_kafka"
    admin = AdminClient({"bootstrap.servers": "kafka:9092"})
    api.wait("Kafka", lambda: admin.list_topics(timeout=5) is not None, 120, 2)
    for f in admin.create_topics([NewTopic(t, 1, 1) for t in ("types_in", "types_out")]).values():
        try:
            f.result()
        except Exception as e:
            api.log(f"create topic: {e}")
    fmt = '{"name": "json", "config": {"update_format": "insert_delete", "array": false}}'
    api.pipeline(name, f"""CREATE TABLE wide (id INT NOT NULL, {COLS}) WITH ('connectors' = '[{{
  "transport": {{"name": "kafka_input", "config": {{"bootstrap.servers": "kafka:9092", "topics": ["types_in"], "auto.offset.reset": "earliest"}}}},
  "format": {fmt}}}]');
CREATE MATERIALIZED VIEW wide_v WITH ('connectors' = '[{{
  "transport": {{"name": "kafka_output", "config": {{"bootstrap.servers": "kafka:9092", "topic": "types_out"}}}},
  "format": {fmt}}}]') AS SELECT * FROM wide;""")
    p = Producer({"bootstrap.servers": "kafka:9092"})
    for r in rows():
        p.produce("types_in", json.dumps({"insert": r}).encode())
    p.flush(30)
    c = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "oracle", "auto.offset.reset": "earliest"})
    c.subscribe(["types_out"])
    got, deadline = [], time.time() + 90
    while len(got) < len(rows()) and time.time() < deadline:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        for line in msg.value().decode().splitlines():
            if not line.strip():
                continue
            body = json.loads(line, parse_float=decimal.Decimal)
            for change in body if isinstance(body, list) else [body]:
                if "insert" in change:
                    got.append(change["insert"])
    c.close()
    api.log(f"Kafka: {len(got)} of {len(rows())} rows came back")
    return compare("Kafka", got)


def run(subject):
    types, nulls = (http if subject == "transport.http" else kafka)(subject)
    out = {f"types.contract/{m}": n for m, n in types.items()}
    out.update({f"nulls.contract/{m}": n for m, n in nulls.items()})
    out["sql.types"] = sum(out.values())
    out["connector.http" if subject == "transport.http" else "connector.kafka"] = out["sql.types"]
    return out
