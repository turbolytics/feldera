"""op.*: every SQL type inside each operator that keeps state.

The oracle models each type by equality classes from the docs: values the
docs call equal share a class, classes rank in the docs' order. Rows are
inserted by one spelling of a value and often deleted by another ("1.50"
for 1.5, an upper-case UUID, one instant at another offset); Feldera must
treat them as the same value. Each operator's view is compared with what
the classes say, after every request, for three seeds at 1, 4 and 8
workers. Pairs the compiler refuses (SUM over a non-numeric type, = between
ROW values) are proven refused instead, by compiling them alone.
"""
import json
import random
import time

from . import api
from .types_contract import TYPES, norm
from .views import restart

NAME = "type_ops"

# member -> classes in ascending order; each class lists equal spellings.
CLASSES = {
    "bool": [[False], [True]],
    "i8": [[-128], [0], [127]],
    "i16": [[-32768], [0], [32767]],
    "i32": [[-2147483648], [0], [2147483647]],
    "i64": [[-9223372036854775808], [0], [9223372036854775807]],
    "u8": [[0], [7], [255]],
    "u16": [[0], [7], [65535]],
    "u32": [[0], [7], [4294967295]],
    "u64": [[0], [7], [18446744073709551615]],
    "real": [[-2.5], [0.0, -0.0], [1.5]],
    "double": [[-2.5], [0.0, -0.0], [0.1]],
    "dec": [["-0.5", "-0.50"], ["0", "0.0"], ["1.5", "1.50", "1.500"]],
    "varchar": [["A"], ["a"], ["b"]],
    "char": [["a"], ["ab", "ab  "], ["b"]],
    "binary": [[[0, 0, 0, 1]], [[0, 0, 0, 2]], [[255, 0, 0, 0]]],
    "varbinary": [[[]], [[0]], [[1, 2]]],
    "date": [["1970-01-01"], ["2026-01-01"], ["9999-12-31"]],
    "time": [["00:00:00"], ["12:00:00.5"], ["23:59:59"]],
    "ts": [["1970-01-01 00:00:00"], ["2026-01-01 12:00:00"], ["2026-01-01 12:00:00.5"]],
    "tstz": [["2026-01-01T08:00:00Z"], ["2026-01-01T12:00:00+02:00", "2026-01-01T10:00:00Z"], ["2026-01-02T00:00:00Z"]],
    "uuid": [["00000000-0000-0000-0000-000000000001"],
             ["123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000"],
             ["ffffffff-ffff-ffff-ffff-ffffffffffff"]],
    "variant": [[1], [2], ["x"]],
    "arr_int": [[[]], [[1, 2]], [[1, 3]]],
    "arr_str": [[["a"]], [["a", "b"]], [["b"]]],
    "arr_ts": [[["2026-01-01 00:00:00"]], [["2026-01-01 00:00:00", "2026-01-01 00:00:01"]], [["2026-01-02 00:00:00"]]],
    "arr_arr": [[[[1]]], [[[1], [2]]], [[[2]]]],
    "row": [[{"a": 1, "b": "a"}], [{"a": 1, "b": "b"}], [{"a": 2, "b": "a"}]],
    "arr_row": [[[{"a": 1}]], [[{"a": 1}, {"a": 2}]], [[{"a": 2}]]],
    "map": [[{"x": 1}], [{"x": 1, "y": 2}, {"y": 2, "x": 1}], [{"x": 2}]],
}
NUMERIC = {"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "real", "double", "dec"}

# Each operator's view over table t_<m> (id, g, v).
OPS = {
    "distinct": "SELECT DISTINCT v FROM t_{m}",
    "group_by": "SELECT v, COUNT(*) AS n FROM t_{m} GROUP BY v",
    "join": "SELECT a.id AS l, b.id AS r FROM t_{m} a JOIN t_{m} b ON a.v = b.v",
    "min_max": "SELECT g, ARG_MIN(id, v) AS lo, ARG_MAX(id, v) AS hi FROM t_{m} GROUP BY g",
    "order_by": "SELECT id, v FROM (SELECT id, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, id) AS rn FROM t_{m}) WHERE rn <= 2",
    "sum": "SELECT g, SUM(v) AS s FROM t_{m} GROUP BY g",
}


# SUM over a string compiles, but only by casting the strings to numbers.
CAST = {("sum", "varchar"), ("sum", "char")}


def refused(op, m):
    return (op == "sum" and m not in NUMERIC) or (op == "join" and m == "row")


def program():
    parts = []
    for m, (sql_type, _) in TYPES.items():
        parts.append(f"CREATE TABLE t_{m} (id INT NOT NULL, g INT NOT NULL, v {sql_type});")
        for op, body in OPS.items():
            if not refused(op, m):
                parts.append(f"CREATE MATERIALIZED VIEW {op}_{m} AS {body.format(m=m)};")
    return "\n".join(parts)


def cls(m, v):
    """The class index of a value Feldera returned, by the docs' equality."""
    n = norm(m, v)
    for i, spellings in enumerate(CLASSES[m]):
        if any(norm(m, s) == n for s in spellings):
            return i
    return f"unknown {v!r}"


def expected(op, m, rows):
    """What the operator's view holds, in class terms, for rows {id: (g, class)}."""
    if op == "distinct":
        return sorted({c for _, c in rows.values()})
    if op == "group_by":
        counts = {}
        for _, c in rows.values():
            counts[c] = counts.get(c, 0) + 1
        return sorted(counts.items())
    if op == "join":
        return sorted((a, b) for a, (_, ca) in rows.items() for b, (_, cb) in rows.items() if ca == cb)
    if op == "min_max":
        out = []
        for g in sorted({g for g, _ in rows.values()}):
            members = [(c, i) for i, (gg, c) in rows.items() if gg == g]
            lo = min(c for c, _ in members)
            hi = max(c for c, _ in members)
            out.append((g, sorted(i for c, i in members if c == lo), sorted(i for c, i in members if c == hi)))
        return out
    if op == "order_by":
        out = []
        for g in {g for g, _ in rows.values()}:
            members = sorted((c, i) for i, (gg, c) in rows.items() if gg == g)
            out.extend((i, c) for c, i in members[:2])
        return sorted(out)
    if op == "sum":
        sums = {}
        for g, c in rows.values():
            sums[g] = sums.get(g, 0) + float(str(CLASSES[m][c][0]))
        return sorted((g, round(s, 6)) for g, s in sums.items())


def actual(op, m, got):
    if op == "distinct":
        return sorted(cls(m, r["v"]) for r in got)
    if op == "group_by":
        return sorted((cls(m, r["v"]), r["n"]) for r in got)
    if op == "join":
        return sorted((r["l"], r["r"]) for r in got)
    if op == "order_by":
        return sorted((r["id"], cls(m, r["v"])) for r in got)
    if op == "sum":
        return sorted((r["g"], round(float(str(r["s"])), 6)) for r in got)
    return got  # min_max is compared separately: ties may pick any arg


def min_max_ok(want, got):
    by_g = {r["g"]: r for r in got}
    if sorted(by_g) != [g for g, _, _ in want]:
        return False
    return all(by_g[g]["lo"] in lo and by_g[g]["hi"] in hi for g, lo, hi in want)


def workload(op, members, seed, label):
    rng = random.Random(seed)
    rows = {m: {} for m in members}  # id -> (g, class)
    bad = {m: 0 for m in members}
    for step in range(8):
        for m in members:
            updates = []
            for _ in range(rng.randrange(1, 4)):
                live = rows[m]
                if live and rng.random() < 0.35:
                    i = rng.choice(sorted(live))
                    g, c = live.pop(i)
                    # Delete by another spelling of the same value when there is one.
                    updates.append({"delete": {"id": i, "g": g, "v": rng.choice(CLASSES[m][c])}})
                else:
                    i = rng.randrange(1, 30)
                    if i in live:
                        continue
                    g, c = rng.randrange(1, 3), rng.randrange(len(CLASSES[m]))
                    live[i] = (g, c)
                    updates.append({"insert": {"id": i, "g": g, "v": CLASSES[m][c][0]}})
            if not updates:
                continue
            if not api.ingest(NAME, f"t_{m}", updates):
                raise SystemExit(f"{NAME} stopped at {label} step {step}")
            got = api.query(NAME, f"SELECT * FROM {op}_{m}")
            want = expected(op, m, rows[m])
            ok = min_max_ok(want, got) if op == "min_max" else actual(op, m, got) == want
            if not ok:
                bad[m] += 1
                api.log(f"{label} step {step}: {op} over {m} DIFFERS: Feldera {json.dumps(got, default=str)[:300]} vs model {want}")
    return bad


def prove_refused(op, m):
    """0 when the compiler refuses the pair, as declared."""
    name = f"refuse_{op}_{m}"
    sql_type = TYPES[m][0]
    sql = f"CREATE TABLE t_{m} (id INT NOT NULL, g INT NOT NULL, v {sql_type});\nCREATE MATERIALIZED VIEW v AS {OPS[op].format(m=m)};"
    api.call("PUT", f"/pipelines/{name}", {"name": name, "program_code": sql})

    # SqlError: the SQL compiler refused it. SqlCompiled or later: accepted.
    decided = {"SqlError", "SqlCompiled", "CompilingRust", "RustError", "Success", "SystemError"}
    status = api.wait(f"{name} to be refused or accepted",
                      lambda: (lambda s: s if s in decided else None)(str(api.get_json(f"/pipelines/{name}")["program_status"])),
                      1800, 2)
    err = json.dumps(api.get_json(f"/pipelines/{name}").get("program_error"))[:200]
    api.log(f"{op} over {m}: {status}: {err}")
    return 0 if status == "SqlError" else 1


def prove_cast(op, m):
    """0 when SUM accepts the string column only by casting it to a number:
    "1.5" and "2" sum to the number 3.5, not a string."""
    name = f"cast_{op}_{m}"
    api.pipeline(name, f"CREATE TABLE t (g INT NOT NULL, v {TYPES[m][0]});\n"
                       "CREATE MATERIALIZED VIEW s AS SELECT g, SUM(v) AS s FROM t GROUP BY g;")
    api.ingest(name, "t", [{"insert": {"g": 1, "v": "1.5"}}, {"insert": {"g": 1, "v": "2"}}])
    got = api.query(name, "SELECT s FROM s")
    api.log(f"SUM over {m} of '1.5' and '2': {got}")
    try:
        return 0 if len(got) == 1 and not isinstance(got[0]["s"], str) and float(got[0]["s"]) == 3.5 else 1
    except (TypeError, ValueError):
        return 1


def run(subject):
    op = subject.split(".", 1)[1]
    members = [m for m in CLASSES if not refused(op, m)]
    api.pipeline(NAME, program(), workers=4)
    bad = {m: 0 for m in CLASSES}
    first = True
    for seed in (1, 2, 3):
        for w in (4, 1, 8):
            if not first:
                restart(w, NAME)
            first = False
            for m, n in workload(op, members, seed * 10 + w, f"seed {seed}, {w} workers").items():
                bad[m] += n
    for m in CLASSES:
        if (op, m) in CAST:
            bad[m] = prove_cast(op, m)
        elif refused(op, m):
            bad[m] = prove_refused(op, m)
    out = {f"types.operators/{m}": n for m, n in bad.items()}
    out["sql.type_semantics"] = sum(n for m, n in bad.items() if not refused(op, m))
    return out
