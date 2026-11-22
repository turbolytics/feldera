"""Every SQL type inside the stateful operators, under deletes (views_types.py).

Three seeds run on one pipeline, at 1, 4 and 8 workers. After every step
completes, each operator view is read ad hoc and compared with the result the
model computes from value classes. A type passes an operator when its view
matches at every step of every seed.

The compiler rejects some pairs, such as SUM over a DATE. Each rejection is
shown by compiling that pair alone. SUM over a string compiles, because the
compiler casts the string to a number, so it is not a string operation.

A separate pipeline checks the promise that SUM computes "using higher
precision intermediate data types" (Aggregate Operations): a SUM whose total
fits its type survives rows that pass the type's maximum on the way.
"""

import struct
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest

import harness
import views_types as vt

PIPELINE = "temper_types_ops"
RUNS = [(31, 1), (32, 4), (33, 8)]  # (seed, workers)
STEPS = 30
FEATURE = "sql.type_semantics"

# (operator, type) -> a phrase the compiler's message contains
REJECTED = {("op.sum", k): "Cannot apply 'SUM'" for k in (
    "bool", "binary", "varbinary", "date", "time", "ts", "tstz", "uuid", "variant", "arr_int", "arr_str", "arr_ts",
    "arr_arr", "row", "arr_row", "map")}
REJECTED[("op.join", "row")] = "ROW values cannot be compared using '='"
CAST = {("op.sum", "varchar"), ("op.sum", "char")}  # compiles as SUM over a number cast from the string
SKIP = set(REJECTED) | CAST


# --- comparing ---------------------------------------------------------------------


def as_number(v):
    return None if v is None else Decimal(str(v))


def f32(x):
    """The REAL nearest x. Feldera prints a REAL as the shortest text that parses back to it."""
    return struct.unpack("f", struct.pack("f", float(x)))[0]


def sum_matches(key, got, want_xs):
    if key not in vt.FLOATS:
        return as_number(got) == sum(Decimal(str(x)) for x in want_xs)
    if key == "real":
        got, want_xs = f32(got), [f32(x) for x in want_xs]
    got = Decimal(repr(float(got)))
    want = sum(Decimal(repr(float(x))) for x in want_xs)
    scale = sum(abs(Decimal(repr(float(x)))) for x in want_xs)
    # rounding: relative to the magnitudes summed, plus one step of the smallest subnormal
    tol = scale * Decimal("1e-6" if key == "real" else "1e-12") + Decimal("1.5e-45" if key == "real" else "5e-324")
    return abs(got - want) <= tol


def mismatch(op, key, want, got):
    """None if the view holds what the model expects, else a short description."""
    if op == "op.sum":
        w = {r["g"]: r for r in want}
        g = {r["g"]: r for r in got}
        if set(w) != set(g):
            return {"groups_want": sorted(w), "groups_got": sorted(g)}
        for k in w:
            if g[k]["n"] != w[k]["n"] or not sum_matches(key, g[k]["s"], w[k]["s"][2]):
                return {"g": k, "want_n": w[k]["n"], "got": g[k], "want_s": str(sum(Decimal(str(x)) for x in w[k]["s"][2]))}
        return None
    return harness.diff(harness.bag(want), harness.bag(got))


# --- the lane ----------------------------------------------------------------------


def compute():
    harness.create(PIPELINE, vt.program(SKIP), compile_timeout=2400)
    pairs = [(op, k) for op in vt.OPERATORS for k in vt.TYPES if (op, k) not in SKIP]
    bad = {p: None for p in pairs}
    steps = 0
    pool = ThreadPoolExecutor(max_workers=8)
    try:
        for seed, workers in RUNS:
            harness.reset(PIPELINE)
            harness.set_workers(PIPELINE, workers)
            harness.start(PIPELINE)
            import random
            model = vt.Model(random.Random(seed))
            for i in range(STEPS):
                updates = model.step(i)
                tokens = list(pool.map(lambda tu: harness.ingest(PIPELINE, tu[0], tu[1]),
                                       [(t, u) for t, u in updates.items() if u]))
                for tok in tokens:
                    harness.wait_complete(PIPELINE, tok)
                todo = [p for p in pairs if bad[p] is None]
                got = dict(zip(todo, pool.map(
                    lambda p: harness.query(PIPELINE, f"SELECT * FROM {vt.view_name(*p)}", exact=True), todo)))
                for p in todo:
                    d = mismatch(p[0], p[1], model.expected(*p), got[p])
                    if d:
                        bad[p] = {"seed": seed, "workers": workers, "step": i, **d}
                steps += 1
            harness.stop(PIPELINE, force=True)
    finally:
        pool.shutdown()
    return {"bad": bad, "steps": steps}


def compile_alone(name, sql, timeout=300):
    """PUTs a program and waits until SQL compilation decides. Returns the pipeline."""
    harness.requests.delete(f"{harness.API}/pipelines/{name}", timeout=30)
    harness.wait_for(lambda: harness.pipeline(name) is None, 60)
    r = harness.requests.put(f"{harness.API}/pipelines/{name}", json={"name": name, "program_code": sql}, timeout=30)
    r.raise_for_status()
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = harness.pipeline(name) or {}
        if p.get("program_status") not in (None, "Pending", "CompilingSql"):
            return p
        time.sleep(1)
    raise RuntimeError(f"{name}: SQL compilation did not finish")


def rejections():
    """Compiles each rejected pair, and each cast pair, alone."""
    out = {}
    name = "temper_types_rej"
    try:
        for op, key in sorted(REJECTED) + sorted(CAST):
            p = compile_alone(name, vt.single(op, key))
            msgs = [m["message"] for m in ((p.get("program_error") or {}).get("sql_compilation") or {}).get("messages", [])
                    if not m.get("warning")]
            out[(op, key)] = {"status": p["program_status"], "messages": msgs,
                              "schema": (p.get("program_info") or {}).get("schema")}
    finally:
        harness.requests.delete(f"{harness.API}/pipelines/{name}", timeout=30)
    return out


# Partial overflow: one request carries rows whose running total passes the max.
OVERFLOW = {  # key: (sql type, X); the request is +X, +X, -X and the total is X
    "i16": ("SMALLINT", 20000), "i32": ("INT", 2**30), "i64": ("BIGINT", 2**62),
    "dec": ("DECIMAL(38, 10)", "9000000000000000000000000000"),
}


def overflow_sql():
    return "\n".join(f"CREATE TABLE o_{k} (g INT, x {t});\n"
                     f"CREATE MATERIALIZED VIEW os_{k} AS SELECT g, SUM(x) AS s FROM o_{k} GROUP BY g;"
                     for k, (t, _) in OVERFLOW.items())


def overflow():
    name = "temper_types_overflow"
    harness.create(name, overflow_sql(), compile_timeout=1200)
    out = {}
    for key, (_, x) in OVERFLOW.items():
        neg = -x if isinstance(x, int) else "-" + x
        harness.reset(name)
        harness.start(name)
        try:
            tok = harness.ingest(name, f"o_{key}", [{"insert": {"g": 1, "x": x}}, {"insert": {"g": 1, "x": x}},
                                                    {"insert": {"g": 1, "x": neg}}])
            harness.wait_complete(name, tok, timeout=30)
            out[key] = {"rows": harness.query(name, f"SELECT * FROM os_{key}")}
        except Exception as e:
            p = harness.pipeline(name) or {}
            err = (p.get("deployment_error") or {}).get("message", "")
            out[key] = {"error": repr(e)[:200], "status": p.get("deployment_status"),
                        "panic": [ln for ln in err.splitlines() if "panic" in ln][:2]}
        harness.stop(name, force=True)
    return out


# --- tests -------------------------------------------------------------------------


def lane():
    import lanes
    return lanes.result("types_ops")


def cases():
    out = []
    for op in vt.OPERATORS:
        for key in vt.TYPES:
            if (op, key) not in SKIP:
                out.append(pytest.param(op, key, id=f"{op}-{key}", marks=[
                    pytest.mark.covers(f"types.operators/{key}", subject=op), pytest.mark.covers(FEATURE)]))
    return out


@pytest.mark.parametrize("op,key", cases())
def test_operator_matches_model_for_type(op, key):
    r = lane()
    assert r["steps"] == STEPS * len(RUNS), f"only {r['steps']} steps ran"
    bad = r["bad"][(op, key)]
    assert bad is None, f"{vt.view_name(op, key)} differs from the model: {bad}"


def rejected_cases():
    return [pytest.param(op, key, id=f"{op}-{key}", marks=[
        pytest.mark.exempts(f"types.operators/{key}", subject=op), pytest.mark.covers(FEATURE)])
        for op, key in sorted(REJECTED)]


@pytest.mark.parametrize("op,key", rejected_cases())
def test_compiler_rejects(op, key):
    import lanes
    r = lanes.result("types_rej")[(op, key)]
    assert r["status"] == "SqlError", f"{vt.view_name(op, key)} compiled: {r['status']}"
    assert any(REJECTED[(op, key)] in m for m in r["messages"]), r["messages"]


@pytest.mark.parametrize("key", [pytest.param(k, id=k, marks=[
    pytest.mark.exempts(f"types.operators/{k}", subject="op.sum"), pytest.mark.covers(FEATURE)])
    for _, k in sorted(CAST)])
def test_sum_of_string_is_a_cast(key):
    import lanes
    r = lanes.result("types_rej")[("op.sum", key)]
    assert r["status"] != "SqlError", r["messages"]
    view = next(v for v in r["schema"]["outputs"] if v["name"] == vt.view_name("op.sum", key))
    s = next(f for f in view["fields"] if f["name"] == "s")
    assert s["columntype"]["type"] not in ("VARCHAR", "CHAR"), s


@pytest.mark.covers("operator.sum_partial_overflow", subject="op.sum")
@pytest.mark.covers(FEATURE)
def test_sum_survives_partial_overflow():
    import lanes
    r = lanes.result("types_overflow")
    bad = {k: v for k, v in r.items() if "error" in v or len(v["rows"]) != 1
           or as_number(v["rows"][0]["s"]) != as_number(OVERFLOW[k][1])}
    assert not bad, f"a SUM whose total fits its type failed when one request passed the maximum: {bad}"
