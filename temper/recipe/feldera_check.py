"""Checks for Feldera's fault scenarios (temper/scenarios), standard library only.

    python feldera_check.py decimal-sum     violations of a DECIMAL SUM whose total fits
    python feldera_check.py grouping-empty  grouping-set views that differ from SQL's answer

Each prints one count of violations on stdout and explains itself on stderr.
"""
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal

API = "http://feldera:8080/v0"


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def call(method, path, body=None, params=None, timeout=60):
    url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = None
    headers = {}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode()
    return raw


def get_json(path, **kw):
    return json.loads(call("GET", path, **kw))


def wait(what, cond, timeout, every=1.0):
    deadline = time.time() + timeout
    while True:
        try:
            got = cond()
        except (urllib.error.URLError, ConnectionError, OSError):
            got = None
        if got:
            return got
        if time.time() > deadline:
            raise SystemExit(f"timed out after {timeout}s waiting for {what}")
        time.sleep(every)


def pipeline(name, sql):
    """Create pipeline name from sql, compile it, and start it running."""
    wait("the Feldera API", lambda: call("GET", "/config", timeout=5), 600, 2)
    call("PUT", f"/pipelines/{name}", {"name": name, "program_code": sql})
    log(f"{name}: compiling")

    def compiled():
        s = get_json(f"/pipelines/{name}")["program_status"]
        if "Error" in str(s):
            raise SystemExit(f"{name} did not compile: {s}")
        return s == "Success"

    wait(f"{name} to compile", compiled, 1800, 5)
    call("POST", f"/pipelines/{name}/start", params={"initial": "running"})
    wait(f"{name} to run", lambda: status(name)[0] == "Running", 300)
    log(f"{name}: running")


def status(name):
    p = get_json(f"/pipelines/{name}")
    return p.get("deployment_status"), p.get("deployment_error")


def ingest(name, table, updates):
    """Send updates in one request; True once processed, False if the pipeline stopped."""
    raw = call("POST", f"/pipelines/{name}/ingress/{table}", json.dumps(updates).encode(),
               params={"format": "json", "update_format": "insert_delete", "array": "true"})
    token = json.loads(raw)["token"]

    def done():
        st, err = status(name)
        if st != "Running":
            return ("stopped", st, err)
        r = get_json(f"/pipelines/{name}/completion_status", params={"token": token}, timeout=10)
        return ("done",) if r.get("status") == "complete" else None

    out = wait(f"{name} to process the request", done, 120, 0.5)
    if out[0] == "stopped":
        log(f"{name}: the pipeline stopped ({out[1]}): {json.dumps(out[2])[:600]}")
        return False
    return True


def query(name, sql):
    raw = call("GET", f"/pipelines/{name}/query", params={"sql": sql, "format": "json"}, timeout=120)
    raw = raw.strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


# --- decimal-sum ---------------------------------------------------------------

def decimal_sum():
    """+X, +X, -X in one request: the total is X, which fits DECIMAL(38, 10)."""
    name = "decimal_sum"
    pipeline(name, """CREATE TABLE t (g INT, x DECIMAL(38, 10));
CREATE MATERIALIZED VIEW s AS SELECT g, SUM(x) AS s FROM t GROUP BY g;""")
    x = "9000000000000000000000000000"
    if not ingest(name, "t", [{"insert": {"g": 1, "x": x}}, {"insert": {"g": 1, "x": x}},
                              {"insert": {"g": 1, "x": "-" + x}}]):
        return 1
    rows = query(name, "SELECT g, s FROM s")
    ok = len(rows) == 1 and rows[0]["g"] == 1 and Decimal(str(rows[0]["s"])) == Decimal(x)
    log(f"{name}: SUM = {rows}; expected g=1, s={x}")
    return 0 if ok else 1


# --- grouping-empty ------------------------------------------------------------

COLS = ("status", "cust")
VIEWS = {
    "r": ("GROUP BY ROLLUP(status, cust)", [("status", "cust"), ("status",), ()]),
    "c": ("GROUP BY CUBE(status, cust)", [("status", "cust"), ("status",), ("cust",), ()]),
    "gs": ("GROUP BY GROUPING SETS ((status), (cust), ())", [("status",), ("cust",), ()]),
}


def sql_answer(rows, sets):
    """What SQL returns for SELECT status, cust, COUNT(*) n, SUM(amount) s over
    these grouping sets: one row per group of each set, and for the empty set
    exactly one row, even over no rows (COUNT 0, SUM NULL)."""
    out = []
    for gset in sets:
        groups = {}
        for r in rows:
            groups.setdefault(tuple(r[c] for c in gset), []).append(r)
        if not gset and not groups:
            groups[()] = []
        for key, members in groups.items():
            row = {c: None for c in COLS}
            row.update(dict(zip(gset, key)))
            row["n"] = len(members)
            row["s"] = sum(m["amount"] for m in members) if members else None
            out.append(row)
    return out


def bag(rows):
    return sorted(json.dumps({k: r.get(k) for k in COLS + ("n", "s")}, sort_keys=True) for r in rows)


def grouping_empty():
    name = "grouping_empty"
    views = "\n".join(
        f"CREATE MATERIALIZED VIEW {v} AS SELECT status, cust, COUNT(*) AS n, SUM(amount) AS s FROM orders {g};"
        for v, (g, _) in VIEWS.items())
    pipeline(name, "CREATE TABLE orders (id INT NOT NULL, cust INT, amount INT NOT NULL, status VARCHAR);\n" + views
             + "\nCREATE MATERIALIZED VIEW plain AS SELECT COUNT(*) AS n, SUM(amount) AS s FROM orders;")
    row = {"id": 1, "cust": 2, "amount": 5, "status": "paid"}
    steps = [("empty, fresh", None, []), ("one row", [{"insert": row}], [row]),
             ("empty again", [{"delete": row}], [])]
    violations = 0
    for label, updates, table in steps:
        if updates and not ingest(name, "orders", updates):
            raise SystemExit(f"{name} stopped at step {label}")
        # The control: a plain aggregate over an empty table is one row in Feldera too.
        plain = query(name, "SELECT n, s FROM plain")
        if len(plain) != 1:
            raise SystemExit(f"control failed at {label}: plain aggregate returned {plain}")
        for v, (_, sets) in VIEWS.items():
            got, want = query(name, f"SELECT status, cust, n, s FROM {v}"), sql_answer(table, sets)
            same = bag(got) == bag(want)
            violations += 0 if same else 1
            log(f"{label:12} {v:2} {'matches' if same else 'DIFFERS'}: Feldera {bag(got)}"
                + ("" if same else f"; SQL {bag(want)}"))
    return violations


if __name__ == "__main__":
    cmds = {"decimal-sum": decimal_sum, "grouping-empty": grouping_empty}
    if len(sys.argv) != 2 or sys.argv[1] not in cmds:
        raise SystemExit("usage: feldera_check.py decimal-sum|grouping-empty")
    print(cmds[sys.argv[1]]())
