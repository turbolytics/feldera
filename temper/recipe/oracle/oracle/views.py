"""view.*: every materialized view equals its query run from scratch.

One program holds a view per subject (some subjects have two). A seeded
workload inserts and deletes rows in single requests, including draining a
table to empty. Before the first request and after each one, two reads of
each view are compared with DuckDB running the same query over the oracle's
own copy of the tables:

- the ad-hoc SELECT (view.matches_batch), and
- the view's HTTP change stream, summed from an empty start
  (view.changes_add_up).

The workload then runs again at 1 and at 8 workers (workers.contract).
"""
import datetime
import decimal
import json
import random
import threading
import time
import urllib.request

import sys

import duckdb

from . import api

NAME = "views"

TABLES = {
    "orders": "id INT NOT NULL, cust INT, amount INT NOT NULL, status VARCHAR, ts TIMESTAMP NOT NULL",
    "customers": "cid INT NOT NULL, region VARCHAR, tier INT NOT NULL",
    "quotes": "cust INT NOT NULL, ts TIMESTAMP NOT NULL, price INT NOT NULL",
    "edges": "src INT NOT NULL, dst INT NOT NULL",
}

# Each view: (subject, Feldera SQL body, DuckDB query). Where the two
# dialects differ, the DuckDB query states the same meaning its own way.
VIEWS = {
    "agg": ("view.aggregate",
            "SELECT status, COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS mn, MAX(amount) AS mx FROM orders GROUP BY status",
            None),
    "roll": ("view.grouping",
             "SELECT status, cust, COUNT(*) AS n, SUM(amount) AS s FROM orders GROUP BY ROLLUP(status, cust)",
             None),
    "inner_j": ("view.join_inner",
                "SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.cid",
                None),
    "left_j": ("view.join_left",
               "SELECT o.id, c.region, c.tier FROM orders o LEFT JOIN customers c ON o.cust = c.cid",
               None),
    "full_j": ("view.join_full",
               "SELECT o.id, c.cid, c.region FROM orders o FULL OUTER JOIN customers c ON o.cust = c.cid",
               None),
    "uni": ("view.setop",
            "SELECT cust AS k FROM orders WHERE cust IS NOT NULL UNION SELECT cid AS k FROM customers",
            None),
    "exc": ("view.setop",
            "SELECT cid AS k FROM customers EXCEPT SELECT cust AS k FROM orders WHERE cust IS NOT NULL",
            None),
    "sub": ("view.subquery",
            "SELECT id, amount FROM orders WHERE cust IN (SELECT cid FROM customers WHERE tier > 1)",
            None),
    "ex_sub": ("view.subquery",
               "SELECT cid FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.cid AND o.amount > 50)",
               None),
    "win": ("view.window",
            "SELECT id, cust, SUM(amount) OVER (PARTITION BY cust ORDER BY ts RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS running FROM orders",
            None),
    "tum": ("view.tumble",
            "SELECT window_start, COUNT(*) AS n, SUM(amount) AS s FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '1' HOUR)) GROUP BY window_start",
            "SELECT time_bucket(INTERVAL '1 hour', ts) AS window_start, COUNT(*) AS n, SUM(amount) AS s FROM orders GROUP BY 1"),
    "hop": ("view.hop",
            "SELECT window_start, COUNT(*) AS n FROM TABLE(HOP(TABLE orders, DESCRIPTOR(ts), INTERVAL '30' MINUTE, INTERVAL '1' HOUR)) GROUP BY window_start",
            # A 1-hour window every 30 minutes: each row falls in the window
            # starting at its 30-minute bucket and in the one before it.
            "SELECT ws AS window_start, COUNT(*) AS n FROM ("
            " SELECT time_bucket(INTERVAL '30 minutes', ts) AS ws FROM orders"
            " UNION ALL SELECT time_bucket(INTERVAL '30 minutes', ts) - INTERVAL '30 minutes' FROM orders) GROUP BY ws"),
    "asof_j": ("view.asof",
               "SELECT o.id, q.price FROM orders o LEFT ASOF JOIN quotes q MATCH_CONDITION (o.ts >= q.ts) ON o.cust = q.cust",
               "SELECT o.id, q.price FROM orders o ASOF LEFT JOIN quotes q ON o.cust = q.cust AND o.ts >= q.ts"),
    "reach": ("view.recursive", None,
              "WITH RECURSIVE reach(src, dst) AS (SELECT src, dst FROM edges"
              " UNION SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src) SELECT src, dst FROM reach"),
}

RECURSIVE = """DECLARE RECURSIVE VIEW reach (src INT NOT NULL, dst INT NOT NULL);
CREATE MATERIALIZED VIEW reach AS
  SELECT src, dst FROM edges UNION SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src;"""


def program():
    parts = [f"CREATE TABLE {t} ({cols});" for t, cols in TABLES.items()]
    for v, (_, body, _) in VIEWS.items():
        if body is not None:
            parts.append(f"CREATE MATERIALIZED VIEW {v} AS {body};")
    parts.append(RECURSIVE)
    return "\n".join(parts)


# --- the workload ------------------------------------------------------------

BASE = datetime.datetime(2026, 1, 1)


def stamp(rng):
    return (BASE + datetime.timedelta(minutes=10 * rng.randrange(0, 36))).strftime("%Y-%m-%d %H:%M:%S")


def new_row(rng, table, state):
    if table == "orders":
        return {"id": rng.randrange(1, 40), "cust": rng.choice([None, 1, 2, 3, 4, 5]), "amount": rng.randrange(1, 100),
                "status": rng.choice(["paid", "open", "void", None]), "ts": stamp(rng)}
    if table == "customers":
        return {"cid": rng.randrange(1, 7), "region": rng.choice(["eu", "us", None]), "tier": rng.randrange(1, 4)}
    if table == "quotes":
        # One quote per (cust, ts), so the latest quote at or before a time is unique.
        taken = {(q["cust"], q["ts"]) for q in state["quotes"]}
        for _ in range(20):
            q = {"cust": rng.randrange(1, 6), "ts": stamp(rng), "price": rng.randrange(1, 500)}
            if (q["cust"], q["ts"]) not in taken:
                return q
        return None
    return {"src": rng.randrange(0, 7), "dst": rng.randrange(0, 7)}


def steps(seed, n=10):
    """Yield (table, updates) requests, applying them to a model of the tables."""
    rng = random.Random(seed)
    state = {t: [] for t in TABLES}
    for i in range(n):
        for table in TABLES:
            rows = state[table]
            updates = []
            if i == n // 2 and table == "orders":
                updates = [{"delete": r} for r in rows]  # drain to empty
                rows.clear()
            else:
                for _ in range(rng.randrange(1, 5)):
                    if rows and rng.random() < 0.35:
                        r = rows.pop(rng.randrange(len(rows)))
                        updates.append({"delete": r})
                    else:
                        r = new_row(rng, table, state)
                        if r is not None:
                            rows.append(r)
                            updates.append({"insert": r})
            if updates:
                yield table, updates, {t: list(v) for t, v in state.items()}


# --- reading and comparing ---------------------------------------------------

def norm(v):
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, str) and len(v) >= 19 and v[4] == "-" and v[10] in "T ":
        return v[:19].replace("T", " ")
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral() else float(v)
    return v


def bag(rows):
    return sorted(json.dumps({k: norm(v) for k, v in r.items()}, sort_keys=True, default=str) for r in rows)


def reference(state, view, spec=None):
    spec = spec or sys.modules[__name__]
    db = duckdb.connect()
    for t, cols in spec.TABLES.items():
        db.execute(f"CREATE TABLE {t} ({cols.replace(' NOT NULL', '')})")
        names = [c.split()[0] for c in cols.split(",")]
        for r in state[t]:
            db.execute(f"INSERT INTO {t} VALUES ({', '.join('?' for _ in names)})", [r[c] for c in names])
    _, body, duck = spec.VIEWS[view]
    cur = db.execute(duck or body)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


class Stream(threading.Thread):
    """A view's HTTP change stream, summed into a bag of rows."""

    def __init__(self, view, name=NAME, normalize=True):
        super().__init__(daemon=True)
        self.view, self.name, self.rows, self.lock, self.err = view, name, {}, threading.Lock(), None
        # How many delivery messages carried changes, for transactions.
        self.messages = 0
        # normalize keys rows the way the views compare them (whole-second
        # timestamps); the types check needs the exact values instead.
        self.key = (lambda v: norm(v)) if normalize else (lambda v: v)

    def run(self):
        url = f"{api.API}/pipelines/{self.name}/egress/{self.view}?format=json&array=true"
        try:
            req = urllib.request.Request(url, data=b"", method="POST")
            with urllib.request.urlopen(req, timeout=3600) as resp:
                for line in resp:
                    line = line.strip()
                    if not line:
                        continue
                    msg = json.loads(line, parse_float=decimal.Decimal)
                    if msg.get("json_data"):
                        with self.lock:
                            self.messages += 1
                    for change in msg.get("json_data") or []:
                        with self.lock:
                            for kind, sign in (("insert", 1), ("delete", -1)):
                                if kind in change:
                                    key = json.dumps({k: self.key(v) for k, v in change[kind].items()}, sort_keys=True, default=str)
                                    self.rows[key] = self.rows.get(key, 0) + sign
        except Exception as e:  # the stream ends when the pipeline stops
            self.err = e

    def bag(self):
        with self.lock:
            out = []
            for key, n in self.rows.items():
                out.extend([key] * max(n, 0))
            neg = [k for k, n in self.rows.items() if n < 0]
        return sorted(out), neg


def compare(views, state, streams, label, spec=None):
    spec = spec or sys.modules[__name__]
    """Mismatches by read: (adhoc, stream) counts over views."""
    adhoc = stream = 0
    for v in views:
        want = bag(reference(state, v, spec))
        got = bag(api.query(spec.NAME, f"SELECT * FROM {v}"))
        if got != want:
            adhoc += 1
            api.log(f"{label}: {v} ad-hoc DIFFERS: Feldera {got[:6]} vs batch {want[:6]}")
        s = streams.get(v)
        if s is not None:
            deadline = time.time() + 10
            while True:
                got_s, neg = s.bag()
                if (got_s == want and not neg) or time.time() > deadline:
                    break
                time.sleep(0.2)
            if got_s != want or neg:
                stream += 1
                api.log(f"{label}: {v} change stream DIFFERS: sums to {got_s[:6]} (negative {neg[:3]}) vs batch {want[:6]}")
    return adhoc, stream


def restart(workers, name=NAME):
    api.call("POST", f"/pipelines/{name}/stop", params={"force": "true"})
    api.wait("the pipeline to stop", lambda: api.status(name)[0] == "Stopped", 300)
    api.call("POST", f"/pipelines/{name}/clear")
    api.wait("storage to clear", lambda: api.get_json(f"/pipelines/{name}").get("storage_status") in (None, "Cleared"), 300)
    api.call("PATCH", f"/pipelines/{name}", {"runtime_config": {"workers": workers}})
    api.call("POST", f"/pipelines/{name}/start", params={"initial": "running"})
    api.wait("the pipeline to run", lambda: api.status(name)[0] == "Running", 300)


def workload(views, seed, label, spec=None):
    spec = spec or sys.modules[__name__]
    streams = {v: Stream(v, spec.NAME) for v in views}
    for s in streams.values():
        s.start()
    time.sleep(1)
    adhoc, stream = compare(views, {t: [] for t in spec.TABLES}, streams, f"{label} empty", spec)
    for i, (table, updates, state) in enumerate(spec.steps(seed)):
        if not api.ingest(spec.NAME, table, updates):
            raise SystemExit(f"{spec.NAME} stopped at {label} step {i}")
        a, s = compare(views, state, streams, f"{label} step {i} ({table})", spec)
        adhoc, stream = adhoc + a, stream + s
    return adhoc, stream


def run(subject):
    views = [v for v, (subj, _, _) in VIEWS.items() if subj == subject]
    if not views:
        raise SystemExit(f"no views for subject {subject}")
    api.pipeline(NAME, program(), workers=4)
    feature = {"view.aggregate": "sql.aggregate", "view.grouping": "sql.grouping", "view.join_inner": "sql.join",
               "view.join_left": "sql.join", "view.join_full": "sql.join", "view.setop": "sql.setop",
               "view.subquery": "sql.subquery", "view.window": "sql.window", "view.tumble": "sql.tumble",
               "view.hop": "sql.hop", "view.asof": "sql.asof", "view.recursive": "sql.recursion"}[subject]
    out = {}
    a, s = workload(views, 1, "4 workers")
    out["view.matches_batch"], out["view.changes_add_up"] = a, s
    for w in (1, 8):
        restart(w)
        a, s = workload(views, 1 + w, f"{w} workers")
        out[f"workers.contract/w{w}"] = a + s
    out[feature] = sum(out.values())
    out["runtime.workers"] = out["workers.contract/w1"] + out["workers.contract/w8"]
    return out
