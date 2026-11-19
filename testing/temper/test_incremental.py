"""Incremental views equal the batch query, after every input step.

Feldera promises that "the state of the views always corresponds to what you'd
get if you ran the queries in a batch system for the same input" (What is
Feldera). Each seed drives one pipeline with random inserts, deletes and
updates over two tables, including NULLs, duplicate rows, dangling keys and
steps that empty a table. After each step completes (completion token), every
materialized view is read two ways and compared with DuckDB running the same
query from scratch over the same rows:

- the view's contents, from an ad-hoc SELECT;
- the sum of the changes the view streamed to an HTTP output since start.
"""

import datetime as dt
import random
from collections import Counter

import duckdb
import pytest

import harness
import views

SEEDS = [11, 23, 37]
STEPS = 40
PIPELINE = "temper_views"
BASE = dt.datetime(2026, 1, 1, 0, 0, 0)


class Model:
    """The tables as the test believes them to be."""

    def __init__(self, rng):
        self.rng = rng
        self.customers = Counter()
        self.orders = {}
        self.next_id = 1

    def customer(self):
        r = self.rng
        return (r.randint(1, 12), r.choice(["eu", "us", "ap", None]), r.choice([1, 2, 3, None]))

    def order(self, oid=None):
        r = self.rng
        if oid is None:
            oid, self.next_id = self.next_id, self.next_id + 1
        return (oid, r.choice(list(range(1, 15)) + [None]), r.randint(1, 30), r.choice(["paid", "open", None]),
                BASE + dt.timedelta(minutes=r.randint(0, 360)))

    def step(self, i):
        """Returns (customer updates, order updates) for step i and applies them."""
        r = self.rng
        cu, ou = [], []
        if i % 13 == 12:  # empty the orders table
            for o in list(self.orders.values()):
                ou.append({"delete": orow(o)})
            self.orders.clear()
            return cu, ou
        n = r.choice([1, 2, 5, 10, 40])
        for _ in range(n):
            x = r.random()
            if x < 0.45 or not self.orders:
                o = self.order()
                self.orders[o[0]] = o
                ou.append({"insert": orow(o)})
            elif x < 0.65:
                o = self.orders.pop(r.choice(list(self.orders)))
                ou.append({"delete": orow(o)})
            elif x < 0.75:  # update: same id, new values
                old = self.orders[r.choice(list(self.orders))]
                new = self.order(old[0])
                self.orders[old[0]] = new
                ou += [{"delete": orow(old)}, {"insert": orow(new)}]
            elif x < 0.9 or not self.customers:
                c = self.customer()
                self.customers[c] += 1
                cu.append({"insert": crow(c)})
            else:
                c = r.choice(sorted(self.customers, key=repr))
                self.customers[c] -= 1
                if not self.customers[c]:
                    del self.customers[c]
                cu.append({"delete": crow(c)})
        return cu, ou

    def load(self, db):
        db.execute("DELETE FROM customers")
        db.execute("DELETE FROM orders")
        rows = [c for c, k in self.customers.items() for _ in range(k)]
        if rows:
            db.executemany("INSERT INTO customers VALUES (?, ?, ?)", rows)
        if self.orders:
            db.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", list(self.orders.values()))


def crow(c):
    return {"id": c[0], "region": c[1], "tier": c[2]}


def orow(o):
    return {"id": o[0], "cust": o[1], "amount": o[2], "status": o[3], "ts": o[4].strftime("%Y-%m-%d %H:%M:%S")}


def duck_rows(db, sql):
    cur = db.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


_results = None


def results():
    """Runs every seed once. Returns {view: first mismatch or None} for both reads."""
    global _results
    if _results is not None:
        return _results
    harness.create(PIPELINE, views.program())
    content = {v: None for v in views.VIEWS}
    changes = {v: None for v in views.VIEWS}
    for seed in SEEDS:
        harness.reset(PIPELINE)
        harness.start(PIPELINE)
        streams = {v: harness.ChangeStream(PIPELINE, v) for v in views.VIEWS}
        for s in streams.values():
            s.start()
            s.ready.wait(30)
        db = duckdb.connect()
        db.execute(views.DUCK_TABLES)
        model = Model(random.Random(seed))
        try:
            for i in range(STEPS):
                cu, ou = model.step(i)
                tokens = []
                if cu:
                    tokens.append(harness.ingest(PIPELINE, "customers", cu))
                if ou:
                    tokens.append(harness.ingest(PIPELINE, "orders", ou))
                for t in tokens:
                    harness.wait_complete(PIPELINE, t)
                model.load(db)
                for v in views.VIEWS:
                    want = harness.bag(duck_rows(db, views.oracle_sql(v)))
                    if content[v] is None:
                        got = harness.bag(harness.query(PIPELINE, f"SELECT * FROM {v}"))
                        d = harness.diff(want, got)
                        if d:
                            content[v] = {"seed": seed, "step": i, **d}
                    if changes[v] is None:
                        s = streams[v]
                        d = harness.diff(want, s.snapshot()) if not s.error else {"error": repr(s.error)}
                        if d:
                            changes[v] = {"seed": seed, "step": i, **d}
        finally:
            for s in streams.values():
                s.close()
    harness.stop(PIPELINE, force=True)
    _results = {"content": content, "changes": changes}
    return _results


def params(invariant):
    return [pytest.param(v, id=v, marks=[pytest.mark.covers(invariant, subject=subj)])
            for v, (subj, _, _) in views.VIEWS.items()]


@pytest.mark.parametrize("view", params("view.matches_batch"))
def test_view_matches_batch(view):
    bad = results()["content"][view]
    assert bad is None, f"{view} differs from the batch query: {bad}"


@pytest.mark.parametrize("view", params("view.changes_add_up"))
def test_changes_add_up_to_batch(view):
    bad = results()["changes"][view]
    assert bad is None, f"{view}'s change stream differs from the batch query: {bad}"
