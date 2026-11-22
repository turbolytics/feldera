"""Transactions: no output while open, one batch on commit, then batch-equal views.

The transactions page promises:
- "During a transaction, the pipeline ingests incoming data without producing output."
- "Once the commit is complete, the pipeline outputs a batch of updates for every view."
- "after committing a transaction the contents of all [views] matches all the inputs
  received so far, including inputs received as part of the transaction."

Jepsen's Kafka analyses call a transaction whose effects show up in pieces a
torn transaction. Here, each commit must reach every change stream as one
batch (one sequence number), and nothing may arrive while it is open.
"""

import random
import time

import duckdb
import pytest
import requests

import harness
import views
from test_incremental import Model, duck_rows

PIPELINE = "temper_views"
SEED, TXNS = 51, 12
WATCH = ["v_global", "v_agg", "v_join", "v_not_in", "v_topk", "v_tumble"]


def txn_status():
    return requests.get(f"{harness.API}/pipelines/{PIPELINE}/stats", timeout=10).json()["global_metrics"]


_results = None


def results():
    import lanes
    return lanes.part("views", "transactions")


def compute():
    global _results
    if _results is not None:
        return _results
    harness.create(PIPELINE, views.program())
    harness.reset(PIPELINE)
    harness.start(PIPELINE)
    streams = {v: harness.ChangeStream(PIPELINE, v) for v in WATCH}
    for s in streams.values():
        s.start()
        s.ready.wait(30)
    db = duckdb.connect()
    db.execute(views.DUCK_TABLES)
    model = Model(random.Random(SEED))
    out = {"leak": None, "torn": None, "batch": None, "content": None}
    try:
        for t in range(TXNS):
            time.sleep(1)
            before = {v: (s.snapshot(), len(s.batches)) for v, s in streams.items()}
            snap_before = {v: harness.bag(harness.query(PIPELINE, f"SELECT * FROM {v}")) for v in WATCH}
            r = requests.post(f"{harness.API}/pipelines/{PIPELINE}/start_transaction", timeout=30)
            r.raise_for_status()
            tid = r.json()["transaction_id"]
            for i in range(random.Random(t).randint(2, 5)):
                for table, ups in model.step(t * 10 + i).items():
                    if ups:
                        harness.ingest(PIPELINE, table, ups)
            time.sleep(2)  # give a leaky pipeline time to emit
            for v, s in streams.items():
                if out["leak"] is None and len(s.batches) != before[v][1] and s.snapshot() != before[v][0]:
                    out["leak"] = {"txn": t, "view": v, "batches": s.batches[before[v][1]:]}
            cut = {v: len(s.batches) for v, s in streams.items()}
            requests.post(f"{harness.API}/pipelines/{PIPELINE}/commit_transaction",
                          params={"transaction_id": tid}, timeout=30).raise_for_status()
            if not harness.wait_for(lambda: txn_status()["transaction_status"] == "NoTransaction", 120):
                raise TimeoutError(f"transaction {tid} never finished committing")
            time.sleep(2)
            model.load(db)
            for v, s in streams.items():
                want = harness.bag(duck_rows(db, views.oracle_sql(v)))
                new = [b for b in s.batches[cut[v]:] if b[1]]
                if out["torn"] is None and len({b[0] for b in new}) > 1:
                    out["torn"] = {"txn": t, "view": v, "batches": new}
                if out["batch"] is None and harness.diff(want, s.snapshot()):
                    out["batch"] = {"txn": t, "view": v, **harness.diff(want, s.snapshot())}
                got = harness.bag(harness.query(PIPELINE, f"SELECT * FROM {v}"))
                if out["content"] is None and harness.diff(want, got):
                    out["content"] = {"txn": t, "view": v, **harness.diff(want, got)}
            del snap_before
    finally:
        for s in streams.values():
            s.close()
        harness.stop(PIPELINE, force=True)
    _results = out
    return out


@pytest.mark.covers("txn.no_output_while_open", subject="txn.http")
@pytest.mark.covers("transactions")
def test_no_output_while_open():
    assert results()["leak"] is None, f"output escaped an open transaction: {results()['leak']}"


@pytest.mark.covers("txn.commit_is_one_batch", subject="txn.http")
@pytest.mark.covers("transactions")
def test_commit_is_one_batch():
    assert results()["torn"] is None, f"a commit reached a view in pieces: {results()['torn']}"


@pytest.mark.covers("txn.matches_batch_after_commit", subject="txn.http")
@pytest.mark.covers("transactions")
def test_matches_batch_after_commit():
    r = results()
    assert r["content"] is None and r["batch"] is None, f"view: {r['content']}; changes: {r['batch']}"
