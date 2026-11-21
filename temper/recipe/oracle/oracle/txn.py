"""txn.http: transactions over HTTP ingress (Transactions docs).

Inside a transaction the pipeline ingests without producing output; the
commit delivers one batch per view; afterwards every view matches all
inputs, those in the transaction included.
"""
import json
import time

from . import api
from .views import Stream, bag

NAME = "txn"
SQL = """
CREATE TABLE t (k INT NOT NULL, v INT NOT NULL);
CREATE MATERIALIZED VIEW total AS SELECT SUM(v) AS s, COUNT(*) AS n FROM t;
CREATE MATERIALIZED VIEW per_k AS SELECT k, SUM(v) AS s FROM t GROUP BY k;
"""


def stats():
    try:
        return api.get_json(f"/pipelines/{NAME}/stats")
    except api.HTTPError as e:
        return {"error": str(e)}


def transaction_state():
    g = stats().get("global_metrics", {})
    return {k: v for k, v in g.items() if "transaction" in k}


def run(subject):
    api.pipeline(NAME, SQL)
    streams = {v: Stream(v, NAME) for v in ("total", "per_k")}
    for s in streams.values():
        s.start()
    time.sleep(1)
    rows = []
    # Before the transaction: a committed baseline.
    base = [{"insert": {"k": 1, "v": 10}}]
    api.ingest(NAME, "t", base)
    rows.append({"k": 1, "v": 10})
    time.sleep(1)
    before = {v: s.messages for v, s in streams.items()}

    out = {}
    tid = api.call("POST", f"/pipelines/{NAME}/start_transaction")
    api.log(f"start_transaction -> {tid.strip()}; state {transaction_state()}")
    for i in range(5):
        batch = [{"insert": {"k": 1 + i % 3, "v": i + 1}}, {"insert": {"k": 4, "v": 100 + i}}]
        token, err = api.send(NAME, "t", batch)
        if err:
            raise SystemExit(f"ingest inside a transaction was rejected: {err}")
        rows.extend(u["insert"] for u in batch)
    time.sleep(3)
    during = {v: s.messages - before[v] for v, s in streams.items()}
    out["txn.no_output_while_open"] = sum(during.values())
    api.log(f"while open: messages per view {during} (want 0)")

    committed_at = {v: s.messages for v, s in streams.items()}
    api.log(f"commit_transaction -> {api.call('POST', f'/pipelines/{NAME}/commit_transaction').strip()}")
    want = {
        "total": bag([{"s": sum(r["v"] for r in rows), "n": len(rows)}]),
        "per_k": bag([{"k": k, "s": sum(r["v"] for r in rows if r["k"] == k)} for k in sorted({r["k"] for r in rows})]),
    }

    def committed():
        return all(bag(api.query(NAME, f"SELECT * FROM {v}")) == want[v] for v in want)

    deadline = time.time() + 60
    while not committed() and time.time() < deadline:
        time.sleep(0.5)
    time.sleep(2)
    after = {v: s.messages - committed_at[v] for v, s in streams.items()}
    out["txn.commit_is_one_batch"] = sum(1 for n in after.values() if n != 1)
    api.log(f"after commit: messages per view {after} (want 1 each); state {transaction_state()}")

    mismatched = [v for v in want if bag(api.query(NAME, f"SELECT * FROM {v}")) != want[v]]
    stream_mismatched = [v for v, s in streams.items() if s.bag()[0] != want[v]]
    out["txn.matches_batch_after_commit"] = len(mismatched) + len(stream_mismatched)
    api.log(f"after commit: ad-hoc differs {mismatched}, stream differs {stream_mismatched}")
    out["transactions"] = sum(out.values())
    return out
