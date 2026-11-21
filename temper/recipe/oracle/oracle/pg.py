"""sink.postgres: in materialized mode the PostgreSQL output connector keeps
the target table a snapshot of the view, after every completion token,
including after PostgreSQL pauses (PostgreSQL output connector).

    python -m oracle pg setup|steps|feed
"""
import random
import time

import psycopg

from . import api

NAME = "pg_sink"
URI = "postgres://postgres:temper@postgres:5432/sink"


def pg_rows():
    with psycopg.connect(URI) as c:
        return sorted(tuple(r) for r in c.execute("SELECT k, n, s FROM mirror"))


def view_rows():
    return sorted((r["k"], r["n"], r["s"]) for r in api.query(NAME, "SELECT k, n, s FROM agg_v"))


def setup():
    api.wait("PostgreSQL", lambda: psycopg.connect(URI).close() or True, 120, 2)
    with psycopg.connect(URI, autocommit=True) as c:
        c.execute("CREATE TABLE IF NOT EXISTS mirror (k INT PRIMARY KEY, n BIGINT NOT NULL, s BIGINT NOT NULL)")
    api.pipeline(NAME, f"""CREATE TABLE t (id INT NOT NULL, k INT NOT NULL, v INT NOT NULL);
CREATE MATERIALIZED VIEW agg_v WITH ('connectors' = '[{{
  "index": "agg_k",
  "transport": {{"name": "postgres_output", "config": {{"uri": "{URI}", "table": "mirror", "mode": "materialized"}}}}}}]')
  AS SELECT k, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY k;
CREATE INDEX agg_k ON agg_v(k);""")
    return 0


def steps(seed=None):
    """Six requests of inserts and deletes; after each token the PostgreSQL
    table must equal the view. Returns the steps where it did not."""
    rng = random.Random(seed or time.time_ns())
    live = {}
    base = rng.randrange(10_000, 1_000_000)
    bad = 0
    for i in range(6):
        updates = []
        for _ in range(rng.randrange(2, 8)):
            if live and rng.random() < 0.35:
                rid = rng.choice(sorted(live))
                updates.append({"delete": live.pop(rid)})
            else:
                rid = base + rng.randrange(0, 10_000)
                if rid in live:
                    continue
                live[rid] = {"id": rid, "k": rng.randrange(1, 6), "v": rng.randrange(1, 100)}
                updates.append({"insert": live[rid]})
        if not updates or not api.ingest(NAME, "t", updates):
            continue
        want, got = view_rows(), pg_rows()
        if got != want:
            bad += 1
            api.log(f"step {i}: PostgreSQL DIFFERS from the view after the token: {got[:5]} vs {want[:5]}")
    api.log(f"steps: {bad} of 6 tokens left PostgreSQL different from the view")
    return bad


def feed():
    """Insert rows steadily until stopped, so writes are in flight when
    PostgreSQL pauses."""
    i = 2_000_000
    while True:
        api.send(NAME, "t", [{"insert": {"id": i, "k": i % 5 + 1, "v": i % 50}}])
        i += 1
        time.sleep(0.1)


def run(arg):
    return {"setup": setup, "steps": steps, "feed": feed}[arg]()
