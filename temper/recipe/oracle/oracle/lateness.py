"""late.*: views over LATENESS tables still equal the batch query.

Feldera's time series guide promises that "the output of the program with
lateness annotations is guaranteed to be identical to outputs of the same
program without annotations" when inputs respect their bounds. The workload
moves time forward and never sends a row, or deletes one, more than the
bound behind the latest timestamp seen. Both reads of each view are compared
with DuckDB running the query without any lateness, at 4 workers and again
at 1 and 8 (the late subjects carry no worker claims; the reruns only add
reads).
"""
import datetime
import random

from . import api, views as base

NAME = "lateness"
BOUND = datetime.timedelta(hours=1)

# Columns as DuckDB reads them; program() adds Feldera's LATENESS.
TABLES = {
    "events": "id INT NOT NULL, k INT NOT NULL, v INT NOT NULL, ts TIMESTAMP NOT NULL",
    "prices": "k INT NOT NULL, ts TIMESTAMP NOT NULL, p INT NOT NULL",
    "dims": "k INT NOT NULL, label VARCHAR",
}
LATE = {"events": "ts", "prices": "ts"}

VIEWS = {
    "l_agg": ("late.aggregate", "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY k", None),
    "l_tum": ("late.tumble",
              "SELECT window_start, COUNT(*) AS n, SUM(v) AS s FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' HOUR)) GROUP BY window_start",
              "SELECT time_bucket(INTERVAL '1 hour', ts) AS window_start, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY 1"),
    "l_hop": ("late.hop",
              "SELECT window_start, COUNT(*) AS n FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), INTERVAL '30' MINUTE, INTERVAL '1' HOUR)) GROUP BY window_start",
              "SELECT ws AS window_start, COUNT(*) AS n FROM ("
              " SELECT time_bucket(INTERVAL '30 minutes', ts) AS ws FROM events"
              " UNION ALL SELECT time_bucket(INTERVAL '30 minutes', ts) - INTERVAL '30 minutes' FROM events) GROUP BY ws"),
    "l_win": ("late.window",
              "SELECT id, k, SUM(v) OVER (PARTITION BY k ORDER BY ts RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS running FROM events",
              None),
    "l_join": ("late.join", "SELECT e.id, e.v, d.label FROM events e JOIN dims d ON e.k = d.k", None),
    "l_asof": ("late.asof",
               "SELECT e.id, p.p FROM events e LEFT ASOF JOIN prices p MATCH_CONDITION (e.ts >= p.ts) ON e.k = p.k",
               "SELECT e.id, p.p FROM events e ASOF LEFT JOIN prices p ON e.k = p.k AND e.ts >= p.ts"),
    "l_sub": ("late.subquery",
              "SELECT id, k, v FROM events e WHERE v >= (SELECT MAX(v) FROM events e2 WHERE e2.k = e.k)",
              None),
}


def program():
    parts = []
    for t, cols in TABLES.items():
        if t in LATE:
            col = LATE[t]
            cols = cols.replace(f"{col} TIMESTAMP NOT NULL", f"{col} TIMESTAMP NOT NULL LATENESS INTERVAL '1' HOUR")
        parts.append(f"CREATE TABLE {t} ({cols});")
    for v, (_, body, _) in VIEWS.items():
        parts.append(f"CREATE MATERIALIZED VIEW {v} AS {body};")
    return "\n".join(parts)


START = datetime.datetime(2026, 1, 1)


def fmt(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def steps(seed, n=12):
    """Time moves forward. No row is sent, or deleted, more than 50 minutes
    behind the latest timestamp seen, inside the 1-hour bound."""
    rng = random.Random(seed)
    state = {t: [] for t in TABLES}
    latest = START
    for i in range(n):
        for table in TABLES:
            rows, updates = state[table], []
            for _ in range(rng.randrange(1, 5)):
                floor = latest - datetime.timedelta(minutes=50)
                live = [r for r in rows if table not in LATE or r["ts"] >= fmt(floor)]
                if live and rng.random() < 0.3:
                    r = rng.choice(live)
                    rows.remove(r)
                    updates.append({"delete": r})
                    continue
                if table == "dims":
                    r = {"k": rng.randrange(1, 5), "label": rng.choice(["a", "b", None])}
                else:
                    ts = latest + datetime.timedelta(minutes=10 * rng.randrange(-5, 4))
                    latest = max(latest, ts)
                    if table == "events":
                        r = {"id": rng.randrange(1, 60), "k": rng.randrange(1, 5), "v": rng.randrange(1, 100), "ts": fmt(ts)}
                    else:
                        r = {"k": rng.randrange(1, 5), "ts": fmt(ts), "p": rng.randrange(1, 500)}
                        if any((q["k"], q["ts"]) == (r["k"], r["ts"]) for q in rows):
                            continue  # one price per (k, ts)
                rows.append(r)
                updates.append({"insert": r})
            if updates:
                yield table, updates, {t: list(v) for t, v in state.items()}


def run(subject):
    import sys
    spec = sys.modules[__name__]
    names = [v for v, (subj, _, _) in VIEWS.items() if subj == subject]
    if not names:
        raise SystemExit(f"no views for subject {subject}")
    api.pipeline(NAME, program(), workers=4)
    a, s = base.workload(names, 11, "4 workers", spec)
    out = {"view.lateness_preserves_output": a, "late.changes_add_up": s}
    for w in (1, 8):
        base.restart(w, NAME)
        a, s = base.workload(names, 11 + w, f"{w} workers", spec)
        out["view.lateness_preserves_output"] += a
        out["late.changes_add_up"] += s
    out["sql.lateness"] = sum(out.values())
    return out
