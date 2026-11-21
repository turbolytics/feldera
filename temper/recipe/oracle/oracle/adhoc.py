"""adhoc.datafusion: ad-hoc queries read a consistent snapshot, and only of
materialized tables and views (Ad-hoc SQL Queries)."""
import threading
import time

from . import api

NAME = "adhoc"
SQL = """
CREATE TABLE s (k INT NOT NULL, v INT NOT NULL) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW s_total AS SELECT SUM(v) AS total FROM s;
CREATE MATERIALIZED VIEW s_count AS SELECT COUNT(*) AS n FROM s;
CREATE TABLE hidden (k INT NOT NULL);
CREATE VIEW hidden_v AS SELECT k FROM hidden;
"""
# One query reads the table and both views; in a consistent snapshot they agree.
SNAPSHOT = ("SELECT (SELECT COALESCE(SUM(v), 0) FROM s) AS a, (SELECT COALESCE(MAX(total), 0) FROM s_total) AS b,"
            " (SELECT COUNT(*) FROM s) AS c, (SELECT COALESCE(MAX(n), 0) FROM s_count) AS d")


def torn(label, n):
    bad = 0
    for _ in range(n):
        r = api.query(NAME, SNAPSHOT)[0]
        if r["a"] != r["b"] or r["c"] != r["d"]:
            bad += 1
            api.log(f"{label}: TORN snapshot {r}")
    return bad


def run(subject):
    api.pipeline(NAME, SQL)
    out = {}
    stop = threading.Event()

    def feed():
        i = 0
        while not stop.is_set():
            api.send(NAME, "s", [{"insert": {"k": i, "v": i % 7 + 1}} for i in range(i, i + 20)])
            i += 20
            time.sleep(0.05)

    t = threading.Thread(target=feed, daemon=True)
    t.start()
    bad = torn("while inputs stream", 60)
    stop.set()
    t.join()
    api.call("POST", f"/pipelines/{NAME}/pause")
    api.wait("the pipeline to pause", lambda: api.status(NAME)[0] == "Paused", 60)
    bad += torn("while paused", 20)
    api.call("POST", f"/pipelines/{NAME}/resume")
    out["adhoc.consistent_snapshot"] = bad
    api.log(f"consistent snapshot: {bad} torn reads out of 80")

    visible = 0
    for name in ("hidden", "hidden_v"):
        try:
            rows = api.query(NAME, f"SELECT * FROM {name}")
            visible += 1
            api.log(f"{name} is not materialized, but an ad-hoc SELECT returned {rows[:3]}")
        except api.HTTPError as e:
            api.log(f"{name} is not materialized; the ad-hoc SELECT was refused ({e.status})")
    try:
        api.query(NAME, "SELECT * FROM s_total")
    except api.HTTPError as e:
        visible += 1
        api.log(f"the materialized view s_total was refused: {e}")
    out["adhoc.only_materialized"] = visible
    out["sql.adhoc"] = sum(out.values())
    return out
