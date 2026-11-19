"""A third program: time-series views over tables with LATENESS.

The docs promise: "Assuming lateness annotations are accurate, i.e., input
records do not arrive more than lateness time units out of order, the output of
the program with lateness annotations is guaranteed to be identical to outputs
of the same program without annotations" (Time series guide). Lateness lets
Feldera discard old state, so a garbage-collection bug shows up as a view that
differs from the batch query. The generator keeps every insert and delete well
inside the bound.
"""

TABLES = """
CREATE TABLE events (id INT NOT NULL, k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
CREATE TABLE clicks (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
"""

DUCK_TABLES = """
CREATE TABLE events (id INT NOT NULL, k INT, v INT NOT NULL, ts TIMESTAMP NOT NULL);
CREATE TABLE clicks (id INT NOT NULL, k INT, ts TIMESTAMP NOT NULL);
"""

TB = "time_bucket(INTERVAL '10 minutes', ts)"

VIEWS = {
    "l_tumble": ("view.tumble",
                 "SELECT window_start, COUNT(*) AS n, SUM(v) AS s FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), "
                 "INTERVAL '10' MINUTE)) GROUP BY window_start",
                 f"SELECT {TB} AS window_start, COUNT(*) AS n, SUM(v) AS s FROM events GROUP BY 1"),
    "l_tumble_k": ("view.tumble",
                   "SELECT window_start, k, MAX(v) AS hi, COUNT(DISTINCT v) AS dv FROM TABLE(TUMBLE(TABLE events, "
                   "DESCRIPTOR(ts), INTERVAL '10' MINUTE)) GROUP BY window_start, k",
                   f"SELECT {TB} AS window_start, k, MAX(v) AS hi, COUNT(DISTINCT v) AS dv FROM events GROUP BY 1, 2"),
    "l_hop": ("view.hop",
              "SELECT window_start, COUNT(*) AS n FROM TABLE(HOP(TABLE events, DESCRIPTOR(ts), "
              "INTERVAL '10' MINUTE, INTERVAL '30' MINUTE)) GROUP BY window_start",
              f"SELECT w AS window_start, COUNT(*) AS n FROM (SELECT {TB} AS w FROM events UNION ALL "
              f"SELECT {TB} - INTERVAL '10 minutes' FROM events UNION ALL "
              f"SELECT {TB} - INTERVAL '20 minutes' FROM events) GROUP BY w"),
    "l_interval_join": ("view.join_inner",
                        "SELECT e.id AS eid, c.id AS cid FROM events e JOIN clicks c ON e.k = c.k "
                        "AND c.ts BETWEEN e.ts - INTERVAL 5 MINUTES AND e.ts + INTERVAL 5 MINUTES", None),
    "l_lag": ("view.window",
              "SELECT id, LAG(v) OVER (PARTITION BY k ORDER BY ts) AS prev FROM events", None),
    "l_range": ("view.window",
                "SELECT id, SUM(v) OVER (PARTITION BY k ORDER BY ts "
                "RANGE BETWEEN INTERVAL 15 MINUTES PRECEDING AND CURRENT ROW) AS recent FROM events", None),
    "l_topk": ("view.window",
               "SELECT window_start, id, r FROM (SELECT window_start, id, RANK() OVER "
               "(PARTITION BY window_start ORDER BY v DESC) AS r FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), "
               "INTERVAL '10' MINUTE))) t WHERE r <= 2",
               f"SELECT window_start, id, r FROM (SELECT {TB} AS window_start, id, RANK() OVER "
               f"(PARTITION BY {TB} ORDER BY v DESC) AS r FROM events) t WHERE r <= 2"),
    "l_asof": ("view.asof",
               "SELECT e.id, c.id AS cid FROM events e LEFT ASOF JOIN clicks c MATCH_CONDITION (e.ts >= c.ts) "
               "ON e.k = c.k",
               "SELECT e.id, c.id AS cid FROM events e ASOF LEFT JOIN clicks c ON e.k = c.k AND e.ts >= c.ts"),
    "l_no_click": ("view.subquery",
                   "SELECT e.id FROM events e WHERE NOT EXISTS (SELECT 1 FROM clicks c WHERE c.k = e.k "
                   "AND c.ts >= e.ts AND c.ts <= e.ts + INTERVAL 10 MINUTES)", None),
    "l_by_k": ("view.aggregate",
               "SELECT k, COUNT(*) AS n, MAX(ts) AS last_ts, MIN(ts) AS first_ts FROM events GROUP BY k", None),
}


def program():
    return "\n".join([TABLES] + [f"CREATE MATERIALIZED VIEW {n} AS {s};" for n, (_, s, _) in VIEWS.items()])


def oracle_sql(name):
    _, sql, duck = VIEWS[name]
    return duck or sql
