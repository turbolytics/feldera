"""A second program: grouping sets, FILTER, ASOF joins, framed windows, outer
aggregates and aggregates over aggregates. Same tables as views.py, plus a
rates table keyed by (cust, ts) for the ASOF join.
"""

import views

TABLES = views.TABLES + "CREATE TABLE rates (cust INT NOT NULL, ts TIMESTAMP NOT NULL, rate INT NOT NULL);\n"
DUCK_TABLES = views.DUCK_TABLES + "CREATE TABLE rates (cust INT NOT NULL, ts TIMESTAMP NOT NULL, rate INT NOT NULL);\n"

VIEWS = {
    "a_rollup": ("view.grouping",
                 "SELECT status, cust, COUNT(*) AS n, SUM(amount) AS s FROM orders GROUP BY ROLLUP(status, cust)", None),
    "a_cube": ("view.grouping",
               "SELECT c.region, o.status, COUNT(*) AS n FROM orders o JOIN customers c ON o.cust = c.id "
               "GROUP BY CUBE(c.region, o.status)", None),
    "a_sets": ("view.grouping",
               "SELECT status, cust, GROUPING(status) AS gs, COUNT(*) AS n FROM orders "
               "GROUP BY GROUPING SETS ((status), (cust), ())", None),
    "a_filter": ("view.aggregate",
                 "SELECT c.region, COUNT(*) FILTER (WHERE o.status = 'paid') AS paid, "
                 "SUM(o.amount) FILTER (WHERE o.amount > 15) AS big FROM customers c "
                 "LEFT JOIN orders o ON o.cust = c.id GROUP BY c.region", None),
    "a_outer_agg": ("view.aggregate",
                    "SELECT c.id, COUNT(o.id) AS n, SUM(o.amount) AS s FROM customers c "
                    "LEFT JOIN orders o ON o.cust = c.id GROUP BY c.id", None),
    "a_agg_of_agg": ("view.aggregate",
                     "SELECT MAX(s) AS top, MIN(s) AS bottom, COUNT(*) AS groups FROM "
                     "(SELECT cust, SUM(amount) AS s FROM orders GROUP BY cust) t", None),
    "a_bool": ("view.aggregate",
               "SELECT cust, BOOL_AND(amount > 10) AS all_big, BOOL_OR(status IS NULL) AS any_null "
               "FROM orders GROUP BY cust", None),
    "a_distinct_aggs": ("view.aggregate",
                        "SELECT status, COUNT(DISTINCT cust) AS dc, SUM(DISTINCT amount) AS ds FROM orders GROUP BY status",
                        None),
    "a_on_filter": ("view.join_left",
                    "SELECT o.id, c.region FROM orders o LEFT JOIN customers c ON o.cust = c.id AND c.tier > 1", None),
    "a_three_way": ("view.join_inner",
                    "SELECT o.id, c.region, r.rate FROM orders o JOIN customers c ON o.cust = c.id "
                    "JOIN rates r ON r.cust = o.cust", None),
    "a_asof": ("view.asof",
               "SELECT o.id, r.rate FROM orders o LEFT ASOF JOIN rates r "
               "MATCH_CONDITION (o.ts >= r.ts) ON o.cust = r.cust",
               "SELECT o.id, r.rate FROM orders o ASOF LEFT JOIN rates r ON o.cust = r.cust AND o.ts >= r.ts"),
    "a_range_ts": ("view.window",
                   "SELECT id, SUM(amount) OVER (PARTITION BY cust ORDER BY ts "
                   "RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS hour_sum FROM orders",
                   "SELECT id, SUM(amount) OVER (PARTITION BY cust ORDER BY ts "
                   "RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS hour_sum FROM orders"),
    "a_range_num": ("view.window",
                    "SELECT id, COUNT(*) OVER (PARTITION BY cust ORDER BY amount "
                    "RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS near FROM orders", None),
    "a_first": ("view.window",
                "SELECT id, FIRST_VALUE(amount) OVER (PARTITION BY cust ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_amt FROM orders", None),
    "a_rownum": ("view.window",
                 "SELECT id, cust, rn FROM (SELECT id, cust, ROW_NUMBER() OVER "
                 "(PARTITION BY cust ORDER BY amount DESC, id) AS rn FROM orders) t WHERE rn <= 2", None),
    "a_max_min_win": ("view.window",
                      "SELECT id, MAX(amount) OVER (PARTITION BY status ORDER BY id "
                      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS hi FROM orders", None),
    "a_exists_agg": ("view.subquery",
                     "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust = c.id "
                     "GROUP BY o.cust HAVING SUM(o.amount) > 40)", None),
    "a_scalar": ("view.subquery",
                 "SELECT o.id FROM orders o WHERE o.amount > (SELECT COUNT(*) FROM customers c WHERE c.id = o.cust)",
                 None),
    "a_except_all": ("view.setop",
                     "SELECT cust AS k FROM orders EXCEPT ALL SELECT id AS k FROM customers", None),
    "a_intersect_all": ("view.setop",
                        "SELECT cust AS k FROM orders INTERSECT ALL SELECT id AS k FROM customers", None),
}


def program():
    return "\n".join([TABLES] + [f"CREATE MATERIALIZED VIEW {n} AS {s};" for n, (_, s, _) in VIEWS.items()])


def oracle_sql(name):
    _, sql, duck = VIEWS[name]
    return duck or sql
