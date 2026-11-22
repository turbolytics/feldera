"""The SQL program under test and its batch oracle.

Each view is written once for Feldera and once for DuckDB. Where the two
dialects agree, the text is the same. The DuckDB query runs from scratch over
the current table contents, which is what Feldera promises its incremental
view equals.
"""

TABLES = """
CREATE TABLE customers (id INT NOT NULL, region VARCHAR, tier INT);
CREATE TABLE orders (id INT NOT NULL, cust INT, amount INT NOT NULL, status VARCHAR, ts TIMESTAMP NOT NULL);
"""

DUCK_TABLES = """
CREATE TABLE customers (id INT NOT NULL, region VARCHAR, tier INT);
CREATE TABLE orders (id INT NOT NULL, cust INT, amount INT NOT NULL, status VARCHAR, ts TIMESTAMP NOT NULL);
"""

# name -> (subject, feldera sql, duckdb sql or None when identical)
VIEWS = {
    # joins
    "v_join": ("view.join_inner",
               "SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.id", None),
    "v_left": ("view.join_left",
               "SELECT o.id, c.region, c.tier FROM orders o LEFT JOIN customers c ON o.cust = c.id", None),
    "v_full": ("view.join_full",
               "SELECT o.id AS oid, c.id AS cid FROM orders o FULL OUTER JOIN customers c ON o.cust = c.id", None),
    "v_self": ("view.join_inner",
               "SELECT a.cust, COUNT(*) AS pairs FROM orders a JOIN orders b ON a.cust = b.cust AND a.id < b.id "
               "GROUP BY a.cust", None),
    # aggregates
    "v_agg": ("view.aggregate",
              "SELECT c.region, COUNT(*) AS n, SUM(o.amount) AS s, MIN(o.amount) AS lo, MAX(o.amount) AS hi "
              "FROM orders o JOIN customers c ON o.cust = c.id GROUP BY c.region", None),
    "v_agg_nulls": ("view.aggregate",
                    "SELECT status, COUNT(*) AS n, COUNT(cust) AS nc, SUM(amount) AS s, "
                    "COUNT(DISTINCT cust) AS dc FROM orders GROUP BY status", None),
    "v_having": ("view.aggregate",
                 "SELECT cust, SUM(amount) AS s FROM orders GROUP BY cust HAVING COUNT(*) > 2", None),
    "v_global": ("view.aggregate",
                 "SELECT COUNT(*) AS n, SUM(amount) AS s, MIN(amount) AS lo, MAX(amount) AS hi, "
                 "COUNT(DISTINCT status) AS ds FROM orders", None),
    "v_case": ("view.aggregate",
               "SELECT cust, SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid, "
               "SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS unknown FROM orders GROUP BY cust", None),
    "v_nested": ("view.aggregate",
                 "SELECT region, n FROM v_agg WHERE n > 1", None),
    # window functions
    "v_topk": ("view.window",
               "SELECT id, cust, amount, r FROM (SELECT id, cust, amount, "
               "RANK() OVER (PARTITION BY cust ORDER BY amount DESC) AS r FROM orders) t WHERE r <= 2", None),
    "v_dense": ("view.window",
                "SELECT id, cust, r FROM (SELECT id, cust, "
                "DENSE_RANK() OVER (PARTITION BY cust ORDER BY amount) AS r FROM orders) t WHERE r <= 3", None),
    "v_lag": ("view.window",
              "SELECT id, cust, LAG(amount) OVER (PARTITION BY cust ORDER BY id) AS prev FROM orders", None),
    "v_running": ("view.window",
                  "SELECT id, cust, SUM(amount) OVER (PARTITION BY cust ORDER BY id "
                  "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM orders", None),
    # time windows
    "v_tumble": ("view.tumble",
                 "SELECT window_start, COUNT(*) AS n, SUM(amount) AS s FROM TABLE(TUMBLE(TABLE orders, "
                 "DESCRIPTOR(ts), INTERVAL '1' HOUR)) GROUP BY window_start",
                 "SELECT time_bucket(INTERVAL '1 hour', ts) AS window_start, COUNT(*) AS n, SUM(amount) AS s "
                 "FROM orders GROUP BY 1"),
    "v_hop": ("view.hop",
              "SELECT window_start, COUNT(*) AS n FROM TABLE(HOP(TABLE orders, DESCRIPTOR(ts), "
              "INTERVAL '1' HOUR, INTERVAL '2' HOUR)) GROUP BY window_start",
              "SELECT w AS window_start, COUNT(*) AS n FROM (SELECT time_bucket(INTERVAL '1 hour', ts) AS w FROM orders "
              "UNION ALL SELECT time_bucket(INTERVAL '1 hour', ts) - INTERVAL '1 hour' AS w FROM orders) GROUP BY w"),
    # subqueries
    "v_not_exists": ("view.subquery",
                     "SELECT c.id FROM customers c WHERE NOT EXISTS "
                     "(SELECT 1 FROM orders o WHERE o.cust = c.id)", None),
    "v_not_in": ("view.subquery",
                 "SELECT id FROM customers WHERE id NOT IN (SELECT cust FROM orders)", None),
    "v_in": ("view.subquery",
             "SELECT id FROM orders WHERE cust IN (SELECT id FROM customers WHERE tier > 1)", None),
    "v_corr_max": ("view.subquery",
                   "SELECT o.id FROM orders o WHERE o.amount = "
                   "(SELECT MAX(o2.amount) FROM orders o2 WHERE o2.cust = o.cust)", None),
    # set operations
    "v_union": ("view.setop", "SELECT cust AS k FROM orders UNION SELECT id AS k FROM customers", None),
    "v_union_all": ("view.setop", "SELECT cust AS k FROM orders UNION ALL SELECT id AS k FROM customers", None),
    "v_except": ("view.setop", "SELECT id AS k FROM customers EXCEPT SELECT cust AS k FROM orders", None),
    "v_intersect": ("view.setop", "SELECT id AS k FROM customers INTERSECT SELECT cust AS k FROM orders", None),
    "v_distinct": ("view.setop", "SELECT DISTINCT region, tier FROM customers", None),
}


def program():
    parts = [TABLES]
    for name, (_, sql, _) in VIEWS.items():
        parts.append(f"CREATE MATERIALIZED VIEW {name} AS {sql};")
    return "\n".join(parts)


def oracle_sql(name):
    _, sql, duck = VIEWS[name]
    if duck:
        return duck
    if name == "v_nested":
        return ("SELECT region, n FROM (SELECT c.region, COUNT(*) AS n FROM orders o JOIN customers c "
                "ON o.cust = c.id GROUP BY c.region) WHERE n > 1")
    return sql
