"""Private probe: the grand-total row of ROLLUP on an empty table.

PostgreSQL 17 and DuckDB return one row (NULL, NULL, 0, NULL) for
SELECT status, cust, COUNT(*), SUM(amount) FROM orders GROUP BY ROLLUP(status, cust)
when orders is empty. This prints what Feldera's view holds when the pipeline
starts, after one insert, and after that row is deleted.
"""

import time

import harness

p = "temper_views_adv"
harness.reset(p)
harness.start(p)
time.sleep(2)
print("fresh:", harness.query(p, "SELECT * FROM a_rollup"), "| plain global:", harness.query(p, "SELECT * FROM a_agg_of_agg"))
row = {"id": 1, "cust": 2, "amount": 5, "status": "paid", "ts": "2026-01-01 00:00:00"}
harness.wait_complete(p, harness.ingest(p, "orders", [{"insert": row}]))
print("one row:", harness.query(p, "SELECT * FROM a_rollup"))
harness.wait_complete(p, harness.ingest(p, "orders", [{"delete": row}]))
print("deleted:", harness.query(p, "SELECT * FROM a_rollup"), "| plain global:", harness.query(p, "SELECT * FROM a_agg_of_agg"))
harness.stop(p)
