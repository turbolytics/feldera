#!/bin/sh
# Repro: a GROUP BY ROLLUP view has no grand-total row when its table is empty.
#
# Postgres 17 and DuckDB return one row (NULL, NULL, 0, NULL) for the ROLLUP
# view on an empty table. The same aggregate without GROUP BY returns (0, NULL)
# in Feldera too. This script reads both views on an empty table, after one
# insert, and after that row is deleted.
#
# usage: FELDERA_URL=http://localhost:8080 testing/temper/repro_rollup_empty.sh
# Needs curl and python3. Creates (and replaces) a pipeline named rollup_demo.
API=${FELDERA_URL:-http://localhost:8080}/v0
P=rollup_demo
SQL='CREATE TABLE orders (id INT NOT NULL, cust INT, amount INT NOT NULL, status VARCHAR, ts TIMESTAMP NOT NULL);
CREATE MATERIALIZED VIEW r AS SELECT status, cust, COUNT(*) AS n, SUM(amount) AS s FROM orders GROUP BY ROLLUP(status, cust);
CREATE MATERIALIZED VIEW plain AS SELECT COUNT(*) AS n, SUM(amount) AS s FROM orders;'

field() { python3 -c "import json,sys; print(json.load(sys.stdin).get('$1'))"; }
q() { curl -s -G "$API/pipelines/$P/query" --data-urlencode "sql=SELECT * FROM $1" --data-urlencode format=json; }
send() { curl -s -X POST "$API/pipelines/$P/ingress/orders?format=json&update_format=insert_delete&array=true" -d "$1" | field token; }
wait_token() {
  until [ "$(curl -s -G "$API/pipelines/$P/completion_status" --data-urlencode "token=$1" | field status)" = complete ]; do sleep 0.5; done
}

# Replace an earlier run.
curl -s -o /dev/null -X POST "$API/pipelines/$P/stop?force=true"
until [ "$(curl -s "$API/pipelines/$P" | field deployment_status)" != Running ]; do sleep 1; done
curl -s -o /dev/null -X POST "$API/pipelines/$P/clear"
sleep 2
curl -s -o /dev/null -X DELETE "$API/pipelines/$P"

# dict(...) rather than a {...} literal: bash 3.2 brace-expands a {a, b} literal.
BODY=$(python3 -c "import json,sys; print(json.dumps(dict(name=sys.argv[1], program_code=sys.argv[2])))" "$P" "$SQL")
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$API/pipelines/$P" -H 'content-type: application/json' -d "$BODY")
case "$code" in 2*) ;; *) echo "creating the pipeline failed: HTTP $code"; exit 1 ;; esac
printf 'compiling'
while :; do
  s=$(curl -s "$API/pipelines/$P" | field program_status)
  case "$s" in Success) break ;; *Error|None) echo " $s"; exit 1 ;; esac
  printf .; sleep 5
done
echo " done"
curl -s -X POST "$API/pipelines/$P/start" > /dev/null
until [ "$(curl -s "$API/pipelines/$P" | field deployment_status)" = Running ]; do sleep 1; done

ROW='{"id":1,"cust":2,"amount":5,"status":"paid","ts":"2026-01-01 00:00:00"}'
echo "--- 1. empty table"
echo "ROLLUP: [$(q r)]"
echo "plain:  [$(q plain)]"
wait_token "$(send "[{\"insert\":$ROW}]")"
echo "--- 2. after one insert"
q r
wait_token "$(send "[{\"delete\":$ROW}]")"
echo "--- 3. after the delete"
echo "ROLLUP: [$(q r)]"
echo "plain:  [$(q plain)]"
