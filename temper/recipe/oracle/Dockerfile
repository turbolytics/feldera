# The oracle for Feldera's scenarios: drives the REST API and decides each
# claim. DuckDB is the reference engine; confluent-kafka and psycopg reach
# the Kafka and Postgres connectors' ends.
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.4.2 confluent-kafka==2.12.2 'psycopg[binary]==3.2.12' \
    requests==2.32.5 pytest==8.4.2
# legacy/: the original harness (testing/temper), run unchanged by oracle/legacy.py.
COPY legacy /oracle/legacy
COPY oracle /oracle/oracle
# The checker the one-command reproductions (scenarios/repro.yml) call, at
# the path they name.
COPY feldera_check.py /recipe/feldera_check.py
WORKDIR /oracle
ENTRYPOINT []
