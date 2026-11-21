# The oracle for Feldera's scenarios: drives the REST API and decides each
# claim. DuckDB is the reference engine; confluent-kafka and psycopg reach
# the Kafka and Postgres connectors' ends.
FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.4.2 confluent-kafka==2.12.2 'psycopg[binary]==3.2.12'
COPY oracle /oracle/oracle
WORKDIR /oracle
ENTRYPOINT []
