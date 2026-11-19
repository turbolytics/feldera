import time

import pytest

import harness

PG_CONTAINER = "temper-feldera-pg"


def start_postgres():
    if "true" in harness.sh("docker", "inspect", "-f", "{{.State.Running}}", PG_CONTAINER).stdout:
        return
    harness.sh("docker", "rm", "-f", PG_CONTAINER)
    harness.sh("docker", "run", "-d", "--name", PG_CONTAINER, "--network", harness.NETWORK, "--memory", "512m",
               "-p", "26432:5432", "-e", "POSTGRES_PASSWORD=temper", "postgres:17", check=True)
    import psycopg
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            psycopg.connect("postgresql://postgres:temper@localhost:26432/postgres", connect_timeout=2).close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("PostgreSQL never came up")


def start_kafka():
    if "true" not in harness.sh("docker", "inspect", "-f", "{{.State.Running}}", "temper-feldera-kafka").stdout:
        harness.sh("docker", "start", "temper-feldera-kafka", check=True)
    import test_gap
    import test_kafka
    deadline = time.time() + 60
    while True:
        try:
            test_kafka.create_topics()
            test_gap.ensure_topics("ty-in", "ty-out")
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)


@pytest.fixture(scope="session", autouse=True)
def feldera():
    """Starts Feldera (open-source edition) and its neighbors, compiles every
    program at once, then starts every lane in the background."""
    harness.start_feldera()
    start_kafka()
    start_postgres()
    import gap
    import lanes
    import test_kafka
    import views
    import views_adv
    import views_churn
    import views_late
    programs = {
        "temper_views": views.program(), "temper_views_adv": views_adv.program(),
        "temper_views_late": views_late.program(), test_kafka.PIPELINE: test_kafka.program(),
        gap.GAP: gap.GAP_SQL, gap.TYPES: gap.types_sql(), gap.PG: gap.pg_sql(),
        "temper_churn": views_churn.program()}
    # The worker-count pipelines run the same programs (test_workers.py).
    programs.update({name: programs[lanes.BASE_PIPELINE[prog]] for (prog, _), name in lanes.WORKER_PIPELINES.items()})
    errors = gap.compile_all(programs)
    bad = {k: v for k, v in errors.items() if v}
    if bad:
        print(f"\n[compile errors] {bad}")
    for name, fn in lanes.LANES.items():
        harness.submit(name, fn)
