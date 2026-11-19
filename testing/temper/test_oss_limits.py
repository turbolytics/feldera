"""What the open-source edition does instead of fault tolerance.

The docs say checkpoints and fault tolerance "are only available in Feldera
Enterprise Edition". These tests prove the open-source behavior the map's
unsupported answers rest on: after a crash, or a forced stop, the pipeline
begins again empty. (A graceful stop does resume, which test_kafka.py checks.)
"""

import time

import pytest

import harness
from test_kafka import pipeline_pids

PIPELINE = "temper_views"
ROW = {"id": 1, "cust": 2, "amount": 5, "status": "paid", "ts": "2026-01-01 00:00:00"}


def loaded():
    import views
    harness.create(PIPELINE, views.program())
    harness.reset(PIPELINE)
    harness.start(PIPELINE)
    harness.wait_complete(PIPELINE, harness.ingest(PIPELINE, "orders", [{"insert": ROW}]))
    assert harness.query(PIPELINE, "SELECT n FROM v_global") == [{"n": 1}]


@pytest.mark.exempts("faults.contract/process_kill", subject="path.kafka_kafka")
def test_kill_restarts_empty():
    loaded()
    pids = pipeline_pids()
    assert pids, "no pipeline process"
    harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -9 {' '.join(pids)}")
    assert harness.wait_for(lambda: (harness.pipeline(PIPELINE) or {}).get("deployment_status") != "Running", 60)
    harness.stop(PIPELINE, force=True)
    harness.start(PIPELINE)
    time.sleep(1)
    assert harness.query(PIPELINE, "SELECT n FROM v_global") == [{"n": 0}]
    harness.stop(PIPELINE, force=True)


def test_force_stop_restarts_empty():
    loaded()
    harness.stop(PIPELINE, force=True)
    harness.start(PIPELINE)
    time.sleep(1)
    assert harness.query(PIPELINE, "SELECT n FROM v_global") == [{"n": 0}]
    harness.stop(PIPELINE, force=True)
