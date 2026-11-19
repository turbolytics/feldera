"""What the open-source edition does instead of fault tolerance.

The docs say checkpoints and fault tolerance "are only available in Feldera
Enterprise Edition". These tests prove the open-source behavior the map's
unsupported answers rest on: after a crash, or a forced stop, the pipeline
begins again empty, and the API refuses a graceful stop (the one that
checkpoints) with 501.
"""

import time

import pytest

import harness
import lanes

PIPELINE = "temper_views"
ROW = {"id": 1, "cust": 2, "amount": 5, "status": "paid", "ts": "2026-01-01 00:00:00"}


def loaded():
    import views
    lanes.wait("views")  # the views lane uses this pipeline first
    harness.create(PIPELINE, views.program())
    harness.reset(PIPELINE)
    harness.start(PIPELINE)
    harness.wait_complete(PIPELINE, harness.ingest(PIPELINE, "orders", [{"insert": ROW}]))
    assert harness.query(PIPELINE, "SELECT n FROM v_global") == [{"n": 1}]


@pytest.mark.exempts("faults.contract/process_kill", subject="path.kafka_kafka")
def test_kill_restarts_empty():
    loaded()
    pids = harness.pids_for(PIPELINE)
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


@pytest.mark.exempts("faults.contract/stop_and_resume", subject="path.kafka_kafka")
@pytest.mark.exempts("delivery.no_duplicates", subject="path.kafka_kafka")
def test_graceful_stop_is_enterprise_only():
    """The lifecycle page describes a graceful stop that checkpoints, but the
    community edition refuses it, and the pipeline keeps running."""
    loaded()
    r = harness.requests.post(f"{harness.API}/pipelines/{PIPELINE}/stop", params={"force": "false"}, timeout=30)
    assert r.status_code == 501 and r.json().get("error_code") == "EnterpriseFeature", (r.status_code, r.text)
    time.sleep(2)
    assert (harness.pipeline(PIPELINE) or {}).get("deployment_status") == "Running"
    assert harness.query(PIPELINE, "SELECT n FROM v_global") == [{"n": 1}]
    harness.stop(PIPELINE, force=True)
