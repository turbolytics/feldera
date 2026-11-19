"""Delivery through faults.contract for the Kafka-to-Kafka path.

A producer writes sequence numbers base..base+N-1 to a 4-partition topic while
a pipeline copies them to another topic. The fault fires three times. A
read_committed consumer then reads the output topic.

The open-source edition has no fault tolerance, so only faults the pipeline
lives through are asked of it. The docs promise that the non-fault-tolerant
Kafka output retries "temporary broker failures ... without a delivery
deadline". Duplicates are not promised away, so they are reported, not failed.

The topics and the program are fixed, because compiling a program takes
minutes. Each run uses its own range of sequence numbers.
"""

import json
import random
import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import harness

PIPELINE = "temper_kafka"
KAFKA = "localhost:26092"
KAFKA_INSIDE = "kafka:9092"
BROKER = "temper-feldera-kafka"
SRC, DST = "fk-in", "fk-out"
N, RATE, RECOVERY = 30000, 1000, 120
FAULTS = ["pause_api", "process_pause", "broker_pause", "network_cut"]


def program():
    def conn(name, topic, extra):
        return json.dumps([{"transport": {"name": name, "config": {
            "bootstrap.servers": KAFKA_INSIDE, "topic": topic, **extra}},
            "format": {"name": "json", "config": {"update_format": "raw", "array": False}}}])
    return f"""
CREATE TABLE src (seq BIGINT NOT NULL) WITH ('connectors' = '{conn("kafka_input", SRC, {"start_from": "earliest"})}');
CREATE VIEW dst WITH ('connectors' = '{conn("kafka_output", DST, {})}') AS SELECT seq FROM src;
"""


def create_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    have = admin.list_topics(timeout=20).topics
    new = [NewTopic(t, num_partitions=4, replication_factor=1) for t in (SRC, DST) if t not in have]
    for f in admin.create_topics(new).values() if new else []:
        f.result(timeout=30)


class SeqProducer(threading.Thread):
    def __init__(self, base, n, rate):
        super().__init__(daemon=True)
        self.base, self.n, self.rate = base, n, rate

    def run(self):
        p = Producer({"bootstrap.servers": KAFKA, "linger.ms": 5, "enable.idempotence": True,
                      "message.timeout.ms": 600000})
        start = time.time()
        for i in range(self.n):
            p.produce(SRC, key=str(i).encode(), value=json.dumps({"seq": self.base + i}).encode())
            if i % 500 == 0:
                p.poll(0)
                ahead = i / self.rate - (time.time() - start)
                if ahead > 0:
                    time.sleep(ahead)
        p.flush(600)


def read_range(base, n, timeout, quiet=10):
    """Reads the output topic and returns the counts of values in base..base+n-1."""
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"temper-{uuid.uuid4().hex}",
                  "isolation.level": "read_committed", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([DST])
    counts, reached_at = {}, None
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                v = json.loads(msg.value())
                seq = (v.get("insert") or v).get("seq") if isinstance(v, dict) else None
                if seq is not None and base <= seq < base + n:
                    counts[seq] = counts.get(seq, 0) + 1
            if reached_at is None and len(counts) >= n:
                reached_at = time.time()
            if reached_at is not None and time.time() - reached_at > quiet:
                break
    finally:
        c.close()
    return counts, reached_at is not None


def pipeline_pids():
    out = harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c",
                     'for p in /proc/[0-9]*; do c=$(tr "\\0" " " < $p/cmdline 2>/dev/null); '
                     'case "$c" in *pipeline-manager*|*postgres*|"") ;; *) echo ${p#/proc/};; esac; done').stdout
    return out.split()


def inject(fault, rng):
    if fault == "pause_api":
        r = harness.requests.post(f"{harness.API}/pipelines/{PIPELINE}/pause", timeout=30)
        r.raise_for_status()
        time.sleep(10)
        harness.requests.post(f"{harness.API}/pipelines/{PIPELINE}/resume", timeout=30).raise_for_status()
        return "paused and resumed via the API"
    if fault == "process_pause":
        pids = pipeline_pids()
        if not pids:
            raise RuntimeError("no pipeline process to pause")
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -STOP {' '.join(pids)}")
        time.sleep(20)
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -CONT {' '.join(pids)}")
        return f"SIGSTOP {pids} for 20s"
    if fault == "broker_pause":
        harness.sh("docker", "pause", BROKER)
        time.sleep(20)
        harness.sh("docker", "unpause", BROKER)
        return "paused the broker 20s"
    if fault == "network_cut":
        harness.sh("docker", "network", "disconnect", harness.NETWORK, BROKER)
        time.sleep(20)
        harness.sh("docker", "network", "connect", "--alias", "kafka", harness.NETWORK, BROKER)
        return "cut the pipeline from Kafka for 20s"
    raise ValueError(fault)


_cache = {}
_runs = [0]


def result(fault):
    if fault in _cache:
        return _cache[fault]
    harness.create(PIPELINE, program())
    create_topics()
    harness.reset(PIPELINE)
    harness.start(PIPELINE)
    _runs[0] += 1
    base = int(time.time()) * 100000
    guard = harness.SleepGuard()
    rng = random.Random(fault)
    producer = SeqProducer(base, N, RATE)
    producer.start()
    log = []
    for _ in range(3):
        time.sleep(rng.uniform(4, 7))
        log.append(inject(fault, rng))
    producer.join()
    counts, reached = read_range(base, N, RECOVERY)
    state = (harness.pipeline(PIPELINE) or {}).get("deployment_status")
    harness.stop(PIPELINE, force=True)
    if guard.slept():
        pytest.exit("run void: the host slept", returncode=3)
    missing = sorted(set(range(base, base + N)) - set(counts))
    dupes = sorted(k for k, v in counts.items() if v > 1)
    r = {"log": log, "state": state, "reached": reached, "missing": [m - base for m in missing],
         "dupes": len(dupes)}
    print(f"\n[{fault}] {log} state={state} missing={len(missing)} dupes={len(dupes)} reached={reached}")
    _cache[fault] = r
    return r


@pytest.mark.parametrize("fault", [pytest.param(f, marks=[
    pytest.mark.covers(f"faults.contract/{f}", subject="path.kafka_kafka"),
    pytest.mark.covers("connector.kafka")]) for f in FAULTS])
def test_fault_keeps_every_delivery_invariant(fault):
    r = result(fault)
    assert not r["missing"], f"{len(r['missing'])} of {N} missing, e.g. {r['missing'][:10]}; {r['log']}"
    assert r["reached"] and r["state"] == "Running", f"no recovery; state {r['state']}"


@pytest.mark.parametrize("fault", [pytest.param(f, marks=[
    pytest.mark.covers("delivery.no_loss", subject="path.kafka_kafka")]) for f in FAULTS])
def test_no_loss(fault):
    r = result(fault)
    assert not r["missing"], f"{len(r['missing'])} of {N} missing, e.g. {r['missing'][:10]}; {r['log']}"


@pytest.mark.parametrize("fault", [pytest.param(f, marks=[
    pytest.mark.covers("delivery.recovers", subject="path.kafka_kafka")]) for f in FAULTS])
def test_recovers(fault):
    r = result(fault)
    assert r["reached"] and r["state"] == "Running", \
        f"output did not catch up within {RECOVERY}s; state {r['state']}; {r['log']}"
