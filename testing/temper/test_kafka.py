"""Delivery through faults.contract for the Kafka-to-Kafka path.

A producer writes sequence numbers base..base+N-1 to a 4-partition topic while
a pipeline copies them to another topic. The fault fires three times. A
read_committed consumer then reads the output topic.

The open-source edition has no fault tolerance, so only faults the pipeline
lives through are asked of it.

Time is scaled: the connectors' librdkafka socket and request timeouts drop
from 60 s to 10 s, and the "long" faults from 90 s to 15 s, so they still
outlast the timeouts. Short faults drop from 20 s to 6 s. Each fault fires
once. The docs promise that the non-fault-tolerant
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
N, RATE, RECOVERY = 10000, 2000, 60
TIMEOUT_MS = "10000"  # librdkafka default 60000
SECONDS = {"pause_api": 4, "process_pause": 6, "broker_pause": 6, "network_cut": 6,
           "long_broker_pause": 15, "long_network_cut": 15}
FAULTS = ["pause_api", "process_pause", "broker_pause", "network_cut", "long_broker_pause", "long_network_cut"]


def program():
    def conn(name, topic, extra, fmt):
        return json.dumps([{"transport": {"name": name, "config": {
            "bootstrap.servers": KAFKA_INSIDE, "topic": topic, **extra}},
            "format": {"name": "json", "config": {"update_format": fmt, "array": False}}}])
    scaled = {"socket.timeout.ms": TIMEOUT_MS}
    src = conn("kafka_input", SRC, {"start_from": "earliest", **scaled}, "raw")
    dst = conn("kafka_output", DST, {**scaled, "request.timeout.ms": TIMEOUT_MS}, "insert_delete")
    return f"""
CREATE TABLE src (seq BIGINT NOT NULL) WITH ('connectors' = '{src}');
CREATE VIEW dst WITH ('connectors' = '{dst}') AS SELECT seq FROM src;
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
                # A message carries one or more newline-separated records.
                for line in msg.value().splitlines():
                    if not line.strip():
                        continue
                    v = json.loads(line)
                    seq = (v.get("insert") or {}).get("seq")
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
    """The pipeline's own process, which the manager names program_<pipeline id>."""
    out = harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c",
                     'for p in /proc/[0-9]*; do case "$(cat $p/comm 2>/dev/null)" in program_*) '
                     'grep -q "^State:.*Z" $p/status || echo ${p#/proc/};; esac; done').stdout
    return out.split()


def inject(fault, rng):
    if fault == "pause_api":
        r = harness.requests.post(f"{harness.API}/pipelines/{PIPELINE}/pause", timeout=30)
        r.raise_for_status()
        time.sleep(SECONDS[fault])
        harness.requests.post(f"{harness.API}/pipelines/{PIPELINE}/resume", timeout=30).raise_for_status()
        return "paused and resumed via the API"
    if fault == "process_pause":
        pids = harness.pids_for(PIPELINE)  # only this pipeline; others run at the same time
        if not pids:
            raise RuntimeError("no pipeline process to pause")
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -STOP {' '.join(pids)}")
        time.sleep(SECONDS[fault])
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -CONT {' '.join(pids)}")
        return f"SIGSTOP {pids} for {SECONDS[fault]}s"
    if fault in ("broker_pause", "long_broker_pause"):
        secs = SECONDS[fault]
        harness.sh("docker", "pause", BROKER)
        time.sleep(secs)
        harness.sh("docker", "unpause", BROKER)
        return f"paused the broker {secs}s"
    if fault in ("network_cut", "long_network_cut"):
        # The long cut outlasts the connectors' (scaled) socket and request timeouts.
        secs = SECONDS[fault]
        harness.sh("docker", "network", "disconnect", harness.NETWORK, BROKER)
        time.sleep(secs)
        harness.sh("docker", "network", "connect", "--alias", "kafka", harness.NETWORK, BROKER)
        return f"cut the pipeline from Kafka for {secs}s"
    raise ValueError(fault)


def compute_all():
    """Every fault, one after another (they share the broker and the pipeline)."""
    out = {}
    for fault in FAULTS:
        try:
            out[fault] = run_fault(fault)
        except Exception as e:
            out[fault] = e
    return out


def result(fault):
    import lanes
    return lanes.part("kafka", fault)


def run_fault(fault):
    harness.create(PIPELINE, program())
    create_topics()
    harness.reset(PIPELINE)
    harness.start(PIPELINE)
    base = int(time.time()) * 100000
    guard = harness.SleepGuard()
    rng = random.Random(fault)
    producer = SeqProducer(base, N, RATE)
    producer.start()
    log = []
    time.sleep(rng.uniform(2, 4))
    log.append(inject(fault, rng))
    producer.join()
    counts, reached = read_range(base, N, RECOVERY)
    state = (harness.pipeline(PIPELINE) or {}).get("deployment_status")
    harness.stop(PIPELINE, force=True)
    if guard.slept():
        raise RuntimeError("run void: the host slept")
    missing = sorted(set(range(base, base + N)) - set(counts))
    dupes = sorted(k for k, v in counts.items() if v > 1)
    r = {"log": log, "state": state, "reached": reached, "missing": [m - base for m in missing],
         "dupes": len(dupes)}
    print(f"\n[{fault}] {log} state={state} missing={len(missing)} dupes={len(dupes)} reached={reached}")
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
