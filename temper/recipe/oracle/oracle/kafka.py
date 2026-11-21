"""path.kafka_kafka: records flow from a Kafka topic through a pipeline to
a Kafka topic, while faults come and go (Kafka input and output connectors).

The scenario holds `kafka produce` running for its whole length, and the
runner injects each fault. After each, `kafka caught-up` snapshots the input
topic and waits up to two minutes for every record in it to reach the output
topic, with the pipeline still Running. The Enterprise-only cases are proofs
of refusal: a graceful stop answers 501, and a killed pipeline restarts
empty.

    python -m oracle kafka setup|produce|pause-api|caught-up|kill-restarts-empty|graceful-stop-refused
"""
import json
import time

from . import api

NAME = "kafka_path"
BROKER = "kafka:9092"


def admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": BROKER})


def setup():
    from confluent_kafka.admin import NewTopic
    a = admin()
    api.wait("Kafka", lambda: a.list_topics(timeout=5) is not None, 120, 2)
    for f in a.create_topics([NewTopic(t, 1, 1) for t in ("path_in", "path_out")]).values():
        try:
            f.result()
        except Exception as e:
            api.log(f"create topic: {e}")
    fmt = '{"name": "json", "config": {"update_format": "insert_delete", "array": false}}'
    api.pipeline(NAME, f"""CREATE TABLE events (id BIGINT NOT NULL, v INT) WITH ('connectors' = '[{{
  "transport": {{"name": "kafka_input", "config": {{"bootstrap.servers": "{BROKER}", "topics": ["path_in"], "auto.offset.reset": "earliest"}}}},
  "format": {fmt}}}]');
CREATE MATERIALIZED VIEW out WITH ('connectors' = '[{{
  "transport": {{"name": "kafka_output", "config": {{"bootstrap.servers": "{BROKER}", "topic": "path_out"}}}},
  "format": {fmt}}}]') AS SELECT * FROM events;""")
    return 0


def produce():
    """Produce a record every 20 ms until stopped."""
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": BROKER, "message.timeout.ms": 0})
    i = 0
    while True:
        p.produce("path_in", json.dumps({"insert": {"id": i, "v": i % 97}}).encode())
        p.poll(0)
        i += 1
        time.sleep(0.02)


def ids(topic, kind="insert"):
    """Every id in a topic, read from the beginning to its current end."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BROKER, "group.id": f"oracle-{time.time()}", "enable.auto.commit": False})
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=30)
    c.assign([TopicPartition(topic, 0, lo)])
    out = set()
    pos = lo
    while pos < hi:
        msg = c.poll(5.0)
        if msg is None:
            break
        if msg.error():
            continue
        pos = msg.offset() + 1
        body = json.loads(msg.value())
        for change in body if isinstance(body, list) else [body]:
            if kind in change:
                out.add(change[kind]["id"])
    c.close()
    return out


def pause_api():
    """The pause and resume API, which every pipeline supports."""
    api.call("POST", f"/pipelines/{NAME}/pause")
    api.wait("the pipeline to pause", lambda: api.status(NAME)[0] == "Paused", 60)
    time.sleep(10)
    api.call("POST", f"/pipelines/{NAME}/resume")
    api.wait("the pipeline to resume", lambda: api.status(NAME)[0] == "Running", 60)
    return 0


def caught_up():
    """Missing records: input records not in the output two minutes after
    the snapshot, plus one if the pipeline is not Running."""
    want = ids("path_in")
    deadline = time.time() + 120
    missing = want
    while time.time() < deadline:
        missing = want - ids("path_out")
        if not missing:
            break
        time.sleep(3)
    st, err = api.status(NAME)
    api.log(f"caught up: {len(want)} input records, {len(missing)} missing from the output"
            f"{' e.g. ' + str(sorted(missing)[:5]) if missing else ''}; pipeline {st}")
    return len(missing) + (0 if st == "Running" else 1)


def kill_restarts_empty():
    """Proof that recovery from a kill needs Enterprise fault tolerance. The
    scenario has just killed the pipeline's process with SIGKILL; start it
    again with input paused and find its state gone. 0 when it restarts
    empty, as declared."""
    api.wait("the pipeline to notice the kill", lambda: api.status(NAME)[0] != "Running", 120)
    st, err = api.status(NAME)
    api.log(f"after SIGKILL the pipeline is {st}: {json.dumps(err)[:300]}")
    api.call("POST", f"/pipelines/{NAME}/stop", params={"force": "true"})
    api.wait("the pipeline to stop", lambda: api.status(NAME)[0] == "Stopped", 120)
    api.call("POST", f"/pipelines/{NAME}/start", params={"initial": "paused"})
    api.wait("the pipeline to start", lambda: api.status(NAME)[0] == "Paused", 120)
    after = api.query(NAME, "SELECT COUNT(*) AS n FROM out")[0]["n"]
    api.log(f"restarted after the kill with input paused: the view holds {after} rows")
    return 0 if after == 0 else 1


def graceful_stop_refused():
    """Proof that a graceful stop is Enterprise-only: 0 when stop?force=false
    answers 501."""
    try:
        api.call("POST", f"/pipelines/{NAME}/stop", params={"force": "false"})
        api.log("graceful stop was accepted")
        return 1
    except api.HTTPError as e:
        api.log(f"graceful stop answered {e.status}: {e.body[:200]}")
        return 0 if e.status == 501 else 1


def run(arg):
    return {"setup": setup, "produce": produce, "pause-api": pause_api, "caught-up": caught_up,
            "kill-restarts-empty": kill_restarts_empty, "graceful-stop-refused": graceful_stop_refused}[arg]()
