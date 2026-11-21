"""clock.now: NOW() is UTC, always increasing, and steps every second with
no input; a temporal filter on NOW() lets rows go with no input
(Date/time functions)."""
import datetime
import time

from . import api

NAME = "clock"
SQL = """
CREATE TABLE ev (id INT NOT NULL, ts TIMESTAMP NOT NULL);
CREATE MATERIALIZED VIEW clock AS SELECT NOW() AS t;
CREATE MATERIALIZED VIEW recent AS SELECT id FROM ev WHERE ts >= NOW() - INTERVAL '5' SECOND;
"""


def utc():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def parse(v):
    return datetime.datetime.fromisoformat(str(v).replace("T", " ")[:26].rstrip("Z"))


def run(subject):
    api.pipeline(NAME, SQL)
    out = {}
    seen, bad = [], 0
    for _ in range(16):
        rows = api.query(NAME, "SELECT t FROM clock")
        if rows:
            seen.append((time.time(), parse(rows[0]["t"]), utc()))
        time.sleep(0.5)
    for (_, a, _), (_, b, _) in zip(seen, seen[1:]):
        if b < a:
            bad += 1
            api.log(f"NOW() went backwards: {a} then {b}")
    for _, t, wall in seen:
        if abs((t - wall).total_seconds()) > 10:
            bad += 1
            api.log(f"NOW() = {t} is not UTC: the wall clock read {wall}")
    distinct = len({t for _, t, _ in seen})
    if distinct < 4:
        bad += 1
        api.log(f"NOW() took only {distinct} values over 8 s with no input; a step should come every second")
    out["now.increasing_utc"] = bad
    api.log(f"NOW(): {len(seen)} reads, {distinct} distinct values, {bad} violations")

    ts = utc().strftime("%Y-%m-%d %H:%M:%S")
    api.ingest(NAME, "ev", [{"insert": {"id": 1, "ts": ts}}])
    start = time.time()
    present = api.query(NAME, "SELECT id FROM recent") == [{"id": 1}]
    gone_after = None
    while time.time() - start < 15:
        if api.query(NAME, "SELECT id FROM recent") == []:
            gone_after = time.time() - start
            break
        time.sleep(0.5)
    ok = present and gone_after is not None and 3 <= gone_after <= 12
    out["now.temporal_filter_advances"] = 0 if ok else 1
    api.log(f"temporal filter: present at first {present}, left after {gone_after} s with no input (want about 5)")
    out["sql.now"] = sum(out.values())
    return out
