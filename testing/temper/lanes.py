"""Lanes: work that can run at the same time, one lane per pipeline.

conftest starts every lane when the session begins. A test waits for its
lane. A lane that shares a pipeline runs its parts in order.
"""

import harness


def views():
    import test_incremental
    import test_transactions
    out = {}
    for key, fn in (("incremental", test_incremental.compute), ("transactions", test_transactions.compute)):
        try:
            out[key] = fn()
        except Exception as e:
            out[key] = e
    return out


def part(lane, key):
    r = harness.get(lane, LANES[lane])[key]
    if isinstance(r, Exception):
        raise r
    return r


def wait(lane):
    try:
        harness.get(lane, LANES[lane])
    except Exception:
        pass


def kafka():
    import test_kafka
    wait("types")  # the types lane also uses the broker; no broker faults during it
    return test_kafka.compute_all()


def gap_lane():
    import test_gap
    return test_gap.run_gap_lane()


def types():
    import test_gap
    return test_gap.run_types_lane(test_gap.types_columns())


def pg():
    import test_gap
    return test_gap.run_pg_lane()


def adv():
    import test_incremental_adv
    return test_incremental_adv.compute()


def late():
    import test_lateness
    return test_lateness.compute()


LANES = {"views": views, "kafka": kafka, "gap": gap_lane, "types": types, "pg": pg, "adv": adv, "late": late}
