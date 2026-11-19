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


def result(lane):
    """A lane's whole result; raises the exception the lane stored, if any."""
    r = harness.get(lane, LANES[lane])
    if isinstance(r, Exception):
        raise r
    return r


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


def churn():
    import test_churn
    return test_churn.compute()


# Worker-count lanes (test_workers.py): each program again, on its own pipeline.
# The extra pipelines share the compiled program, so they compile in about a minute.
WORKER_SEEDS = {"views": [11, 23], "adv": [101, 202]}


def at_workers(prog, n):
    def lane():
        import test_churn
        import test_incremental
        import test_incremental_adv
        import views
        import views_adv
        name = WORKER_PIPELINES[(prog, n)]
        if prog == "churn":
            return test_churn.compute(name, n)
        module, model = {"views": (views, test_incremental.Model),
                         "adv": (views_adv, test_incremental_adv.AdvModel)}[prog]
        return test_incremental.run_program(name, module, model, WORKER_SEEDS[prog], 40, workers=n)
    return lane


BASE_PIPELINE = {"views": "temper_views", "adv": "temper_views_adv", "churn": "temper_churn"}
WORKER_PIPELINES = {(prog, n): f"{BASE_PIPELINE[prog]}_w{n}" for prog in BASE_PIPELINE for n in (1, 8)}

LANES = {"views": views, "kafka": kafka, "gap": gap_lane, "types": types, "pg": pg, "adv": adv, "late": late,
         "churn": churn}
LANES.update({f"{prog}_w{n}": at_workers(prog, n) for (prog, n) in WORKER_PIPELINES})
