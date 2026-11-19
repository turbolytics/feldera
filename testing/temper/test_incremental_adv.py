"""The incremental-vs-batch check for the second program (views_adv.py).

Same method as test_incremental.py, with a rates table for the ASOF join.
Rates have unique (cust, ts) pairs, so the ASOF match is always defined.
"""

import datetime as dt

import pytest

import views_adv
from test_incremental import BASE, Model, run_program

SEEDS = [101, 202, 303, 404]
STEPS = 40
PIPELINE = "temper_views_adv"


class AdvModel(Model):
    def __init__(self, rng):
        super().__init__(rng)
        self.rates = {}

    def step(self, i):
        out = super().step(i)
        r = self.rng
        ru = []
        for _ in range(r.choice([0, 1, 3, 6])):
            if r.random() < 0.7 or not self.rates:
                key = (r.randint(1, 14), BASE + dt.timedelta(minutes=10 * r.randint(-6, 36)))
                if key in self.rates:
                    continue
                self.rates[key] = r.randint(1, 9)
                ru.append({"insert": rrow(key, self.rates[key])})
            else:
                key = r.choice(sorted(self.rates))
                ru.append({"delete": rrow(key, self.rates.pop(key))})
        out["rates"] = ru
        return out

    def load(self, db):
        super().load(db)
        db.execute("DELETE FROM rates")
        if self.rates:
            db.executemany("INSERT INTO rates VALUES (?, ?, ?)", [(c, t, v) for (c, t), v in self.rates.items()])


def rrow(key, rate):
    return {"cust": key[0], "ts": key[1].strftime("%Y-%m-%d %H:%M:%S"), "rate": rate}


_results = None


def results():
    global _results
    if _results is None:
        _results = run_program(PIPELINE, views_adv, AdvModel, SEEDS, STEPS)
    return _results


def params(invariant):
    return [pytest.param(v, id=v, marks=[pytest.mark.covers(invariant, subject=subj)])
            for v, (subj, _, _) in views_adv.VIEWS.items()]


@pytest.mark.parametrize("view", params("view.matches_batch"))
def test_view_matches_batch(view):
    bad = results()["content"][view]
    assert bad is None, f"{view} differs from the batch query: {bad}"


@pytest.mark.parametrize("view", params("view.changes_add_up"))
def test_changes_add_up_to_batch(view):
    bad = results()["changes"][view]
    assert bad is None, f"{view}'s change stream differs from the batch query: {bad}"
