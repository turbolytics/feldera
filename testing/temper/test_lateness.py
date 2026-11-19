"""Views over LATENESS tables equal the batch query when inputs respect lateness.

Time moves forward up to 30 minutes a step, so over 100 steps it covers many
hours and Feldera can discard state far behind the 1-hour bound. Every insert
and delete in a step has a timestamp no more than 25 minutes behind the largest
timestamp seen before the step, and new timestamps run at most 30 minutes
ahead of it. So no two timestamps in a step, or across steps, are more than 55
minutes apart in the wrong order: every input is inside the bound.
"""

import datetime as dt
import random

import pytest

import views_late
from test_incremental import run_program

SEEDS = [7, 8]
STEPS = 100
PIPELINE = "temper_views_late"
BASE = dt.datetime(2026, 3, 1)
BEHIND = 25 * 60
AHEAD = 30 * 60


class LateModel:
    def __init__(self, rng):
        self.rng = rng
        self.events, self.clicks = {}, {}
        self.max_ts = BASE
        self.next_id = 1
        self.used_ts = set()

    def ts(self):
        """A fresh timestamp within [max - 25 min, max + 30 min], unique across all rows."""
        while True:
            t = self.max_ts + dt.timedelta(seconds=self.rng.randint(-BEHIND, AHEAD))
            if t not in self.used_ts:
                self.used_ts.add(t)
                return t

    def deletable(self, table):
        floor = self.max_ts - dt.timedelta(seconds=BEHIND)
        return [i for i, row in table.items() if row[-1] >= floor]

    def step(self, i):
        r = self.rng
        eu, cu = [], []
        new_max = self.max_ts
        for _ in range(r.choice([1, 3, 8, 20])):
            x = r.random()
            if x < 0.55:
                row = (self.next_id, r.choice([1, 2, 3, 4, None]), r.randint(1, 20), self.ts())
                self.next_id += 1
                self.events[row[0]] = row
                eu.append({"insert": erow(row)})
                new_max = max(new_max, row[-1])
            elif x < 0.85:
                row = (self.next_id, r.choice([1, 2, 3, 4, None]), self.ts())
                self.next_id += 1
                self.clicks[row[0]] = row
                cu.append({"insert": crow(row)})
                new_max = max(new_max, row[-1])
            else:
                table, out, fmt = (self.events, eu, erow) if r.random() < 0.6 else (self.clicks, cu, crow)
                ids = self.deletable(table)
                if ids:
                    out.append({"delete": fmt(table.pop(r.choice(sorted(ids))))})
        self.max_ts = max(new_max, self.max_ts)
        return {"events": eu, "clicks": cu}

    def load(self, db):
        db.execute("DELETE FROM events")
        db.execute("DELETE FROM clicks")
        if self.events:
            db.executemany("INSERT INTO events VALUES (?, ?, ?, ?)", list(self.events.values()))
        if self.clicks:
            db.executemany("INSERT INTO clicks VALUES (?, ?, ?)", list(self.clicks.values()))


def erow(row):
    return {"id": row[0], "k": row[1], "v": row[2], "ts": row[3].strftime("%Y-%m-%d %H:%M:%S")}


def crow(row):
    return {"id": row[0], "k": row[1], "ts": row[2].strftime("%Y-%m-%d %H:%M:%S")}


_results = None


def results():
    global _results
    if _results is None:
        _results = run_program(PIPELINE, views_late, LateModel, SEEDS, STEPS)
    return _results


def params(invariant):
    return [pytest.param(v, id=v, marks=[pytest.mark.covers(invariant, subject=subj)])
            for v, (subj, _, _) in views_late.VIEWS.items()]


@pytest.mark.parametrize("view", params("view.lateness_preserves_output"))
def test_view_matches_batch(view):
    bad = results()["content"][view]
    assert bad is None, f"{view} differs from the batch query: {bad}"


@pytest.mark.parametrize("view", params("view.changes_add_up"))
def test_changes_add_up_to_batch(view):
    bad = results()["changes"][view]
    assert bad is None, f"{view}'s change stream differs from the batch query: {bad}"
