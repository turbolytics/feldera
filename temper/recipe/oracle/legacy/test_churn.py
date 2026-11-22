"""Incremental views equal the batch query under churn (views_churn.py).

Same method as test_incremental.py: after every step completes, each view is
read ad hoc and as the sum of its change stream, and compared with DuckDB
running the query from scratch. Three seeds of 60 steps, at the default 4
workers. test_workers.py runs the same seeds at 1 and 8 workers.
"""

import pytest

import lanes
import views_churn
from features import FEATURES

SEEDS = [5, 6, 7]
STEPS = 60
PIPELINE = "temper_churn"


def compute(pipeline=PIPELINE, workers=4):
    from test_incremental import run_program
    return run_program(pipeline, views_churn, views_churn.Model, SEEDS, STEPS, workers=workers)


def params(invariant):
    return [pytest.param(v, id=v, marks=[pytest.mark.covers(invariant, subject=subj), pytest.mark.covers(FEATURES[subj]),
                                           pytest.mark.covers("sql.primary_key"), pytest.mark.covers("connector.http")])
            for v, (subj, _, _) in views_churn.VIEWS.items()]


@pytest.mark.parametrize("view", params("view.matches_batch"))
def test_view_matches_batch_under_churn(view):
    bad = lanes.result("churn")["content"][view]
    assert bad is None, f"{view} differs from the batch query: {bad}"


@pytest.mark.parametrize("view", params("view.changes_add_up"))
def test_changes_add_up_under_churn(view):
    bad = lanes.result("churn")["changes"][view]
    assert bad is None, f"{view}'s change stream differs from the batch query: {bad}"
