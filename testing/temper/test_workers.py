"""Views equal the batch query at 1 and 8 workers (workers.contract).

The default runs use 4 workers. Here each program runs again on its own
pipeline at 1 worker and at 8, with the same method: after every step, each
view is read ad hoc and as the sum of its change stream, and compared with
DuckDB. A view passes a member when both reads match at every step of every
seed.
"""

import pytest

import lanes
import views
import views_adv
import views_churn
from features import FEATURES

MEMBERS = {"w1": 1, "w8": 8}
PROGRAMS = {"views": views, "adv": views_adv, "churn": views_churn}


def cases():
    out = []
    for member in MEMBERS:
        for prog, module in PROGRAMS.items():
            for v, (subj, _, _) in module.VIEWS.items():
                out.append(pytest.param(member, prog, v, id=f"{member}-{v}", marks=[
                    pytest.mark.covers(f"workers.contract/{member}", subject=subj),
                    pytest.mark.covers(FEATURES[subj]), pytest.mark.covers("runtime.workers")]))
    return out


@pytest.mark.parametrize("member,prog,view", cases())
def test_view_matches_batch_at_worker_count(member, prog, view):
    r = lanes.result(f"{prog}_{member}")
    for read in ("content", "changes"):
        bad = r[read][view]
        assert bad is None, f"{view} at {MEMBERS[member]} workers, {read} read, differs from the batch query: {bad}"
