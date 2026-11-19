"""Private probe: more seeds and steps for the three view programs.

Prints the first mismatch per view, for each program.
"""

import sys

import test_incremental as base
import test_incremental_adv as adv
import test_lateness as late
import views
import views_adv
import views_late

PLANS = {
    "base": (base.PIPELINE, views, base.Model, list(range(1000, 1008)), 60),
    "adv": (adv.PIPELINE, views_adv, adv.AdvModel, list(range(2000, 2006)), 60),
    "late": (late.PIPELINE, views_late, late.LateModel, list(range(3000, 3004)), 150),
}

for name in sys.argv[1:] or PLANS:
    r = base.run_program(*PLANS[name])
    for kind in ("content", "changes"):
        bad = {v: x for v, x in r[kind].items() if x}
        print(f"{name} {kind}: {len(bad)} views differ", flush=True)
        for v, x in bad.items():
            print(f"   {v}: seed {x.get('seed')} step {x.get('step')} missing {x.get('missing')} extra {x.get('extra')}",
                  flush=True)
