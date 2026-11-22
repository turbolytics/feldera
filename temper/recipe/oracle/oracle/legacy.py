"""The original harness's tests, run unchanged under Temper's runner.

legacy/ is a copy of testing/temper, the harness behind the @68df7b1 map.
The one change is in legacy/harness.py: the API address comes from the
FELDERA_API environment variable. Every workload, value table, model and
assertion is the original's, so a cell here means what it meant there.

Each original test names the claims it decides in its pytest marks:
covers(<ref>, subject=<subject>) for a subject's claim, covers(<feature>)
for a feature, exempts(<ref>, subject=<subject>) for an exemption proof.
For one subject, this module calls every test with a mark for that subject,
with the test's own parameters, and counts a failed assertion as one
violation of each reference the test's marks name. Any other exception is
an error, as it was under pytest: the references it would decide are left
out of the results, so Temper records error for them.

The original computed each lane (one pipeline's workload) once for every
subject. Temper runs one scenario per subject, so a lane's results are kept
in CACHE for TTL seconds, keyed by a hash of legacy/: the next subject's
scenario reuses them, and says so in the log. A scenario run alone, or after
TTL, computes them itself.
"""
import contextlib
import hashlib
import importlib
import os
import pickle
import sys
import time

from . import api

LEGACY = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "legacy")
CACHE = os.environ.get("LEGACY_CACHE", "/cache")
TTL = 6 * 3600

# The original test modules for the areas this port covers: views (with the
# advanced views, churn and worker counts), lateness, transactions and
# operator type semantics. test_gap supplies the recursive view's dedicated
# checks; its other cases decide subjects the new oracle covers, so they never
# run here.
MODULES = ["test_incremental", "test_incremental_adv", "test_churn", "test_workers",
           "test_lateness", "test_transactions", "test_types_ops", "test_gap"]


def _setup():
    os.environ.setdefault("FELDERA_API", api.API)
    if LEGACY not in sys.path:
        sys.path.insert(0, LEGACY)


def code_hash():
    h = hashlib.sha256()
    for name in sorted(os.listdir(LEGACY)):
        if name.endswith(".py"):
            with open(os.path.join(LEGACY, name), "rb") as f:
                h.update(name.encode() + b"\0" + f.read())
    return h.hexdigest()[:16]


def _install_cache():
    """Keep each lane's result on disk, so the next subject reuses it."""
    import harness

    if getattr(harness, "_temper_cached", False):
        return
    orig, digest = harness.get, code_hash()

    def get(key, fn):
        path = os.path.join(CACHE, f"{key}-{digest}.pkl")
        try:
            age = time.time() - os.path.getmtime(path)
            if age < TTL:
                with open(path, "rb") as f:
                    r = pickle.load(f)
                api.log(f"lane {key}: reusing results computed {int(age)}s ago (legacy code {digest})")
                return r
        except (OSError, pickle.PickleError, EOFError):
            pass
        api.log(f"lane {key}: computing")
        t0 = time.time()
        r = orig(key, fn)  # raises when the lane raised: nothing is cached
        api.log(f"lane {key}: computed in {int(time.time() - t0)}s")
        if os.path.isdir(CACHE):
            tmp = path + ".tmp"
            with open(tmp, "wb") as f:
                pickle.dump(r, f)
            os.replace(tmp, path)
        return r

    harness.get = get
    harness._temper_cached = True


def _marks(obj):
    """Mark objects, whether pytest stored marks or mark decorators."""
    out = []
    for m in getattr(obj, "pytestmark", obj if isinstance(obj, (list, tuple)) else []):
        out.append(getattr(m, "mark", m))
    return out


def cases():
    """(test name, function, args, marks) for every original test case."""
    from _pytest.mark.structures import ParameterSet

    out = []
    for mod_name in MODULES:
        mod = importlib.import_module(mod_name)
        for name, fn in sorted(vars(mod).items()):
            if not name.startswith("test_") or not callable(fn):
                continue
            marks = _marks(fn)
            own = [m for m in marks if m.name in ("covers", "exempts")]
            params = [m for m in marks if m.name == "parametrize"]
            if not params:
                out.append((f"{mod_name}::{name}", fn, (), own))
                continue
            argnames, argvalues = params[0].args[0], params[0].args[1]
            multi = isinstance(argnames, str) and "," in argnames or isinstance(argnames, (list, tuple)) and len(argnames) > 1
            for v in argvalues:
                if isinstance(v, ParameterSet):
                    args, vm = tuple(v.values), [getattr(m, "mark", m) for m in v.marks]
                else:
                    args, vm = (tuple(v) if multi else (v,)), []
                vm = [m for m in vm if m.name in ("covers", "exempts")]
                out.append((f"{mod_name}::{name}{list(args)}", fn, args, own + vm))
    return out


def _for(subject, marks):
    """The references a case decides for subject; empty when none is its."""
    mine = [m for m in marks if m.kwargs.get("subject") == subject]
    if not mine:
        return []
    return [m.args[0] for m in mine] + [m.args[0] for m in marks if "subject" not in m.kwargs]


def refs(subject):
    """Every reference the original tests decide for subject, sorted."""
    _setup()
    got = set()
    for _, _, _, marks in cases():
        got.update(_for(subject, marks))
    return sorted(got)


def run(subject):
    _setup()
    _install_cache()
    out, errored, ran = {}, set(), 0
    selected = [(n, f, a, m) for n, f, a, m in cases() if _for(subject, m)]
    if any(n.startswith("test_gap::") for n, _, _, _ in selected):
        # The gap lane does not create its pipeline; the original session
        # compiled it first (conftest.py), so do the same.
        import gap
        import harness
        harness.create(gap.GAP, gap.GAP_SQL, compile_timeout=1500)
    for name, fn, args, marks in selected:
        decided = _for(subject, marks)
        ran += 1
        try:
            # The original tests print diagnostics, which pytest captured;
            # here they go to the log, so stdout carries only the results.
            with contextlib.redirect_stdout(sys.stderr):
                fn(*args)
            bad = 0
        except AssertionError as e:
            bad = 1
            api.log(f"FAIL {name}: {str(e)[:400]}")
        except Exception as e:  # an error, as under pytest: decides nothing
            errored.update(decided)
            api.log(f"ERROR {name}: {type(e).__name__}: {str(e)[:400]}")
            continue
        for ref in decided:
            out[ref] = out.get(ref, 0) + bad
    for ref in errored:
        out.pop(ref, None)
    api.log(f"{subject}: ran {ran} original test cases; {sum(1 for v in out.values() if v)} references failing; "
            f"{len(errored)} left out by errors")
    if not ran:
        raise SystemExit(f"no original test decides anything for {subject}")
    return out
