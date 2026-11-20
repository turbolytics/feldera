"""Docker and REST helpers for the Feldera Temper tests.

Feldera runs in one container (temper-feldera) on the temper-feldera network,
open-source edition, with its HTTP API on port 26080.
"""

import json
import os
import subprocess
import threading
import time
from collections import Counter

import requests

IMAGE = os.environ.get("TEMPER_FELDERA_IMAGE", "images.feldera.com/feldera/pipeline-manager:0.383.0")
CONTAINER = "temper-feldera"
NETWORK = "temper-feldera"
API = "http://localhost:26080/v0"


def sh(*args, check=False):
    return subprocess.run(args, capture_output=True, text=True, check=check)


def start_feldera():
    """Starts Feldera unless it is already up."""
    try:
        if requests.get(f"{API}/config", timeout=3).ok:
            return
    except requests.RequestException:
        pass
    # A stopped container keeps its compiled programs (compiling takes minutes).
    if sh("docker", "start", CONTAINER).returncode == 0:
        wait_api()
        return
    sh("docker", "rm", "-f", CONTAINER)
    sh("docker", "network", "create", NETWORK)
    sh("docker", "run", "-d", "--name", CONTAINER, "--network", NETWORK, "--memory", "3g", "--cpus", "4",
       "-p", "26080:8080", IMAGE, check=True)
    wait_api()


def wait_api(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{API}/config", timeout=3).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError("Feldera API never came up")


# --- pipelines ----------------------------------------------------------------


def pipeline(name):
    # The API answers slowly while a large program compiles; retry a slow status read.
    for attempt in range(6):
        try:
            r = requests.get(f"{API}/pipelines/{name}", timeout=30)
            return r.json() if r.ok else None
        except requests.exceptions.Timeout:
            if attempt == 5:
                raise
            time.sleep(5)


def create(name, sql, runtime=None, compile_timeout=900):
    """Creates (or replaces) a pipeline and waits for its program to compile."""
    existing = pipeline(name)
    if existing and existing.get("program_code") == sql and existing.get("program_status") == "Success":
        return existing  # compiled already; compiling takes minutes
    compiling = existing and existing.get("program_code") == sql and \
        existing.get("program_status") in ("Pending", "CompilingSql", "CompilingRust")
    if existing and not compiling:
        stop(name, force=True)
        requests.post(f"{API}/pipelines/{name}/clear", timeout=30)
        wait_for(lambda: (pipeline(name) or {}).get("storage_status") in (None, "Cleared"), 120)
        requests.delete(f"{API}/pipelines/{name}", timeout=30)
    if not compiling:  # an identical program mid-compile: wait for it instead of starting over
        body = {"name": name, "program_code": sql, "runtime_config": {"workers": 4, **(runtime or {})}}
        r = requests.put(f"{API}/pipelines/{name}", json=body, timeout=30)
        if not r.ok:
            raise RuntimeError(f"create {name}: {r.status_code} {r.text}")
    deadline = time.time() + compile_timeout
    while time.time() < deadline:
        p = pipeline(name)
        status = p.get("program_status")
        if status == "Success":
            return p
        if status and ("Error" in status or status in ("SqlError", "RustError", "SystemError")):
            raise RuntimeError(f"compile {name}: {status}: {json.dumps(p.get('program_error'))[:3000]}")
        time.sleep(3)
    raise RuntimeError(f"compile {name}: timed out")


def wait_for(cond, timeout, every=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(every)
    return False


def start(name, timeout=300):
    r = requests.post(f"{API}/pipelines/{name}/start", params={"initial": "running", "dismiss_error": "true"}, timeout=30)
    time.sleep(1)
    if not r.ok:
        raise RuntimeError(f"start {name}: {r.status_code} {r.text}")
    def running():
        p = pipeline(name) or {}
        if p.get("deployment_error") and p.get("deployment_status") == "Stopped":
            raise RuntimeError(f"start {name}: {p['deployment_error'].get('message')}")
        return p.get("deployment_status") == "Running"
    if not wait_for(running, timeout):
        raise RuntimeError(f"start {name}: status {(pipeline(name) or {}).get('deployment_status')}")


def stop(name, force=True, timeout=180):
    """Stops a pipeline. Raises if the API refuses or the stop never finishes.
    (The community edition refuses a graceful stop, force=False, with a 501.)"""
    r = requests.post(f"{API}/pipelines/{name}/stop", params={"force": str(force).lower()}, timeout=30)
    if not r.ok:
        raise RuntimeError(f"stop {name} force={force}: {r.status_code} {r.text[:300]}")
    if not wait_for(lambda: (pipeline(name) or {}).get("deployment_status") == "Stopped", timeout):
        raise RuntimeError(f"stop {name}: still {(pipeline(name) or {}).get('deployment_status')} after {timeout}s")


def reset(name):
    """Stops a pipeline and clears its storage, so the next start begins empty."""
    stop(name, force=True)
    requests.post(f"{API}/pipelines/{name}/clear", timeout=30)
    if not wait_for(lambda: (pipeline(name) or {}).get("storage_status") == "Cleared", 120):
        raise RuntimeError(f"clear {name}: storage {(pipeline(name) or {}).get('storage_status')}")


def set_workers(name, n):
    """Sets the worker count. Feldera allows it only while storage is cleared,
    so call it after reset(). The compiled program is reused."""
    r = requests.patch(f"{API}/pipelines/{name}", json={"runtime_config": {"workers": n}}, timeout=30)
    if not r.ok:
        raise RuntimeError(f"set workers {name}={n}: {r.status_code} {r.text[:300]}")
    got = (pipeline(name) or {}).get("runtime_config", {}).get("workers")
    if got != n:
        raise RuntimeError(f"set workers {name}={n}: pipeline reports {got}")


def ingest(name, table, updates):
    """Sends insert/delete updates and returns a completion token."""
    r = requests.post(f"{API}/pipelines/{name}/ingress/{table}",
                      params={"format": "json", "update_format": "insert_delete", "array": "true"},
                      data=json.dumps(updates), timeout=60)
    if not r.ok:
        raise RuntimeError(f"ingest {table}: {r.status_code} {r.text}")
    return r.json()["token"]


def wait_complete(name, token, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API}/pipelines/{name}/completion_status", params={"token": token}, timeout=10)
        if r.ok and r.json().get("status") == "complete":
            return r.json()
        time.sleep(0.1)
    raise TimeoutError(f"token {token} not complete after {timeout}s")


def query(name, sql, exact=False):
    """Runs an ad-hoc query and returns rows as dicts. exact keeps numbers with a
    fraction as Decimal, digit for digit."""
    r = requests.get(f"{API}/pipelines/{name}/query", params={"sql": sql, "format": "json"}, timeout=120)
    if not r.ok:
        raise RuntimeError(f"query {sql!r}: {r.status_code} {r.text[:500]}")
    kw = {"parse_float": __import__("decimal").Decimal} if exact else {}
    return [json.loads(line, **kw) for line in r.text.splitlines() if line.strip()]


class ChangeStream(threading.Thread):
    """Follows a view's egress stream and keeps the multiset its changes add up to."""

    def __init__(self, name, view):
        super().__init__(daemon=True)
        self.name, self.view = name, view
        self.state = Counter()
        self.batches = []  # (sequence_number, number of changes), one per message
        self.lock = threading.Lock()
        self.error = None
        self.ready = threading.Event()
        self.resp = None

    def run(self):
        try:
            self.resp = requests.post(f"{API}/pipelines/{self.name}/egress/{self.view}",
                                      params={"format": "json", "backpressure": "true", "array": "false", "send_snapshot": "true"},
                                      stream=True, timeout=(10, None))
            self.resp.raise_for_status()
            self.ready.set()
            for line in self.resp.iter_lines():
                if not line:
                    continue
                msg = json.loads(line)
                with self.lock:
                    self.batches.append((msg.get("sequence_number"), len(msg.get("json_data") or [])))
                for change in msg.get("json_data") or []:
                    with self.lock:
                        if "insert" in change:
                            self.state[freeze(change["insert"])] += 1
                        elif "delete" in change:
                            self.state[freeze(change["delete"])] -= 1
        except Exception as e:  # recorded and asserted on by the test
            if not getattr(self, "closing", False):
                self.error = e
            self.ready.set()

    def snapshot(self):
        with self.lock:
            return Counter({k: v for k, v in self.state.items() if v != 0})

    def close(self):
        self.closing = True
        if self.resp is not None:
            self.resp.close()


# --- values -------------------------------------------------------------------


def norm(v):
    """Puts a value from Feldera JSON or DuckDB into one comparable form."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if hasattr(v, "isoformat") and not isinstance(v, str):
        return v.isoformat(sep=" ")
    if isinstance(v, float):
        return round(v, 6)
    if isinstance(v, (int,)):
        return int(v)
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return round(float(v), 6) if v != v.to_integral_value() else int(v)
    except Exception:
        pass
    if isinstance(v, str):
        # Feldera prints TIMESTAMP as "YYYY-MM-DD HH:MM:SS" and DECIMAL as a string.
        try:
            if "." in v or v.lstrip("-").isdigit():
                f = float(v)
                return int(f) if f == int(f) else round(f, 6)
        except ValueError:
            pass
        return v.replace("T", " ")
    return v


def freeze(row):
    return tuple(sorted((k.lower(), norm(v)) for k, v in row.items()))


def bag(rows):
    return Counter(freeze(r) for r in rows)


def diff(expected, actual, limit=5):
    missing = expected - actual
    extra = actual - expected
    if not missing and not extra:
        return None
    return {"missing": list(missing.items())[:limit], "extra": list(extra.items())[:limit],
            "n_missing": sum(missing.values()), "n_extra": sum(extra.values())}


class SleepGuard:
    """On macOS the monotonic clock stops while the host sleeps; the wall clock does not."""

    def __init__(self):
        self.wall, self.mono = time.time(), time.monotonic()

    def slept(self, slack=5.0):
        return (time.time() - self.wall) - (time.monotonic() - self.mono) > slack


# --- concurrency ----------------------------------------------------------------
#
# Scenarios on different pipelines run at the same time. conftest submits
# every lane at session start; each test then waits for its lane's result. A
# lane that raises stores the exception, so a broken lane runs once and every
# test that reads it reports the same error.

from concurrent.futures import ThreadPoolExecutor

_pool = ThreadPoolExecutor(max_workers=24)
_futures = {}
_futures_lock = threading.Lock()


def submit(key, fn):
    with _futures_lock:
        if key not in _futures:
            _futures[key] = _pool.submit(fn)
        return _futures[key]


def get(key, fn):
    return submit(key, fn).result()


def pids_for(name):
    """The process of one pipeline. The manager names it program_<pipeline id>,
    which the kernel truncates to 15 characters."""
    pid = (pipeline(name) or {}).get("id", "")
    want = ("program_" + pid)[:15]
    out = sh("docker", "exec", CONTAINER, "sh", "-c",
             'for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = "' + want + '" ] && '
             '! grep -q "^State:.*Z" $p/status && echo ${p#/proc/}; done').stdout
    return out.split()
