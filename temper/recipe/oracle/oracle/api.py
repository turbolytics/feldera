"""Feldera's REST API, the calls the scenarios use. Standard library only."""
import decimal
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "http://feldera:8080/v0"


def log(*args):
    print(*args, file=sys.stderr, flush=True)


class HTTPError(Exception):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body[:300]}")
        self.status, self.body = status, body


def call(method, path, body=None, params=None, timeout=60, raw=False):
    """One request. Returns the body text; raises HTTPError on a 4xx or 5xx."""
    url = API + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data, headers = None, {}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as e:
        raise HTTPError(e.code, e.read().decode(errors="replace")) from None


def get_json(path, **kw):
    return json.loads(call("GET", path, **kw))


def wait(what, cond, timeout, every=1.0):
    deadline = time.time() + timeout
    while True:
        try:
            got = cond()
        except (urllib.error.URLError, ConnectionError, OSError, HTTPError):
            got = None
        if got:
            return got
        if time.time() > deadline:
            raise SystemExit(f"timed out after {timeout}s waiting for {what}")
        time.sleep(every)


def status(name):
    p = get_json(f"/pipelines/{name}")
    return p.get("deployment_status"), p.get("deployment_error")


def pipeline(name, sql, workers=None):
    """Create pipeline name from sql, compile it and start it running."""
    wait("the Feldera API", lambda: call("GET", "/config", timeout=5), 600, 2)
    body = {"name": name, "program_code": sql}
    if workers:
        body["runtime_config"] = {"workers": workers}
    call("PUT", f"/pipelines/{name}", body)
    log(f"{name}: compiling")

    def compiled():
        s = get_json(f"/pipelines/{name}")["program_status"]
        if "Error" in str(s):
            p = get_json(f"/pipelines/{name}")
            raise SystemExit(f"{name} did not compile: {s}: {json.dumps(p.get('program_error'))[:800]}")
        return s == "Success"

    wait(f"{name} to compile", compiled, 3600, 5)
    call("POST", f"/pipelines/{name}/start", params={"initial": "running"})
    wait(f"{name} to run", lambda: status(name)[0] == "Running", 300)
    log(f"{name}: running")


def send(name, table, body, update_format="insert_delete"):
    """POST one ingress request. Returns (token or None, error or None)."""
    raw = body if isinstance(body, bytes) else json.dumps(body).encode()
    try:
        out = call("POST", f"/pipelines/{name}/ingress/{table}", raw,
                   params={"format": "json", "update_format": update_format, "array": "true"})
    except HTTPError as e:
        return None, e
    return json.loads(out).get("token"), None


def settle(name, token, timeout=120):
    """Wait until the request with token is fully processed. False if the
    pipeline stopped instead."""
    def done():
        st, err = status(name)
        if st != "Running":
            return ("stopped", st, err)
        r = get_json(f"/pipelines/{name}/completion_status", params={"token": token}, timeout=10)
        return ("done",) if r.get("status") == "complete" else None

    out = wait(f"{name} to process a request", done, timeout, 0.5)
    if out[0] == "stopped":
        log(f"{name}: the pipeline stopped ({out[1]}): {json.dumps(out[2])[:600]}")
        return False
    return True


def ingest(name, table, updates):
    """Send updates in one request and wait for them. False if the pipeline
    stopped; raises on a rejected request."""
    token, err = send(name, table, updates)
    if err:
        raise err
    return settle(name, token)


def query(name, sql):
    raw = call("GET", f"/pipelines/{name}/query", params={"sql": sql, "format": "json"}, timeout=120).strip()
    if not raw:
        return []
    # Numbers with a fraction stay exact: a 38-digit DECIMAL is not a float.
    if raw.startswith("["):
        return json.loads(raw, parse_float=decimal.Decimal)
    return [json.loads(line, parse_float=decimal.Decimal) for line in raw.splitlines() if line.strip()]
