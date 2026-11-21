"""table.primary_key: insert, delete and update events on a keyed table
(Uniqueness Constraints; JSON format)."""
from . import api

NAME = "pk"
SQL = "CREATE TABLE p (k INT NOT NULL PRIMARY KEY, a INT, b VARCHAR) WITH ('materialized' = 'true');"


def table():
    return {r["k"]: r for r in api.query(NAME, "SELECT k, a, b FROM p")}


def step(label, updates, want):
    """Send updates in one request; 0 when the table then equals want."""
    token, err = api.send(NAME, "p", updates)
    if err:
        api.log(f"{label}: request rejected: {err}")
        return 1
    api.settle(NAME, token)
    got = table()
    ok = got == want
    api.log(f"{label}: table {'matches' if ok else 'DIFFERS'}: {sorted(got.values(), key=lambda r: r['k'])}"
            + ("" if ok else f"; want {sorted(want.values(), key=lambda r: r['k'])}"))
    return 0 if ok else 1


def row(k, a, b):
    return {"k": k, "a": a, "b": b}


def run(subject):
    api.pipeline(NAME, SQL)
    out = {}
    step("setup", [{"insert": row(1, 1, "x")}, {"insert": row(2, 2, "y")}], {1: row(1, 1, "x"), 2: row(2, 2, "y")})
    out["pk.insert_overwrites"] = step(
        "insert over an existing key", [{"insert": row(1, 10, "xx")}], {1: row(1, 10, "xx"), 2: row(2, 2, "y")})
    out["pk.insert_overwrites"] += step(
        "two inserts of one key in one request", [{"insert": row(3, 3, "a")}, {"insert": row(3, 30, "b")}],
        {1: row(1, 10, "xx"), 2: row(2, 2, "y"), 3: row(3, 30, "b")})
    out["pk.delete_missing_ignored"] = step(
        "delete a missing key", [{"delete": row(99, 0, "")}], {1: row(1, 10, "xx"), 2: row(2, 2, "y"), 3: row(3, 30, "b")})
    out["pk.delete_by_key"] = step(
        "delete by key with other columns wrong", [{"delete": row(3, 999, "zz")}], {1: row(1, 10, "xx"), 2: row(2, 2, "y")})
    out["pk.update_changes_columns"] = step(
        "update one column", [{"update": {"k": 2, "a": 20}}], {1: row(1, 10, "xx"), 2: row(2, 20, "y")})
    out["pk.update_missing_ignored"] = step(
        "update a missing key", [{"update": {"k": 77, "a": 7}}], {1: row(1, 10, "xx"), 2: row(2, 20, "y")})
    out["sql.primary_key"] = sum(out.values())
    return out
