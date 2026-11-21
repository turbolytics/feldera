"""input.http_json: how the JSON ingress handles malformed records.

Each claim quotes Feldera's JSON format docs. A rejected request must leave
nothing behind, so each check sends a well-formed control row after the bad
one and reads the table back: exactly the control row may be there.
"""
from . import api

NAME = "input_errors"
SQL = """
CREATE TABLE ints (k INT NOT NULL, i TINYINT) WITH ('materialized' = 'true');
CREATE TABLE dates (k INT NOT NULL, d DATE) WITH ('materialized' = 'true');
CREATE TABLE strs (k INT NOT NULL, s VARCHAR(4)) WITH ('materialized' = 'true');
CREATE TABLE reals (k INT NOT NULL, r DOUBLE) WITH ('materialized' = 'true');
CREATE TABLE docs (k INT NOT NULL, v INT) WITH ('materialized' = 'true');
"""


def keys(table):
    return sorted(r["k"] for r in api.query(NAME, f"SELECT k FROM {table}"))


def rejected(table, bad, label):
    """Send bad, then a control row k=0. 0 when bad was rejected and nothing
    of it ingested; 1 otherwise."""
    _, err = api.send(NAME, table, bad)
    api.ingest(NAME, table, [{"insert": {"k": 0}}])
    got = keys(table)
    ok = err is not None and got == [0]
    api.log(f"{label}: request {'rejected (' + str(err.status) + ')' if err else 'ACCEPTED'}; table holds k={got}")
    return 0 if ok else 1


def run(subject):
    api.pipeline(NAME, SQL)
    out = {}
    out["input.rejects_out_of_range"] = rejected("ints", [{"insert": {"k": 1, "i": 300}}], "TINYINT 300")
    out["input.rejects_invalid_date"] = rejected("dates", [{"insert": {"k": 1, "d": "1997-02-29"}}], "DATE 1997-02-29")
    # JSON has no NaN; the parser must refuse the token rather than ingest it.
    out["input.rejects_nan"] = rejected("reals", b'[{"insert": {"k": 1, "r": NaN}}]', "DOUBLE NaN")
    # A body cut off mid-array: the whole request is rejected, including the
    # well-formed record before the break.
    out["input.rejects_broken_json"] = rejected("docs", b'[{"insert": {"k": 1, "v": 1}}, {"insert": {"k": 2, ', "broken JSON")
    long = "abcdefghij"
    api.ingest(NAME, "strs", [{"insert": {"k": 1, "s": long}}])
    got = api.query(NAME, "SELECT s FROM strs WHERE k = 1")
    out["input.keeps_long_strings"] = 0 if got == [{"s": long}] else 1
    api.log(f"VARCHAR(4) given {long!r}: table holds {got}")
    out["input.errors"] = sum(out.values())
    return out
