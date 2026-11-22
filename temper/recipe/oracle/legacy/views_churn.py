"""A third program, for churn: as many deletes and updates as inserts.

Incremental engines break on retractions, not inserts. The input here keeps a
few keys hot and deletes about as often as it inserts, far more than the
other programs do:

- `acct` has a primary key. Steps upsert keys that exist, delete keys that do
  not, update named columns of present and missing keys, and touch one key
  several times in one request. Feldera applies an insert as an overwrite, and
  ignores a delete or update of a missing key (connectors/unique_keys.md).
- `ev` has no key and many identical rows. Deletes prefer the current MIN or
  MAX of a group and whole runs of copies. Every delete names a row that
  exists, because deleting a missing row from a table without a key is
  undefined in the docs.
- `edges` feeds a recursive view, with edges deleted as often as added.

Every few steps a table drains to empty, or is replaced wholesale in one
request.
"""

TABLES = """
CREATE TABLE acct (id INT NOT NULL PRIMARY KEY, grp INT, bal BIGINT NOT NULL);
CREATE TABLE ev (k INT, v INT);
CREATE TABLE edges (x INT, y INT);
"""

DUCK_TABLES = """
CREATE TABLE acct (id INT NOT NULL PRIMARY KEY, grp INT, bal BIGINT NOT NULL);
CREATE TABLE ev (k INT, v INT);
CREATE TABLE edges (x INT, y INT);
"""

RECURSIVE = """
DECLARE RECURSIVE VIEW h_closure(x INT, y INT);
CREATE LOCAL VIEW h_step AS SELECT e.x, h_closure.y FROM edges e JOIN h_closure ON e.y = h_closure.x;
CREATE MATERIALIZED VIEW h_closure AS (SELECT * FROM edges) UNION (SELECT * FROM h_step);
"""

# name -> (subject, feldera sql, duckdb sql or None when identical)
VIEWS = {
    "h_pk_grp": ("view.aggregate",
                 "SELECT grp, COUNT(*) AS n, SUM(bal) AS s, MIN(bal) AS lo, MAX(bal) AS hi FROM acct GROUP BY grp", None),
    "h_pk_global": ("view.aggregate",
                    "SELECT COUNT(*) AS n, SUM(bal) AS s, MIN(bal) AS lo, MAX(bal) AS hi FROM acct", None),
    "h_ev_minmax": ("view.aggregate",
                    "SELECT k, COUNT(*) AS n, COUNT(v) AS nv, MIN(v) AS lo, MAX(v) AS hi, COUNT(DISTINCT v) AS dv "
                    "FROM ev GROUP BY k", None),
    "h_ev_global": ("view.aggregate",
                    "SELECT COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi, SUM(v) AS s FROM ev", None),
    "h_having": ("view.aggregate",
                 "SELECT k, COUNT(*) AS n FROM ev GROUP BY k HAVING COUNT(*) >= 3", None),
    "h_nested": ("view.aggregate",
                 "SELECT grp, hi - lo AS spread FROM h_pk_grp WHERE n > 1",
                 "SELECT grp, MAX(bal) - MIN(bal) AS spread FROM acct GROUP BY grp HAVING COUNT(*) > 1"),
    "h_join": ("view.join_inner",
               "SELECT a.id, a.bal, e.v FROM acct a JOIN ev e ON a.grp = e.k", None),
    "h_join_agg": ("view.join_inner",
                   "SELECT a.grp, COUNT(*) AS n, SUM(e.v) AS s, MAX(a.bal) AS hi FROM acct a JOIN ev e ON a.grp = e.k "
                   "GROUP BY a.grp", None),
    "h_left": ("view.join_left",
               "SELECT a.id, e.v FROM acct a LEFT JOIN ev e ON a.grp = e.k", None),
    "h_full": ("view.join_full",
               "SELECT a.id, e.k FROM acct a FULL OUTER JOIN (SELECT DISTINCT k FROM ev) e ON a.grp = e.k", None),
    "h_top1": ("view.window",
               "SELECT id, grp, bal FROM (SELECT id, grp, bal, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY bal DESC, id) "
               "AS rn FROM acct) t WHERE rn = 1", None),
    "h_rank": ("view.window",
               "SELECT k, v, r FROM (SELECT k, v, RANK() OVER (PARTITION BY k ORDER BY v DESC) AS r FROM ev) t "
               "WHERE r <= 2", None),
    "h_lag": ("view.window",
              "SELECT id, grp, LAG(bal) OVER (PARTITION BY grp ORDER BY id) AS prev FROM acct", None),
    "h_max_corr": ("view.subquery",
                   "SELECT id FROM acct a WHERE bal = (SELECT MAX(bal) FROM acct b WHERE b.grp = a.grp)", None),
    "h_not_exists": ("view.subquery",
                     "SELECT DISTINCT k FROM ev e WHERE NOT EXISTS (SELECT 1 FROM acct a WHERE a.grp = e.k)", None),
    "h_distinct": ("view.setop", "SELECT DISTINCT k, v FROM ev", None),
    "h_union": ("view.setop", "SELECT grp AS g FROM acct UNION SELECT k AS g FROM ev", None),
    "h_except": ("view.setop", "SELECT k AS g FROM ev EXCEPT SELECT grp AS g FROM acct", None),
    "h_closure": ("view.recursive", None,
                  "WITH RECURSIVE c(x, y) AS (SELECT x, y FROM edges UNION SELECT e.x, c.y FROM edges e "
                  "JOIN c ON e.y = c.x) SELECT x, y FROM c"),
}


def program():
    parts = [TABLES, RECURSIVE]
    for name, (_, sql, _) in VIEWS.items():
        if sql is not None:
            parts.append(f"CREATE MATERIALIZED VIEW {name} AS {sql};")
    return "\n".join(parts)


def oracle_sql(name):
    _, sql, duck = VIEWS[name]
    return duck or sql


class Model:
    """The tables as the test believes them to be, applying each request in order."""

    def __init__(self, rng):
        self.rng = rng
        self.acct = {}        # id -> (id, grp, bal)
        self.ev = {}          # (k, v) -> copies
        self.edges = set()

    # --- acct (primary key) ---------------------------------------------------

    def _acct_row(self, i):
        r = self.rng
        return (i, r.choice([1, 2, 3, 4, None]), r.choice([r.randint(-50, 50), r.randint(1000, 1010)]))

    def _acct_ops(self, i):
        r, ops = self.rng, []
        if i % 17 == 16:  # replace the whole table in one request
            for row in self.acct.values():
                ops.append({"delete": arow(row)})
            self.acct = {}
            for k in r.sample(range(1, 21), r.randint(0, 12)):
                row = self._acct_row(k)
                self.acct[k] = row
                ops.append({"insert": arow(row)})
            return ops
        for _ in range(r.choice([1, 3, 6, 12])):
            key = r.randint(1, 20)
            x = r.random()
            if x < 0.30:  # insert: overwrites a present key
                row = self._acct_row(key)
                self.acct[key] = row
                ops.append({"insert": arow(row)})
            elif x < 0.60:  # delete by key; other columns need not match; a missing key is ignored
                self.acct.pop(key, None)
                ops.append({"delete": {"id": key, "grp": r.choice([1, None]), "bal": 0}})
            elif x < 0.80:  # update named columns; a missing key is ignored
                bal = r.randint(-50, 50)
                if key in self.acct:
                    self.acct[key] = (key, self.acct[key][1], bal)
                ops.append({"update": {"id": key, "bal": bal}})
            else:  # the same key twice in one request
                a, b = self._acct_row(key), self._acct_row(key)
                if r.random() < 0.5:
                    self.acct[key] = b
                    ops += [{"insert": arow(a)}, {"insert": arow(b)}]
                else:
                    self.acct.pop(key, None)
                    ops += [{"insert": arow(a)}, {"delete": arow(a)}]
        return ops

    # --- ev (no key, many copies) -----------------------------------------------

    def _ev_insert(self, row, ops, copies=1):
        self.ev[row] = self.ev.get(row, 0) + copies
        ops += [{"insert": erow(row)}] * copies

    def _ev_delete(self, row, ops, copies=1):
        copies = min(copies, self.ev.get(row, 0))
        if not copies:
            return
        self.ev[row] -= copies
        if not self.ev[row]:
            del self.ev[row]
        ops += [{"delete": erow(row)}] * copies

    def _ev_ops(self, i):
        r, ops = self.rng, []
        if i % 11 == 10:  # drain
            for row, n in list(self.ev.items()):
                self._ev_delete(row, ops, n)
            return ops
        for _ in range(r.choice([1, 4, 8, 16])):
            x = r.random()
            if x < 0.40 or not self.ev:
                row = (r.choice([1, 2, 3, 4, 5, None]), r.choice(list(range(1, 9)) + [None]))
                self._ev_insert(row, ops, r.choice([1, 1, 2, 3]))
            elif x < 0.70:  # delete a group's current MIN or MAX
                k = r.choice(sorted({k for k, _ in self.ev}, key=repr))
                vals = [v for kk, v in self.ev if kk == k and v is not None]
                if not vals:
                    continue
                target = (k, min(vals) if r.random() < 0.5 else max(vals))
                self._ev_delete(target, ops, r.choice([1, self.ev[target]]))
            elif x < 0.85:  # delete one copy of any row
                self._ev_delete(r.choice(sorted(self.ev, key=repr)), ops)
            else:  # insert and delete the same row in one request
                row = (r.choice([1, 2, 3]), r.randint(1, 8))
                self._ev_insert(row, ops)
                self._ev_delete(row, ops)
        return ops

    # --- edges ----------------------------------------------------------------------

    def _edge_ops(self, i):
        r, ops = self.rng, []
        for _ in range(r.choice([0, 1, 2, 4])):
            if self.edges and r.random() < 0.5:
                e = r.choice(sorted(self.edges))
                self.edges.discard(e)
                ops.append({"delete": {"x": e[0], "y": e[1]}})
            else:
                e = (r.randint(0, 7), r.randint(0, 7))
                if e not in self.edges:
                    self.edges.add(e)
                    ops.append({"insert": {"x": e[0], "y": e[1]}})
        return ops

    def step(self, i):
        return {"acct": self._acct_ops(i), "ev": self._ev_ops(i), "edges": self._edge_ops(i)}

    def load(self, db):
        for t in ("acct", "ev", "edges"):
            db.execute(f"DELETE FROM {t}")
        if self.acct:
            db.executemany("INSERT INTO acct VALUES (?, ?, ?)", list(self.acct.values()))
        rows = [row for row, n in self.ev.items() for _ in range(n)]
        if rows:
            db.executemany("INSERT INTO ev VALUES (?, ?)", rows)
        if self.edges:
            db.executemany("INSERT INTO edges VALUES (?, ?)", sorted(self.edges))


def arow(row):
    return {"id": row[0], "grp": row[1], "bal": row[2]}


def erow(row):
    return {"k": row[0], "v": row[1]}
