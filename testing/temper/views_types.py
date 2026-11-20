"""A fourth program: every SQL type inside the operators that keep state.

Incremental operators keep state for each type: a hash for GROUP BY,
DISTINCT and joins, an ordered index for MIN, MAX and window ORDER BY, and a
running total for SUM. A delete must find its way back through that state.

Each type gets one table `t_<key> (g INT, c INT, v INT, x <type>)`. `v` is
unique per row. `c` is the class of `x`: values that the docs say are equal
share a class, and classes rank in the order the docs give. The expected result
is computed from `c` alone, so the oracle never parses a Feldera value:

- equality: floats have one zero and one NaN, and NaN = NaN (Floating Point
  Operations). Arrays and maps compare NULL elements as equal (Comparison
  Operations on Arrays and Maps). A TIMESTAMP WITH TIME ZONE is an instant.
- order: NaN is greater than Infinity. Arrays, maps and rows are
  lexicographic, and a NULL element is the smallest (Comparing complex values).
  Strings compare by code point.

Several members of a class carry different text, such as a decimal written
"1.5" and "1.50", or a UUID in upper case. Deletes often name a row by another
member of its class, so a retraction has to find the stored row through the
type's own equality.

JSON cannot carry NaN or an infinity. For REAL and DOUBLE, those rows arrive
with x NULL, and a local view puts the value back from c.
"""

N = None  # a NULL x

# key: (sql type, [[member, ...] per class, lowest first], small classes for SUM or None)
# A member is the JSON value sent, or "@nan" / "@inf" / "@-inf" for floats.
TYPES = {
    "bool": ("BOOLEAN", [[False], [True]], None),
    "i8": ("TINYINT", [[-128], [-1], [0], [1], [127]], [1, 2, 3]),
    "i16": ("SMALLINT", [[-32768], [-7], [0], [7], [32767]], [1, 2, 3]),
    "i32": ("INT", [[-2147483648], [-5], [0], [5], [2147483647]], [1, 2, 3]),
    "i64": ("BIGINT", [[-9223372036854775808], [-1], [0], [1], [9223372036854775807]], [1, 2, 3]),
    "u8": ("TINYINT UNSIGNED", [[0], [1], [3], [255]], [0, 1, 2]),
    "u16": ("SMALLINT UNSIGNED", [[0], [9], [65535]], [0, 1]),
    "u32": ("INT UNSIGNED", [[0], [9], [4294967295]], [0, 1]),
    "u64": ("BIGINT UNSIGNED", [[0], [1], [18446744073709551615]], [0, 1]),
    "real": ("REAL", [["@-inf"], [-3.0e38], [-1.5], [0.0, -0.0], [1.5e-45], [0.25], [3.0e38], ["@inf"], ["@nan"]],
             [2, 3, 4, 5]),
    "double": ("DOUBLE", [["@-inf"], [-1.7976931348623157e308], [-1.5], [0.0, -0.0], [5e-324], [1.0], [1e16],
                          ["@inf"], ["@nan"]], [2, 3, 4, 5, 6]),
    "dec": ("DECIMAL(38, 10)", [["-9999999999999999999999999999.9999999999"], ["-1.5", "-1.50"], ["0", "0.0000000000"],
                                ["0.0000000001"], ["1.5", "1.500"], ["123456789.0123456789"],
                                ["9999999999999999999999999999.9999999999"]], [1, 2, 3, 4, 5]),
    "varchar": ("VARCHAR", [[""], ["A"], ["a"], ["a "], ["ab"], ["é"], ["é"], ["\U0001F389"]], None),
    "char": ("CHAR(4)", [["    "], ["a   "], ["ab  "], ["b   "]], None),
    "binary": ("BINARY(4)", [[[0, 0, 0, 0]], [[1, 2, 3, 4]], [[255, 0, 0, 0]]], None),
    "varbinary": ("VARBINARY", [[[]], [[0]], [[0, 255]], [[16]], [[255]]], None),
    "date": ("DATE", [["0001-01-01"], ["1970-01-01"], ["2024-02-29"], ["9999-12-31"]], None),
    "time": ("TIME", [["00:00:00"], ["12:00:00.5"], ["23:59:59.123456789"]], None),
    "ts": ("TIMESTAMP", [["0001-01-01 00:00:00"], ["1970-01-01 00:00:00"], ["2024-02-25 12:12:33.123456"],
                         ["9999-12-31 23:59:59.999999"]], None),
    "tstz": ("TIMESTAMP WITH TIME ZONE", [["1970-01-01T00:00:00Z"],
                                          ["2024-02-25T12:12:33.123+02:00", "2024-02-25T10:12:33.123Z"],
                                          ["2024-02-25T10:12:33.124Z"]], None),
    "uuid": ("UUID", [["00000000-0000-0000-0000-000000000000"],
                      ["123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000"],
                      ["ffffffff-ffff-ffff-ffff-ffffffffffff"]], None),
    "variant": ("VARIANT", [[-3], [1], [2], [10]], None),
    "arr_int": ("INT ARRAY", [[[]], [[None]], [[1]], [[1, None]], [[1, 2]], [[2]]], None),
    "arr_str": ("VARCHAR ARRAY", [[[]], [["a"]], [["a", ""]], [["b"]]], None),
    "arr_ts": ("TIMESTAMP ARRAY", [[["1970-01-01 00:00:00"]], [["2024-01-01 00:00:00", "2024-01-01 00:00:00"]],
                                   [["2024-01-01 00:00:01"]]], None),
    "arr_arr": ("BIGINT ARRAY ARRAY", [[[[]]], [[[1], [2]]], [[[1, 2]]], [[[2]]]], None),
    "row": ("ROW(a INT NULL, b VARCHAR NULL)", [[{"a": None, "b": None}], [{"a": 1, "b": None}], [{"a": 1, "b": "x"}],
                                                [{"a": 2, "b": "x"}]], None),
    "arr_row": ("ROW(a INT NULL) ARRAY", [[[{"a": None}]], [[{"a": 1}]], [[{"a": 1}, {"a": 2}]]], None),
    "map": ("MAP<VARCHAR, INT>", [[{}], [{"a": None}], [{"a": 1}], [{"a": 1, "b": 0}], [{"a": 2}], [{"b": 0}]], None),
}

FLOATS = {"real", "double"}
SPECIAL = {"@nan": "CAST('NaN' AS {t})", "@inf": "CAST('inf' AS {t})", "@-inf": "CAST('-inf' AS {t})"}

# operator subject: (view name prefix, SQL over the local view x_<key>, or None to skip for that type)
OPERATORS = {
    "op.group_by": "grp",
    "op.distinct": "dst",
    "op.join": "jn",
    "op.min_max": "mm",
    "op.order_by": "top",
    "op.sum": "sum",
}


def view_sql(op, key):
    t = f"x_{key}"
    if op == "op.group_by":
        return f"SELECT MIN(c) AS lo, MAX(c) AS hi, COUNT(*) AS n, SUM(v) AS sv FROM {t} GROUP BY x"
    if op == "op.distinct":
        return f"SELECT g, COUNT(*) AS n FROM (SELECT DISTINCT g, x FROM {t}) d GROUP BY g"
    if op == "op.join":
        return f"SELECT a.v AS av, b.v AS bv FROM {t} a JOIN {t} b ON a.x = b.x WHERE a.g = 1 AND b.g = 2"
    if op == "op.min_max":
        return (f"SELECT g, ARG_MIN(c, x) AS lo, ARG_MAX(c, x) AS hi, COUNT(DISTINCT x) AS dx, COUNT(x) AS nx "
                f"FROM {t} GROUP BY g")
    if op == "op.order_by":
        return (f"SELECT g, c, v FROM (SELECT g, c, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY x DESC NULLS LAST, v) "
                f"AS rn FROM {t}) r WHERE rn <= 2")
    if op == "op.sum":
        small = TYPES[key][2]
        if small is None:
            return f"SELECT g, SUM(x) AS s, COUNT(x) AS n FROM {t} GROUP BY g"
        return f"SELECT g, SUM(x) AS s, COUNT(x) AS n FROM {t} WHERE c IN ({', '.join(map(str, small))}) GROUP BY g"
    raise ValueError(op)


def view_name(op, key):
    return f"{OPERATORS[op]}_{key}"


def local_view(key):
    sqltype = TYPES[key][0]
    if key not in FLOATS:
        return f"CREATE LOCAL VIEW x_{key} AS SELECT g, c, v, x FROM t_{key};"
    cases = " ".join(f"WHEN c = {cls} THEN {SPECIAL[members[0]].format(t=sqltype)}"
                     for cls, members in enumerate(TYPES[key][1]) if members[0] in SPECIAL)
    return f"CREATE LOCAL VIEW x_{key} AS SELECT g, c, v, CASE {cases} ELSE x END AS x FROM t_{key};"


def program(skip=()):
    """The program, without the (operator, type) pairs in skip."""
    parts = ["SET FELDERA_IGNORE_WARNING_FLOATING_POINT_EQUALITY = ON;"]
    for key, (sqltype, _, _) in TYPES.items():
        parts.append(f"CREATE TABLE t_{key} (g INT, c INT, v INT, x {sqltype});")
        parts.append(local_view(key))
        for op in OPERATORS:
            if (op, key) not in skip:
                parts.append(f"CREATE MATERIALIZED VIEW {view_name(op, key)} AS {view_sql(op, key)};")
    return "\n".join(parts)


def single(op, key):
    """A program with one table and one operator view: compiles alone to show a rejection."""
    sqltype = TYPES[key][0]
    return "\n".join(["SET FELDERA_IGNORE_WARNING_FLOATING_POINT_EQUALITY = ON;",
                      f"CREATE TABLE t_{key} (g INT, c INT, v INT, x {sqltype});", local_view(key),
                      f"CREATE MATERIALIZED VIEW {view_name(op, key)} AS {view_sql(op, key)};"])


# --- the model --------------------------------------------------------------------


class Model:
    """Every type's rows as the test believes them to be: v -> (g, class or None, member sent)."""

    def __init__(self, rng):
        self.rng = rng
        self.rows = {k: {} for k in TYPES}
        self.next_v = 1

    def _value(self, key):
        classes = TYPES[key][1]
        if self.rng.random() < 0.12:
            return None, None
        c = self.rng.randrange(len(classes))
        return c, self.rng.choice(classes[c])

    def _member(self, key, c, sent):
        """The member a delete names: often another spelling of the same class."""
        if c is None or self.rng.random() < 0.5:
            return sent
        return self.rng.choice(TYPES[key][1][c])

    def _ops(self, key, i):
        r, rows, ops = self.rng, self.rows[key], []

        def delete(v):
            g, c, sent = rows.pop(v)
            ops.append({"delete": record(key, g, c, v, self._member(key, c, sent))})

        if i % 9 == 8:  # drain
            for v in list(rows):
                delete(v)
            return ops
        for _ in range(r.choice([1, 3, 6, 10])):
            x = r.random()
            if x < 0.5 or not rows:
                c, sent = self._value(key)
                v, self.next_v = self.next_v, self.next_v + 1
                g = r.randint(1, 3)
                rows[v] = (g, c, sent)
                ops.append({"insert": record(key, g, c, v, sent)})
            elif x < 0.75:  # delete a row holding the current min or max of a group
                g = r.choice(sorted({gg for gg, _, _ in rows.values()}))
                cs = [c for gg, c, _ in rows.values() if gg == g and c is not None]
                if not cs:
                    continue
                target = min(cs) if r.random() < 0.5 else max(cs)
                delete(r.choice(sorted(v for v, (gg, c, _) in rows.items() if gg == g and c == target)))
            else:
                delete(r.choice(sorted(rows)))
        return ops

    def step(self, i):
        return {f"t_{k}": self._ops(k, i) for k in TYPES}

    # --- expected view contents, from classes alone ---------------------------------

    def expected(self, op, key):
        rows = self.rows[key]
        live = [(g, c, v) for v, (g, c, _) in rows.items()]
        out = []
        if op == "op.group_by":
            groups = {}
            for g, c, v in live:
                groups.setdefault(c, []).append(v)
            for c, vs in groups.items():
                out.append({"lo": c, "hi": c, "n": len(vs), "sv": sum(vs)})
        elif op == "op.distinct":
            per = {}
            for g, c, v in live:
                per.setdefault(g, set()).add(c)
            out = [{"g": g, "n": len(cs)} for g, cs in per.items()]
        elif op == "op.join":
            out = [{"av": va, "bv": vb} for ga, ca, va in live for gb, cb, vb in live
                   if ga == 1 and gb == 2 and ca is not None and ca == cb]
        elif op == "op.min_max":
            per = {}
            for g, c, v in live:
                per.setdefault(g, []).append(c)
            for g, cs in per.items():
                nn = [c for c in cs if c is not None]
                out.append({"g": g, "lo": min(nn) if nn else None, "hi": max(nn) if nn else None,
                            "dx": len(set(nn)), "nx": len(nn)})
        elif op == "op.order_by":
            per = {}
            for g, c, v in live:
                per.setdefault(g, []).append((c, v))
            for g, cv in per.items():
                cv.sort(key=lambda t: (t[0] is None, -(t[0] if t[0] is not None else 0), t[1]))
                out += [{"g": g, "c": c, "v": v} for c, v in cv[:2]]
        elif op == "op.sum":
            small = TYPES[key][2]
            per = {}
            for v, (g, c, sent) in rows.items():
                if c is not None and c in small:
                    per.setdefault(g, []).append(TYPES[key][1][c][0])
            out = [{"g": g, "s": ("sum", key, xs), "n": len(xs)} for g, xs in per.items()]
        else:
            raise ValueError(op)
        return out


def record(key, g, c, v, member):
    if isinstance(member, str) and member in SPECIAL:
        member = None
    return {"g": g, "c": c, "v": v, "x": member}
