#!/usr/bin/env python3
"""Guard against queries that can overload the OpenSearch cluster.

Usage: check_query.py ENDPOINT BODY_FILE
Exit 0 = allowed, 5 = refused.

Rules (from the skill owner, to prevent server crashes):
  - Every _search/_count/_msearch body must bound @timestamp with a lower bound.
  - Time range > 7 days is never allowed.
  - Time range > 1 day needs at least one query condition besides the time range.
"""
import json
import re
import sys
from datetime import datetime, timezone

HOUR = 3600
DAY = 24 * HOUR
MAX_RANGE = 7 * DAY
NEED_FILTER_ABOVE = 1 * DAY

UNITS = {"s": 1, "m": 60, "h": HOUR, "H": HOUR, "d": DAY, "w": 7 * DAY, "M": 31 * DAY, "y": 366 * DAY}
LEAF_QUERIES = {
    "term", "terms", "match", "match_phrase", "match_phrase_prefix", "multi_match", "prefix",
    "wildcard", "regexp", "fuzzy", "exists", "ids", "query_string", "simple_query_string",
    "range", "geo_distance", "geo_bounding_box", "geo_polygon", "geo_shape", "nested", "terms_set",
}


class Refuse(Exception):
    pass


def parse_time(value, now):
    """Return epoch seconds for an OpenSearch date or date-math expression.
    Rounding (/d, /h) is widened by one unit so the estimate is never smaller than the truth."""
    if isinstance(value, (int, float)):
        return value / 1000.0  # epoch millis
    s = str(value).strip()
    if "||" in s:
        anchor, math = s.split("||", 1)
        base = as_pair(anchor, now)[0]
    elif s.startswith("now"):
        base, math = now, s[3:]
    else:
        base, math = parse_absolute(s), ""
    widen = 0
    for op, num, unit in re.findall(r"([+\-/])(\d*)([smhHdwMy])", math):
        if unit not in UNITS:
            raise Refuse(f"cannot parse date math '{s}'")
        if op == "/":
            widen = max(widen, UNITS[unit])
        else:
            base += (1 if op == "+" else -1) * int(num or 1) * UNITS[unit]
    rest = re.sub(r"([+\-/])(\d*)([smhHdwMy])", "", math)
    if rest.strip():
        raise Refuse(f"cannot parse date math '{s}'")
    return base, widen


def parse_absolute(s):
    if re.fullmatch(r"\d{10,13}", s):
        n = int(s)
        return n / 1000.0 if len(s) == 13 else float(n)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        raise Refuse(f"cannot parse date '{s}'; use ISO 8601 or now-<n><unit>")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def as_pair(v, now):
    r = parse_time(v, now)
    return r if isinstance(r, tuple) else (r, 0)


def walk(node, ranges, conditions, negated=False):
    """Collect @timestamp ranges that constrain the result and count other conditions."""
    if isinstance(node, list):
        for x in node:
            walk(x, ranges, conditions, negated)
        return
    if not isinstance(node, dict):
        return
    for key, val in node.items():
        if key == "bool" and isinstance(val, dict):
            for clause in ("must", "filter"):
                walk(val.get(clause, []), ranges, conditions, negated)
            # should / must_not cannot narrow the time window, but do count as conditions
            for clause in ("should", "must_not"):
                sub_ranges = []
                walk(val.get(clause, []), sub_ranges, conditions, True)
        elif key == "range" and isinstance(val, dict) and "@timestamp" in val:
            if not negated:
                ranges.append(val["@timestamp"])
            else:
                conditions.append("range")
        elif key in LEAF_QUERIES:
            conditions.append(key)
        elif key in ("constant_score", "boosting", "function_score", "dis_max"):
            walk(val, ranges, conditions, negated)
        elif key in ("filter", "query", "queries", "positive", "negative"):
            walk(val, ranges, conditions, negated)


def window_seconds(query, now):
    ranges, conditions = [], []
    walk(query or {}, ranges, conditions)
    if not ranges:
        raise Refuse("no @timestamp range in a must/filter clause. Every query must bound @timestamp.")
    lo, hi = None, None
    for r in ranges:
        g = r.get("gte", r.get("gt", r.get("from")))
        l = r.get("lte", r.get("lt", r.get("to")))
        if g is None:
            continue
        gs, gw = as_pair(g, now)
        gs -= gw
        ls = now
        if l is not None:
            ls, lw = as_pair(l, now)
            ls += lw
        lo = gs if lo is None else max(lo, gs)
        hi = ls if hi is None else min(hi, ls)
    if lo is None:
        raise Refuse("@timestamp range has no lower bound (gte/gt).")
    return max(0.0, hi - lo), conditions


def human(sec):
    return f"{sec / DAY:.1f} days" if sec >= DAY else f"{sec / HOUR:.1f} hours"


def check_body(body, now):
    win, conditions = window_seconds(body.get("query"), now)
    if win > MAX_RANGE:
        raise Refuse(f"time range is {human(win)}; the limit is 7 days. Split it or narrow it.")
    if win > NEED_FILTER_ABOVE and not conditions:
        raise Refuse(f"time range is {human(win)} (> 1 day) with no other query condition. "
                     "Add a filter such as client_request_host.")
    return win


def main():
    endpoint, body_file = sys.argv[1], sys.argv[2]
    if endpoint not in ("_search", "_count", "_msearch"):
        return 0
    text = open(body_file, encoding="utf-8").read()
    now = datetime.now(timezone.utc).timestamp()
    try:
        if endpoint == "_msearch":
            lines = [ln for ln in text.splitlines() if ln.strip()]
            if not lines or len(lines) % 2:
                raise Refuse("_msearch body must be header/body line pairs")
            bodies = [json.loads(ln) for ln in lines[1::2]]
        else:
            if not text.strip():
                raise Refuse("empty body. Every query must bound @timestamp.")
            bodies = [json.loads(text)]
        for b in bodies:
            check_body(b, now)
    except Refuse as e:
        print(f"osq: REFUSED: {e}", file=sys.stderr)
        return 5
    except json.JSONDecodeError as e:
        print(f"osq: REFUSED: body is not valid JSON ({e})", file=sys.stderr)
        return 5
    return 0


if __name__ == "__main__":
    sys.exit(main())
