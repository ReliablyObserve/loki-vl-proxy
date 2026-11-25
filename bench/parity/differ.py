"""Semantic diff of one Loki answer against the proxy's answer to the same request.

Pure functions, no I/O, so the rules are unit-tested (tests/test_parity.py).

`diff(endpoint, loki, proxy)` returns a list of facets, one per distinct way
the answers differ. A facet is a dict:

  kind     what differs: status, error-text, result-type, shape, order,
           entries, line, labels, category, series, values, points, names,
           field, stats, ...
  detail   a generalised description (label names replaced by their class),
           stable across queries that fail for the same reason, so facets
           cluster into gap signatures
  example  a short concrete excerpt (Loki vs proxy) for the report
  documented  the conformance registry case id of a recorded deviation
           (status documented, owner-kept or upstream), or absent. Only the
           ids in DOCUMENTED are used, and each covers exactly the difference
           its case describes; conformance/scripts/parity_gaps.py fails on an
           id the registry does not hold.

What is ignored, because it says how an answer was produced rather than what
it is: `stats` blocks, `warnings` (a request whose Loki answer carries
warnings is blocked by run.py before it reaches the diff), execution
metadata, the order of streams, series and names (Loki leaves it
unspecified; the order of entries inside a stream is compared against the
query's direction), and log entries at the boundary timestamp of a result
that hit the line limit (Loki breaks ties at the limit arbitrarily). Error
texts are compared after collapsing whitespace and a trailing period only.
"""
import json
import math
import re

# Label names whose presence or absence is a known, named mechanism; every
# other name is generalised to its class so one root cause clusters once.
SPECIAL_LABELS = ("__error__", "__error_details__", "detected_level", "service_name", "level",
                  "__stream_shard__", "__line__", "__timestamp__")
REL_TOL = 1e-6

# Registry case ids of the deviations the diff marks as documented. Each id names a case under
# conformance/registry/cases/ whose gap status is documented, owner-kept or upstream.
DOC_INDEX_STATS_BYTES = "quality/index-stats-bytes-and-chunks"
DOC_INDEX_STATS_LOKI_CHUNKS = "quality/index-stats-loki-chunk-accounting"
DOC_VOLUME_BYTES = "profiles/volume-bytes-accounting"
DOC_DETECTED_LABELS_CARDINALITY = "drilldown/detected-labels-sampled-cardinality"
DOC_DETECTED_FIELDS_SERVICE = "profiles/detected-fields-no-extracted-suffix"
# Loki splits a stream's metric series per structured-metadata set, the proxy keeps the stream (owner-accepted): a count of
# series, such as a sum over a ratio of two per-stream metrics, differs by that split. A shape names it (shapes.json
# "documented_values"); no other value difference is covered by it.
DOC_METADATA_SPLIT = "metric-identity/structured-metadata-splits-series"
DOCUMENTED = (DOC_INDEX_STATS_BYTES, DOC_INDEX_STATS_LOKI_CHUNKS, DOC_VOLUME_BYTES, DOC_DETECTED_LABELS_CARDINALITY,
              DOC_DETECTED_FIELDS_SERVICE, DOC_METADATA_SPLIT)
# The owner's decision covers only the service name: detected_fields lists `service` / `service.name` where Loki
# lists their `_extracted` collision. Every other `_extracted` field is an ordinary difference.
SERVICE_EXTRACTED = {"service_extracted": ("service",), "service_name_extracted": ("service", "service.name")}


def label_class(name, stream_labels=()):
    """The class a label name falls in: itself when special, else its origin."""
    if name in SPECIAL_LABELS:
        return name
    if name.endswith("_extracted"):
        return "*_extracted"
    if name in stream_labels:
        return "<stream-label>"
    if "." in name:
        return "<dotted-key>"
    return "<line-or-metadata-key>"


def classes(names, stream_labels=()):
    return sorted({label_class(n, stream_labels) for n in names})


def facet(kind, detail, example, documented=None):
    out = {"kind": kind, "detail": detail, "example": example}
    assert documented is None or documented in DOCUMENTED, documented
    if documented:
        out["documented"] = documented
    return out


def short(value, limit=240):
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def error_text(body):
    if isinstance(body, dict):
        return str(body.get("error") or body.get("message") or body.get("_text") or "")
    return str(body or "")


def norm_error(text):
    """An error message without the parts that legitimately vary: whitespace and quoting."""
    text = re.sub(r"\s+", " ", str(text)).strip()
    return text.rstrip(".")


def size_class(loki_n, proxy_n):
    if loki_n == proxy_n:
        return "equal"
    if proxy_n == 0:
        return "proxy-empty"
    if loki_n == 0:
        return "loki-empty"
    return "proxy-fewer" if proxy_n < loki_n else "proxy-more"


def same_number(a, b):
    try:
        x, y = float(a), float(b)
    except (TypeError, ValueError):
        return a == b
    if math.isnan(x) and math.isnan(y):
        return True
    return math.isclose(x, y, rel_tol=REL_TOL, abs_tol=1e-9)


def same_sample(a, b):
    """A scalar or string result ([ts, value]): timestamps and values compared as numbers (1 equals 1.0)."""
    if not isinstance(a, list) or not isinstance(b, list) or len(a) != len(b):
        return a == b
    return all(same_number(x, y) for x, y in zip(a, b))


# ---------------------------------------------------------------- log streams

def entries(body):
    """Every log entry: (ts, line, stream labels, {category: {k: v}})."""
    out = []
    for stream in ((body.get("data") or {}).get("result") or []):
        labels = stream.get("stream") or {}
        for value in stream.get("values") or []:
            meta = value[2] if len(value) > 2 and isinstance(value[2], dict) else {}
            out.append((str(value[0]), value[1], dict(labels), meta))
    return out


def drop_limit_boundary(rows, limit, direction):
    """Loki fills `limit` lines and breaks ties at the last timestamp arbitrarily: drop that timestamp."""
    if not limit or len(rows) < limit or not rows:
        return rows
    stamps = [int(r[0]) for r in rows]
    edge = min(stamps) if direction != "forward" else max(stamps)
    return [r for r in rows if int(r[0]) != edge]


def order_violations(body, direction):
    """Streams whose entries are not in the query's direction (backward: newest first)."""
    bad = []
    for stream in ((body.get("data") or {}).get("result") or []):
        stamps = [int(v[0]) for v in stream.get("values") or []]
        ordered = sorted(stamps, reverse=direction != "forward")
        if stamps != ordered:
            bad.append(stream.get("stream") or {})
    return bad


def diff_streams(loki, proxy, stream_labels=(), limit=0, direction="backward", check_order=True):
    out = []
    if check_order:
        bad = order_violations(proxy, direction)
        if bad and not order_violations(loki, direction):
            out.append(facet("order", f"entries not in {direction} order within a stream",
                             f"{len(bad)} streams, e.g. {short(bad[0], 160)}"))
    a = drop_limit_boundary(entries(loki), limit, direction)
    b = drop_limit_boundary(entries(proxy), limit, direction)
    if len(a) != len(b):
        out.append(facet("entries", size_class(len(a), len(b)), f"loki {len(a)} entries, proxy {len(b)}"))
    index = {}
    for row in b:
        index.setdefault((row[0], row[1]), []).append(row)
    unmatched_a = []
    label_only_loki, label_only_proxy, label_value = {}, {}, {}
    cat_only_loki, cat_only_proxy = {}, {}
    for row in a:
        bucket = index.get((row[0], row[1]))
        if not bucket:
            unmatched_a.append(row)
            continue
        # Pair with the proxy row whose labels agree best: a line repeated in two streams.
        best = max(range(len(bucket)), key=lambda i: len(set(bucket[i][2].items()) & set(row[2].items())))
        other = bucket.pop(best)
        la, lb = merged_labels(row), merged_labels(other)
        for k in set(la) - set(lb):
            label_only_loki.setdefault(k, (row, other))
        for k in set(lb) - set(la):
            label_only_proxy.setdefault(k, (row, other))
        for k in set(la) & set(lb):
            if la[k] != lb[k]:
                label_value.setdefault(k, (row, other))
        if row[3] or other[3]:
            for cat in ("structuredMetadata", "parsed"):
                ka, kb = set((row[3].get(cat) or {})), set((other[3].get(cat) or {}))
                for k in ka - kb:
                    cat_only_loki.setdefault((cat, k), (row, other))
                for k in kb - ka:
                    cat_only_proxy.setdefault((cat, k), (row, other))
    unmatched_b = [r for rows in index.values() for r in rows]
    if unmatched_a and unmatched_b:
        stamps_b = {r[0] for r in unmatched_b}
        same_ts = [r for r in unmatched_a if r[0] in stamps_b]
        if same_ts:
            other = next(r for r in unmatched_b if r[0] == same_ts[0][0])
            out.append(facet("line", "line text differs at the same timestamp",
                             f"loki {short(same_ts[0][1], 120)} | proxy {short(other[1], 120)}"))
    if unmatched_a and len(a) == len(b):
        out.append(facet("entries", "different entries, same count",
                         f"loki-only {short(unmatched_a[0][1], 120)}"))
    if label_only_loki:
        names = sorted(label_only_loki)
        out.append(facet("labels", "loki-only " + ",".join(classes(names, stream_labels)),
                         f"names {names[:6]}; e.g. loki {short(merged_labels(label_only_loki[names[0]][0]), 200)}"))
    if label_only_proxy:
        names = sorted(label_only_proxy)
        out.append(facet("labels", "proxy-only " + ",".join(classes(names, stream_labels)),
                         f"names {names[:6]}; e.g. proxy {short(merged_labels(label_only_proxy[names[0]][1]), 200)}"))
    if label_value:
        names = sorted(label_value)
        row, other = label_value[names[0]]
        out.append(facet("labels", "value differs " + ",".join(classes(names, stream_labels)),
                         f"{names[0]}: loki {short(merged_labels(row).get(names[0]), 80)} | "
                         f"proxy {short(merged_labels(other).get(names[0]), 80)}"))
    for side, found in (("loki-only", cat_only_loki), ("proxy-only", cat_only_proxy)):
        by_cat = {}
        for cat, k in found:
            by_cat.setdefault(cat, []).append(k)
        for cat, names in sorted(by_cat.items()):
            out.append(facet("category", f"{side} {cat} " + ",".join(classes(names, stream_labels)),
                             f"{cat} names {sorted(names)[:6]}"))
    return out


def merged_labels(row):
    """Stream labels plus categorised metadata/parsed labels: what a client sees as the entry's labels."""
    out = dict(row[2])
    for cat in ("structuredMetadata", "parsed"):
        out.update(row[3].get(cat) or {})
    return out


# ---------------------------------------------------------------- metric series

def series_map(body):
    data = body.get("data") or {}
    out = {}
    for item in data.get("result") or []:
        key = json.dumps(item.get("metric") or {}, sort_keys=True)
        points = item.get("values") or ([item["value"]] if item.get("value") else [])
        out[key] = {str(float(p[0])): p[1] for p in points}
    return out


def malformed(m):
    """Samples whose value is not a number Loki could render (Loki writes FormatFloat; NaN/Inf are spelled out)."""
    bad = []
    for key, points in m.items():
        for t, v in points.items():
            try:
                float(v)
            except (TypeError, ValueError):
                bad.append((key, t, v))
    return bad


def raw_series_count(body):
    return len((body.get("data") or {}).get("result") or [])


def valid_sample_value(value, nonnegative=False):
    if not isinstance(value, str):
        return False
    if value in ("NaN", "+Inf", "-Inf"):
        return not nonnegative
    if not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value):
        return False
    number = float(value)
    return math.isfinite(number) and (not nonnegative or number >= 0)


def valid_metric_result(body, nonnegative=False):
    data = body.get("data")
    if (not isinstance(data, dict) or data.get("resultType") not in ("matrix", "vector")
            or not isinstance(data.get("result"), list)):
        return False
    try:
        identities = set()
        for row in data["result"]:
            if (not isinstance(row, dict) or not isinstance(row.get("metric"), dict)
                    or any(not isinstance(k, str) or not isinstance(v, str) for k, v in row["metric"].items())):
                return False
            identity = tuple(sorted(row["metric"].items()))
            if identity in identities:
                return False
            identities.add(identity)
            samples = row.get("values") if data["resultType"] == "matrix" else [row.get("value")]
            if not isinstance(samples, list) or not samples:
                return False
            timestamps = set()
            for point in samples:
                if (not isinstance(point, list) or len(point) != 2 or type(point[0]) not in (int, float)
                        or not math.isfinite(point[0]) or point[0] in timestamps
                        or not valid_sample_value(point[1], nonnegative)):
                    return False
                timestamps.add(point[0])
    except (TypeError, ValueError, OverflowError):
        return False
    return True


def valid_volume_result(body):
    return valid_metric_result(body, nonnegative=True)


def is_ranked_query(query):
    """True for a topk or bottomk expression (it starts with the operator, as bench/visual's is_ranked reads it)."""
    return str(query or "").lstrip().lower().startswith(("topk", "bottomk"))


def ranked_steps(m):
    """{timestamp: sorted values} of a series map: what a top-k answer holds at each step, whichever series carry it."""
    def number(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return math.inf  # a value that is no number sorts last and is compared as written by same_number
    out = {}
    for points in m.values():
        for t, v in points.items():
            out.setdefault(t, []).append(v)
    return {t: sorted(vs, key=number) for t, vs in out.items()}


def diff_ranked(a, b):
    """Facets of two top-k/bottom-k answers that compare the ranked values at every step and not the series kept.

    When several series tie across the k-th place at a step, Loki keeps the tied one its own iteration yields, and that
    differs between equivalent spellings of the same query on one Loki, while the ranked values never do; the proxy
    breaks the tie on the label key. Which tied series carries a value is therefore not a difference: a different
    value, a missing or extra step, a different number of ranked series at a step is."""
    out = []
    sa, sb = ranked_steps(a), ranked_steps(b)
    if set(sa) != set(sb):
        missing, extra = sorted(set(sa) - set(sb)), sorted(set(sb) - set(sa))
        out.append(facet("points", "timestamps differ in ranked answer",
                         f"loki {len(sa)} steps, proxy {len(sb)}; loki-only {missing[:2]} proxy-only {extra[:2]}"))
    counts = values = 0
    example = None
    for t in set(sa) & set(sb):
        u, v = sa[t], sb[t]
        if len(u) != len(v):
            counts += 1
            example = example or f"@{t}: loki {len(u)} ranked series, proxy {len(v)}"
        elif any(not same_number(x, y) for x, y in zip(u, v)):
            values += 1
            example = example or f"@{t}: loki {u} proxy {v}"
    if counts:
        out.append(facet("series", "ranked series count differs at a step", short(example)))
    if values:
        out.append(facet("values", "ranked values differ", short(example)))
    return out


def diff_series(loki, proxy, stream_labels=(), documented_values=None, ranked=False):
    out = []
    a, b = series_map(loki), series_map(proxy)
    dup_a, dup_b = raw_series_count(loki) - len(a), raw_series_count(proxy) - len(b)
    if dup_b > dup_a:
        out.append(facet("shape", "proxy returns one label set as several series",
                         f"proxy {raw_series_count(proxy)} series, {len(b)} distinct label sets; "
                         f"loki {raw_series_count(loki)} / {len(a)}"))
    bad = malformed(b)
    if bad and not malformed(a):
        key, t, v = bad[0]
        out.append(facet("shape", "proxy sample value is not a number",
                         f"{len(bad)} samples, e.g. {short(key, 120)} @{t}: {json.dumps(v)}"))
    keys_a = {k for s in a for k in json.loads(s)}
    keys_b = {k for s in b for k in json.loads(s)}
    if keys_a - keys_b:
        names = sorted(keys_a - keys_b)
        out.append(facet("labels", "loki-only " + ",".join(classes(names, stream_labels)), f"label names {names[:6]}"))
    if keys_b - keys_a:
        names = sorted(keys_b - keys_a)
        out.append(facet("labels", "proxy-only " + ",".join(classes(names, stream_labels)), f"label names {names[:6]}"))
    if ranked:
        return out + diff_ranked(a, b)
    only_a, only_b = sorted(set(a) - set(b)), sorted(set(b) - set(a))
    if (only_a or only_b) and not (keys_a ^ keys_b):
        out.append(facet("series", size_class(len(a), len(b)) if len(a) != len(b) else "different series, same count",
                         f"loki {len(a)} series, proxy {len(b)}; loki-only {short(only_a[:2], 160)} "
                         f"proxy-only {short(only_b[:2], 160)}"))
    elif len(a) != len(b):
        out.append(facet("series", size_class(len(a), len(b)), f"loki {len(a)} series, proxy {len(b)}"))
    missing_points = value_diffs = 0
    example = None
    for key in set(a) & set(b):
        pa, pb = a[key], b[key]
        if set(pa) != set(pb):
            missing_points += 1
            example = example or f"{key}: loki {len(pa)} points, proxy {len(pb)}"
        for t in set(pa) & set(pb):
            if not same_number(pa[t], pb[t]):
                value_diffs += 1
                example = example or f"{key} @{t}: loki {pa[t]} proxy {pb[t]}"
                break
    if missing_points:
        # A missing or extra sample is never byte accounting: only values are covered by documented_values.
        out.append(facet("points", "timestamps differ in matched series", short(example)))
    if value_diffs:
        ratio = totals_ratio(a, b)
        out.append(facet("values", f"values differ ({ratio})", short(example), documented_values))
    return out


def totals_ratio(a, b):
    def total(m):
        s = 0.0
        for points in m.values():
            for v in points.values():
                try:
                    s += float(v)
                except (TypeError, ValueError):
                    pass
        return s
    ta, tb = total(a), total(b)
    if not ta:
        return "loki total 0"
    r = tb / ta
    return "proxy higher" if r > 1 + REL_TOL else "proxy lower" if r < 1 - REL_TOL else "same total"


# ---------------------------------------------------------------- metadata endpoints

def name_set_diff(kind, a, b, stream_labels=(), documented=None):
    out = []
    a, b = set(a), set(b)
    if a - b:
        names = sorted(a - b)
        out.append(facet(kind, "loki-only " + ",".join(classes(names, stream_labels)), f"{names[:8]}", documented))
    if b - a:
        names = sorted(b - a)
        out.append(facet(kind, "proxy-only " + ",".join(classes(names, stream_labels)), f"{names[:8]}", documented))
    return out


def data_list(body):
    data = body.get("data") if isinstance(body, dict) else body
    return data if isinstance(data, list) else []


def unique_rows(rows, key=None):
    if not isinstance(rows, list):
        return None
    out = {}
    for row in rows:
        name = row.get(key) if key and isinstance(row, dict) else row if key is None else None
        if not isinstance(name, str) or name in out:
            return None
        out[name] = row
    return out


def diff_labels(loki, proxy, stream_labels=()):
    a, b = (unique_rows(body.get("data")) for body in (loki, proxy))
    if a is None or b is None:
        return [facet("shape", "invalid or duplicate label identity", "expected unique string labels")]
    return name_set_diff("names", a, b, stream_labels)


def diff_values(loki, proxy):
    a, b = set(map(str, data_list(loki))), set(map(str, data_list(proxy)))
    out = []
    if a != b:
        out.append(facet("values-set", size_class(len(a), len(b)) if len(a) != len(b) else "different values",
                         f"loki-only {sorted(a - b)[:5]} proxy-only {sorted(b - a)[:5]}"))
    return out


def diff_series_endpoint(loki, proxy, stream_labels=()):
    a = {json.dumps(x, sort_keys=True) for x in data_list(loki)}
    b = {json.dumps(x, sort_keys=True) for x in data_list(proxy)}
    out = []
    ka = {k for s in a for k in json.loads(s)}
    kb = {k for s in b for k in json.loads(s)}
    out += name_set_diff("labels", ka, kb, stream_labels)
    if a != b and not (ka ^ kb):
        out.append(facet("series", size_class(len(a), len(b)) if len(a) != len(b) else "different series, same count",
                         f"loki {len(a)}, proxy {len(b)}; loki-only {short(sorted(a - b)[:1], 160)} "
                         f"proxy-only {short(sorted(b - a)[:1], 160)}"))
    return out


def diff_index_stats(loki, proxy):
    out = []
    for key in ("streams", "entries"):
        if loki.get(key) != proxy.get(key):
            ratio = f" (loki/proxy {loki.get(key) / proxy.get(key):.2f})" if proxy.get(key) else ""
            out.append(facet("stats", f"{key} differ", f"loki {loki.get(key)} proxy {proxy.get(key)}{ratio}"))
    for key in ("bytes", "chunks"):
        if loki.get(key) != proxy.get(key):
            out.append(facet("stats", f"{key} differ", f"loki {loki.get(key)} proxy {proxy.get(key)}",
                             DOC_INDEX_STATS_BYTES))
    return out


def diff_detected_fields(loki, proxy, stream_labels=()):
    limits = [{"limit": body["limit"]} if "limit" in body else {} for body in (loki, proxy)]
    if any("limit" in value and (type(value["limit"]) is not int or value["limit"] < 0) for value in limits):
        return [facet("shape", "invalid detected fields limit", "expected a nonnegative integer limit")]
    a, b = (unique_rows([] if body == {} else body.get("fields"), "label") for body in (loki, proxy))
    if a is None or b is None:
        return [facet("shape", "invalid or duplicate field identity", "expected unique field labels")]
    for row in [*a.values(), *b.values()]:
        if ("type" in row and not isinstance(row["type"], str)
                or "cardinality" in row and (type(row["cardinality"]) is not int or row["cardinality"] < 0)
                or any(not isinstance(row.get(key, []), list) or any(not isinstance(v, str) for v in row[key])
                       for key in ("parsers", "jsonPath") if row.get(key) is not None)):
            return [facet("shape", "invalid detected field attributes", short(row.get("label")))]
    out = []
    if limits[0] != limits[1]:
        out.append(facet("field-limit", "detected fields limit differs", f"loki {limits[0]} proxy {limits[1]}"))
    only_a, only_b = set(a) - set(b), set(b) - set(a)
    pairs = [(old, names[0]) for old in sorted(only_a)
             if len(names := [name for name in SERVICE_EXTRACTED.get(old, ()) if name in only_b]) == 1]
    pairs = [(old, new) for old, new in pairs if sum(target == new for _, target in pairs) == 1]
    service = [old for old, _ in pairs]
    if service:
        out.append(facet("field", "loki-only service *_extracted", f"{sorted(service)}", DOC_DETECTED_FIELDS_SERVICE))
    out += name_set_diff("field", only_a - set(service), [], stream_labels)
    out += name_set_diff("field", [], only_b - {new for _, new in pairs}, stream_labels)
    for k, target in sorted([(k, k) for k in set(a) & set(b)] + pairs):
        x, y = a[k], b[target]
        if x.get("type") != y.get("type"):
            out.append(facet("field-type", f"type {x.get('type')} vs {y.get('type')}", f"{k}"))
        if sorted(x.get("parsers") or []) != sorted(y.get("parsers") or []):
            out.append(facet("field-parsers", f"parsers {sorted(x.get('parsers') or [])} vs "
                                              f"{sorted(y.get('parsers') or [])}", f"{k}"))
        if (x.get("jsonPath") or []) != (y.get("jsonPath") or []):
            out.append(facet("field-path", "JSON extraction path differs", f"{k}: {short(x.get('jsonPath'))} vs {short(y.get('jsonPath'))}"))
        ca, cb = x.get("cardinality") or 0, y.get("cardinality") or 0
        # Loki estimates cardinality with a HyperLogLog sketch: within 5% is the same answer.
        if abs(ca - cb) > max(1, 0.05 * ca):
            out.append(facet("field-cardinality", size_class(ca, cb), f"{k}: loki {ca} proxy {cb}"))
    return dedupe(out)


def diff_detected_labels(loki, proxy, stream_labels=()):
    rows = [unique_rows(body.get("detectedLabels"), "label") for body in (loki, proxy)]
    if any(side is None for side in rows):
        return [facet("shape", "invalid or duplicate detected label identity", "expected unique label identities")]
    a, b = ({key: row.get("cardinality") for key, row in side.items()} for side in rows)
    out = name_set_diff("names", a, b, stream_labels)
    differ = [k for k in set(a) & set(b) if a[k] != b[k]]
    if differ:
        k = sorted(differ)[0]
        out.append(facet("cardinality", "label cardinality differs", f"{k}: loki {a[k]} proxy {b[k]}",
                         DOC_DETECTED_LABELS_CARDINALITY))
    return out


def diff_patterns(loki, proxy):
    sides = []
    try:
        for body in (loki, proxy):
            rows, entries = body.get("data"), {}
            if not isinstance(rows, list):
                raise ValueError("pattern data is not a list")
            for row in rows:
                if (not isinstance(row, dict) or not isinstance(row.get("pattern"), str)
                        or not isinstance(row.get("level"), str) or not isinstance(row.get("samples"), list)
                        or not row["samples"]):
                    raise ValueError("invalid pattern entry")
                key, samples = (row["pattern"], row["level"]), {}
                if key in entries:
                    raise ValueError("duplicate pattern-level identity")
                for pair in row["samples"]:
                    if (not isinstance(pair, list) or len(pair) != 2
                            or any(type(v) not in (int, float) or not math.isfinite(v) for v in pair)
                            or pair[1] < 0 or pair[1] != int(pair[1])):
                        raise ValueError("invalid pattern sample")
                    if pair[0] in samples:
                        raise ValueError("duplicate pattern timestamp")
                    samples[pair[0]] = pair[1]
                entries[key] = samples
            sides.append(entries)
    except (TypeError, ValueError, OverflowError) as error:
        return [facet("patterns", "invalid pattern response", str(error))]
    a, b = sides
    out = []
    if set(a) != set(b):
        out.append(facet("patterns", size_class(len(a), len(b)) if len(a) != len(b) else "different patterns",
                         f"loki {len(a)} proxy {len(b)}; loki-only {short(sorted(set(a) - set(b))[:2], 160)}"))
    for key in sorted(set(a) & set(b)):
        if set(a[key]) != set(b[key]):
            out.append(facet("patterns", "sample timestamps differ", short(key)))
        if any(a[key][t] != b[key][t] for t in set(a[key]) & set(b[key])):
            out.append(facet("patterns", "sample counts differ", short(key)))
    return dedupe(out)


def diff_format_query(loki, proxy):
    a, b = (loki.get("data") if isinstance(loki, dict) else loki), (proxy.get("data") if isinstance(proxy, dict) else proxy)
    return [] if a == b else [facet("format", "formatted text differs", f"loki {short(a, 120)} | proxy {short(b, 120)}")]


def dedupe(facets):
    seen, out = set(), []
    for f in facets:
        key = (f["kind"], f["detail"])
        if key not in seen:
            seen.add(key)
            out.append(f)
    return out


def result_type(body):
    return ((body.get("data") or {}).get("resultType")) if isinstance(body, dict) else None


def diff(endpoint, loki_status, loki, proxy_status, proxy, stream_labels=(), limit=0, direction="backward", query="",
         documented_values=None):
    """Facets of the difference between two answers to one request (an empty list: equivalent).

    query is the request's expression: a top-k answer is compared by its ranked values (diff_ranked). documented_values
    is the registry id of a deviation a shape declares for its value differences (DOC_METADATA_SPLIT)."""
    if loki_status != proxy_status:
        return [facet("status", f"loki {loki_status} proxy {proxy_status}",
                      f"loki {short(error_text(loki) or '-', 160)} | proxy {short(error_text(proxy) or '-', 160)}")]
    if loki_status != 200:
        out = []
        if norm_error(error_text(loki)) != norm_error(error_text(proxy)):
            out.append(facet("error-text", f"{loki_status} with different text",
                             f"loki {short(error_text(loki), 160)} | proxy {short(error_text(proxy), 160)}"))
        types = [{key: body[key] for key in ("errorType",) if key in body} if isinstance(body, dict) else {}
                 for body in (loki, proxy)]
        if types[0] != types[1]:
            out.append(facet("error-type", f"{loki_status} with different errorType",
                             f"loki {short(types[0], 160)} | proxy {short(types[1], 160)}"))
        return out
    if not isinstance(loki, dict) or not isinstance(proxy, dict):
        return [facet("shape", "answer not JSON on one side", f"loki {short(loki, 80)} | proxy {short(proxy, 80)}")]
    out = []
    if endpoint in ("query_range", "query", "tail"):
        ta, tb = result_type(loki), result_type(proxy)
        if endpoint != "tail" and ta != tb:
            return [facet("result-type", f"{ta} vs {tb}", "")]
        if endpoint == "tail" or ta == "streams":
            # A tail merges frames as they arrive, so only a query answer has an entry order to compare.
            out += diff_streams(loki, proxy, stream_labels, limit, direction, check_order=endpoint != "tail")
        elif ta in ("matrix", "vector"):
            out += diff_series(loki, proxy, stream_labels, documented_values=documented_values, ranked=is_ranked_query(query))
        else:
            a, b = (loki.get("data") or {}).get("result"), (proxy.get("data") or {}).get("result")
            if not same_sample(a, b):
                out.append(facet("values", f"{ta} differs", f"loki {short(a, 80)} proxy {short(b, 80)}"))
    elif endpoint in ("volume", "volume_range"):
        if not all(valid_volume_result(body) for body in (loki, proxy)):
            return [facet("shape", "invalid metric volume answer", "expected a complete vector or matrix")]
        if result_type(loki) != result_type(proxy):
            return [facet("result-type", f"{result_type(loki)} vs {result_type(proxy)}", "")]
        out += diff_series(loki, proxy, stream_labels, documented_values=DOC_VOLUME_BYTES)
    elif endpoint == "labels":
        out += diff_labels(loki, proxy, stream_labels)
    elif endpoint in ("label_values", "detected_field_values"):
        if endpoint == "detected_field_values":
            loki, proxy = {"data": loki.get("values") or []}, {"data": proxy.get("values") or []}
        out += diff_values(loki, proxy)
    elif endpoint == "series":
        out += diff_series_endpoint(loki, proxy, stream_labels)
    elif endpoint == "index_stats":
        out += diff_index_stats(loki, proxy)
    elif endpoint == "detected_fields":
        out += diff_detected_fields(loki, proxy, stream_labels)
    elif endpoint == "detected_labels":
        out += diff_detected_labels(loki, proxy, stream_labels)
    elif endpoint == "patterns":
        out += diff_patterns(loki, proxy)
    elif endpoint == "format_query":
        out += diff_format_query(loki, proxy)
    return out


def vacuous(endpoint, loki_status, loki, proxy_status, proxy):
    """Both sides answered 200 with nothing: proves nothing, so it is not counted as parity."""
    if loki_status != 200 or proxy_status != 200:
        return False

    def empty(body):
        if not isinstance(body, dict):
            return not body
        data = body.get("data")
        if isinstance(data, dict):
            return not data.get("result")
        if isinstance(data, list):
            return not data
        for key in ("fields", "detectedLabels", "values"):
            if key in body:
                return not body[key]
        if endpoint == "index_stats":
            return not body.get("entries")
        return False
    return empty(loki) and empty(proxy)
