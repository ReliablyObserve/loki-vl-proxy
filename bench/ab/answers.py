"""Loki query rejection evidence shared by API and browser measurements."""
from functools import lru_cache
from pathlib import Path
import json
import math
import re
import secrets
import sys
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def source_endpoint(url):
    """Comparable endpoint identity; malformed/credential-bearing sources are not proof."""
    try:
        value = urlsplit(url)
        if value.scheme not in ("http", "https") or not value.hostname or value.username is not None or value.password is not None:
            return None
        return value.scheme, value.hostname.lower().rstrip("."), value.port if value.port is not None else (443 if value.scheme == "https" else 80), value.path.rstrip("/")
    except (ValueError, TypeError, AttributeError):
        return None


def strict_json(body):
    """Wire JSON numbers are finite; Loki's quoted NaN/Inf samples remain strings."""
    def number(text, convert):
        value = convert(text)
        try:
            if math.isfinite(value):
                return value
        except OverflowError:
            pass
        raise ValueError("non-finite JSON number")
    def constant(_):
        raise ValueError("nonstandard JSON constant")
    return json.loads(body, parse_constant=constant, parse_float=lambda v: number(v, float), parse_int=lambda v: number(v, int))


def correctness_spelling(query, shape):
    """Equivalent spelling only for extra correctness reads; measured queries are unchanged."""
    if not query or shape.get("path") and not shape.get("query") and not shape.get("instant"):
        return query, "unchanged-no-query"
    return query + "".join("\t" if byte & (1 << bit) else " "
                           for byte in secrets.token_bytes(16) for bit in range(8)), "space-tab-suffix-128"


def correctness_endpoint(shape):
    from bench.parity.run import PATHS
    path = shape.get("path")
    return next((name for name, template in PATHS.items() if re.fullmatch(
        re.escape(template).replace(r"\{name\}", "[^/]+"), path)), None) if path else "query" if shape.get("instant") else "query_range"


def metadata_endpoint(shape):
    return correctness_endpoint(shape) in ("labels", "label_values", "detected_labels", "detected_fields", "patterns", "index_stats", "volume", "volume_range")


def metadata_schema(parsed, endpoint):
    """Actual Loki API envelopes, not query-result lookalikes or unwrapped guesses."""
    uint = lambda value: type(value) is int and value >= 0
    strings = lambda value: isinstance(value, list) and all(isinstance(v, str) for v in value)
    if endpoint in ("labels", "label_values"):
        rows = parsed.get("data")
        return strings(rows) and len(set(rows)) == len(rows)
    if endpoint == "index_stats":
        return all(uint(parsed.get(k)) for k in ("streams", "chunks", "bytes", "entries"))
    if endpoint in ("detected_fields", "detected_labels"):
        if endpoint == "detected_fields" and not parsed:
            return True  # semantics/detected-fields-empty-answer: Loki omits empty fields and limit.
        rows = parsed.get("fields" if endpoint == "detected_fields" else "detectedLabels")
        return isinstance(rows, list) and all(isinstance(v, dict) and isinstance(v.get("label"), str)
            and uint(v.get("cardinality")) and (endpoint != "detected_fields" or isinstance(v.get("type"), str)
            and (v.get("parsers") is None or strings(v["parsers"])) and strings(v.get("jsonPath", []))) for v in rows) and len({v["label"] for v in rows}) == len(rows)
    if endpoint == "patterns":
        rows = parsed.get("data")
        return isinstance(rows, list) and all(isinstance(v, dict) and isinstance(v.get("pattern"), str)
            and isinstance(v.get("level", ""), str) and isinstance(v.get("samples"), list)
            and all(isinstance(p, list) and len(p) == 2 and uint(p[0]) and uint(p[1]) for p in v["samples"]) for v in rows)
    return None


@lru_cache(maxsize=1)
def rejection_patterns():
    # The registry CLI prepends its script directory for sibling imports. Keep the caller's module resolution.
    original_path = sys.path[:]
    try:
        from conformance.scripts import error_registry
    finally:
        sys.path[:] = original_path
    messages = error_registry.parse_messages((ROOT / error_registry.MESSAGES_GO).read_text(),
                                             (ROOT / error_registry.KINDS_GO).read_text())
    patterns = []
    rendered = set()
    for msg in messages.values():
        if msg["kind"] not in ("parse", "param", "limit"):
            continue
        for template in msg["templates"]:
            pieces = re.split(r"<\w+>", template)
            if not any(pieces):
                rendered.add(msg["id"])
                continue
            patterns.append(re.compile(".+?".join(map(re.escape, pieces)), re.S))
    # Constructors that forward arbitrary text are identified only by the diagnostics Loki actually recorded.
    _, rows = error_registry.load_rows(ROOT / error_registry.ANSWERS)
    observed = set()
    for row in rows:
        if row.get("msg") not in rendered:
            continue
        reply = row["loki"]
        if reply["status"] not in (400, 422, 500) or not reply.get("body"):
            continue
        body = reply["body"].strip()
        try:
            envelope = json.loads(body)
        except ValueError:
            envelope = None
        if isinstance(envelope, dict):
            body = envelope.get("error")
        if isinstance(body, str) and body:
            observed.add(body)
    patterns.extend(re.compile(re.escape(body)) for body in observed)
    return patterns


def query_rejection(message):
    """Recognize the parser's rendered diagnostics and the error registry's concrete templates."""
    if not isinstance(message, str):
        return False
    detail = message.strip()
    if re.match(r"(?:parse error(?: at line \d+, col \d+)?|pipeline error|syntax error)\s*:\s*\S", detail, re.I):
        return True
    return any(pattern.fullmatch(detail) for pattern in rejection_patterns())
