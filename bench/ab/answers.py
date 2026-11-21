"""Loki query rejection evidence shared by API and browser measurements."""
from functools import lru_cache
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from bench.visual.vio import error_text as error_text  # noqa: E402


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
