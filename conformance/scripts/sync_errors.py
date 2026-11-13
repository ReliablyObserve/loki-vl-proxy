#!/usr/bin/env python3
"""Audit the proxy's client-facing error surface into the conformance registry.

Every message the proxy can return to a client is a compatibility contract: the
status, the Loki errorType derived from it, and the text. This script collects
them from the source (every error writer in WRITERS, http.Error and bodyless
error statuses, and the strings they format), classifies each site as a literal
message, an error passed through the error layer (lokiClientError: Loki's text
or Loki's fixed text for the status), a message formatted at run time, or no
body, groups them by status, and seeds one registry file per failure class
under conformance/registry/loki/errors/ (only files that do not exist yet).

Writes conformance/registry/generated/proxy/errors.json.

Usage: sync_errors.py
"""
import json, os, re, sys
from registry_io import read_text, write_text

SOURCE_DIR = "internal/proxy"
STATUS_NAMES = {
    "http.StatusBadRequest": 400, "http.StatusUnauthorized": 401, "http.StatusForbidden": 403,
    "http.StatusNotFound": 404, "http.StatusMethodNotAllowed": 405, "http.StatusNotAcceptable": 406,
    "http.StatusRequestEntityTooLarge": 413, "http.StatusUnprocessableEntity": 422,
    "http.StatusTooManyRequests": 429, "http.StatusInternalServerError": 500,
    "http.StatusBadGateway": 502, "http.StatusServiceUnavailable": 503,
    "http.StatusGatewayTimeout": 504, "http.StatusRequestTimeout": 408,
}
# A status computed at run time (from the backend's answer or the error) is
# grouped under one key instead of one key per variable name.
DERIVED = "derived"
# The errorType the proxy derives from a status (lokiErrorType in
# internal/proxy/http_utils.go): the listed codes, else bad_data for 4xx.
ERROR_TYPE = {400: "bad_data", 404: "not_found", 406: "not_acceptable", 422: "execution",
              499: "canceled", 500: "internal", 502: "unavailable", 503: "timeout", 504: "timeout"}


def error_type(status):
    if status is None:
        return None
    return ERROR_TYPE.get(status, "bad_data" if 400 <= status < 500 else "internal")
# Every way the proxy writes a client-facing error, by helper name: where the status and the message sit in
# the call's arguments. "msg" helpers write the message they are given (internal/proxy/http_utils.go
# writeError, writeErrorCause and writeErrorShaped, the delete route's writeDeleteError and writeLokiTextError);
# "layer" helpers pass an error through lokiClientError, which replaces any text the proxy did not write in
# Loki's words with Loki's fixed text for the status (writeErr, writeErrShaped and the helpers built on them), or
# write a typed internal/lokierr error as it is (fail).
WRITERS = {
    "writeError": ("msg", 1, 2), "writeErrorCause": ("msg", 1, 2), "writeErrorShaped": ("msg", 1, 2),
    "writeDeleteError": ("msg", 1, 2), "writeLokiTextError": ("msg", 1, 2),
    "writeErr": ("layer", None, None), "writeErrShaped": ("layer", None, None),
    "writeBackendError": ("layer", None, None), "writeGrafanaStatsFailure": ("layer", None, None),
    "writeVLStatsErrorBody": ("layer", None, None), "fail": ("layer", None, None),
}
WRITER_RE = re.compile(r'\bp\.(' + "|".join(WRITERS) + r')\(')
# Bodies Loki's own handlers write with net/http: http.Error(w, msg, code) and a status with no body.
HTTP_ERROR_RE = re.compile(r'\bhttp\.Error\(')
BODYLESS_RE = re.compile(r'\bwriteBodylessError\(')
FUNC_RE = re.compile(r'^func (?:\([^)]*\) )?(\w+)\(', re.M)
CONST_RE = re.compile(r'^\s*(?:(?:const|var)\s+)?(\w+)\s*(?:string\s*)?=\s*("(?:[^"\\]|\\.)*")\s*(?://.*)?$', re.M)
MESSAGE_RE = re.compile(r'"([^"]{4,})"')
ONLY_LITERAL_RE = re.compile(r'^"(?:[^"\\]|\\.)*"$')


def call_arguments(text, start):
    """The top-level arguments of the call whose opening parenthesis is at
    text[start - 1], and the offset just past its closing parenthesis. A call
    may span several lines."""
    depth, quote, current, args, i = 1, None, [], [], start
    while i < len(text) and depth:
        c = text[i]
        if quote:
            current.append(c)
            if c == "\\" and quote != "`":
                current.append(text[i + 1])
                i += 1
            elif c == quote:
                quote = None
        elif c in "\"'`":
            quote = c
            current.append(c)
        elif c in "([{":
            depth += 1
            current.append(c)
        elif c in ")]}":
            depth -= 1
            if depth:
                current.append(c)
        elif c == "," and depth == 1:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(c)
        i += 1
    args.append("".join(current).strip())
    return [" ".join(a.split()) for a in args], i


def sources():
    for name in sorted(os.listdir(SOURCE_DIR)):
        if name.endswith(".go") and not name.endswith("_test.go"):
            path = os.path.join(SOURCE_DIR, name)
            yield path, read_text(path)


def string_constants(files):
    """Package-level string constants and variables of internal/proxy: name -> literal."""
    found = {}
    for _, text in files:
        for name, literal in CONST_RE.findall(text):
            found.setdefault(name, literal)
    return found


def is_literal(argument, constants):
    """One string literal, a string constant, or a concatenation of those: text the proxy wrote."""
    parts = [p.strip() for p in argument.split("+")]
    return all(ONLY_LITERAL_RE.match(p) or p in constants for p in parts) and bool(parts)


def message_of(argument, constants):
    literal = MESSAGE_RE.findall(argument)
    if literal:
        return literal[0]
    for part in (p.strip() for p in argument.split("+")):
        if part in constants:
            return constants[part][1:-1]
    return None


def enclosing(text, offset):
    names = [m.group(1) for m in FUNC_RE.finditer(text, 0, offset)]
    return names[-1] if names else ""


def site(path, text, offset, writer, status_token, argument, kind, constants):
    status = STATUS_NAMES.get(status_token) or (int(status_token) if status_token and status_token.isdigit() else None)
    return {
        # The enclosing function, not the line: a site is found again from its name, and an edit above it in
        # the file does not change this file (the conformance gate compares it byte for byte).
        "where": f"{path} {enclosing(text, offset) or '(package level)'}",
        "writer": writer,
        "status_token": status_token,
        "status": status,
        "error_type": error_type(status),
        "message": message_of(argument, constants) if kind != "layer" else None,
        "kind": kind,
        # Text formatted at run time can carry what the proxy did not write; an error's own text most of all.
        "dynamic": kind == "dynamic",
        "error_text": kind == "dynamic" and ".Error()" in argument,
        "expression": argument[:160],
    }


def main():
    entries = []
    files = list(sources())
    constants = string_constants(files)
    for path, text in files:
        for match in WRITER_RE.finditer(text):
            # A writer's own body passes its parameters on; its callers are the sites.
            if enclosing(text, match.start()) in WRITERS:
                continue
            args, _ = call_arguments(text, match.end())
            mode, status_at, message_at = WRITERS[match.group(1)]
            if mode == "layer":
                entries.append(site(path, text, match.start(), match.group(1), None, ", ".join(args[1:]), "layer",
                                    constants))
                continue
            if len(args) <= message_at:
                continue
            argument = args[message_at]
            entries.append(site(path, text, match.start(), match.group(1), args[status_at], argument,
                                "literal" if is_literal(argument, constants) else "dynamic", constants))
        for match in HTTP_ERROR_RE.finditer(text):
            args, _ = call_arguments(text, match.end())
            if len(args) < 3:
                continue
            entries.append(site(path, text, match.start(), "http.Error", args[2], args[1],
                                "literal" if is_literal(args[1], constants) else "dynamic", constants))
        for match in BODYLESS_RE.finditer(text):
            if enclosing(text, match.start()) == "writeBodylessError" or text[match.start() - 5:match.start()] == "func ":
                continue
            args, _ = call_arguments(text, match.end())
            entries.append(site(path, text, match.start(), "writeBodylessError", args[1] if len(args) > 1 else None,
                                "", "no_body", constants))
    by_status = {}
    for entry in entries:
        key = str(entry["status"] or DERIVED)
        by_status.setdefault(key, []).append(entry)
    payload = {"call_sites": len(entries), "by_status": {
        key: {"count": len(value), "error_type": value[0]["error_type"],
              "messages": sorted({e["message"] for e in value if e["message"]}),
              "literal_sites": sum(1 for e in value if e["kind"] == "literal"),
              "layer_sites": sum(1 for e in value if e["kind"] == "layer"),
              "no_body_sites": sum(1 for e in value if e["kind"] == "no_body"),
              "dynamic_sites": sum(1 for e in value if e["dynamic"]),
              "error_text_sites": sum(1 for e in value if e["error_text"])}
        for key, value in sorted(by_status.items())}}
    os.makedirs("conformance/registry/generated/proxy", exist_ok=True)
    with open("conformance/registry/generated/proxy/errors.json", "w") as handle:
        json.dump({"entries": entries, "summary": payload}, handle, indent=2, sort_keys=True)
        handle.write("\n")

    os.makedirs("conformance/registry/loki/errors", exist_ok=True)
    written = 0
    for key, summary in payload["by_status"].items():
        identifier = f"status-{key}"
        target = f"conformance/registry/loki/errors/{identifier}.yaml"
        if os.path.exists(target):
            continue
        messages = "\n".join(f"    - {json.dumps(m)}" for m in summary["messages"][:40])
        write_text(target,
            f"id: {identifier}\nstatus: {key}\nerror_type: {summary['error_type']}\n"
            f"generated:\n  proxy_call_sites: {summary['count']}\n"
            f"  dynamic_sites: {summary['dynamic_sites']}\n  literal_messages:\n{messages}\n\n"
            "loki_behaviour: |\n  TODO: when Loki returns this status, with its own message text\n"
            "  and errorType, and which errors take precedence over it.\n"
            "precedence: []   # failure classes Loki evaluates before this one\n"
            "consumers: |\n  TODO: how Grafana Explore and Logs Drilldown render it.\n"
            "cases: []        # case ids that assert status, errorType, message and headers together\n")
        written += 1
    print(f"errors: {len(entries)} call sites across {len(payload['by_status'])} statuses "
          f"({written} registry files written)")
    for key, summary in payload["by_status"].items():
        print(f"  {key:7} sites={summary['count']:3} literal={summary['literal_sites']:3} "
              f"layer={summary['layer_sites']:3} dynamic={summary['dynamic_sites']:3} "
              f"(error text {summary['error_text_sites']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
