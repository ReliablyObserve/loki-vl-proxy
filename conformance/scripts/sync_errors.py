#!/usr/bin/env python3
"""Audit the proxy's client-facing error surface into the conformance registry.

Every message the proxy can return to a client is a compatibility contract: the
status, the Loki errorType derived from it, and the text. This script collects
them from the source (writeError / writeBackendError / writeGrafanaStatsFailure
call sites and the error strings they format), groups them by status, and seeds
one registry file per failure class under conformance/registry/loki/errors/.

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
    "http.StatusGatewayTimeout": 504,
}
ERROR_TYPE = {400: "bad_data", 404: "not_found", 406: "not_acceptable", 422: "execution",
              499: "canceled", 500: "internal", 502: "unavailable", 503: "timeout", 504: "timeout"}
CALL_RE = re.compile(r'p\.writeError\(\s*w,\s*([A-Za-z0-9_.]+)\s*,\s*(.+?)\)\s*$')
MESSAGE_RE = re.compile(r'"([^"]{4,})"')


def sources():
    for name in sorted(os.listdir(SOURCE_DIR)):
        if name.endswith(".go") and not name.endswith("_test.go"):
            path = os.path.join(SOURCE_DIR, name)
            yield path, read_text(path)


def main():
    entries = []
    for path, text in sources():
        for number, line in enumerate(text.splitlines(), 1):
            match = CALL_RE.search(line.strip())
            if not match:
                continue
            status_token, argument = match.group(1), match.group(2)
            status = STATUS_NAMES.get(status_token)
            literal = MESSAGE_RE.findall(argument)
            entries.append({
                "where": f"{path}:{number}",
                "status_token": status_token,
                "status": status,
                "error_type": ERROR_TYPE.get(status) if status else None,
                "message": literal[0] if literal else None,
                "dynamic": not literal,
                "expression": argument.strip()[:160],
            })
    by_status = {}
    for entry in entries:
        key = str(entry["status"] or entry["status_token"])
        by_status.setdefault(key, []).append(entry)
    payload = {"call_sites": len(entries), "by_status": {
        key: {"count": len(value), "error_type": value[0]["error_type"],
              "messages": sorted({e["message"] for e in value if e["message"]}),
              "dynamic_sites": sum(1 for e in value if e["dynamic"])}
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
        print(f"  {key:5} {summary['error_type'] or '-':12} sites={summary['count']:3} "
              f"literal={len(summary['messages']):3} dynamic={summary['dynamic_sites']:3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
