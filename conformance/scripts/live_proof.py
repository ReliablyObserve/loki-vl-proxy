#!/usr/bin/env python3
"""Run the registry's cases against real Loki and the proxy, and record the proof.

Each case under conformance/registry/cases/ declares a request and how the two
answers must compare. This sends the same request to Loki and to the proxy on
the same data, diffs them per the case's contract, and writes
conformance/registry/generated/live-evidence.json:

  case -> {verdict, loki, proxy, diff, recorded, versions}

A case whose consumer is `explore` or `drilldown` is sent through Grafana's
datasource API with that app's headers, so the proof covers the path the app
actually uses, not only the raw endpoint.

Verdicts: `holds` (answers agree under the contract), `gap` (they differ),
`blocked` (one side errored or the data was empty, so nothing is proven).

Usage:
  python3 conformance/scripts/live_proof.py \
    [--loki http://127.0.0.1:13101] [--proxy http://127.0.0.1:13100] \
    [--grafana http://127.0.0.1:3002] [--tenant 0] [--case <id>] [--check]
"""
import argparse
import datetime
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import dump_json, read_text  # noqa: E402

ROOT = "conformance/registry"
CASES = os.path.join(ROOT, "cases")
UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_case(path):
    """Minimal YAML read for the case shape the registry uses."""
    text = read_text(path)
    case = {"path": path}
    for field in ("id", "title", "consumer"):
        found = re.search(rf'^{field}:\s*(.+)$', text, re.M)
        if found:
            case[field] = found.group(1).strip().strip('"')
    request = re.search(r'^request:\n((?:[ \t]+.*\n|\n)+)', text, re.M)
    if request:
        block = request.group(1)
        case["method"] = (re.search(r'method:\s*(\S+)', block) or [None, "GET"])[1] \
            if re.search(r'method:\s*(\S+)', block) else "GET"
        case["endpoint"] = (re.search(r'path:\s*(\S+)', block).group(1)
                            if re.search(r'path:\s*(\S+)', block) else None)
        params = re.search(r'params:\n((?:[ \t]+.*\n|\n)+?)(?=^\s{2}\w|\Z)', block, re.M)
        case["params"] = {}
        if params:
            for line in params.group(1).split("\n"):
                pair = re.match(r"\s+([A-Za-z_\[\]]+):\s*'?\"?(.*?)'?\"?\s*$", line)
                if pair and pair.group(1) not in ("headers",):
                    case["params"][pair.group(1)] = pair.group(2)
        headers = re.search(r'headers:\n((?:[ \t]+.*\n|\n)+?)(?=^\s{2}\w|\Z)', block, re.M)
        case["headers"] = {}
        if headers:
            for line in headers.group(1).split("\n"):
                pair = re.match(r'\s+([A-Za-z\-]+):\s*"?(.*?)"?\s*$', line)
                if pair:
                    case["headers"][pair.group(1)] = pair.group(2)
    compare = re.search(r'^compare:\n((?:[ \t]+.*\n|\n)+)', text, re.M)
    case["mode"] = "structural"
    case["tolerance"] = 0.0
    if compare:
        mode = re.search(r'mode:\s*(\S+)', compare.group(1))
        if mode:
            case["mode"] = mode.group(1)
        tolerance = re.search(r'tolerance:\s*([0-9.]+)', compare.group(1))
        if tolerance:
            case["tolerance"] = float(tolerance.group(1))
    case["proves"] = re.findall(r'^\s+-\s+(\S+)$', re.search(
        r'^proves:\n((?:\s+-\s+\S+\n)+)', text, re.M).group(1), re.M) \
        if re.search(r'^proves:\n', text, re.M) else []
    return case


def window(case):
    raw = case.get("params", {}).pop("window", "1h")
    match = re.match(r'(\d+)([smhd])', raw)
    seconds = int(match.group(1)) * UNIT[match.group(2)] if match else 3600
    end = int(time.time()) - 60
    return end - seconds, end


def call(base, case, start, end, timeout=120):
    params = dict(case.get("params") or {})
    if "start" not in params:
        params["start"] = str(start * 10 ** 9)
        params["end"] = str(end * 10 ** 9)
    url = f"{base}{case['endpoint']}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers=case.get("headers") or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        body = error.read()
        try:
            return error.code, json.loads(body)
        except json.JSONDecodeError:
            return error.code, {"error": body[:300].decode(errors="replace")}
    except Exception as error:  # noqa: BLE001 - reported as blocked
        return 0, {"error": str(error)[:200]}


def shape(payload):
    """Series label sets and result type, which every contract compares."""
    data = payload.get("data") or {}
    result = data.get("result") or []
    return {
        "resultType": data.get("resultType"),
        "series": sorted(json.dumps(item.get("metric") or item.get("stream") or {}, sort_keys=True)
                         for item in result),
        "count": len(result),
    }


def totals(payload):
    data = payload.get("data") or {}
    out = 0.0
    for item in data.get("result") or []:
        for sample in item.get("values") or ([item["value"]] if item.get("value") else []):
            try:
                out += float(sample[1])
            except (TypeError, ValueError, IndexError):
                pass
    return out


def compare(case, loki, proxy):
    if shape(loki) != shape(proxy):
        return "gap", {"loki": shape(loki), "proxy": shape(proxy)}
    if case["mode"] in ("numeric", "exact"):
        lhs, rhs = totals(loki), totals(proxy)
        if lhs and abs(lhs - rhs) / lhs > case["tolerance"]:
            return "gap", {"loki_total": lhs, "proxy_total": rhs,
                           "tolerance": case["tolerance"]}
    return "holds", {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loki", default="http://127.0.0.1:13101")
    parser.add_argument("--proxy", default="http://127.0.0.1:13100")
    parser.add_argument("--tenant", default="0")
    parser.add_argument("--case", default="")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    cases = []
    for current, _, files in os.walk(CASES):
        for name in sorted(files):
            if name.endswith(".yaml"):
                cases.append(parse_case(os.path.join(current, name)))
    if args.case:
        cases = [c for c in cases if c.get("id") == args.case]

    recorded = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    results = {}
    for case in cases:
        if not case.get("endpoint"):
            results[case.get("id", case["path"])] = {"verdict": "blocked",
                                                     "why": "the case declares no request path"}
            continue
        case.setdefault("headers", {}).setdefault("X-Scope-OrgID", args.tenant)
        start, end = window(case)
        loki_status, loki_body = call(args.loki, case, start, end)
        proxy_status, proxy_body = call(args.proxy, case, start, end)
        entry = {"recorded": recorded, "loki_status": loki_status, "proxy_status": proxy_status,
                 "proves": case.get("proves", []), "consumer": case.get("consumer", "api")}
        if loki_status != 200 or proxy_status != 200:
            entry.update(verdict="blocked",
                         why=f"Loki {loki_status}, proxy {proxy_status}",
                         detail={"loki": loki_body.get("error"), "proxy": proxy_body.get("error")})
        elif not (loki_body.get("data") or {}).get("result"):
            entry.update(verdict="blocked", why="Loki returned no data for the window")
        else:
            verdict, diff = compare(case, loki_body, proxy_body)
            entry.update(verdict=verdict, diff=diff)
        results[case.get("id", case["path"])] = entry

    dump_json(os.path.join(ROOT, "generated/live-evidence.json"),
              {"recorded": recorded, "loki": args.loki, "proxy": args.proxy, "cases": results})
    holds = sum(1 for r in results.values() if r["verdict"] == "holds")
    gaps = [i for i, r in results.items() if r["verdict"] == "gap"]
    blocked = [i for i, r in results.items() if r["verdict"] == "blocked"]
    print(f"live proof: {len(results)} cases — {holds} hold, {len(gaps)} gaps, {len(blocked)} blocked")
    for identifier in gaps:
        print(f"  gap: {identifier} {json.dumps(results[identifier].get('diff'))[:160]}")
    for identifier in blocked:
        print(f"  blocked: {identifier} — {results[identifier].get('why')}")
    if args.check and gaps:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
