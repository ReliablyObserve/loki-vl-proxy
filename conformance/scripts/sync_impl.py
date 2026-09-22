#!/usr/bin/env python3
"""Map each Loki endpoint to where the proxy implements it and how it is served.

Walks the Go call graph from each route handler (depth-limited) and records:
  * where: the handler and the functions it reaches, as file:line,
  * vl_endpoints: the VictoriaLogs endpoints those functions call,
  * vl_pipes: the LogsQL pipes and stats functions they emit,
  * execution: native_vl when the answer comes straight from a VictoriaLogs
    endpoint, hybrid when the proxy reshapes or merges a VictoriaLogs answer,
    proxy_side when the proxy computes the result itself (raw rows, own
    evaluators, synthesized labels).

The classification is a static approximation and is meant as the starting
state; a curated `implementation` block in the endpoint file overrides it, and
the differential runner will later confirm it per query shape at runtime.

Usage: sync_impl.py --coverage conformance/registry/generated/proxy/coverage.json
"""
import argparse, json, os, re, sys
from registry_io import load_json, read_text

SOURCE_DIR = "internal/proxy"
VL_ENDPOINT_RE = re.compile(r'"(/select/logsql/[a-z_]+)"')
PIPE_RE = re.compile(r'\| (unpack_json|unpack_logfmt|stats by|stats|filter|math|sort|limit|top|uniq|extract|format|replace|drop|keep|coalesce|len|pack_json|running|unroll)\b')
PROXY_SIDE_MARKERS = {
    "collectRangeMetricSamples": "evaluates raw rows in the proxy",
    "orderedJSONMetric": "ordered JSON evaluator over raw rows",
    "buildBoundedBareParserMetric": "bare-parser evaluator in the proxy",
    "binaryEvaluationContext": "binary expression evaluated in the proxy",
    "zerofillStatsMatrix": "axis zero-filled in the proxy",
    "ensureSyntheticServiceName": "label derived in the proxy",
    "ensureDetectedLevel": "label derived in the proxy",
    "extractLevelFromMsg": "level derived from the line in the proxy",
    "mergeTenant": "multi-tenant merge in the proxy",
}


def load_functions():
    functions, files = {}, {}
    for name in sorted(os.listdir(SOURCE_DIR)):
        if not name.endswith(".go") or name.endswith("_test.go"):
            continue
        path = os.path.join(SOURCE_DIR, name)
        text = read_text(path)
        files[path] = text
        for match in re.finditer(r'^func (?:\([^)]*\) )?(\w+)\(', text, re.M):
            start = match.start()
            depth, index, end = 0, text.index("{", start), None
            for position in range(index, len(text)):
                if text[position] == "{":
                    depth += 1
                elif text[position] == "}":
                    depth -= 1
                    if depth == 0:
                        end = position
                        break
            line = text[:start].count("\n") + 1
            functions.setdefault(match.group(1), []).append(
                {"file": path, "line": line, "body": text[start:end or len(text)]})
    return functions


def walk(handler, functions, depth=3):
    seen, order, frontier = set(), [], [handler]
    for _ in range(depth):
        nxt = []
        for name in frontier:
            if name in seen or name not in functions:
                continue
            seen.add(name)
            for entry in functions[name]:
                order.append((name, entry))
                nxt += re.findall(r'\b(?:p\.)?(\w+)\(', entry["body"])
        frontier = nxt
    return order


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", required=True)
    parser.add_argument("--out", default="conformance/registry/generated/proxy/implementation.json")
    args = parser.parse_args()
    functions = load_functions()
    coverage = load_json(args.coverage)
    result = []
    for endpoint in coverage["endpoints"]:
        route = endpoint.get("proxy_route")
        if not route:
            result.append({"id": endpoint["id"], "implemented": False})
            continue
        reached = walk(route["handler"], functions)
        vl_endpoints, pipes, proxy_side, where = set(), set(), {}, []
        for name, entry in reached:
            vl_endpoints |= set(VL_ENDPOINT_RE.findall(entry["body"]))
            pipes |= set(PIPE_RE.findall(entry["body"]))
            for marker, reason in PROXY_SIDE_MARKERS.items():
                if marker in entry["body"]:
                    proxy_side[marker] = reason
            if name == route["handler"]:
                where.append(f"{entry['file']}:{entry['line']} {name}")
        if proxy_side:
            execution = "hybrid" if vl_endpoints else "proxy_side"
        else:
            execution = "native_vl" if vl_endpoints else "proxy_side"
        result.append({
            "id": endpoint["id"], "implemented": True, "route": route["route"],
            "handler": route["handler"], "where": where,
            "functions_reached": len(reached),
            "vl_endpoints": sorted(vl_endpoints), "vl_pipes": sorted(pipes),
            "proxy_side_work": proxy_side, "execution": execution,
        })
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as handle:
        json.dump({"endpoints": result}, handle, indent=2, sort_keys=True)
        handle.write("\n")
    counts = {}
    for entry in result:
        key = entry.get("execution", "not_implemented")
        counts[key] = counts.get(key, 0) + 1
    print(f"{args.out}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
