#!/usr/bin/env python3
"""Cluster per-request differences into gap signatures and rank them.

  cluster.py RUN_DIR [--top 40]      reads RUN_DIR/results.jsonl, writes clusters.json and report.md

Each differing request belongs to exactly one cluster. Its signature is
(endpoint class, query shape, kind, detail): the query shape is the outer
operation plus the parser, stages and filter kinds the query uses (line
filter, label filter, pattern filter, keep/drop, unwrap, ...), and kind/detail
come from the request's most fundamental differing facet (status before
series before labels before values), with error texts reduced to a template
and label names to their class. Two requests share a cluster only when they
differ from Loki the same way on the same kind of query.

Rank: user impact first (explore- and drilldown-visible before the Grafana
datasource path, API-only and edge cases), then how many distinct corpus
queries hit the signature. A facet the diff marks as a recorded deviation
carries its registry case id in the detail; such clusters are listed apart and
not ranked, and conformance/scripts/parity_gaps.py fails when the id is not a
registry case whose status says the difference is recorded.
"""
import argparse
import hashlib
import json
import os
import re
import sys

IMPACT_ORDER = ("explore-visible", "drilldown-visible", "grafana-datasource", "api-only", "edge")
# Which surface shows an endpoint's answer (the registry's consumer lists, narrowed to the first one that renders it).
ENDPOINT_IMPACT = {
    "query_range": "explore-visible", "query": "grafana-datasource", "labels": "explore-visible",
    "label_values": "explore-visible", "series": "grafana-datasource", "tail": "explore-visible",
    "detected_fields": "drilldown-visible", "detected_field_values": "drilldown-visible",
    "detected_labels": "drilldown-visible", "patterns": "drilldown-visible", "volume": "drilldown-visible",
    "volume_range": "drilldown-visible", "index_stats": "drilldown-visible", "format_query": "api-only",
}
STAGES = ("json", "logfmt", "pattern", "regexp", "unpack", "line_format", "label_format", "keep", "drop",
          "unwrap", "decolorize", "detected_level")


def shape(query):
    """A coarse query shape: parser and stages, label filter, aggregation."""
    if not query:
        return "-"
    parts = [s for s in STAGES if re.search(rf"\|\s*{s}\b", query)]
    if re.search(r"\|\s*[A-Za-z_][\w.]*\s*(=~|!~|!=|>=|<=|==|=|>|<)", query):
        parts.append("label-filter")
    if re.search(r"\|[=~]|[}\"`]\s*![=~]\s*[\"`]", query):
        parts.append("line-filter")
    if re.search(r"\|>|!>", query):
        parts.append("pattern-filter")
    agg = re.match(r"\s*([a-z_]+)", query)
    head = agg.group(1) if agg and not query.lstrip().startswith("{") else "logs"
    return head + ("[" + ",".join(parts) + "]" if parts else "")


def endpoint_class(row):
    if row["endpoint"] == "ds_query":
        return "grafana:ds_query"
    if row["endpoint"] == "resource":
        return "grafana:" + row.get("resource_endpoint", "resource")
    return row["endpoint"]


def impact_of(row):
    if row["endpoint"] in ("ds_query", "resource"):
        page = row.get("origin", "")
        return "drilldown-visible" if page.startswith("dd-") else "explore-visible"
    base = ENDPOINT_IMPACT.get(row["endpoint"], "api-only")
    if row["endpoint"] in ("query_range", "query") and row.get("encoding") == "plain" and base != "api-only":
        return "grafana-datasource" if row["endpoint"] == "query" else base
    return base


def signature_id(signature):
    # A content identifier, not a security control: registry cases claim clusters by this id, so changing the hash
    # would renumber every claim in conformance/registry.
    return hashlib.sha1(json.dumps(signature).encode(), usedforsecurity=False).hexdigest()[:8]  # nosemgrep: python.lang.security.insecure-hash-algorithms.insecure-hash-algorithm-sha1


# The facet that names a request's difference, most fundamental first: a request
# whose status differs also differs in everything after it.
PRIORITY = ("status", "error-text", "result-type", "shape", "order", "entries", "series", "values-set", "names", "field",
            "labels", "category", "line", "values", "points", "stats", "cardinality", "patterns", "format",
            "field-type", "field-parsers", "field-cardinality")


def query_shape(row):
    """The shape of the request's LogQL (a Grafana resource call has none)."""
    if row.get("resource"):
        return "-"
    return shape((row.get("params") or {}).get("query"))


def error_template(text):
    """An error message with its query-specific parts (quoted text, numbers) masked."""
    text = re.sub(r"(\"[^\"]*\"|'[^']*'|`[^`]*`)", "Q", str(text).lower())
    text = re.sub(r"\d+(\.\d+)?", "N", text)
    return " ".join(text.split())[:80]


def primary(facets):
    """(facet, others): the first undocumented facet by PRIORITY, or the first documented one."""
    ranked = sorted(facets, key=lambda f: (bool(f.get("documented")),
                                           PRIORITY.index(f["kind"]) if f["kind"] in PRIORITY else 99))
    return ranked[0], ranked[1:]


def detail_of(item):
    detail = item["detail"]
    if item["kind"] in ("status", "error-text"):
        proxy_text = item["example"].split(" | proxy ", 1)[-1]
        detail = f"{detail}: {error_template(proxy_text)}"
    if item.get("documented"):
        detail = f"{detail} [{item['documented']}]"
    return detail


def signature_of(row):
    """A differing request's signature: endpoint class, query shape, primary facet kind and detail."""
    item, _ = primary(row["facets"])
    return [endpoint_class(row), query_shape(row), item["kind"], detail_of(item)]


def cluster(rows):
    """One cluster per differing request: see signature_of."""
    clusters = {}
    for row in rows:
        if row.get("verdict") != "diff" or not row["facets"]:
            continue
        item, others = primary(row["facets"])
        query = (row.get("params") or {}).get("query") or row.get("resource") or row.get("origin") or row["id"]
        signature = signature_of(row)
        key = signature_id(signature)
        c = clusters.setdefault(key, {
            "id": key, "signature": signature, "documented": item.get("documented"),
            "requests": 0, "queries": set(), "sources": {}, "encodings": {}, "shapes": {},
            "impacts": {}, "also": {}, "examples": []})
        c["requests"] += 1
        c["queries"].add(query)
        for field, value in (("sources", row["source"]), ("encodings", row.get("encoding", "-")),
                             ("shapes", query_shape(row)),
                             ("impacts", impact_of(row))):
            c[field][value] = c[field].get(value, 0) + 1
        for other in others:
            name = f"{other['kind']}: {other['detail']}"
            c["also"][name] = c["also"].get(name, 0) + 1
        if len(c["examples"]) < 3 and not any(e["query"] == query for e in c["examples"]):
            c["examples"].append({"query": query, "endpoint": row["endpoint"], "encoding": row.get("encoding"),
                                  "origin": row.get("origin", ""), "excerpt": item["example"]})
    out = []
    for c in clusters.values():
        c["queries"] = len(c["queries"])
        c["impact"] = min(c["impacts"], key=IMPACT_ORDER.index)
        out.append(c)
    out.sort(key=lambda c: (bool(c["documented"]), IMPACT_ORDER.index(c["impact"]), -c["queries"], -c["requests"]))
    for rank, c in enumerate([c for c in out if not c["documented"]], 1):
        c["rank"] = rank
    return out


def summary(rows):
    total = {}
    for row in rows:
        s = total.setdefault(row["source"], {"requests": 0, "same": 0, "diff": 0, "vacuous": 0, "blocked": 0})
        s["requests"] += 1
        s[row.get("verdict", "blocked")] += 1
    return total


def markdown(clusters, stats, health, top=40):
    lines = ["# Proxy vs Loki differential run", ""]
    if health:
        lines += ["## Both sides healthy", "", "```", json.dumps(health, indent=1, sort_keys=True)[:3000], "```", ""]
    lines += ["## Requests", "", "| source | requests | same | differ | both empty | blocked |", "|---|---:|---:|---:|---:|---:|"]
    for source, s in sorted(stats.items()):
        lines.append(f"| {source} | {s['requests']} | {s['same']} | {s['diff']} | {s['vacuous']} | {s['blocked']} |")
    gaps = [c for c in clusters if not c["documented"]]
    lines += ["", f"## Gap signatures ({len(gaps)})", "",
              "| # | id | impact | endpoint | head | kind | detail | queries | requests | example |",
              "|---:|---|---|---|---|---|---|---:|---:|---|"]
    for c in gaps[:top]:
        example = c["examples"][0]
        lines.append(f"| {c['rank']} | `{c['id']}` | {c['impact']} | " + " | ".join(cell(s, 90) for s in c["signature"])
                     + f" | {c['queries']} | {c['requests']} | `{cell(example['query'])}` — {cell(example['excerpt'])} |")
    documented = [c for c in clusters if c["documented"]]
    if documented:
        lines += ["", "## Documented deviations (not gaps)", "", "| id | rule | endpoint | detail | requests |",
                  "|---|---|---|---|---:|"]
        for c in documented:
            lines.append(f"| `{c['id']}` | {c['documented']} | {c['signature'][0]} | {cell(c['signature'][3])} | {c['requests']} |")
    return "\n".join(lines) + "\n"


def cell(text, limit=160):
    text = str(text).replace("|", "\\|").replace("\n", " ").replace("`", "'")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def load_rows(path):
    with open(path) as handle:
        return [json.loads(line) for line in handle if line.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--top", type=int, default=40)
    a = ap.parse_args()
    rows = load_rows(os.path.join(a.run_dir, "results.jsonl"))
    health_path = os.path.join(a.run_dir, "health.json")
    health = json.load(open(health_path)) if os.path.exists(health_path) else {}
    clusters = cluster(rows)
    stats = summary(rows)
    with open(os.path.join(a.run_dir, "clusters.json"), "w") as handle:
        json.dump({"summary": stats, "clusters": clusters}, handle, indent=1, sort_keys=True)
    with open(os.path.join(a.run_dir, "report.md"), "w") as handle:
        handle.write(markdown(clusters, stats, health, a.top))
    print(f"{len(rows)} requests, {sum(1 for c in clusters if not c['documented'])} gap signatures, "
          f"{sum(1 for c in clusters if c['documented'])} documented; {a.run_dir}/report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
