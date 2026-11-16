#!/usr/bin/env python3
"""Record the LogQL syntax surface from Loki's public documentation and what the proxy supports.

The constructs are the ones Loki's published LogQL documentation lists (log queries, metric queries, the query
reference), each with the page that documents it and a probe query. No Loki source is read: the documentation
pages are fetched and every construct must appear on its page, and the probe queries are checked with Loki's own
parser through the test-only oracle (internal/querycorpus TestLogQLSurfaceMatchesLokiParser, LVP_RUN_ORACLES=1),
so the list follows what Loki accepts. A construct Loki's parser accepts but no page documents is listed with the
oracle as its only source.

For each construct this script records whether the proxy's own parser and translator handle it, and where.

Writes conformance/registry/generated/loki/<version>/logql.json and seeds one registry file per construct under
conformance/registry/loki/logql/.

Usage: sync_ast.py --version v3.7.7 [--docs-version v3.7.x] [--docs-dir DIR]
"""
import argparse, html, json, os, re, subprocess, sys, urllib.request
from registry_io import write_text

DOCS = "https://grafana.com/docs/loki/{version}/query/{page}/"
PAGES = {"log": "log_queries", "metric": "metric_queries", "reference": "query_reference"}

# kind, literal, registry name (the literal when it is a word), documentation page ("" for parser-only), probe query.
SEL = '{app="a"}'
CONSTRUCTS = [
    ("operator", "sum", "sum", "metric", f"sum(count_over_time({SEL}[1m]))"),
    ("operator", "avg", "avg", "metric", f"avg(count_over_time({SEL}[1m]))"),
    ("operator", "min", "min", "metric", f"min(count_over_time({SEL}[1m]))"),
    ("operator", "max", "max", "metric", f"max(count_over_time({SEL}[1m]))"),
    ("operator", "count", "count", "metric", f"count(count_over_time({SEL}[1m]))"),
    ("operator", "stddev", "stddev", "metric", f"stddev(count_over_time({SEL}[1m]))"),
    ("operator", "stdvar", "stdvar", "metric", f"stdvar(count_over_time({SEL}[1m]))"),
    ("operator", "topk", "topk", "metric", f"topk(2, count_over_time({SEL}[1m]))"),
    ("operator", "bottomk", "bottomk", "metric", f"bottomk(2, count_over_time({SEL}[1m]))"),
    ("operator", "sort", "sort", "metric", f"sort(count_over_time({SEL}[1m]))"),
    ("operator", "sort_desc", "sort-desc", "metric", f"sort_desc(count_over_time({SEL}[1m]))"),
    ("operator", "approx_topk", "approx-topk", "metric", f"approx_topk(2, count_over_time({SEL}[1m]))"),
    ("operator", "vector", "vector", "metric", "vector(1)"),
    ("operator", "offset", "offset", "metric", f"count_over_time({SEL}[1m] offset 1m)"),
    ("operator", "unwrap", "unwrap", "metric", f"sum_over_time({SEL} | unwrap x [1m])"),
    ("operator", "+", "add", "reference", "vector(1) + vector(1)"),
    ("operator", "-", "sub", "reference", "vector(1) - vector(1)"),
    ("operator", "*", "mul", "reference", "vector(1) * vector(1)"),
    ("operator", "/", "div", "reference", "vector(1) / vector(1)"),
    ("operator", "%", "mod", "reference", "vector(1) % vector(1)"),
    ("operator", "^", "pow", "reference", "vector(1) ^ vector(1)"),
    ("operator", "==", "eq", "reference", "vector(1) == vector(1)"),
    ("operator", "!=", "neq", "reference", "vector(1) != vector(1)"),
    ("operator", ">", "gt", "reference", "vector(1) > vector(1)"),
    ("operator", ">=", "gte", "reference", "vector(1) >= vector(1)"),
    ("operator", "<", "lt", "reference", "vector(1) < vector(1)"),
    ("operator", "<=", "lte", "reference", "vector(1) <= vector(1)"),
    ("operator", "and", "and", "reference", f"count_over_time({SEL}[1m]) and count_over_time({SEL}[1m])"),
    ("operator", "or", "or", "reference", f"count_over_time({SEL}[1m]) or count_over_time({SEL}[1m])"),
    ("operator", "unless", "unless", "reference", f"count_over_time({SEL}[1m]) unless count_over_time({SEL}[1m])"),
    ("operator", "on", "on", "reference", f"count_over_time({SEL}[1m]) + on(app) count_over_time({SEL}[1m])"),
    ("operator", "ignoring", "ignoring", "reference", f"count_over_time({SEL}[1m]) + ignoring(app) count_over_time({SEL}[1m])"),
    ("operator", "group_left", "group-left", "reference", f"count_over_time({SEL}[1m]) + on(app) group_left count_over_time({SEL}[1m])"),
    ("operator", "group_right", "group-right", "reference", f"count_over_time({SEL}[1m]) + on(app) group_right count_over_time({SEL}[1m])"),
    ("operator", "keep", "keep", "log", f"{SEL} | keep app"),
    ("operator", "drop", "drop", "log", f"{SEL} | drop app"),
    ("operator", "decolorize", "decolorize", "log", f"{SEL} | decolorize"),
    ("operator", "--strict", "strict", "log", f"{SEL} | logfmt --strict"),
    ("operator", "--keep-empty", "keep-empty", "log", f"{SEL} | logfmt --keep-empty"),
    # Accepted by Loki's parser (the oracle) and on no LogQL documentation page.
    ("operator", "variants", "variants", "", f"variants(count_over_time({SEL}[1m])) of ({SEL}[1m])"),
    ("range_function", "rate", "rate", "metric", f"rate({SEL}[1m])"),
    ("range_function", "count_over_time", "count-over-time", "metric", f"count_over_time({SEL}[1m])"),
    ("range_function", "bytes_rate", "bytes-rate", "metric", f"bytes_rate({SEL}[1m])"),
    ("range_function", "bytes_over_time", "bytes-over-time", "metric", f"bytes_over_time({SEL}[1m])"),
    ("range_function", "absent_over_time", "absent-over-time", "metric", f"absent_over_time({SEL}[1m])"),
    ("range_function", "rate_counter", "rate-counter", "metric", f"rate_counter({SEL} | unwrap x [1m])"),
    ("range_function", "sum_over_time", "sum-over-time", "metric", f"sum_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "avg_over_time", "avg-over-time", "metric", f"avg_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "max_over_time", "max-over-time", "metric", f"max_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "min_over_time", "min-over-time", "metric", f"min_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "first_over_time", "first-over-time", "metric", f"first_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "last_over_time", "last-over-time", "metric", f"last_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "stdvar_over_time", "stdvar-over-time", "metric", f"stdvar_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "stddev_over_time", "stddev-over-time", "metric", f"stddev_over_time({SEL} | unwrap x [1m])"),
    ("range_function", "quantile_over_time", "quantile-over-time", "metric", f"quantile_over_time(0.5, {SEL} | unwrap x [1m])"),
    ("parser", "json", "json", "log", f"{SEL} | json"),
    ("parser", "logfmt", "logfmt", "log", f"{SEL} | logfmt"),
    ("parser", "pattern", "pattern", "log", f'{SEL} | pattern "<a> <b>"'),
    ("parser", "regexp", "regexp", "log", f'{SEL} | regexp "(?P<a>.*)"'),
    ("parser", "unpack", "unpack", "log", f"{SEL} | unpack"),
    ("formatter", "line_format", "line-format", "log", f'{SEL} | line_format "{{{{.a}}}}"'),
    ("formatter", "label_format", "label-format", "log", f"{SEL} | label_format a=b"),
    ("conversion", "duration_seconds", "duration-seconds", "metric", f"sum_over_time({SEL} | unwrap duration_seconds(x) [1m])"),
    ("conversion", "duration", "duration", "metric", f"sum_over_time({SEL} | unwrap duration(x) [1m])"),
    ("conversion", "bytes", "bytes", "metric", f"sum_over_time({SEL} | unwrap bytes(x) [1m])"),
    ("filter", "ip", "ip", "log", f'{SEL} |= ip("1.2.3.4")'),
    ("label_op", "label_replace", "label-replace", "reference", f'label_replace(count_over_time({SEL}[1m]), "a", "$1", "b", "(.*)")'),
    ("pipeline_stage", "|", "pipe", "log", f"{SEL} | json"),
]
SEARCH_DIRS = ["internal/logql", "internal/translator", "internal/translate", "internal/proxy"]


def fetch(url):
    request = urllib.request.Request(url, headers={"User-Agent": "loki-vl-proxy-conformance"})
    with urllib.request.urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise SystemExit(f"{url}: HTTP {response.status}")
        return response.read().decode()


def page_text(docs_version, page, docs_dir):
    """The documentation page as text, from docs_dir/<page>.html when given, else fetched."""
    if docs_dir:
        with open(os.path.join(docs_dir, page + ".html")) as handle:
            raw = handle.read()
    else:
        raw = fetch(DOCS.format(version=docs_version, page=page))
    return html.unescape(re.sub(r"<[^>]+>", " ", raw))


def documented(text, literal):
    """Whether the page names the literal as LogQL: as a word, or a symbol between blanks or in code."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", literal.lstrip("-")):
        return re.search(r"(?<![A-Za-z0-9_])" + re.escape(literal) + r"(?![A-Za-z0-9_])", text) is not None
    return re.search(r"(?:^|\s)" + re.escape(literal) + r"(?:\s|$)", text) is not None


def supported(literal):
    """Where the proxy mentions this LogQL construct, outside tests."""
    hits = []
    for directory in SEARCH_DIRS:
        if not os.path.isdir(directory):
            continue
        output = subprocess.run(["grep", "-rn", "--include=*.go", f'"{literal}"', directory],
                                capture_output=True, text=True).stdout
        for line in output.splitlines():
            path = line.split(":", 1)[0]
            if not path.endswith("_test.go"):
                hits.append(path)
    return sorted(set(hits))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--docs-version", default="", help="the documentation version, such as v3.7.x (default: the minor of --version)")
    parser.add_argument("--docs-dir", default="", help="read <page>.html from this directory instead of fetching")
    args = parser.parse_args()
    docs_version = args.docs_version or re.sub(r"^(v\d+\.\d+)\..*$", r"\1.x", args.version)
    texts = {key: page_text(docs_version, page, args.docs_dir) for key, page in PAGES.items()}
    constructs, missing = [], []
    for kind, literal, name, page, probe in CONSTRUCTS:
        if page and not documented(texts[page], literal):
            missing.append(f"{literal} ({PAGES[page]})")
        where = supported(literal)
        source = DOCS.format(version=docs_version, page=PAGES[page]) if page else "loki parser (test/oracle/loki check)"
        constructs.append({
            "literal": literal, "kind": kind, "name": f"{kind}-{name}", "source": source, "probe": probe,
            "proxy_mentions": where, "supported": bool(where),
        })
    if missing:
        raise SystemExit("not on their documentation page any more: " + ", ".join(missing))
    directory = f"conformance/registry/generated/loki/{args.version}"
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "logql.json"), "w") as handle:
        json.dump({"version": args.version, "docs_version": docs_version,
                   "constructs": sorted(constructs, key=lambda c: c["name"])},
                  handle, indent=2, sort_keys=True)
        handle.write("\n")

    os.makedirs("conformance/registry/loki/logql", exist_ok=True)
    written = 0
    for construct in constructs:
        identifier = construct["name"]
        target = f"conformance/registry/loki/logql/{identifier}.yaml"
        if os.path.exists(target):
            continue
        where = "\n".join(f"    - {path}" for path in construct["proxy_mentions"][:6]) or "    []"
        write_text(target,
            f"id: {identifier}\nkind: {construct['kind']}\n"
            f"logql: {json.dumps(construct['literal'])}\nsource: {construct['source']}\n"
            f"generated:\n  supported_by_proxy: {str(construct['supported']).lower()}\n"
            f"  seen_in:\n{where}\n\n"
            "semantics: |\n  TODO: Loki's exact semantics, including how it treats missing labels,\n"
            "  parse errors, ordering and empty input.\n"
            "victorialogs: |\n  TODO: the native LogsQL construct used, or the proxy-side\n"
            "  transformation and the VictoriaLogs limitation that forces it.\n"
            "consumers: []   # explore, drilldown, datasource, api\n"
            "cases: []       # case ids proving the semantics match\n")
        written += 1
    kinds = {}
    for construct in constructs:
        kinds.setdefault(construct["kind"], [0, 0])
        kinds[construct["kind"]][0] += 1
        kinds[construct["kind"]][1] += 1 if construct["supported"] else 0
    print(f"LogQL surface {args.version} (documentation {docs_version}): {len(constructs)} constructs ({written} files written)")
    for kind, (total, ok) in sorted(kinds.items()):
        print(f"  {kind:16} {ok}/{total} referenced in proxy code")
    return 0


if __name__ == "__main__":
    sys.exit(main())
