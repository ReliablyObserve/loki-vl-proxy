#!/usr/bin/env python3
"""Link every LogQL construct to the VictoriaLogs constructs that can serve it.

Answers, per LogQL construct: which VictoriaLogs pipe or stats function can do
the work, from which VictoriaLogs release, and whether the proxy emits it today.
Writes the link into both registry files and reports reuse opportunities: a
VictoriaLogs construct that exists, would serve a LogQL construct, and is
emitted nowhere by the proxy. A construct the proxy emits for other queries is
not listed even where it computes this LogQL construct itself; telling those
apart needs translating each construct. Which constructs the proxy emits comes from
conformance/registry/generated/vl/logsql-surface.json; refresh it offline with
`sync_vl_surface.py --offline` first.

Usage: link_ast.py [--loki-version v3.7.7]
"""
import argparse, json, os, re, sys
from registry_io import load_json, read_text, write_text

# Curated map: LogQL construct -> VictoriaLogs constructs that can serve it.
# Only entries where VictoriaLogs has a real equivalent; everything else stays
# empty and the registry item must explain the transformation instead.
# `count` (series count) is the stats count() over one row per inner series; topk/bottomk per step
# are `sort by (v) partition by (_time) limit k` (lib/logstorage/pipe_sort_topk.go is the processor of
# a sorted pipe with a limit, not a pipe of its own); `unpack` is `unpack_json` of the packed line.
MAP = {
    "count_over_time": ["stats-count"], "bytes_over_time": ["stats-sum-len"],
    "rate": ["stats-count", "pipe-math"], "bytes_rate": ["stats-sum-len", "pipe-math"],
    "sum_over_time": ["stats-sum"], "avg_over_time": ["stats-avg"],
    "max_over_time": ["stats-max"], "min_over_time": ["stats-min"],
    "quantile_over_time": ["stats-quantile"], "stddev_over_time": ["stats-stddev"],
    "stdvar_over_time": ["stats-stddev"], "first_over_time": ["stats-row-min"],
    "last_over_time": ["stats-row-max"], "count": ["stats-count"],
    "sum": ["stats-sum"], "avg": ["stats-avg"], "min": ["stats-min"], "max": ["stats-max"],
    "stddev": ["stats-stddev"], "stdvar": ["stats-stddev"], "quantile": ["stats-quantile"],
    "topk": ["pipe-sort"], "bottomk": ["pipe-sort"],
    "sort": ["pipe-sort"], "sort_desc": ["pipe-sort"],
    "json": ["pipe-unpack-json"], "logfmt": ["pipe-unpack-logfmt"],
    "regexp": ["pipe-extract-regexp"], "pattern": ["pipe-extract"],
    "unpack": ["pipe-unpack-json"], "line_format": ["pipe-format"],
    "label_format": ["pipe-copy", "pipe-rename", "pipe-format"],
    "drop": ["pipe-delete"], "keep": ["pipe-fields"], "decolorize": ["pipe-decolorize"],
    "unwrap": ["pipe-math"], "label_replace": ["pipe-replace-regexp"],
    "absent_over_time": ["stats-count"], "bytes": ["pipe-math"], "duration": ["pipe-math"],
    "duration_seconds": ["pipe-math"], "ip": ["pipe-filter"],
    # Building blocks that cover part of Loki's semantics (PARTIAL says which part).
    "+": ["pipe-math"], "-": ["pipe-math"], "*": ["pipe-math"], "/": ["pipe-math"], "%": ["pipe-math"],
    "^": ["pipe-math"], "==": ["pipe-filter"], "!=": ["pipe-filter"], ">": ["pipe-filter"],
    ">=": ["pipe-filter"], "<": ["pipe-filter"], "<=": ["pipe-filter"],
    "or": ["pipe-union"], "and": ["pipe-join"],
}
ARITHMETIC = ("math works on the fields of one row: a scalar operand, or two metrics computed in one "
              "stats pipe over the same groups and buckets")
COMPARISON = "filter compares a field of one row with a number: a scalar comparison, not vector matching"
PARTIAL = {**{op: ARITHMETIC for op in ("+", "-", "*", "/", "%", "^")},
           **{op: COMPARISON for op in ("==", "!=", ">", ">=", "<", "<=")},
           "or": "union appends the rows of a second query; Loki's or keeps a right-side series only when no "
                 "left-side series has its labels",
           "and": "join inner keeps the rows with a match on the join fields; Loki's and matches whole label "
                  "sets (or on/ignoring) per step"}
# Not a construct VictoriaLogs could serve: syntax, or work done on the request rather than the query.
NOT_APPLICABLE = {"|": "pipeline syntax",
                  "offset": "the proxy shifts the window it sends to VictoriaLogs, "
                            "internal/proxy/query_translation.go extractLogQLOffset"}
def version_key(tag):
    return tuple(int(x) for x in re.findall(r"\d+", tag))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loki-version", default="v3.7.7")
    args = parser.parse_args()
    root = "conformance/registry"
    surface = {f"{e['kind']}-{e['name'].replace('_', '-')}": e for e in
               load_json(f"{root}/generated/vl/logsql-surface.json")["constructs"]}
    logql = {c["literal"]: c for c in
             load_json(f"{root}/generated/loki/{args.loki_version}/logql.json")["constructs"]}

    links, opportunities, unmapped, not_applicable = [], [], [], []
    for literal, construct in sorted(logql.items()):
        candidates = MAP.get(literal, [])
        resolved = []
        for candidate in candidates:
            entry = surface.get(candidate)
            if entry:
                resolved.append({"id": candidate, "since": entry["since"],
                                 "used_by_proxy": entry["used_by_proxy"]})
        links.append({"logql": literal, "kind": construct["kind"],
                      "supported_by_proxy": construct["supported"], "victorialogs": resolved})
        if not construct["supported"]:
            continue
        if literal in NOT_APPLICABLE:
            not_applicable.append({"logql": literal, "why": NOT_APPLICABLE[literal]})
        elif resolved and not any(r["used_by_proxy"] for r in resolved):
            opportunities.append((literal, [r["id"] for r in resolved],
                                  max((r["since"] for r in resolved), key=version_key), PARTIAL.get(literal, "")))
        elif not resolved:
            unmapped.append(literal)

    with open(f"{root}/generated/ast-links.json", "w") as handle:
        json.dump({"loki_version": args.loki_version, "links": links,
                   "reuse_opportunities": [{"logql": l, "victorialogs": v, "since": s, "partial": p}
                                            for l, v, s, p in opportunities],
                   "not_applicable": not_applicable,
                   "no_victorialogs_equivalent": unmapped}, handle, indent=2, sort_keys=True)
        handle.write("\n")

    # write the link into each LogQL registry item
    for link in links:
        identifier = f"{link['kind']}-" + re.sub(r'[^a-z0-9]+', '-', link["logql"].lower()).strip('-')
        path = f"{root}/loki/logql/{identifier}.yaml"
        if not os.path.exists(path):
            continue
        text = read_text(path)
        rendered = ["victorialogs_candidates:"]
        if link["victorialogs"]:
            for candidate in link["victorialogs"]:
                rendered.append(f"  - id: {candidate['id']}")
                rendered.append(f"    since: {candidate['since']}")
                rendered.append(f"    emitted_by_proxy: {str(candidate['used_by_proxy']).lower()}")
        else:
            rendered.append("  []  # no VictoriaLogs equivalent: the proxy must compute it")
        block = "\n".join(rendered) + "\n"
        text = re.sub(r'(?ms)^victorialogs_candidates:.*?(?=^\w|\Z)', '', text)
        if not text.endswith("\n"):
            text += "\n"
        write_text(path, text + block)

    print(f"linked {len(links)} LogQL constructs to the VictoriaLogs surface")
    print(f"reuse opportunities (VictoriaLogs has it, the proxy does not emit it): {len(opportunities)}")
    for literal, candidates, since, partial in opportunities:
        print(f"  {literal:22} -> {', '.join(candidates)} (since {since}){' partial' if partial else ''}")
    print(f"LogQL constructs the proxy supports with no VictoriaLogs equivalent: {len(unmapped)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
