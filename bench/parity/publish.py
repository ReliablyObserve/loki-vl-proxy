#!/usr/bin/env python3
"""Record a differential run in the conformance registry.

  publish.py RUN_DIR --ref <git ref the proxy was built from>

Writes conformance/registry/generated/parity-discovery.json: the run's request
counts per corpus source, its health proof, and every cluster (signature,
impact, queries, requests, shapes, up to two short examples). The registry's
report generator (conformance/scripts/parity_gaps.py) ranks the registered
differences with these counts and fails when a cluster is accounted for by no
registry case.
"""
import argparse
import glob
import json
import os
import sys

OUT = "conformance/registry/generated/parity-discovery.json"


def short(text, limit=300):
    text = str(text)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--ref", required=True)
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args()
    data = json.load(open(os.path.join(a.run_dir, "clusters.json")))
    health = json.load(open(os.path.join(a.run_dir, "health.json")))
    clusters = []
    for c in data["clusters"]:
        clusters.append({
            "id": c["id"], "signature": c["signature"], "documented": c.get("documented"), "impact": c["impact"],
            "queries": c["queries"], "requests": c["requests"], "sources": c["sources"],
            "shapes": dict(sorted(c["shapes"].items(), key=lambda kv: -kv[1])[:5]),
            "examples": [{"query": short(e["query"]), "endpoint": e["endpoint"], "encoding": e.get("encoding"),
                          "excerpt": short(e["excerpt"])} for e in c["examples"][:2]],
        })
    before, after = health.get("before", {}), health.get("after", {})
    proof = {k: before.get(k) for k in ("lines_loki", "lines_vl", "slices", "slices_differing", "services_listed",
                                         "services_in_loki_metric", "vl_restarts", "loki_restarts", "ok")}
    proof["loki_restarts_after"] = after.get("loki_restarts")
    proof["not_reproducible"] = health.get("not_reproducible")
    proof["vl_restarts_after"] = after.get("vl_restarts")
    proof["ok_after"] = after.get("ok")
    # Runs merged into this one (an earlier pass, a rerun of one source) keep their own proof.
    passes = {}
    for path in sorted(glob.glob(os.path.join(a.run_dir, "health-*.json"))):
        h = json.load(open(path))
        name = os.path.basename(path)[len("health-"):-len(".json")]
        passes[name] = {"checked": [h.get("before", {}).get("checked"), h.get("after", {}).get("checked")],
                        "ok": [h.get("before", {}).get("ok"), h.get("after", {}).get("ok")],
                        "lines": h.get("before", {}).get("lines_loki"),
                        "restarts_unchanged": bool(h.get("vl_restarts_unchanged") and h.get("loki_restarts_unchanged"))}
    doc = {"ref": a.ref, "recorded": before.get("checked"), "window": health.get("window"),
           "health": proof, "passes": passes, "summary": data["summary"], "clusters": clusters}
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w") as handle:
        json.dump(doc, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(f"{a.out}: {len(clusters)} clusters")
    return 0


if __name__ == "__main__":
    sys.exit(main())
