#!/usr/bin/env python3
"""The "Local resource comparison" section of the README and docs/benchmarks.md, rendered from committed results.

  resource_comparison.py --readme README.md        rewrite the section between the markers from RESULTS
  resource_comparison.py --docs docs/benchmarks.md the same, with the links the docs site needs

bench/resources/compare.py measures (the same read workload sent to local Loki, then to the proxy plus
VictoriaLogs, CPU-seconds and peak memory per side) and writes RESULTS. The conformance gate re-renders both sections
from the committed RESULTS and fails when a page differs, so the pages always state the last measured run, with its
date, commit, images and host. A figure is never printed without that provenance, and a run with a failed request is
shown with a warning, because a side that errors looks cheaper than it is.
"""
import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
RESULTS = "bench/resources/results/latest.json"
START, END = "<!-- resource-comparison:start -->", "<!-- resource-comparison:end -->"
NOT_MEASURED = ("Not measured yet: `bench/resources/compare.py` measures the local resource comparison and writes "
                "`bench/resources/results/latest.json`, which this section is rendered from "
                "(`conformance/scripts/resource_comparison.py`).")
# What a result must say about itself before any number from it is printed.
PROVENANCE = ("date", "commit", "images", "host")  # after normalise(): proxy_commit is read as commit
SIDES = (("loki", "Loki"), ("proxy+victorialogs", "Proxy + VictoriaLogs"))
# The workload bench/resources/compare.py sends, for results that do not carry a `method` block (a test holds the two equal).
WORKLOAD = ("9 queries (count, rate, bytes, topk and unwrap metrics, plain and json/logfmt log queries) over 15 min, "
            "30 min, 60 min, three times each, 8 concurrent workers, every request a cold miss")
LINKS = {  # the pointer to the production cost case, per page
    "readme": "[The Cost Case](#the-cost-case)",
    "docs": "[Cost model](cost-model.md) and the production figures in the [README](https://github.com/ReliablyObserve/loki-vl-proxy#the-cost-case)",
}


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def normalise(results):
    """The result shape of compare.py: date, proxy_commit, images, host and the measured sides (`sides`, each run of a
    side named "<side>" or "<side>-again"). A bare `commit` and `runs` are read too."""
    out = dict(results)
    out["commit"] = results.get("proxy_commit") or results.get("commit")
    out["runs"] = results.get("sides") or results.get("runs") or []
    host = results.get("host")
    out["host"] = ", ".join(str(v) for v in host.values() if v) if isinstance(host, dict) else host
    return out


def runs_of(results, side):
    """The measured runs of one side, in order: the first run, then its `-again` repeat when there is one."""
    return [r for r in results["runs"] if r["side"] in (side, side + "-again")]


def totals(run):
    """CPU-seconds above the idle ingest rate, the sum of the containers' peak memory and of their memory before the
    workload, in MiB, of one run."""
    cpu, mem, before = 0.0, 0, 0
    for name, c in run["containers"].items():
        if c.get("cpu_s_workload") is None or c.get("mem_peak_mib") is None:
            raise ValueError(f"{run['side']}: container {name} has no measured CPU or memory")
        cpu += c["cpu_s_workload"]
        mem += c["mem_peak_mib"]
        before += c.get("mem_before_mib") or 0
    if cpu <= 0:
        raise ValueError(f"{run['side']}: the workload used no CPU above the idle rate; the run is not a measurement")
    return cpu, mem, before


def failed(run):
    """Requests of a run that did not answer 200."""
    return sum(n for code, n in run["statuses"].items() if str(code) != "200")


def check(results):
    missing = [k for k in PROVENANCE if not results.get(k)]
    if missing:
        raise ValueError(f"results state no {', '.join(missing)}: a figure is not published without its provenance")
    for side, _ in SIDES:
        if not runs_of(results, side):
            raise ValueError(f"results hold no run of {side}")
        if len(runs_of(results, side)) > 2:
            raise ValueError(f"results hold more than a first run and a repeat of {side}")
    if len({r["requests"] for r in results["runs"]}) != 1:
        raise ValueError("the sides did not send the same number of requests")


def render(results, page="readme"):
    if not results:
        return NOT_MEASURED
    results = normalise(results)
    check(results)
    images, host, method = results["images"], results["host"], results.get("method", {})
    rows, warn, totalled = [], [], {}
    for side, label in SIDES:
        runs = runs_of(results, side)
        totalled[side] = [totals(r) for r in runs]
        bad = [(r, failed(r)) for r in runs if failed(r)]
        for r, n in bad:
            warn.append(f"{label} ({r['side']}): {n} of {r['requests']} requests did not answer 200 "
                        f"({', '.join(f'{c}: {k}' for c, k in sorted(r['statuses'].items()))})")
        first, again = runs[0], runs[1] if len(runs) > 1 else None
        t = totalled[side]
        rows.append(f"| {label} | {first['requests']} | {t[0][0]:.1f} | {t[0][1]} ({t[0][2]}) | "
                    + (f"{t[1][0]:.1f} | {t[1][1]} ({t[1][2]}) | " if again else "- | - | ")
                    + f"{first['seconds']:.0f} s" + (f" / {again['seconds']:.0f} s" if again else "") + " |")
    out = [f"Measured on {results['date']} at commit `{results['commit']}`, on one machine ({host}): the same read "
           f"workload sent to a local Loki and then to the proxy with VictoriaLogs, the stack's own ingest running "
           f"on both. **A local single-machine test, not a production figure**: at production scale the gap can be "
           f"much larger, see {LINKS[page]}.", "",
           "| Side | Requests | First run (cold): CPU-seconds | Peak memory, MiB (before) | Repeat (warm): CPU-seconds | Peak memory, MiB (before) | Wall time, first / repeat |",
           "|---|---:|---:|---:|---:|---:|---:|", *rows, ""]
    loki, ours = totalled["loki"], totalled["proxy+victorialogs"]
    parts = [f"on the first, cold run Loki used **{loki[0][0] / ours[0][0]:.1f}x** the CPU-seconds and peaked at "
             f"**{loki[0][1] / ours[0][1]:.1f}x** the memory of the proxy plus VictoriaLogs"]
    if len(loki) > 1 and len(ours) > 1:
        parts.append(f"on the repeat of the identical requests (caches warm on both sides) **{loki[1][0] / ours[1][0]:.1f}x** "
                     f"the CPU-seconds and **{loki[1][1] / ours[1][1]:.1f}x** the memory")
    out += ["For the same requests " + "; ".join(parts) + ".", ""]
    if warn:
        out += [f"**Not a valid comparison: {'; '.join(warn)}.**", ""]
    notes = f" {method['notes']}" if method.get("notes") else ""
    out.append(f"Method: {method.get('workload', WORKLOAD)}. CPU-seconds are the containers' user plus system time during "
               f"the workload minus each container's idle rate measured just before it ({method.get('idle_s', 30)} s), since "
               f"ingest keeps running; peak memory is the resident set sampled every 0.5 s, summed over the side's "
               f"containers, with the memory before the workload in brackets (Loki's was already large after earlier "
               f"work on the same stack). Wall time is secondary and noisy on a shared host. A busybox sidecar in each "
               f"container's PID namespace reads `/proc/1`, so scratch images work.{notes} Images: "
               f"{', '.join(f'{k}: {v}' for k, v in sorted(images.items()))}. Re-measure with "
               f"`python3 bench/resources/compare.py`; the results are in `{RESULTS}`.")
    return "\n".join(out)


def replace_section(text, body):
    if text.count(START) != 1 or text.count(END) != 1 or text.index(START) > text.index(END):
        raise ValueError(f"expected one {START} before one {END}")
    return text.split(START)[0] + f"{START}\n\n{body}\n\n{END}" + text.split(END)[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--readme", metavar="PATH")
    ap.add_argument("--docs", metavar="PATH")
    a = ap.parse_args()
    path = os.path.join(ROOT, RESULTS)
    results = load(path) if os.path.exists(path) else None
    for target, page in ((a.readme, "readme"), (a.docs, "docs")):
        if not target:
            continue
        with open(target, encoding="utf-8") as f:
            text = f.read()
        with open(target, "w", encoding="utf-8") as f:
            f.write(replace_section(text, render(results, page)))
        print(f"{target}: resource comparison written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
