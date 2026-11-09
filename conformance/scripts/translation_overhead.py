#!/usr/bin/env python3
"""The README's translation overhead table, measured by the weekly run and rendered from its results.

  translation_overhead.py --measure [--count 5]   run the translator benchmarks, write RESULTS
  translation_overhead.py --readme README.md      rewrite the README section between the markers from RESULTS

bench/ab/daily.py measures and renders on every weekly run; the conformance gate re-renders the section from the
committed RESULTS and fails when the README differs, so the table always states the last measured run.
"""
import argparse
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
RESULTS = "bench/ab/history/translation-overhead.json"
START, END = "<!-- translation-overhead:start -->", "<!-- translation-overhead:end -->"
# The benchmarks the table shows, in its order, with the query each translates (internal/translator).
ROWS = (
    ("BenchmarkTranslateLogQL_Simple", 'Selector `{app="nginx"}`'),
    ("BenchmarkTranslate_Rate", '`rate({app="nginx",env="production"}[5m])`'),
    ("BenchmarkTranslate_SumRate", '`sum(rate({...}[5m])) by (host)`'),
    ("BenchmarkTranslate_IPFilter", '`\\| ip("10.0.0.0/8")` filter'),
    ("BenchmarkTranslate_BinaryRateDiv", 'Binary metric `sum(rate) / sum(rate)`'),
)
LINE = re.compile(r"^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([\d.]+) ns/op\s+([\d.]+) B/op\s+(\d+) allocs/op")


def parse(output):
    """Per benchmark, the median ns/op and allocs/op over the runs in `go test -bench` output; cpu and goarch."""
    runs, meta = {}, {}
    for line in output.splitlines():
        if line.startswith(("cpu:", "goos:", "goarch:")):
            key, _, value = line.partition(":")
            meta[key] = value.strip()
        m = LINE.match(line.strip())
        if m:
            runs.setdefault(m.group(1), []).append((float(m.group(2)), int(m.group(4))))
    rows = {name: {"ns": statistics.median(r[0] for r in v), "allocs": int(statistics.median(r[1] for r in v)),
                   "runs": len(v)} for name, v in runs.items()}
    return rows, meta


def measure(count):
    names = "|".join(re.escape(n) + "$" for n, _ in ROWS)
    out = subprocess.run(["go", "test", "./internal/translator/", "-run", "^$", "-bench", f"^({names})",
                          "-benchmem", f"-count={count}"], cwd=ROOT, check=True, capture_output=True, text=True,
                         env=dict(os.environ, GOMAXPROCS=os.environ.get("GOMAXPROCS", "4"))).stdout
    rows, meta = parse(out)
    missing = [n for n, _ in ROWS if n not in rows]
    if missing:
        raise SystemExit(f"benchmarks did not run: {', '.join(missing)}")
    commit = subprocess.run(["git", "rev-parse", "--short=10", "HEAD"], cwd=ROOT, capture_output=True,
                            text=True).stdout.strip()
    go = subprocess.run(["go", "env", "GOVERSION"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    return {"date": time.strftime("%Y-%m-%d", time.gmtime()), "commit": commit, "go": go,
            "cpu": meta.get("cpu", platform.processor() or "unknown"), "goarch": meta.get("goarch", ""),
            "gomaxprocs": os.environ.get("GOMAXPROCS", "4"), "count": count,
            "rows": [{"benchmark": n, **rows[n]} for n, _ in ROWS]}


NOT_MEASURED = ("Not measured yet: the weekly performance run measures the translator benchmarks and writes this "
                "table (`conformance/scripts/translation_overhead.py`).")


def render(results):
    if not results:
        return NOT_MEASURED
    by = {r["benchmark"]: r for r in results["rows"]}
    times = [by[n]["ns"] / 1000 for n, _ in ROWS if n in by]
    out = [f"Measured on {results['date']} at commit `{results['commit']}` by the weekly run: LogQL to LogsQL "
           f"translation took **{min(times):.1f} to {max(times):.1f} µs per query**. For a 100 to 500 ms "
           f"VictoriaLogs round-trip this is under {max(times) / 1000:.2f}% of wall time.", "",  # µs over 100 ms
           "| Query type | Time | Allocs |", "|---|---:|---:|"]
    for name, label in ROWS:
        if name in by:
            out.append(f"| {label} | {by[name]['ns'] / 1000:.1f} µs | {by[name]['allocs']} |")
    out += ["", f"Median of {results['count']} runs of `go test ./internal/translator/ -bench BenchmarkTranslate "
            f"-benchmem` with `GOMAXPROCS={results['gomaxprocs']}`, {results['go']} on {results['cpu']} "
            f"({results['goarch']}), the weekly run's machine, which runs other work too: the times are an upper bound."]
    return "\n".join(out)


def replace_section(text, body):
    if text.count(START) != 1 or text.count(END) != 1 or text.index(START) > text.index(END):
        raise ValueError(f"expected one {START} before one {END}")
    return text.split(START)[0] + f"{START}\n\n{body}\n\n{END}" + text.split(END)[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--measure", action="store_true")
    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--readme", metavar="PATH")
    a = ap.parse_args()
    path = os.path.join(ROOT, RESULTS)
    if a.measure:
        results = measure(a.count)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, sort_keys=True)
            f.write("\n")
        print(f"{RESULTS}: {len(results['rows'])} benchmarks")
    if a.readme:
        results = None
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                results = json.load(f)
        with open(a.readme, encoding="utf-8") as f:
            text = f.read()
        with open(a.readme, "w", encoding="utf-8") as f:
            f.write(replace_section(text, render(results)))
        print(f"{a.readme}: translation overhead written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
