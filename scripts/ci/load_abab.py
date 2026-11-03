#!/usr/bin/env python3
"""Measure the high-concurrency load test of base and head under the same conditions.

The PR quality report used to run TestLoad_HighConcurrency_MemoryStability on the head tree,
then (minutes later, after other work) on the base tree. A shared runner's speed moves by tens of
percent over minutes, so identical code differed by 28-34% and failed pull requests. Here both
trees' test binaries are built first and then run alternately, base head base head ..., in one
process on one machine, so slow and fast stretches hit both sides alike. The medians go into the two
quality JSON files together with every run, which check_quality_gate.py uses to decide.

Usage: load_abab.py --base-dir D --head-dir D --base-json F --head-json F [--rounds N]
A tree without the test, a failed build or a failed run leaves the JSON files unchanged (the
throughput check then has no data and does not decide).
"""
import argparse
import json
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

TEST = "TestLoad_HighConcurrency_MemoryStability"
THROUGHPUT = re.compile(r"Throughput: ([0-9.]+) req/s")
GROWTH = re.compile(r"Memory growth: ([0-9.]+) MB")


def parse_run(output):
    """(req/s, memory growth MB) of one run of the test, or None when the test did not report."""
    throughput = THROUGHPUT.search(output)
    growth = GROWTH.search(output)
    if not throughput or not growth:
        return None
    return float(throughput.group(1)), float(growth.group(1))


def build(tree, out):
    subprocess.run(
        ["go", "test", "-c", "-o", str(out), "./internal/proxy"],
        cwd=tree, check=True, capture_output=True, text=True,
    )


def run_once(binary, tree):
    done = subprocess.run(
        [str(binary), f"-test.run=^{TEST}$", "-test.v", "-test.count=1", "-test.timeout=180s"],
        cwd=Path(tree) / "internal" / "proxy", capture_output=True, text=True,
    )
    if done.returncode != 0:
        raise RuntimeError(f"{binary} failed: {(done.stdout + done.stderr)[-400:]}")
    return parse_run(done.stdout)


def alternate(run_base, run_head, rounds, warmup=True):
    """Run base and head alternately: warm-up base, head (dropped), then base, head x rounds."""
    if warmup:
        run_base()
        run_head()
    base, head = [], []
    for _ in range(rounds):
        base.append(run_base())
        head.append(run_head())
    return base, head


def summarize(runs):
    """The load entry for one side from its runs [(req/s, growth MB)]."""
    throughputs = [r[0] for r in runs]
    growths = [r[1] for r in runs]
    return {
        "high_concurrency_req_per_s": statistics.median(throughputs),
        "high_concurrency_req_per_s_samples": throughputs,
        "high_concurrency_memory_growth_mb": statistics.median(growths),
        "high_concurrency_memory_growth_mb_samples": growths,
    }


def merge(path, entry):
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    if doc.get("performance", {}).get("mode") != "full":
        return False
    doc["performance"]["load"].update(entry)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    return True


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", required=True)
    ap.add_argument("--head-dir", required=True)
    ap.add_argument("--base-json", required=True)
    ap.add_argument("--head-json", required=True)
    ap.add_argument("--rounds", type=int, default=7)
    args = ap.parse_args(argv)
    if args.rounds < 3:
        print("load_abab: at least 3 rounds are needed", file=sys.stderr)
        return 2
    try:
        with tempfile.TemporaryDirectory() as tmp:
            base_bin, head_bin = Path(tmp) / "base.test", Path(tmp) / "head.test"
            build(args.base_dir, base_bin)
            build(args.head_dir, head_bin)

            def checked(binary, tree):
                result = run_once(binary, tree)
                if result is None:
                    raise RuntimeError(f"{tree}: {TEST} did not report a throughput (missing or skipped)")
                return result

            base, head = alternate(lambda: checked(base_bin, args.base_dir), lambda: checked(head_bin, args.head_dir), args.rounds)
    except (subprocess.CalledProcessError, RuntimeError, OSError) as err:
        detail = getattr(err, "stderr", "") or ""
        print(f"load_abab: not measured: {err} {detail[-400:]}", file=sys.stderr)
        return 0
    base_entry, head_entry = summarize(base), summarize(head)
    for name, path, entry in (("base", args.base_json, base_entry), ("head", args.head_json, head_entry)):
        merge(path, entry)
        print(f"load_abab: {name} req/s runs {entry['high_concurrency_req_per_s_samples']} median {entry['high_concurrency_req_per_s']:.0f}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
