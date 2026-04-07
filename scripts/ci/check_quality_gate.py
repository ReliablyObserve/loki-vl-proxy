#!/usr/bin/env python3
import json
import math
import sys


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pct_delta(base, head):
    if base == 0:
        return None
    return ((head - base) / base) * 100.0


def fmt_pct(delta):
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}%"


def check_non_decreasing(failures, label, base, head, epsilon=0.0):
    if head < (base - epsilon):
        failures.append(f"{label} regressed: base={base}, head={head}")


def check_threshold(failures, label, base, head, better, threshold):
    delta = pct_delta(base, head)
    if delta is None:
        return
    if better == "lower" and delta >= threshold:
        failures.append(f"{label} regressed by {fmt_pct(delta)}")
    elif better == "higher" and delta <= -threshold:
        failures.append(f"{label} regressed by {fmt_pct(delta)}")


def main():
    if len(sys.argv) != 3:
        print("usage: check_quality_gate.py <base-json> <head-json>", file=sys.stderr)
        return 2

    base = load(sys.argv[1])
    head = load(sys.argv[2])

    failures = []

    check_non_decreasing(
        failures,
        "test count",
        int(base["tests"]["count"]),
        int(head["tests"]["count"]),
    )
    check_non_decreasing(
        failures,
        "coverage",
        float(base["tests"]["coverage_pct"]),
        float(head["tests"]["coverage_pct"]),
        epsilon=0.1,
    )

    for key, label in (
        ("loki", "Loki compatibility"),
        ("drilldown", "Logs Drilldown compatibility"),
        ("vl", "VictoriaLogs compatibility"),
    ):
        check_non_decreasing(
            failures,
            label,
            float(base["compatibility"][key]["pct"]),
            float(head["compatibility"][key]["pct"]),
        )

    threshold = 5.0
    cpu_threshold = 10.0
    benchmarks = (
        ("query_range_cache_hit_ns_per_op", "QueryRange cache-hit CPU cost", "lower", cpu_threshold),
        ("query_range_cache_hit_bytes_per_op", "QueryRange cache-hit memory", "lower", threshold),
        ("query_range_cache_hit_allocs_per_op", "QueryRange cache-hit allocations", "lower", threshold),
        ("query_range_cache_bypass_ns_per_op", "QueryRange cache-bypass CPU cost", "lower", cpu_threshold),
        ("query_range_cache_bypass_bytes_per_op", "QueryRange cache-bypass memory", "lower", threshold),
        ("query_range_cache_bypass_allocs_per_op", "QueryRange cache-bypass allocations", "lower", threshold),
        ("labels_cache_hit_ns_per_op", "Labels cache-hit CPU cost", "lower", cpu_threshold),
        ("labels_cache_hit_bytes_per_op", "Labels cache-hit memory", "lower", threshold),
        ("labels_cache_hit_allocs_per_op", "Labels cache-hit allocations", "lower", threshold),
        ("labels_cache_bypass_ns_per_op", "Labels cache-bypass CPU cost", "lower", cpu_threshold),
        ("labels_cache_bypass_bytes_per_op", "Labels cache-bypass memory", "lower", threshold),
        ("labels_cache_bypass_allocs_per_op", "Labels cache-bypass allocations", "lower", threshold),
    )
    for key, label, better, metric_threshold in benchmarks:
        check_threshold(
            failures,
            label,
            float(base["performance"]["benchmarks"].get(key, 0)),
            float(head["performance"]["benchmarks"].get(key, 0)),
            better,
            metric_threshold,
        )

    check_threshold(
        failures,
        "High-concurrency throughput",
        float(base["performance"]["load"]["high_concurrency_req_per_s"]),
        float(head["performance"]["load"]["high_concurrency_req_per_s"]),
        "higher",
        10.0,
    )
    check_threshold(
        failures,
        "High-concurrency memory growth",
        float(base["performance"]["load"]["high_concurrency_memory_growth_mb"]),
        float(head["performance"]["load"]["high_concurrency_memory_growth_mb"]),
        "lower",
        threshold,
    )

    if failures:
        print("Quality gate failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("Quality gate passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
