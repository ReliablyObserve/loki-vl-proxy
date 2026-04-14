#!/usr/bin/env python3
import json
import sys

REQUIRED_COMPONENTS = {
    "loki": [
        "labels",
        "label_values",
        "query_range",
        "metrics",
        "otel",
        "series",
    ],
    "drilldown": [
        "service_selection",
        "level_volume",
        "detected_fields",
        "label_values",
        "service_logs",
        "patterns",
    ],
    "vl": [
        "stream_translation",
        "detected_fields",
        "synthetic_labels",
        "index_stats",
        "volume_range",
        "field_values",
    ],
}


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
    # Avoid floating-point edge failures (for example: 90.2 -> 90.1 with epsilon 0.1).
    if head < (base - epsilon - 1e-9):
        failures.append(f"{label} regressed: base={base}, head={head}")


def check_minimum(failures, label, head, minimum, epsilon=0.0):
    if head < (minimum - epsilon - 1e-9):
        failures.append(f"{label} below minimum: minimum={minimum}, head={head}")


def check_threshold(
    failures,
    label,
    base,
    head,
    better,
    threshold,
    absolute_threshold=0.0,
    min_base=0.0,
):
    if base < min_base:
        return
    delta = pct_delta(base, head)
    if delta is None:
        return
    absolute_delta = abs(head - base)
    if better == "lower" and delta >= threshold and absolute_delta >= absolute_threshold:
        failures.append(
            f"{label} regressed by {fmt_pct(delta)} (base={base}, head={head})"
        )
    elif better == "higher" and delta <= -threshold and absolute_delta >= absolute_threshold:
        failures.append(
            f"{label} regressed by {fmt_pct(delta)} (base={base}, head={head})"
        )


def check_component_breakdown(failures, track_key, track_label, base_track, head_track):
    base_components = base_track.get("components") or {}
    head_components = head_track.get("components") or {}

    if not head_components:
        failures.append(f"{track_label} component breakdown missing in head metrics")
        return

    for component in REQUIRED_COMPONENTS.get(track_key, []):
        if component not in head_components:
            failures.append(f"{track_label} required component missing: {component}")
            continue
        check_minimum(
            failures,
            f"{track_label} component {component}",
            float(head_components[component].get("pct", 0)),
            100.0,
            epsilon=0.1,
        )

    missing_from_head = sorted(set(base_components.keys()) - set(head_components.keys()))
    for component in missing_from_head:
        failures.append(f"{track_label} component disappeared from head metrics: {component}")

    shared_components = sorted(set(base_components.keys()) & set(head_components.keys()))
    for component in shared_components:
        check_non_decreasing(
            failures,
            f"{track_label} component {component}",
            float(base_components[component].get("pct", 0)),
            float(head_components[component].get("pct", 0)),
            epsilon=0.1,
        )


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
        # End-to-end and compatibility-heavy suites can introduce small run-to-run
        # variance in aggregate Go coverage. Keep the gate strict, but avoid
        # failing on sub-point jitter.
        epsilon=0.7,
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
        check_component_breakdown(
            failures,
            key,
            label,
            base["compatibility"][key],
            head["compatibility"][key],
        )

    check_minimum(
        failures,
        "Loki compatibility",
        float(head["compatibility"]["loki"]["pct"]),
        100.0,
        epsilon=0.1,
    )

    mem_pct_threshold = 15.0
    alloc_pct_threshold = 15.0
    cpu_pct_threshold = 25.0
    cpu_abs_threshold = 750.0
    cpu_min_base = 500.0
    mem_abs_threshold = 32.0
    mem_min_base = 64.0
    alloc_abs_threshold = 1.0
    alloc_min_base = 1.0
    benchmarks = (
        (
            "query_range_cache_hit_ns_per_op",
            "QueryRange cache-hit CPU cost",
            "lower",
            cpu_pct_threshold,
            cpu_abs_threshold,
            cpu_min_base,
        ),
        (
            "query_range_cache_hit_bytes_per_op",
            "QueryRange cache-hit memory",
            "lower",
            mem_pct_threshold,
            mem_abs_threshold,
            mem_min_base,
        ),
        (
            "query_range_cache_hit_allocs_per_op",
            "QueryRange cache-hit allocations",
            "lower",
            alloc_pct_threshold,
            alloc_abs_threshold,
            alloc_min_base,
        ),
        (
            "query_range_cache_bypass_ns_per_op",
            "QueryRange cache-bypass CPU cost",
            "lower",
            cpu_pct_threshold,
            cpu_abs_threshold,
            cpu_min_base,
        ),
        (
            "query_range_cache_bypass_bytes_per_op",
            "QueryRange cache-bypass memory",
            "lower",
            mem_pct_threshold,
            mem_abs_threshold,
            mem_min_base,
        ),
        (
            "query_range_cache_bypass_allocs_per_op",
            "QueryRange cache-bypass allocations",
            "lower",
            alloc_pct_threshold,
            alloc_abs_threshold,
            alloc_min_base,
        ),
        (
            "labels_cache_hit_ns_per_op",
            "Labels cache-hit CPU cost",
            "lower",
            cpu_pct_threshold,
            cpu_abs_threshold,
            cpu_min_base,
        ),
        (
            "labels_cache_hit_bytes_per_op",
            "Labels cache-hit memory",
            "lower",
            mem_pct_threshold,
            mem_abs_threshold,
            mem_min_base,
        ),
        (
            "labels_cache_hit_allocs_per_op",
            "Labels cache-hit allocations",
            "lower",
            alloc_pct_threshold,
            alloc_abs_threshold,
            alloc_min_base,
        ),
        (
            "labels_cache_bypass_ns_per_op",
            "Labels cache-bypass CPU cost",
            "lower",
            cpu_pct_threshold,
            cpu_abs_threshold,
            cpu_min_base,
        ),
        (
            "labels_cache_bypass_bytes_per_op",
            "Labels cache-bypass memory",
            "lower",
            mem_pct_threshold,
            mem_abs_threshold,
            mem_min_base,
        ),
        (
            "labels_cache_bypass_allocs_per_op",
            "Labels cache-bypass allocations",
            "lower",
            alloc_pct_threshold,
            alloc_abs_threshold,
            alloc_min_base,
        ),
    )
    for (
        key,
        label,
        better,
        metric_threshold,
        absolute_threshold,
        min_base,
    ) in benchmarks:
        check_threshold(
            failures,
            label,
            float(base["performance"]["benchmarks"].get(key, 0)),
            float(head["performance"]["benchmarks"].get(key, 0)),
            better,
            metric_threshold,
            absolute_threshold=absolute_threshold,
            min_base=min_base,
        )

    check_threshold(
        failures,
        "High-concurrency throughput",
        float(base["performance"]["load"]["high_concurrency_req_per_s"]),
        float(head["performance"]["load"]["high_concurrency_req_per_s"]),
        "higher",
        25.0,
        absolute_threshold=2_000.0,
        min_base=5_000.0,
    )
    check_threshold(
        failures,
        "High-concurrency memory growth",
        float(base["performance"]["load"]["high_concurrency_memory_growth_mb"]),
        float(head["performance"]["load"]["high_concurrency_memory_growth_mb"]),
        "lower",
        300.0,
        absolute_threshold=5.0,
        min_base=5.0,
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
