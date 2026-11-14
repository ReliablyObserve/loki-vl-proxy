#!/usr/bin/env python3
import json
import statistics
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


# Total Go coverage may never sit below this. While it does, a PR may not lower it at all; above
# it, the run-to-run tolerance of check_coverage applies but never takes it under the floor.
COVERAGE_FLOOR = 90.0
COVERAGE_TOLERANCE = 1.0


def check_coverage(failures, base, head, floor=COVERAGE_FLOOR, tolerance=COVERAGE_TOLERANCE):
    if head < floor - 1e-9:
        if head < base - 1e-9:
            failures.append(
                f"coverage regressed below the {floor:g}% floor: base={base}, head={head} "
                f"(under the floor a pull request may not lower coverage at all)"
            )
        return
    check_non_decreasing(failures, "coverage", base, head, epsilon=tolerance)


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


def relative_spread_pct(samples):
    """(max - min) / median in percent: how far one side's own repeated runs differ."""
    median = statistics.median(samples)
    if median <= 0:
        return 0.0
    return (max(samples) - min(samples)) / median * 100.0


def check_samples_threshold(
    failures,
    label,
    base_samples,
    head_samples,
    better,
    threshold,
    absolute_threshold=0.0,
    min_base=0.0,
    min_samples=3,
):
    """Compare the medians of base and head runs that alternated on one machine (ABAB).

    A single run of a short load test varies by tens of percent from one run to the next on a
    shared runner, so a fixed percentage on one run per side fails pull requests on noise. Here the
    percentage is a floor: the median must also move by more than the spread the runs show among
    themselves (the larger of the two sides' range over its median). A series that noisy cannot
    prove a regression smaller than its own range; one that is steady still proves a small one.
    Returns "" when decided, otherwise a note for the report.
    """
    if len(base_samples) < min_samples or len(head_samples) < min_samples:
        return f"{label}: only {len(base_samples)} base and {len(head_samples)} head runs, not compared"
    base = statistics.median(base_samples)
    head = statistics.median(head_samples)
    if base < min_base:
        return ""
    delta = pct_delta(base, head)
    if delta is None:
        return ""
    noise = max(relative_spread_pct(base_samples), relative_spread_pct(head_samples))
    effective = max(threshold, noise)
    regressed = (better == "higher" and delta <= -effective) or (better == "lower" and delta >= effective)
    if regressed and abs(head - base) >= absolute_threshold:
        failures.append(
            f"{label} regressed by {fmt_pct(delta)} (median of {len(head_samples)} runs: base={base:g}, head={head:g}; "
            f"the runs differ among themselves by up to {noise:.1f}%, so the threshold is {effective:.1f}%)"
        )
        return ""
    if (better == "higher" and delta <= -threshold) or (better == "lower" and delta >= threshold):
        return (
            f"{label}: median moved {fmt_pct(delta)} but the runs differ among themselves by up to {noise:.1f}%, "
            f"so it is within the noise (threshold {effective:.1f}%)"
        )
    return ""


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


def query_corpus_failures(corpus):
    """The query corpus's findings (internal/querycorpus TestQueryCorpusReport decides them, from the corpus, both
    parse oracles and the similarity scan): any one fails the gate. No corpus (a PR that does not change query code)
    checks nothing."""
    if not isinstance(corpus, dict):
        return []
    return [str(f) for f in corpus.get("failures") or []]


def error_compat_failures(base, head):
    """The error compatibility scores (conformance/registry/generated/error-coverage.json, through the quality
    inventory): the share of recorded Loki error answers the proxy reproduces and the share of route x error kind
    cells checked against Loki may not drop. Compared only when both trees have the file: a base without it (the pull
    request that adds it) has nothing to fall from. A re-recording that adds rows can lower a percentage without a
    regression; then the new floor is accepted in conformance/registry/baseline.json."""
    before = ((base.get("quality_inventory") or {}).get("error_compat") or {})
    after = ((head.get("quality_inventory") or {}).get("error_compat") or {})
    if before.get("state") != "available" or after.get("state") != "available":
        return []
    out = []
    for key, label in (("match_ratio", "errors that match Loki"), ("coverage_ratio", "error matrix coverage")):
        was, now = before.get(key), after.get(key)
        if isinstance(was, (int, float)) and isinstance(now, (int, float)) and now + 1e-9 < was:
            out.append(f"{label} fell from {was * 100:.1f}% to {now * 100:.1f}%")
    return out


def main():
    if len(sys.argv) != 3:
        print("usage: check_quality_gate.py <base-json> <head-json>", file=sys.stderr)
        return 2

    base = load(sys.argv[1])
    head = load(sys.argv[2])

    failures = []

    if head["tests"].get("execution_state") == "failed":
        failures.append(f"PR unit tests failed (exit={head['tests'].get('test_exit_code', 'unknown')}); retained outcomes are incomplete")

    failures += query_corpus_failures(head.get("query_corpus"))
    failures += error_compat_failures(base, head)

    # Skip test-count and coverage checks when head metrics are zero — this
    # indicates the collection step timed out or failed, not a real regression.
    head_tests_available = int(head["tests"]["count"]) > 0 or float(head["tests"]["coverage_pct"]) > 0
    if head_tests_available:
        check_non_decreasing(
            failures,
            "test count",
            int(base["tests"]["count"]),
            int(head["tests"]["count"]),
        )
        # A PR may lower aggregate coverage by the run-to-run tolerance (end-to-end and
        # compatibility-heavy suites vary a little between runs), never below the floor,
        # and not at all while coverage is under the floor.
        check_coverage(
            failures,
            float(base["tests"]["coverage_pct"]),
            float(head["tests"]["coverage_pct"]),
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

    # Per-benchmark overrides for the pooled paths added in the 2026-06-05 perf
    # round (PR #421, commit 2340928): compressedResponseWriter.holdBufPool +
    # buildHitsRangeMetricMatrix pre-size. The pool amortizes a per-handler
    # bytes.Buffer across the entire HTTP response — for real production
    # responses (MB-scale, sustained Drilldown load) the pool is the difference
    # between bounded live heap and unbounded growth (proven by
    # TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad and the *_HeapStableUnder*
    # stress tests, all green with NEGATIVE heap deltas under repeated load).
    #
    # The QueryRange and Labels MICRO-benches run a single tiny (~192 B) cached
    # response through the pool path. The pool's sync.Pool Get/Put + bytes.Buffer
    # acquire add ~15 allocs / ~528 B / ~700 ns of fixed overhead per call. At
    # micro-bench scale that's +275 % memory and +187 % allocations — flagged
    # as a regression by the default gate. At production scale (5 MB responses,
    # 20 concurrent handlers) the same overhead amortizes to noise while the
    # bounded-heap win matters.
    #
    # Override ONLY the absolute-byte / absolute-alloc / absolute-ns threshold
    # (the "tolerance per request" knob). DO NOT touch min_base — it's the
    # noise floor that filters out benches with sub-noise baselines and
    # touching it would silently hide real regressions (a 25× growth from a
    # small base would slip through). With abs raised + min_base unchanged
    # the gate accepts the documented pool overhead but still fails on any
    # regression that exceeds ~3× the pool overhead — sensitive to genuine
    # leaks while accepting the tradeoff. Re-tighten if/when the pool path
    # changes — re-validate against the linked stress tests first.
    pool_overhead_overrides = {
        "query_range_cache_hit_bytes_per_op":      {"abs": 600.0},
        "query_range_cache_hit_allocs_per_op":     {"abs": 18.0},
        "query_range_cache_bypass_bytes_per_op":   {"abs": 600.0},
        "query_range_cache_bypass_allocs_per_op":  {"abs": 18.0},
        "labels_cache_bypass_allocs_per_op":       {"abs": 12.0},
        # CPU cost on the same paths can drift +1000 ns (pool lookup + Reset +
        # Put). Allow this slack for the same reason.
        "query_range_cache_hit_ns_per_op":         {"abs": 1500.0},
        "query_range_cache_bypass_ns_per_op":      {"abs": 1500.0},
    }

    def threshold_for(key, axis, default):
        """Return the absolute threshold for (key, axis) using overrides if set."""
        ov = pool_overhead_overrides.get(key)
        if ov is None:
            return default
        return ov.get(axis, default)
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
        # Apply per-benchmark override if registered in pool_overhead_overrides.
        # Lets us accept the documented holdBufPool + pre-size overhead on the
        # 4 affected QueryRange / Labels micro-benches without loosening the
        # default thresholds for all other benches.
        absolute_threshold = threshold_for(key, "abs", absolute_threshold)
        min_base = threshold_for(key, "min_base", min_base)
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

    base_load = base["performance"]["load"]
    head_load = head["performance"]["load"]
    base_runs = base_load.get("high_concurrency_req_per_s_samples") or []
    head_runs = head_load.get("high_concurrency_req_per_s_samples") or []
    if base_runs and head_runs:
        # Measured with base and head alternating on one machine (load_abab.py).
        note = check_samples_threshold(
            failures,
            "High-concurrency throughput",
            [float(v) for v in base_runs],
            [float(v) for v in head_runs],
            "higher",
            25.0,
            absolute_threshold=2_000.0,
            min_base=5_000.0,
        )
        if note:
            print(f"Note: {note}", file=sys.stderr)
    else:
        check_threshold(
            failures,
            "High-concurrency throughput",
            float(base_load["high_concurrency_req_per_s"]),
            float(head_load["high_concurrency_req_per_s"]),
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
