import unittest

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from scripts.ci.check_quality_gate import REQUIRED_COMPONENTS, check_component_breakdown, check_samples_threshold, error_compat_failures, query_corpus_failures, relative_spread_pct


class CheckQualityGateComponentTests(unittest.TestCase):
    def test_missing_head_component_breakdown_fails(self):
        failures = []
        check_component_breakdown(
            failures,
            "loki",
            "Loki compatibility",
            {"components": {"labels": {"passed": 2, "total": 2, "pct": 100.0}}},
            {"components": {}},
        )
        self.assertTrue(any("component breakdown missing" in failure for failure in failures))

    def test_required_component_missing_fails(self):
        failures = []
        check_component_breakdown(
            failures,
            "loki",
            "Loki compatibility",
            {"components": {"labels": {"passed": 2, "total": 2, "pct": 100.0}}},
            {"components": {"labels": {"passed": 2, "total": 2, "pct": 100.0}}},
        )
        self.assertTrue(any("required component missing" in failure for failure in failures))

    def test_component_regression_fails(self):
        failures = []
        check_component_breakdown(
            failures,
            "vl",
            "VictoriaLogs compatibility",
            {
                "components": {
                    "stream_translation": {"passed": 3, "total": 3, "pct": 100.0},
                    "detected_fields": {"passed": 3, "total": 3, "pct": 100.0},
                    "synthetic_labels": {"passed": 3, "total": 3, "pct": 100.0},
                    "index_stats": {"passed": 3, "total": 3, "pct": 100.0},
                    "volume_range": {"passed": 3, "total": 3, "pct": 100.0},
                    "field_values": {"passed": 3, "total": 3, "pct": 100.0},
                }
            },
            {
                "components": {
                    "stream_translation": {"passed": 3, "total": 3, "pct": 100.0},
                    "detected_fields": {"passed": 3, "total": 3, "pct": 100.0},
                    "synthetic_labels": {"passed": 3, "total": 3, "pct": 100.0},
                    "index_stats": {"passed": 3, "total": 3, "pct": 100.0},
                    "volume_range": {"passed": 2, "total": 3, "pct": 66.7},
                    "field_values": {"passed": 3, "total": 3, "pct": 100.0},
                }
            },
        )
        self.assertTrue(any("component volume_range" in failure for failure in failures))


def throughput(base, head, **kw):
    failures = []
    note = check_samples_threshold(
        failures, "High-concurrency throughput", base, head, "higher", 25.0,
        absolute_threshold=2_000.0, min_base=5_000.0, **kw,
    )
    return failures, note


class ThroughputAbabTests(unittest.TestCase):
    """Series are runs of base and head that alternated on one machine (req/s)."""

    # Seen in CI: the same code measured 324k and 645k on different runs.
    NOISY_BASE = [645_000, 410_000, 520_000, 330_000, 590_000, 480_000, 360_000]

    def test_identical_code_with_noisy_runs_passes(self):
        # Medians 480k vs 336k: -30%, past the fixed 25%, but the runs themselves span 65%.
        head = [336_000, 640_000, 300_000, 520_000, 330_000, 420_000, 345_000]
        failures, note = throughput(self.NOISY_BASE, head)
        self.assertEqual(failures, [])
        self.assertIn("within the noise", note)

    def test_the_same_series_on_both_sides_passes(self):
        failures, note = throughput(self.NOISY_BASE, list(reversed(self.NOISY_BASE)))
        self.assertEqual((failures, note), ([], ""))

    def test_a_real_regression_with_steady_runs_fails(self):
        base = [500_000, 510_000, 495_000, 505_000, 498_000]
        head = [300_000, 310_000, 295_000, 305_000, 298_000]
        failures, _ = throughput(base, head)
        self.assertEqual(len(failures), 1)
        self.assertIn("regressed by -40", failures[0])

    def test_a_regression_just_past_the_threshold_with_steady_runs_fails(self):
        base = [500_000, 505_000, 495_000, 502_000, 498_000]
        head = [345_000, 350_000, 340_000, 348_000, 346_000]  # -30.4%, spread 3%
        failures, _ = throughput(base, head)
        self.assertEqual(len(failures), 1)

    def test_a_large_regression_is_caught_even_when_the_runs_are_noisy(self):
        head = [110_000, 90_000, 130_000, 100_000, 120_000, 105_000, 125_000]  # about -77%
        failures, _ = throughput(self.NOISY_BASE, head)
        self.assertEqual(len(failures), 1)

    def test_a_small_slowdown_under_the_threshold_passes(self):
        base = [500_000, 510_000, 495_000, 505_000, 498_000]
        head = [400_000, 410_000, 395_000, 405_000, 398_000]  # -20%
        self.assertEqual(throughput(base, head), ([], ""))

    def test_a_faster_head_passes(self):
        self.assertEqual(throughput([300_000] * 5, [450_000] * 5), ([], ""))

    def test_one_outlier_run_does_not_decide(self):
        base = [500_000, 505_000, 495_000, 502_000, 498_000]
        head = [499_000, 505_000, 100_000, 501_000, 497_000]  # one stall; the median is unchanged
        self.assertEqual(throughput(base, head)[0], [])

    def test_too_few_runs_are_not_compared(self):
        failures, note = throughput([500_000, 500_000], [100_000, 100_000])
        self.assertEqual(failures, [])
        self.assertIn("not compared", note)

    def test_a_base_below_the_minimum_is_not_compared(self):
        self.assertEqual(throughput([3_000] * 5, [1_000] * 5), ([], ""))

    def test_the_spread_is_range_over_median(self):
        self.assertAlmostEqual(relative_spread_pct([90, 100, 110]), 20.0)
        self.assertEqual(relative_spread_pct([0, 0, 0]), 0.0)

    def run_gate(self, base_load, head_load):
        def doc(load):
            return {
                "tests": {"count": 10, "coverage_pct": 80.0},
                "compatibility": {
                    track: {"pct": 100.0, "components": {c: {"passed": 1, "total": 1, "pct": 100.0} for c in components}}
                    for track, components in REQUIRED_COMPONENTS.items()
                },
                "performance": {"mode": "full", "benchmarks": {}, "load": load},
            }
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for name, load in (("base", base_load), ("head", head_load)):
                path = Path(tmp) / f"{name}.json"
                path.write_text(json.dumps(doc(load)))
                paths.append(str(path))
            return subprocess.run(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "check_quality_gate.py"), *paths],
                capture_output=True, text=True,
            )

    def test_the_gate_uses_the_runs_when_both_sides_have_them(self):
        steady = [500_000, 505_000, 495_000, 502_000, 498_000]
        load = lambda runs: {"high_concurrency_req_per_s": sorted(runs)[len(runs) // 2],
                             "high_concurrency_req_per_s_samples": runs,
                             "high_concurrency_memory_growth_mb": 0}
        slow = [300_000, 310_000, 295_000, 305_000, 298_000]
        result = self.run_gate(load(steady), load(slow))
        self.assertIn("High-concurrency throughput regressed", result.stderr)
        self.assertNotEqual(result.returncode, 0)

    def test_the_gate_falls_back_to_the_single_values_without_runs(self):
        result = self.run_gate(
            {"high_concurrency_req_per_s": 500_000, "high_concurrency_memory_growth_mb": 0},
            {"high_concurrency_req_per_s": 300_000, "high_concurrency_memory_growth_mb": 0},
        )
        self.assertIn("High-concurrency throughput regressed by -40.0%", result.stderr)


if __name__ == "__main__":
    unittest.main()


class PoolOverheadOverrideTests(unittest.TestCase):
    """Lock the 2026-06-05 holdBufPool / hits-pre-size threshold overrides.

    Pins the specific values that allow the documented per-call pool overhead
    (~15 allocs / ~528 B / ~700 ns on a 192 B baseline micro-bench) without
    failing the gate. Re-tighten if/when the pool path changes — but make sure
    TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad and the *_HeapStableUnder*
    stress tests still pass first.
    """

    def _baseline(self):
        # Helper: build a full passing-component dict matching REQUIRED_COMPONENTS
        # so the gate's component-presence checks don't flag the test as missing
        # data (those checks are tested elsewhere; these tests target the
        # perf-bench override path specifically).
        def comps(names):
            return {n: {"passed": 1, "total": 1, "pct": 100.0} for n in names}

        return {
            "tests": {"count": 100, "coverage_pct": 84.8},
            "compatibility": {
                "loki": {"pct": 100.0, "components": comps(
                    ["labels", "label_values", "query_range", "metrics", "otel", "series"]
                )},
                "drilldown": {"pct": 100.0, "components": comps(
                    ["service_selection", "level_volume", "detected_fields",
                     "label_values", "service_logs", "patterns"]
                )},
                "vl": {"pct": 100.0, "components": comps(
                    ["stream_translation", "detected_fields", "synthetic_labels",
                     "index_stats", "volume_range", "field_values"]
                )},
            },
            "performance": {
                "benchmarks": {
                    "query_range_cache_hit_ns_per_op": 1252,
                    "query_range_cache_hit_bytes_per_op": 192,
                    "query_range_cache_hit_allocs_per_op": 8,
                    "query_range_cache_bypass_ns_per_op": 1445,
                    "query_range_cache_bypass_bytes_per_op": 209,
                    "query_range_cache_bypass_allocs_per_op": 8,
                    "labels_cache_hit_ns_per_op": 1000,
                    "labels_cache_hit_bytes_per_op": 100,
                    "labels_cache_hit_allocs_per_op": 5,
                    "labels_cache_bypass_ns_per_op": 1100,
                    "labels_cache_bypass_bytes_per_op": 110,
                    "labels_cache_bypass_allocs_per_op": 3,
                },
                "load": {
                    "high_concurrency_req_per_s": 614_613.0,
                    "high_concurrency_memory_growth_mb": 100.0,
                },
            },
        }

    def _run_gate(self, base, head):
        import json, os, subprocess, tempfile
        with tempfile.NamedTemporaryFile("w", suffix="-base.json", delete=False) as bf:
            json.dump(base, bf)
            base_path = bf.name
        with tempfile.NamedTemporaryFile("w", suffix="-head.json", delete=False) as hf:
            json.dump(head, hf)
            head_path = hf.name
        try:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            result = subprocess.run(
                ["python3", "scripts/ci/check_quality_gate.py", base_path, head_path],
                cwd=repo_root, capture_output=True, text=True,
            )
            return result
        finally:
            os.unlink(base_path)
            os.unlink(head_path)

    def test_pool_overhead_within_overrides_passes(self):
        """The observed CI regression (commit e7f378d → d5179fa) values must
        pass the gate with the override in place."""
        base = self._baseline()
        head = self._baseline()
        head["performance"]["benchmarks"].update({
            "query_range_cache_hit_ns_per_op": 2228,
            "query_range_cache_hit_bytes_per_op": 720,
            "query_range_cache_hit_allocs_per_op": 23,
            "query_range_cache_bypass_ns_per_op": 2427,
            "query_range_cache_bypass_bytes_per_op": 781,
            "query_range_cache_bypass_allocs_per_op": 24,
            "labels_cache_bypass_allocs_per_op": 14,
        })
        result = self._run_gate(base, head)
        self.assertEqual(result.returncode, 0,
            f"gate failed unexpectedly: {result.stderr}")

    def test_known_test_failure_is_not_treated_as_missing_measurement(self):
        base, head = self._baseline(), self._baseline()
        head["tests"] = {"count": 0, "coverage_pct": 0, "execution_state": "failed", "test_exit_code": 42,
                         "outcomes": {"api\tTestQuery": "fail"}}
        result = self._run_gate(base, head)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PR unit tests failed", result.stderr)

    def test_pool_overhead_beyond_overrides_still_fails(self):
        """A real regression (3× the override allowance) MUST still fail —
        the override is a calibrated allowance, not a 'turn off the gate'."""
        base = self._baseline()
        head = self._baseline()
        head["performance"]["benchmarks"]["query_range_cache_hit_bytes_per_op"] = 5000  # ~25x base
        result = self._run_gate(base, head)
        self.assertNotEqual(result.returncode, 0,
            "gate accepted a 25x memory regression — override may have over-loosened")
        self.assertIn("QueryRange cache-hit memory", result.stderr)

    def test_other_benches_still_use_strict_thresholds(self):
        """The override is scoped to specific bench keys; non-overridden
        benches still hit the default 32 B / 1 alloc thresholds."""
        base = self._baseline()
        head = self._baseline()
        # labels_cache_hit_bytes_per_op has NO override and base=100 (above
        # min_base=64), so the default 32 B abs + 15 % thresholds apply.
        head["performance"]["benchmarks"]["labels_cache_hit_bytes_per_op"] = 500
        result = self._run_gate(base, head)
        self.assertNotEqual(result.returncode, 0,
            "gate accepted a 5x growth on a non-overridden bench")
        self.assertIn("Labels cache-hit memory", result.stderr)


class QueryCorpusFailureTests(unittest.TestCase):
    def test_no_corpus_checks_nothing(self):
        self.assertEqual(query_corpus_failures(None), [])
        self.assertEqual(query_corpus_failures({"summary": {}, "failures": []}), [])

    def test_the_reports_failures_fail_the_gate_as_written(self):
        corpus = {"failures": ["query corpus: 2 queries on both the base and the PR turned untyped: q-a, q-b",
                               "Loki: 1 queries whose error text is no longer Loki's: client/q-c"]}
        self.assertEqual(query_corpus_failures(corpus), corpus["failures"])


class ErrorCompatGateTests(unittest.TestCase):
    @staticmethod
    def snap(match=None, coverage=None):
        if match is None:
            return {"quality_inventory": {"error_compat": {"state": "unavailable"}}}
        return {"quality_inventory": {"error_compat": {"state": "available", "match_ratio": match, "coverage_ratio": coverage}}}

    def test_a_drop_in_either_score_fails(self):
        self.assertEqual(error_compat_failures(self.snap(0.5, 0.6), self.snap(0.5, 0.6)), [])
        self.assertEqual(error_compat_failures(self.snap(0.5, 0.6), self.snap(0.7, 0.9)), [])
        failures = error_compat_failures(self.snap(0.5, 0.6), self.snap(0.4, 0.5))
        self.assertEqual(len(failures), 2)
        self.assertIn("errors that match Loki fell from 50.0% to 40.0%", failures[0])
        self.assertIn("error matrix coverage fell from 60.0% to 50.0%", failures[1])

    def test_no_base_or_no_head_scores_compare_nothing(self):
        self.assertEqual(error_compat_failures({}, self.snap(0.1, 0.1)), [])
        self.assertEqual(error_compat_failures(self.snap(0.5, 0.6), {}), [])
        self.assertEqual(error_compat_failures(self.snap(), self.snap()), [])
