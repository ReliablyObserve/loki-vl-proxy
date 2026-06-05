import unittest

from scripts.ci.check_quality_gate import check_component_breakdown


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
