"""Exercise the public renderer CLI with measured and missing evidence."""
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def snapshot(count=10, coverage=85):
    return {"tests": {"count": count, "coverage_pct": coverage},
            "compatibility": {k: {"passed": 2, "total": 2, "pct": 100}
                              for k in ("loki", "drilldown", "vl")},
            "performance": {"mode": "full", "benchmarks": {
                "query_range_cache_hit_ns_per_op": 1000,
                "query_range_cache_hit_bytes_per_op": 100,
                "query_range_cache_hit_allocs_per_op": 3}}}


class ReportTests(unittest.TestCase):
    def test_successful_run_with_stale_artifacts_warns_above_fold(self):
        report = self.render(evidence={"sources": {"perf": {"state": "completed", "conclusion": "success", "note": "Stale source identity; measurements omitted."}}})
        self.assertIn("Stale source identity", report.split("<details>", 1)[0])

    def test_stale_response_is_not_promoted_as_faster_proof(self):
        report = self.render(evidence={"sources": {"perf": {"state": "completed", "conclusion": "success", "artifacts": {
            "verdict.json": {"counts": {"faster": 1}},
            "summary-x.json": {"rows": [{"shape": "stale-query", "verdict": "faster", "stale": ["pr"], "p50": {"base": .4, "pr": .1}}]}}}}})
        self.assertNotIn("faster: unspecified", report.split("<details>", 1)[0])
        self.assertIn("stale answer", report)

    def test_failure_is_not_hidden_by_many_other_focus_signals(self):
        sources = {"perf": {"state": "completed", "conclusion": "failure", "artifacts": {"verdict.json": {"counts": {"slower": 1}}}}}
        sources.update({f"source-{i}": {"state": "unavailable"} for i in range(20)})
        sources["security"] = {"state": "completed", "conclusion": "failure", "jobs": [{"name": "runtime", "status": "completed", "conclusion": "failure"}]}
        report = self.render(base=snapshot(0, 0), head=snapshot(0, 0), evidence={"sources": sources})
        self.assertIn("Check security / runtime: failure", report.split("<details>", 1)[0])

    def render(self, base=None, head=None, evidence=None):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / f"{name}.json" for name in ("base", "head", "evidence")]
            for p, data in zip(paths, (base or snapshot(), head or snapshot(), evidence)):
                p.write_text(json.dumps(data))
            output = Path(directory) / "report.md"
            command = ["bash", str(ROOT / "scripts/ci/render_pr_report.sh"),
                       str(paths[0]), str(paths[1]), str(output)]
            if evidence is not None:
                command.append(str(paths[2]))
            subprocess.run(command, check=True, capture_output=True, text=True)
            return output.read_text()

    def test_unavailable_base_has_no_fake_improvement(self):
        report = self.render(base=snapshot(0, 0))
        self.assertIn("| Test count | unavailable | 10 | n/a |", report)
        self.assertIn("| Coverage | unavailable | 85% | n/a |", report)

    def test_smoke_is_scoped_and_synthetic_costs_collapsed(self):
        report = self.render()
        self.assertIn("Smoke contracts", report)
        self.assertIn("not full API compatibility", report)
        self.assertIn("<details><summary>Synthetic handler microbenchmarks", report)
        self.assertNotIn("CPU cost |", report)
        self.assertIn("elapsed time", report)

    def test_pending_and_skipped_never_claim_pass_or_reuse_values(self):
        report = self.render(evidence={"sources": {
            "perf": {"state": "pending", "artifacts": {"verdict.json": {"rows": 999}}},
            "visual": {"state": "completed", "conclusion": "skipped"}}})
        self.assertIn("pending", report)
        self.assertIn("skipped", report)
        self.assertNotIn("999", report)

    def test_real_ab_counts_and_profiles_preserve_warnings(self):
        source = {"state": "completed", "conclusion": "success", "artifacts": {
            "verdict.json": {"rows": 6, "counts": {"faster": 2, "same": 4},
                             "new_differences": 0, "preexisting_differences": 1,
                             "over_2x": {"mode": "warn", "new": 1, "known": 2}},
            "summary-x.json": {"profile": "loki-defaults", "rows": [
                {"shape": "x", "range": "1h", "status": {"base": "200", "pr": "200", "loki": "200"},
                 "p50": {"base": .4, "pr": .2, "loki": .1}, "p95": {"pr": .3},
                 "cold": {"base": .5, "pr": .3, "loki": .2},
                 "parity_vs_reference": "same"}]}}}
        report = self.render(evidence={"sources": {"perf": source}})
        self.assertIn("2 faster", report)
        self.assertIn("1 new", report)
        self.assertIn("warning", report)
        self.assertIn("loki-defaults", report)
        self.assertIn("200 ms", report)

    def test_untrusted_content_cannot_add_markup_or_links(self):
        report = self.render(evidence={"sources": {"perf": {
            "state": "<img src=x>", "conclusion": "bad|row\n@owner",
            "url": "javascript:alert(1)"}}, "context": {"base": "<script>"}})
        self.assertNotIn("<img", report)
        self.assertNotIn("<script>", report)
        self.assertNotIn("javascript:", report)
        self.assertNotIn("@owner", report)

    def test_candidate_fleet_has_resources_but_no_baseline_delta(self):
        fleet = {"replication_factor": 2, "summary": [{"phase": "disk_kept", "layer": "stats_extent",
                 "replica": "b", "first": {"count": 1, "p50_ms": 4, "p95_ms": 4},
                 "repeat": {"count": 3, "p50_ms": 1, "p95_ms": 2}, "vl_calls": 0,
                 "peer_hits": 3}], "phases": [{"phase": "disk_kept", "replicas": {"b": {
                 "cpu_seconds": .1, "rss_peak_bytes": 1048576, "memory_cache_bytes_end": 100,
                 "disk_cache_bytes_end": 200}}}]}
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "success",
                        "artifacts": {"fleet-peer-cache-proof-rf2.json": fleet}}}})
        self.assertIn("candidate only", report)
        self.assertIn(r"stats\_extent", report)
        self.assertIn("RSS", report)
        self.assertIn("physical SSD", report)

    def test_failure_and_stale_evidence_are_actionable(self):
        report = self.render(evidence={"sources": {
            "perf": {"state": "stale", "conclusion": "success", "artifacts": {
                "verdict.json": {"counts": {"faster": 999}}}},
            "visual": {"state": "completed", "conclusion": "failure", "artifacts": {
                "verdict.json": {"captures": 3, "failures": ["bad <script>|row"],
                                 "missing": ["fields 1h"], "improved": 0, "warnings": 1}}}}})
        self.assertNotIn("999", report)
        self.assertIn("1 failures, 1 missing", report)
        self.assertIn("stale", report)
        self.assertNotIn("<script>", report)

    def test_ci_summary_and_failures_use_existing_run_scope(self):
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "failure",
                "jobs": [{"name": "e2e-fleet", "status": "completed", "conclusion": "failure"}],
                "artifacts": {"ci-regression-summary.json": {
                    "tests": {"scope": "internal packages (existing CI artifact)", "passed": 40,
                              "failed": 1, "skipped": 2, "top_level_passed": 10,
                              "subtests_passed": 30, "packages": 3, "failed_names": ["TestBroken"]},
                    "coverage": {"overall_pct": 89.6, "cache_pct": 82}}}}}})
        self.assertIn(r"internal packages \(existing CI artifact\)", report)
        self.assertIn("TestBroken", report)
        self.assertIn("82%", report)
        self.assertIn("1 failed", report)

    def test_missing_head_is_unavailable_and_counted_zero_is_not_parity(self):
        report = self.render(head=snapshot(0, 0))
        self.assertIn("| Test count | 10 | unavailable | n/a |", report)
        self.assertIn("| Coverage | 85% | unavailable | n/a |", report)

    def test_microbench_pool_allowance_matches_existing_gate(self):
        base, head = snapshot(), snapshot()
        head["performance"]["benchmarks"]["query_range_cache_hit_bytes_per_op"] = 600
        report = self.render(base, head)
        self.assertIn("+500.0% (stable)", report)
        head["performance"]["benchmarks"]["query_range_cache_hit_bytes_per_op"] = 900
        report = self.render(base, head)
        self.assertIn("+800.0% (regressed)", report)

    def test_error_latency_and_no_sample_fleet_are_not_speed_proof(self):
        report = self.render(evidence={"sources": {
            "perf": {"state": "completed", "conclusion": "failure", "artifacts": {
                "verdict.json": {"state": "failed", "counts": {"broken": 1}},
                "summary-x.json": {"rows": [{"shape": "bad", "range": "1h", "status": {"pr": "500"},
                                                "p50": {"pr": .001}}]}}},
            "ci": {"state": "completed", "conclusion": "success", "artifacts": {
                "fleet-peer-cache-proof-rf1.json": {"replication_factor": 1, "summary": [
                    {"first": {"count": 0, "p50_ms": 0, "p95_ms": 0}}]}}}}})
        self.assertIn("HTTP 500", report)
        self.assertNotIn("1 ms", report)
        self.assertIn("no samples", report)

    def test_all_sources_failed_checks_and_missing_snapshot_fallback(self):
        report = self.render(head=snapshot(0, 0), evidence={"sources": {
            "ci": {"state": "completed", "conclusion": "success", "artifacts": {
                "ci-regression-summary.json": {"tests": {"passed": 50, "failed": 0, "skipped": 3,
                    "scope": "internal only"}, "coverage": {"overall_pct": 89.6}}}},
            "security": {"state": "completed", "conclusion": "failure", "note": "static failed",
                         "jobs": [{"name": "scan", "status": "completed", "conclusion": "failure"}]},
            "loki": {"state": "pending", "note": "artifact not yet available", "jobs": [
                {"name": "loki-pinned", "status": "in_progress", "conclusion": None}]}}})
        before_details = report.split("<details>")[0]
        self.assertIn("50 passed, 0 failed, 3 skipped", before_details)
        self.assertIn("89.6%", before_details)
        self.assertIn("security / scan: failure", before_details)
        self.assertIn("loki / loki-pinned", before_details)
        self.assertIn("artifact not yet available", report)
        self.assertIn("| security |", report)
        self.assertIn("| Coverage | 85% | unavailable | n/a |", report)

    def test_failed_job_after_many_successes_is_not_truncated_away(self):
        jobs = [{"name": str(i), "status": "completed", "conclusion": "success"} for i in range(80)]
        jobs.append({"name": "critical", "status": "completed", "conclusion": "failure"})
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "failure", "jobs": jobs}}})
        self.assertIn("ci / critical: failure", report.split("<details>")[0])
        self.assertIn("| ci / critical | completed | failure |", report)

    def test_malformed_optional_artifacts_do_not_hide_quality_or_state(self):
        report = self.render(evidence={"sources": {
            "ci": {"state": "completed", "conclusion": "failure", "jobs": None,
                   "artifacts": {"ci-regression-summary.json": ["bad"],
                                 "fleet-peer-cache-proof-rf1.json": {"summary": None, "phases": [None]}}},
            "perf": {"state": "completed", "conclusion": "failure", "artifacts": {
                "verdict.json": ["bad"], "summary-x.json": {"rows": None}}},
            "visual": {"state": "completed", "conclusion": "success", "artifacts": None}}})
        self.assertIn("| Coverage | 85% | 85%", report)
        self.assertIn("perf: completed / failure", report)

    def test_base_unlike_note_and_existing_vl_cpu_are_scoped(self):
        report = self.render(evidence={"sources": {"perf": {"state": "completed", "conclusion": "success",
            "artifacts": {"verdict.json": {"counts": {"fixed": 1}}, "summary-x.json": {
                "not_like_for_like": ["base"], "rows": [{"shape": "x", "range": "1h",
                    "status": {"base": "200", "pr": "400", "loki": "400"},
                    "verdict": "fixed", "note": "now rejected exactly as the reference rejects it",
                    "vl_cpu_max": {"base": 50, "pr": 10, "loki": 0}}]}}}}})
        self.assertIn("not like-for-like; fixed", report)
        self.assertIn("now rejected exactly as the reference rejects it", report)
        self.assertIn("50%", report)
        self.assertIn("not query-attributable CPU seconds", report)

    def test_backend_container_stats_keep_phase_samples_and_units(self):
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "success",
            "artifacts": {"fleet-peer-cache-proof-rf2.json": {"replication_factor": 2,
                "phases": [{"phase": "warm", "seconds": 5, "containers": {"fleet-vl": {
                    "cpu_percent_avg": 35, "cpu_percent_max": 70, "mem_peak_bytes": 2097152, "samples": 4}}}]}}}}})
        self.assertIn("2 / warm / fleet-vl | 5 / 4 | 35% / 70% | 2", report)
        self.assertIn("not proxy RSS", report)
        self.assertIn("not a simultaneous fleet-wide peak", report)

    def test_smoke_failures_and_source_notes_are_above_fold(self):
        head = snapshot()
        head["compatibility"]["loki"] = {"passed": 1, "total": 2, "pct": 50}
        report = self.render(head=head, evidence={"sources": {"visual": {"state": "stale",
            "conclusion": "success", "note": "base revision changed"}}})
        before_details = report.split("<details>")[0]
        self.assertIn("1 not passing (1/2)", before_details)
        self.assertIn("base revision changed", before_details)

    def test_existing_disk_and_failure_counters_and_cache_scope(self):
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "success",
            "artifacts": {"fleet-peer-cache-proof-rf2.json": {"replication_factor": 2,
                "requests": [{"phase": "memory_lost_disk_kept", "layer": "stats_extent", "replica": "b",
                    "disk_hits": 4, "window_hits": 0, "peer_misses": 1, "backup_hits": 2,
                    "fleet_vl_calls": 3, "vl_background_calls": 1, "fleet_vl_routes": {"stats_query_range": 3}}]}}}}})
        self.assertIn("Disk / window hits", report)
        self.assertIn("| 1 | 4 / 0 | 1 / 2 | 3 / 1 |", report)
        self.assertIn(r"stats\_query\_range=3", report)
        self.assertIn("VL-facing cache occupancy is unmeasured", report)
        self.assertIn("Loki-layer logical RAM", report)

    def test_huge_numeric_is_unavailable_not_overflow(self):
        base, head = snapshot(), snapshot()
        head["tests"]["coverage_pct"] = 10 ** 400
        report = self.render(base, head)
        self.assertIn("| Coverage | 85% | unavailable | n/a |", report)

    def test_concrete_changed_shapes_above_fold_and_bad_rows_prioritized(self):
        rows = [{"shape": "unchanged" + str(i), "range": "1h", "verdict": "same"} for i in range(25)]
        rows += [{"shape": "slow-shape", "range": "24h", "verdict": "slower", "p50": {"base": 1, "pr": 2}},
                 {"shape": "better-shape", "range": "6h", "verdict": "faster", "p50": {"base": 1, "pr": .2}}]
        report = self.render(evidence={"sources": {"perf": {"state": "completed", "conclusion": "failure",
            "artifacts": {"verdict.json": {"counts": {"slower": 1, "faster": 1}},
                          "summary-x.json": {"profile": "loki-defaults", "set": "drilldown", "rows": rows}}}}})
        before_details = report.split("<details>")[0]
        self.assertIn("slower: loki-defaults / drilldown / slow-shape / 24h; base 1000 ms → PR 2000 ms", before_details)
        self.assertIn("faster: loki-defaults / drilldown / better-shape / 6h; base 1000 ms → PR 200 ms", before_details)
        latency = report.split("Real workload A/B")[1]
        self.assertLess(latency.index("slow-shape"), latency.index("unchanged0"))


if __name__ == "__main__":
    unittest.main()
