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
    def test_query_semantics_catalog_and_live_matrix_are_separate(self):
        base, head = snapshot(), snapshot()
        base["quality_inventory"] = {"query_semantics": {"state": "available", "cases": 67,
            "operations": 24, "depth_paths": 12, "exact_metric_points": 33, "exact_log_entries": 0,
            "performance_cases": 50, "performance_shared": 13, "performance_unshared": 37,
            "client_errors": 8, "server_errors": 0}}
        head["quality_inventory"] = {"query_semantics": {"state": "available", "cases": 80,
            "operations": 25, "depth_paths": 12, "exact_metric_points": 41, "exact_log_entries": 29,
            "performance_cases": 50, "performance_shared": 21, "performance_unshared": 29,
            "client_errors": 9, "server_errors": 0}}
        evidence = {"sources": {"loki": {"state": "completed", "conclusion": "failure",
            "test_scopes": [{"state": "available", "query_semantics_matrix":
                             {"passed": 79, "failed": 1, "skipped": 0, "failed_cases": ["parser"]}}]}}}
        report = self.render(base=base, head=head, evidence=evidence)
        focus = report.split("<details>", 1)[0]
        self.assertIn("Query semantics catalog: 80 cases", focus)
        self.assertIn("29 exact log-entry contracts", focus)
        self.assertIn("29 performance-only scenarios", focus)
        self.assertIn("Pinned Loki query matrix: 79 passed, 1 failed, 0 skipped", focus)
        self.assertIn("| Registered cases | 67 | 80 | +13 |", report)
        self.assertIn("| Exact log-entry contracts | 0 | 29 | +29 |", report)
        self.assertIn("| Performance-only scenarios | 37 | 29 | -8 |", report)
        self.assertIn("| Matrix cases | 79 | 1 | 0 |", report)
        self.assertIn("Failing case IDs: parser", report)

    def test_recorded_conformance_subtests_are_scoped_and_collapsed(self):
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "success", "artifacts": {
            "ci-conformance-summary.json": {"source": "a" * 40, "parents": {"pass": 3, "fail": 0, "skip": 2, "incomplete": 1, "not-run": 8},
                "subtests": {"pass": 15, "fail": 0, "skip": 4}, "cases": [
                    {"package": "p", "test": "TestCombined/json/sum/level", "outcome": "skip", "kind": "subtest", "items": ["semantics/query"]}]}}}}})
        focus = report.split("<details>", 1)[0]
        self.assertIn("Recorded conformance: 3 passed, 0 failed, 2 skipped, 1 incomplete parent tests", focus)
        self.assertIn("15 passed, 0 failed, 4 skipped subtests", focus)
        self.assertIn("<details><summary>Recorded conformance contracts and subtests", report)
        self.assertIn("TestCombined/json/sum/level", report)
        self.assertIn("package + full test path", report)
        self.assertIn("not full Loki compatibility", report)
        self.assertIn("Declared but not run", report)

    def test_recorded_conformance_failure_is_above_other_signals(self):
        sources = {f"absent-{i}": {"state": "unavailable"} for i in range(20)}
        sources["ci"] = {"state": "completed", "conclusion": "failure", "artifacts": {
            "ci-conformance-summary.json": {"parents": {"pass": 0, "fail": 1, "skip": 0, "incomplete": 0},
                "subtests": {"pass": 4, "fail": 1, "skip": 0}, "cases": []}}}
        report = self.render(evidence={"sources": sources})
        self.assertIn("0 passed, 1 failed", report.split("<details>", 1)[0])

    def test_recorded_conformance_detail_size_is_bounded_after_escaping(self):
        cases = [{"package": '<>&|*' * 100, "test": f"TestCombined/{index}/" + '<>&|*' * 100,
                  "outcome": "fail" if index == 0 else "pass", "kind": "subtest", "items": ['<>&|*' * 100] * 4}
                 for index in range(80)]
        report = self.render(evidence={"sources": {"ci": {"state": "completed", "conclusion": "failure", "artifacts": {
            "ci-conformance-summary.json": {"source": "a" * 40, "parents": {"pass": 0, "fail": 1, "skip": 0, "incomplete": 0},
                "subtests": {"pass": 79, "fail": 1, "skip": 0}, "cases": cases,
                "artifact_url": "https://github.com/owner/repo/actions/runs/12/artifacts/34"}}}}})
        section = report.split('<details><summary>Recorded conformance contracts and subtests</summary>', 1)[1].split("</details>", 1)[0]
        self.assertLessEqual(len(section), 12000)
        self.assertIn("0 passed, 1 failed", report.split("<details>", 1)[0])
        self.assertIn("TestCombined/0/", section)
        self.assertRegex(section, r"Showing [1-9][0-9]?/80 observed identities")
        self.assertNotIn("Showing 40/80", section)
        self.assertIn("[conformance-evidence artifact](https://github.com/owner/repo/actions/runs/12/artifacts/34)", section)

    def test_handler_summary_highlights_guarded_regressions(self):
        base, head = snapshot(), snapshot()
        head["performance"]["benchmarks"].update(query_range_cache_hit_ns_per_op=3000, query_range_cache_hit_bytes_per_op=1000)
        report = self.render(base=base, head=head)
        focus = report.split("<details>", 1)[0]
        self.assertIn("Synthetic handlers: 1 paired case; 2 regressed, 0 improved, 1 stable metric axes", focus)
        self.assertIn("QueryRange cache-hit elapsed", focus)
        self.assertIn("allocated bytes", focus)

    def test_additional_handler_cases_are_candidate_only(self):
        evidence = {"sources": {"ci": {"state": "completed", "conclusion": "success", "artifacts": {
            "ci-handler-benchmarks.json": {"state": "available", "source": "h", "cases": [
                {"name": "BenchmarkProxy_Series_CompatCacheHit", "cpu": 8, "samples": 3, "ns_per_op": 2000, "bytes_per_op": 30, "allocs_per_op": 3},
                {"name": "BenchmarkProxy_DetectedFields_CompatCacheHit_DelayedBackend", "cpu": 8, "samples": 3, "ns_per_op": 3000, "bytes_per_op": 40, "allocs_per_op": 4}]}}}}}
        report = self.render(evidence=evidence)
        self.assertIn("2 cases from the broader CI handler run (candidate only", report.split("<details>", 1)[0])
        self.assertIn("Series", report)
        self.assertIn("DetectedFields", report)
        self.assertIn("No paired base in this artifact", report)
        self.assertIn("2 µs", report)

    def test_handler_summary_does_not_infer_delta_from_missing_or_zero_base(self):
        base, head = snapshot(), snapshot()
        base["performance"]["benchmarks"] = {"query_range_cache_hit_ns_per_op": 0}
        report = self.render(base=base, head=head)
        focus = report.split("<details>", 1)[0]
        self.assertNotIn("1 paired cases", focus)
        self.assertNotIn("regressed metric", focus)
        self.assertIn("no comparable paired metrics", focus)

    def test_report_labels_read_only_endpoint_population(self):
        inventory = {"state": "available", "endpoint_scope": "loki-read-api", "endpoints": {
            "total": 32, "implemented": 22, "differential": 18, "declared_proven": 11},
            "excluded_write_endpoints": 2, "deletion_management": {"total": 1, "implemented": 1}}
        base, head = snapshot(), snapshot()
        base["quality_inventory"] = head["quality_inventory"] = inventory
        report = self.render(base=base, head=head)
        self.assertIn("22/32 read endpoints implemented", report.split("<details>", 1)[0])
        self.assertIn("Read endpoints: declared proven", report)
        self.assertIn("deletion management", report)

    def test_report_exposes_real_inventory_and_exact_case_changes(self):
        base, head = snapshot(), snapshot()
        base["tests"]["outcomes"] = {"api\tTestQuery": "pass", "api\tTestQuery/edge": "pass", "api\tTestRemoved": "pass"}
        head["tests"]["outcomes"] = {"api\tTestQuery": "fail", "api\tTestQuery/edge": "skip"}
        inventory = {"state": "available", "surface_version": "v3.7.7", "endpoints": {"total": 33, "implemented": 23, "differential": 18, "declared_proven": 11}, "gaps": {"open": 67}}
        base["quality_inventory"], head["quality_inventory"] = inventory, inventory
        report = self.render(base=base, head=head)
        focus = report.split("<details>", 1)[0]
        self.assertIn("newly failing", focus)
        self.assertIn("newly skipped", focus)
        self.assertIn("removed", focus)
        self.assertIn("23/33 endpoints implemented", focus)
        self.assertIn("67 open compatibility gaps", focus)
        self.assertIn("Endpoints: linked differential tests", report)
        self.assertIn("Full test name", report)

    def test_broader_scopes_show_candidate_evidence_without_fake_base(self):
        report = self.render(evidence={"sources": {"loki": {"state": "completed", "conclusion": "success", "test_scopes": [
            {"state": "available", "name": "loki-pinned", "kind": "Go", "passed": 420, "failed": 0, "skipped": 8, "versions": {"LOKI_IMAGE": ["grafana/loki:3.7.9"]}},
            {"state": "unavailable", "name": "missing-log", "note": "Expired"}]}}})
        self.assertIn("Broader API/Grafana execution: 1 scopes", report.split("<details>", 1)[0])
        self.assertIn("420", report)
        self.assertIn("Candidate only", report)
        self.assertIn("3.7.9", report)
        self.assertIn("missing-log | unavailable | unavailable", report)

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
        self.assertIn("API contract coverage and regression changes", report)
        self.assertIn("not full runtime compatibility", report)
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
