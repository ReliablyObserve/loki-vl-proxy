"""Exercise the public renderer CLI with measured and missing evidence."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))
import pr_quality_report  # noqa: E402


def snapshot(count=10, coverage=85):
    return {"tests": {"count": count, "coverage_pct": coverage},
            "compatibility": {k: {"passed": 2, "total": 2, "pct": 100}
                              for k in ("loki", "drilldown", "vl")},
            "performance": {"mode": "full", "benchmarks": {
                "query_range_cache_hit_ns_per_op": 1000,
                "query_range_cache_hit_bytes_per_op": 100,
                "query_range_cache_hit_allocs_per_op": 3}}}


class ReportTests(unittest.TestCase):
    def test_catalog_source_populates_report_when_quality_snapshot_is_absent(self):
        catalog = {"state": "completed", "conclusion": "success", "artifacts": {
            "base": {"state": "available", "cases": 80, "exact_log_entries": 0, "ranked_step_values": 0, "performance_unshared": 37},
            "head": {"state": "available", "cases": 88, "exact_log_entries": 29, "ranked_step_values": 2, "performance_unshared": 29}}}
        evidence = {"sources": {"quality": {"state": "completed", "conclusion": "skipped"}, "catalog": catalog,
            "loki": {"state": "completed", "conclusion": "success", "test_scopes": [
                {"state": "available", "query_semantics_matrix": {"passed": 88, "failed": 0, "skipped": 0}}]}}}
        report = self.render(base=snapshot(0, 0), head=snapshot(0, 0), evidence=evidence)
        self.assertIn("Query semantics catalog: 88 cases", report.split("<details>", 1)[0])
        self.assertIn("Pinned Loki query matrix: 88 passed, 0 failed, 0 skipped", report.split("<details>", 1)[0])
        self.assertIn("| Exact log-entry contracts | 0 | 29 | +29 |", report)
        self.assertIn("| Tie-tolerant ranked-step contracts | 0 | 2 | +2 |", report)
        self.assertIn("| Performance-only scenarios | 37 | 29 | -8 |", report)

    def test_query_corpus_section_shows_counts_ratchet_and_golden_changes(self):
        head = snapshot()
        head["query_corpus"] = {
            "summary": {"client_rows": 23022, "internal_parse_call_sites": 70, "golden_read": 610,
                        "golden_versions": ["v1.53.0", "v1.49.0"]},
            "base": {"client_rows": 23000, "internal_parse_call_sites": 70, "golden_read": 604},
            "targets": [{"name": "internal_parse_call_sites", "rule": "max", "want": 70, "got": 70, "state": "held"},
                        {"name": "golden_read", "rule": "min", "want": 604, "got": 610, "state": "better"}],
            "regressions": {"untyped": ["q-zzz"], "golden_unread": []},
            "golden_diffs": [{"version": "v1.53.0", "compared": 22000, "changed": 3, "added": 22, "removed": 0,
                              "examples": ["q-aaa", "q-bbb", "q-ccc"]}]}
        report = self.render(base=snapshot(), head=head)
        self.assertIn("Query corpus: base versus PR", report)
        self.assertIn("| Client queries (tests and routes) | 23000 | 23022 | +22 | — |", report)
        self.assertIn("| Goldens our LogsQL parser reads | 604 | 610 | +6 | min 604: better |", report)
        self.assertIn("| v1.53.0 | 22000 | 3 | 22 | 0 | q-aaa, q-bbb, q-ccc |", report)
        self.assertIn("Regressions (queries on both sides): 1 turned untyped (q-zzz); 0 goldens no longer read.", report)

    def test_query_text_sites_section_compares_base_and_pr(self):
        base, head = snapshot(), snapshot()
        base["quality_inventory"] = {"query_text_sites": {"state": "available", "totals": {"parse_calls": 64, "string_reads": 54}}}
        head["quality_inventory"] = {"query_text_sites": {"state": "available", "totals": {"parse_calls": 60, "string_reads": 54}}}
        report = self.render(base=base, head=head)
        self.assertIn("Query text handled as strings: base versus PR", report)
        self.assertIn("| LogQL/LogsQL parser calls outside the AST packages | 64 | 60 | -4 |", report)

    def test_error_compat_section_shows_base_versus_pr_and_flags_a_drop(self):
        def inventory(matching, covered):
            return {"error_compat": {"state": "available", "recorded_rows": 286, "comparable_rows": 215, "matching_rows": matching,
                                     "cells": 124, "covered_cells": covered, "messages": 33, "messages_recorded": 27,
                                     "match_ratio": round(matching / 215, 4), "coverage_ratio": round(covered / 124, 4)}}
        base, head = snapshot(), snapshot()
        base["quality_inventory"], head["quality_inventory"] = inventory(74, 71), inventory(100, 80)
        report = self.render(base=base, head=head)
        self.assertIn("Error compatibility: base versus PR", report)
        self.assertIn("| Recorded Loki answers matched | 74 | 100 | +26 |", report)
        self.assertIn("| Errors that match Loki | 34.4% | 46.5% | +12.1 pp |", report)
        self.assertIn("Errors match Loki: 46.5% (base 34.4%) of 215 recorded answers", report.split("<details>", 1)[0])
        self.assertNotIn("Error compatibility fell", report)
        base["quality_inventory"], head["quality_inventory"] = inventory(100, 80), inventory(74, 71)
        self.assertIn("Error compatibility fell (match ratio: 46.5% → 34.4%).", self.render(base=base, head=head).split("<details>", 1)[0])
        del base["quality_inventory"]
        self.assertIn("| Errors that match Loki | no base | 34.4% | n/a |", self.render(base=base, head=head))

    def test_query_corpus_oracle_table_and_regressions(self):
        head = snapshot()
        head["query_corpus"] = {
            "summary": {"client_rows": 5, "golden_versions": ["v1.53.0"]}, "targets": [],
            "oracle": [{"version": "v1.53.0", "goldens": 100, "goldens_rejected": 9, "sent_rejected": 2}],
            "oracle_base": [{"version": "v1.53.0", "goldens": 100, "goldens_rejected": 8, "sent_rejected": 2}],
            "oracle_regressions": [{"version": "v1.53.0", "goldens_rejected": ["q-x"], "goldens_meaning_lost": [], "cases_lost": []}],
            "constructs": [{"version": "v1.53.0", "missing": ["pipes/unroll"]}]}
        report = self.render(base=snapshot(), head=head)
        self.assertIn("VictoriaLogs v1.53.0:", report)
        self.assertIn("| Goldens VictoriaLogs rejects | 8 | 9 | +1 |", report)
        self.assertIn("Regressions on VictoriaLogs v1.53.0: 1 goldens now rejected; 0 goldens whose meaning changed; 0 test cases no longer read the same; 0 typed selector filters that differ.", report)
        self.assertIn("Constructs of VictoriaLogs v1.53.0 not covered by `internal/logsql`: pipes/unroll.", report)

    def test_query_corpus_loki_tables_regressions_and_similarity(self):
        head = snapshot()
        head["query_corpus"] = {
            "summary": {"client_rows": 5, "golden_versions": ["v1.53.0"]}, "targets": [],
            "loki": {"version": "3.7.9", "client": {"queries": 10, "both_accept": 8, "ours_only": 1}, "case": {"queries": 3, "both_accept": 3}},
            "loki_base": {"version": "3.7.9", "client": {"queries": 10, "both_accept": 9, "ours_only": 0}, "case": {"queries": 3, "both_accept": 3}},
            "loki_regressions": {"disagree": ["client/q-a"], "error_lost": [], "meaning_lost": []},
            "similar": [{"file": "internal/logql/stage_validate.go", "shared": 9, "allowed": 9, "best": "pkg/logql/log/fmt.go"}]}
        report = self.render(base=snapshot(), head=head)
        self.assertIn("Loki 3.7.9's own parser", report)
        self.assertIn("| The proxy accepts, Loki rejects | 0 | 1 | +1 |", report)
        self.assertIn("Regressions against Loki: 1 no longer agree on accepting; 0 error text no longer Loki's; 0 printed form no longer means the same.", report)
        self.assertIn("| internal/logql/stage\\_validate.go | 9 | 9 | pkg/logql/log/fmt.go |  |", report)

    def test_query_corpus_without_a_base_says_so(self):
        head = snapshot()
        head["query_corpus"] = {"summary": {"client_rows": 5, "golden_versions": ["v1.53.0"]}, "targets": []}
        report = self.render(base=snapshot(), head=head)
        self.assertIn("| Client queries (tests and routes) | no base | 5 | n/a | — |", report)
        self.assertIn("No base corpus", report)

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
        head = snapshot()
        head["tests"]["outcomes"] = {"api\tTestQuery": "pass"}
        report = self.render(head=head)
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

    def test_error_latency_is_shown_with_its_status_and_no_sample_fleet_is_not_speed_proof(self):
        # An error answer still has a time; it is shown with its status code in parentheses.
        report = self.render(evidence={"sources": {
            "perf": {"state": "completed", "conclusion": "failure", "artifacts": {
                "verdict.json": {"state": "failed", "counts": {"broken": 1}},
                "summary-x.json": {"rows": [{"shape": "bad", "range": "1h", "status": {"pr": "500"},
                                                "p50": {"pr": .001}}]}}},
            "ci": {"state": "completed", "conclusion": "success", "artifacts": {
                "fleet-peer-cache-proof-rf1.json": {"replication_factor": 1, "summary": [
                    {"first": {"count": 0, "p50_ms": 0, "p95_ms": 0}}]}}}}})
        self.assertIn("— / 1 ms / — (HTTP 500)", report)
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
        # The proof carries no resource samples: that table has no value in any row, so it is not rendered.
        self.assertNotIn("Loki-layer logical RAM", report)

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


class EmptySectionTests(unittest.TestCase):
    """A section or table with no value is not rendered; what has data stays as it was."""

    render = ReportTests.render

    def test_prune_drops_rows_without_a_value_and_tables_without_rows(self):
        lines = ["intro", "", "| Signal | Base | PR |", "|---|---|---|", "| a | unavailable | unavailable |",
                 "| b | n/a | — |", "| c | 1 | unavailable |", "", "Showing 3/3 rows.", "",
                 "| X | Y |", "|---|---|", "| p | unavailable |", "", "Showing 1/1 rows.", "", "tail"]
        out, kept = pr_quality_report.prune(lines)
        self.assertEqual(kept, 1)
        self.assertEqual(out, ["intro", "", "| Signal | Base | PR |", "|---|---|---|", "| c | 1 | unavailable |", "",
                               "Showing 1/1 rows with a value.", "", "tail"])

    def test_prune_keeps_a_table_with_every_row_valued_untouched(self):
        lines = ["| A | B |", "|---|---|", "| x | 1 |", "| y | 2 |", "", "Showing 2/2 rows."]
        self.assertEqual(pr_quality_report.prune(lines), (lines, 2))

    def test_prune_removes_a_header_only_table(self):
        self.assertEqual(pr_quality_report.prune(["| A | B |", "|---|---|"]), ([], 0))

    def test_escaped_pipes_do_not_split_a_cell(self):
        self.assertEqual(pr_quality_report.table_cells(r"| a\|b | 1 |"), [r"a\|b", "1"])

    def test_empty_report_has_no_empty_table_and_one_no_data_line(self):
        report = self.render(base=snapshot(0, 0), head=snapshot(0, 0))
        self.assertNotIn("| unavailable | unavailable |", report)
        self.assertNotIn("Showing 0/", report)
        self.assertNotIn("Test count", report)
        self.assertNotIn("<summary>API contract coverage", report)
        self.assertNotIn("<summary>Coverage and tests", report)
        self.assertEqual(report.count("No data (sections omitted):"), 1)
        line = next(x for x in report.splitlines() if x.startswith("No data"))
        for title in ("Coverage and tests", "API contract coverage and regression changes"):
            self.assertIn(title, line)
        self.assertLess(len(line), 600)

    def test_every_rendered_table_has_a_row_with_a_value(self):
        for base, head in ((snapshot(0, 0), snapshot(0, 0)), (snapshot(0, 0), snapshot()), (snapshot(), snapshot())):
            report = self.render(base=base, head=head)
            lines = report.splitlines()
            for index, line in enumerate(lines):
                if line.startswith("|---"):
                    rows = []
                    for row in lines[index + 1:]:
                        if not row.startswith("|"):
                            break
                        rows.append(row)
                    self.assertTrue(any(pr_quality_report.has_value(row) for row in rows), "\n".join(lines[index - 1:index + 3]))

    def test_partial_data_keeps_the_valued_side_and_row(self):
        report = self.render(base=snapshot(0, 0), head=snapshot())
        self.assertIn("| Test count | unavailable | 10 | n/a |", report)
        self.assertIn("<summary>Coverage and tests", report)
        self.assertIn("API contract coverage", report.split("No data", 1)[1])

    def test_full_data_report_keeps_all_its_sections_and_has_no_no_data_line_for_them(self):
        head = snapshot()
        head["tests"]["outcomes"] = {"api\tTestQuery": "pass", "api\tTestQuery/edge": "pass"}
        base = snapshot()
        base["tests"]["outcomes"] = {"api\tTestQuery": "pass"}
        report = self.render(base=base, head=head)
        for title in ("Coverage and tests", "API contract coverage and regression changes", "Synthetic handler microbenchmarks"):
            self.assertIn(f"<summary>{title}</summary>", report)
        if "No data" in report:
            self.assertNotIn("Coverage and tests", report.split("No data", 1)[1])

    def test_skipped_perf_is_listed_once_with_its_reason(self):
        head = snapshot()
        head["performance"] = {"mode": "skipped", "benchmarks": {}}
        report = self.render(head=head)
        self.assertNotIn("<summary>Synthetic handler microbenchmarks", report)
        self.assertIn("Synthetic handler microbenchmarks (skipped: no performance-sensitive paths changed)", report)

    def test_a_scope_row_with_only_a_note_is_kept_and_an_all_unavailable_one_is_not(self):
        scopes = [{"name": "ghost", "kind": "Go", "state": "unavailable"},
                  {"name": "broken", "kind": "Go", "state": "unavailable", "note": "job log truncated"}]
        evidence = {"sources": {"loki": {"state": "completed", "conclusion": "success", "test_scopes": scopes}}}
        report = self.render(evidence=evidence)
        self.assertIn("job log truncated", report)
        self.assertNotIn("loki / ghost", report)


if __name__ == "__main__":
    unittest.main()
