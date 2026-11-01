import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import urllib.request
import zipfile
from pathlib import Path

PATH = Path(__file__).resolve().parents[1] / "pr_evidence.py"


def archive(files):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for name, value in files.items():
            z.writestr(name, value if isinstance(value, str) else json.dumps(value))
    return out.getvalue()


class EvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("pr_evidence", PATH)
        cls.m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.m)

    def test_archive_reads_selected_data_without_extracting_paths(self):
        payload = archive({"meta.json": {"head": "a"}, "../verdict.json": {"exit": 0}, "x.png": "ignored"})
        self.assertEqual(self.m.read_artifact(payload, {"meta.json", "verdict.json"}), {"meta.json": {"head": "a"}})

    def test_conformance_summary_deduplicates_registry_links_and_keeps_paths(self):
        commit = "a" * 40
        def row(outcome="pass", package="p", children=None):
            return {"test": "TestPipeline", "package": package, "outcome": outcome, "seconds": .1,
                    "commit": commit, "recorded": "2026-11-01T00:00:00Z", **({"subtests": children} if children else {})}
        children = [{"test": "TestPipeline/json/sum", "outcome": "pass", "seconds": .01},
                    {"test": "TestPipeline/json/max", "outcome": "skip", "seconds": 0}]
        doc = {"commit": commit, "recorded": "2026-11-01T00:00:00Z", "items": {
            "endpoint/query": [row(children=children)], "semantics/parser": [row(children=children)],
            "semantics/other": [row(package="other", outcome="incomplete", children=children[:1])]}}
        result = self.m.conformance_summary(doc, commit)
        self.assertEqual(result["parents"], {"pass": 1, "fail": 0, "skip": 0, "incomplete": 1, "not-run": 0})
        self.assertEqual(result["subtests"], {"pass": 2, "fail": 0, "skip": 1})
        child = next(r for r in result["cases"] if r["test"] == "TestPipeline/json/sum" and r["package"] == "p")
        self.assertEqual(child["items"], ["endpoint/query", "semantics/parser"])
        self.assertEqual(len(result["cases"]), 5)

    def test_conformance_rejects_item_id_amplification_before_aggregation(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 1,
               "commit": commit, "recorded": "now", "subtests": [
                   {"test": f"TestA/child-{i}", "outcome": "pass", "seconds": .1} for i in range(1000)]}
        for item in ("x" * 8192, "a" * 257, "bad/<script>", "bad/../id", "bad//id", "unicode/é"):
            with self.subTest(item=item[:40]), self.assertRaisesRegex(ValueError, "registry item"):
                self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {item: [row]}}, commit)

    def test_conformance_aggregation_limits_identities_and_item_links(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 1,
               "commit": commit, "recorded": "now", "subtests": [{"test": "TestA/child", "outcome": "pass", "seconds": .1}]}
        document = {"commit": commit, "recorded": "now", "items": {"a": [row], "b": [row]}}
        with mock.patch.object(self.m, "MAX_CONFORMANCE_IDENTITIES", 1), self.assertRaisesRegex(ValueError, "identities"):
            self.m.conformance_summary(document, commit)
        with mock.patch.object(self.m, "MAX_CONFORMANCE_LINKS", 2), self.assertRaisesRegex(ValueError, "links"):
            self.m.conformance_summary(document, commit)
        # Repeated identical links do not consume the distinct-link budget.
        with mock.patch.object(self.m, "MAX_CONFORMANCE_LINKS", 2):
            result = self.m.conformance_summary({**document, "items": {"a": [row, row]}}, commit)
        self.assertEqual(len(result["cases"]), 2)

    def test_conformance_byte_budget_accounts_for_json_escaping(self):
        commit = "a" * 40
        row = {"test": "Test" + "\u0000" * 300, "package": "p", "outcome": "pass", "seconds": 1,
               "commit": commit, "recorded": "now"}
        document = {"commit": commit, "recorded": "now", "items": {"a": [row]}}
        with mock.patch.object(self.m, "MAX_CONFORMANCE_SUMMARY", 2048), self.assertRaisesRegex(ValueError, "byte budget"):
            self.m.conformance_summary(document, commit)

    def test_conformance_aggregate_byte_budget_stops_before_final_serialization(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 1,
               "commit": commit, "recorded": "now", "subtests": [
                   {"test": f"TestA/child-{i}", "outcome": "pass", "seconds": .1} for i in range(100)]}
        encode = json.dumps
        with mock.patch.object(self.m, "MAX_CONFORMANCE_SUMMARY", 4096), mock.patch.object(self.m.json, "dumps", wraps=encode) as serialized:
            with self.assertRaisesRegex(ValueError, "byte budget"):
                self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {"a": [row]}}, commit)
        self.assertFalse(any(isinstance(call.args[0], dict) and "cases" in call.args[0] for call in serialized.call_args_list))

    def test_conformance_accepts_bounded_existing_id_syntax(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 1, "commit": commit, "recorded": "now"}
        result = self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {
            "versions/stats-bucket-label-v1.45": [row], "x" * 256: [row]}}, commit)
        self.assertEqual(result["parents"]["pass"], 1)
        self.assertEqual(len(result["cases"][0]["items"]), 2)

    def test_conformance_metadata_cannot_bypass_output_budget(self):
        commit = "a" * 40
        for recorded in ("x" * 129, {"x": "truthy"}):
            with self.subTest(recorded=recorded), self.assertRaises(ValueError):
                self.m.conformance_summary({"commit": commit, "recorded": recorded, "items": {}}, commit)

    def test_conformance_failed_child_overrides_pass_and_rerun(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 1,
               "commit": commit, "recorded": "now", "subtests": [{"test": "TestA/json/sum", "outcome": "fail", "seconds": .1}]}
        rerun = {**row, "subtests": [{"test": "TestA/json/sum", "outcome": "pass", "seconds": .1}]}
        result = self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {"a": [row], "b": [rerun]}}, commit)
        self.assertEqual(result["parents"]["fail"], 1)
        self.assertEqual(result["subtests"]["fail"], 1)

    def test_conformance_missing_parent_and_not_run_are_distinct(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "not-run", "seconds": 0,
               "commit": commit, "recorded": "now"}
        result = self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {
            "a": [row], "b": [{**row, "test": "TestB", "subtests": [{"test": "TestB/child", "outcome": "pass", "seconds": .1}]}]}}, commit)
        self.assertEqual((result["parents"]["incomplete"], result["parents"]["not-run"]), (1, 1))

    def test_conformance_rejects_stale_rows_and_unrelated_child_identity(self):
        commit = "a" * 40
        row = {"test": "TestA", "package": "p", "outcome": "pass", "seconds": 0,
               "commit": commit, "recorded": "now"}
        for changed in ({**row, "commit": "b" * 40}, {**row, "subtests": [{"test": "TestOther/child", "outcome": "pass", "seconds": 0}]},
                        {**row, "seconds": float("inf")}, {**row, "outcome": "success"}):
            with self.subTest(row=changed), self.assertRaises(ValueError):
                self.m.conformance_summary({"commit": commit, "recorded": "now", "items": {"a": [changed]}}, commit)

    def test_conformance_ci_discards_checked_in_evidence_before_generation(self):
        workflow = (PATH.parents[2] / ".github/workflows/ci.yaml").read_text()
        job = re.split(r"\n  [A-Za-z0-9_-]+:", workflow.split("  conformance:", 1)[1], maxsplit=1)[0]
        self.assertLess(job.index("rm -f conformance/registry/generated/evidence.json"), job.index("python3 scripts/ci/check_conformance.py"))
        self.assertIn('if [ -f conformance/registry/generated/evidence.json ]; then', job)
        self.assertIn('name: conformance-evidence', job)
        self.assertIn("steps.conformance_gate.outcome == 'success' || steps.conformance_gate.outcome == 'failure'", job)

    def test_conformance_large_json_has_a_specific_bounded_allowance(self):
        payload = archive({"conformance-evidence.json": {"padding": "x" * (2 * 1024 * 1024)}})
        self.assertIn("conformance-evidence.json", self.m.read_artifact(payload, {"conformance-evidence.json"}))
        with self.assertRaises(ValueError):
            self.m.read_artifact(archive({"conformance-evidence.json": "x" * (12 * 1024 * 1024 + 1)}), {"conformance-evidence.json"})

    def test_conformance_artifact_requires_current_meta_and_collector_commit(self):
        head, merge, base = "a" * 40, "b" * 40, "c" * 40
        pr = {"number": 4, "base": {"sha": base}, "head": {"sha": head}, "merge_commit_sha": merge}

        class API:
            repository = "owner/repo"
            artifact_head = merge
            evidence_commit = merge

            def pages(self, path, field=None, max_pages=10):
                if "workflows/" in path:
                    return [{"id": 10, "head_sha": head, "status": "completed", "conclusion": "failure", "run_started_at": "2026-01-01"}] if "/ci.yaml/" in path else []
                if "/jobs" in path:
                    return [{"id": 1, "name": "conformance", "status": "completed", "conclusion": "failure"}]
                return [{"id": 11, "name": "conformance-evidence", "created_at": "2026-01-02"}]

            def request(self, path, **kwargs):
                return archive({"conformance-meta.json": {"base": base, "head": self.artifact_head},
                    "conformance-evidence.json": {"commit": self.evidence_commit, "recorded": "now", "items": {"endpoint/query": [
                        {"package": "p", "test": "TestA", "outcome": "fail", "seconds": 1, "commit": self.evidence_commit, "recorded": "now",
                         "subtests": [{"test": "TestA/json/sum", "outcome": "fail", "seconds": .1}]}]}}})

        api = API()
        source = self.m.collect(api, pr)["sources"]["ci"]
        summary = source["artifacts"]["ci-conformance-summary.json"]
        self.assertEqual((summary["parents"]["fail"], summary["subtests"]["fail"]), (1, 1))
        self.assertEqual(source["conclusion"], "failure")
        api.artifact_head = "d" * 40
        source = self.m.collect(api, pr)["sources"]["ci"]
        self.assertNotIn("ci-conformance-summary.json", source["artifacts"])
        self.assertIn("stale source identity", source["note"])
        api.artifact_head = merge
        api.evidence_commit = "d" * 40
        source = self.m.collect(api, pr)["sources"]["ci"]
        self.assertEqual(source["state"], "error")
        self.assertFalse(source["artifacts"])

    def test_duplicate_names_and_oversized_json_are_rejected(self):
        for files in ({"a/meta.json": {}, "b/meta.json": {}}, {"meta.json": "x" * (2 * 1024 * 1024 + 1)}):
            with self.subTest(files=list(files)), self.assertRaises(ValueError):
                self.m.read_artifact(archive(files), {"meta.json"})

    def test_invalid_nonfinite_and_wrong_type_json_are_rejected(self):
        for value in ("{bad}", "{\"x\":NaN}", "[]"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.m.read_artifact(archive({"meta.json": value}), {"meta.json"})

    def test_visual_verdict_survives_unused_comparison_array(self):
        pr = {"number": 4, "state": "open", "base": {"sha": "b"}, "head": {"sha": "h"}, "merge_commit_sha": "m"}
        payload = archive({"meta.json": {"base": "b", "head": "h"}, "plan.json": {"captures": []},
                           "compare.json": [{"capture": "example", "state": "same"}],
                           "verdict.json": {"captures": 37, "failures": [], "missing": [], "warnings": 3, "improved": 0}})

        class API:
            repository = "owner/repo"

            def pages(self, path, field=None, max_pages=10):
                if "workflows/" in path:
                    return [{"id": 10, "head_sha": "h", "status": "completed", "conclusion": "success", "run_started_at": "2026-01-01"}] if "visual-smoke" in path else []
                if "/jobs" in path:
                    return [{"id": 1, "name": "visual-smoke", "status": "completed", "conclusion": "success"}]
                return [{"id": 11, "name": "visual-quality-evidence", "created_at": "2026-01-02"}]

            def request(self, path, **kwargs):
                return pr if path == "pulls/4" else payload

        api = API()
        evidence = self.m.collect(api, pr)
        source = evidence["sources"]["visual"]
        self.assertEqual(source["state"], "completed")
        self.assertNotIn("compare.json", source["artifacts"])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "report.md"
            sys.path.insert(0, str(PATH.parent))
            try:
                self.assertTrue(self.m.publish(api, pr, evidence, str(output), dry_run=True))
            finally:
                sys.path.pop(0)
            self.assertIn("Visual proof: 37 captures, 0 failures, 0 missing, 0 improved, 3 warnings", output.read_text())
            self.assertNotIn("Evidence could not be read safely", output.read_text())

    def test_current_merge_and_pr_head_have_different_valid_provenance(self):
        context = {"base": "b", "head": "h", "merge": "m"}
        self.assertTrue(self.m.current_meta({"base": "b", "head": "m"}, context, "perf"))
        self.assertTrue(self.m.current_meta({"base": "b", "head": "h"}, context, "visual"))
        self.assertFalse(self.m.current_meta({"base": "old", "head": "m"}, context, "perf"))
        self.assertFalse(self.m.current_meta({"base": "b", "head": "old"}, context, "visual"))
        self.assertFalse(self.m.current_meta({}, context, "quality"))

    def test_handler_benchmarks_use_medians_and_keep_cpu_variants_separate(self):
        text = "\n".join([
            "BenchmarkProxy_Series_CompatCacheHit-8 100 1000 ns/op 20 B/op 2 allocs/op",
            "BenchmarkProxy_Series_CompatCacheHit-8 100 9000 ns/op 90 B/op 9 allocs/op",
            "BenchmarkProxy_Series_CompatCacheHit-8 100 2000 ns/op 30 B/op 3 allocs/op",
            "BenchmarkProxy_Series_CompatCacheHit-1 100 500 ns/op 10 B/op 1 allocs/op",
            "BenchmarkProxy_LabelValues_Scale_CacheHit/values_10000-8 100 7000 ns/op 60 B/op 5 allocs/op",
            "BenchmarkTranslate-8 100 1 ns/op 1 B/op 1 allocs/op"])
        rows = self.m.handler_benchmarks(text)
        self.assertEqual(len(rows), 3)
        row = next(r for r in rows if r["name"] == "BenchmarkProxy_Series_CompatCacheHit" and r["cpu"] == 8)
        self.assertEqual((row["samples"], row["ns_per_op"], row["bytes_per_op"], row["allocs_per_op"]), (3, 2000, 30, 3))

    def test_handler_benchmarks_reject_invalid_samples(self):
        for value in ("NaN", "inf", "-1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.m.handler_benchmarks(f"BenchmarkProxy_Labels_CacheHit-8 100 {value} ns/op 20 B/op 2 allocs/op")
        with self.assertRaises(ValueError):
            self.m.handler_benchmarks("BenchmarkProxy_Labels_CacheHit-8 100 1 ns/op")

    def test_ci_handler_benchmarks_require_current_checkout(self):
        head, merge, base = "a" * 40, "b" * 40, "c" * 40
        pr = {"number": 4, "base": {"sha": base}, "head": {"sha": head}, "merge_commit_sha": merge}
        payload = archive({"bench-results.txt": "BenchmarkProxy_Series_CompatCacheHit-8 100 2000 ns/op 30 B/op 3 allocs/op"})

        class API:
            repository = "owner/repo"
            checkout = merge

            def pages(self, path, field=None, max_pages=10):
                if "workflows/" in path:
                    return [{"id": 10, "head_sha": head, "status": "completed", "conclusion": "success", "run_started_at": "2026-01-01"}] if "/ci.yaml/" in path else []
                if "/jobs" in path:
                    return [{"id": 1, "name": "bench", "status": "completed", "conclusion": "success"}]
                return [{"id": 11, "name": "benchmarks", "created_at": "2026-01-02"}]

            def request(self, path, **kwargs):
                if "/logs" in path:
                    return ("2026-01-01T00:00:00Z [command]/usr/bin/git log -1 --format=%H\n2026-01-01T00:00:00Z " + self.checkout).encode()
                return payload

        api = API()
        result = self.m.collect(api, pr)["sources"]["ci"]["artifacts"]["ci-handler-benchmarks.json"]
        self.assertEqual(result["source"], merge)
        self.assertEqual(result["cases"][0]["samples"], 1)
        api.checkout = "d" * 40
        result = self.m.collect(api, pr)["sources"]["ci"]["artifacts"]["ci-handler-benchmarks.json"]
        self.assertEqual(result["state"], "unavailable")
        self.assertNotIn("cases", result)

    def test_coverage_weights_statements_and_deduplicates_atomic_blocks(self):
        profile = "mode: atomic\na.go:1.1,2.2 10 1\nb.go:1.1,2.2 2 0\na.go:1.1,2.2 10 2\n"
        self.assertAlmostEqual(self.m.coverage_pct(profile), 1000 / 12)
        self.assertIsNone(self.m.coverage_pct("mode: atomic\n"))

    def test_unit_results_do_not_equate_subtests_with_top_level_tests(self):
        events = [{"Action": a, "Package": "internal/cache", "Test": t} for a, t in
                  [("pass", "TestA"), ("pass", "TestA/edge"), ("skip", "TestB"), ("fail", "TestC")]]
        summary = self.m.unit_summary("\n".join(map(json.dumps, events)))
        self.assertEqual(summary["passed"], 2)
        self.assertEqual(summary["top_level_passed"], 1)
        self.assertEqual(summary["subtests_passed"], 1)
        self.assertEqual(summary["failed_names"], ["internal/cache:TestC"])
        self.assertEqual(summary["skipped"], 1)

    def test_full_named_outcomes_preserve_skipped_children_and_failed_repeats(self):
        events = [{"Action": a, "Package": p, "Test": t} for a, p, t in
                  [("skip", "a", "TestA/edge"), ("pass", "a", "TestA"),
                   ("fail", "b", "TestA"), ("pass", "b", "TestA")]]
        summary = self.m.unit_summary("\n".join(map(json.dumps, events)))
        self.assertEqual(summary["outcomes"], {"a\tTestA/edge": "skip", "a\tTestA": "pass", "b\tTestA": "fail"})

    def test_package_coverage_is_weighted_by_statement_blocks(self):
        text = "mode: atomic\np/a/f.go:1.1,2.1 9 1\np/a/g.go:1.1,2.1 1 0\np/b/h.go:1.1,2.1 2 0\n"
        self.assertEqual(self.m.package_coverage(text), {"p/a": {"covered": 9, "total": 10, "pct": 90}, "p/b": {"covered": 0, "total": 2, "pct": 0}})

    def test_existing_api_log_results_are_source_verified_and_deduplicated(self):
        log = "\n".join("2026-10-31T20:00:00Z " + line for line in [
            "[command]/usr/bin/git log -1 --format=%H", "a" * 40,
            "  LOKI_IMAGE: grafana/loki:3.7.9", "--- PASS: TestQuery (0.01s)",
            "    --- SKIP: TestQuery/known (0.00s)", "--- PASS: TestQuery (0.01s)",
            "--- FAIL: TestLimits (0.01s)", "--- PASS: TestLimits (0.01s)"])
        result = self.m.log_scope(log, {"head": "b" * 40, "merge": "a" * 40})
        self.assertEqual(result["state"], "available")
        self.assertEqual((result["passed"], result["failed"], result["skipped"]), (1, 1, 1))
        self.assertEqual(result["versions"], {"LOKI_IMAGE": ["grafana/loki:3.7.9"]})
        self.assertEqual(self.m.log_scope(log, {"head": "c" * 40})["state"], "stale")

    def test_empty_or_unfinished_logs_are_not_zero_test_success(self):
        self.assertEqual(self.m.log_scope("setup only", {"head": "a" * 40})["state"], "unavailable")

    def test_multiple_playwright_commands_keep_separate_results_and_instability(self):
        lines = ["[command]/usr/bin/git log -1 --format=%H", "a" * 40, "Running 14 tests using 2 workers", "  10 passed (1m)", "  3 failed", "  1 flaky", "Running 9 tests using 1 worker", "  7 passed (20s)", "  1 interrupted", "  1 did not run"]
        result = self.m.log_scope("\n".join(lines), {"head": "a" * 40})
        self.assertNotIn("passed", result)
        self.assertEqual(result["runs"], [{"passed": 10, "failed": 3, "skipped": 0, "flaky": 1, "interrupted": 0, "not_run": 0}, {"passed": 7, "failed": 0, "skipped": 0, "flaky": 0, "interrupted": 1, "not_run": 1}])

    def test_latest_attempt_is_selected_and_unrelated_heads_ignored(self):
        runs = [{"id": 1, "head_sha": "h", "run_attempt": 1}, {"id": 2, "head_sha": "old"},
                {"id": 3, "head_sha": "h", "run_attempt": 2}]
        self.assertEqual(self.m.latest_run(runs, "h")["id"], 3)
        self.assertIsNone(self.m.latest_run(runs, "unknown"))

    def test_all_collected_workflows_refresh_the_summary(self):
        root = PATH.parents[2]
        summary = (root / ".github/workflows/pr-evidence-summary.yaml").read_text()
        triggers = summary.split("workflows: [", 1)[1].split("]", 1)[0].split(",")
        triggers = {name.strip() for name in triggers}
        expected = {(root / ".github/workflows" / filename).read_text().splitlines()[0].split("name:", 1)[1].strip() for filename in self.m.WORKFLOWS.values()}
        self.assertEqual(triggers, expected)

    def test_collector_uses_current_artifacts_and_exposes_pending_jobs(self):
        class API:
            repository = "owner/repo"

            def pages(self, path, field=None, max_pages=10):
                if "workflows/" in path:
                    if "pr-quality-report" in path:
                        return [{"id": 10, "head_sha": "h", "status": "completed", "conclusion": "success", "run_started_at": "2026-01-01"}]
                    if "perf-ab" in path:
                        return [{"id": 20, "head_sha": "h", "status": "in_progress", "conclusion": None}]
                    return []
                if "/jobs" in path:
                    return [{"id": 1, "name": "report", "status": "completed", "conclusion": "success"}]
                return [{"id": 11, "name": "pr-quality-report", "created_at": "2026-01-02"}]

            def request(self, path, **kwargs):
                return archive({"quality-meta.json": {"base": "b", "head": "m"},
                                "pr-base-quality.json": {"tests": {"count": 10}},
                                "pr-head-quality.json": {"tests": {"count": 11}}})

        pr = {"number": 4, "base": {"sha": "b"}, "head": {"sha": "h"}, "merge_commit_sha": "m"}
        evidence = self.m.collect(API(), pr)
        self.assertEqual(evidence["sources"]["quality"]["artifacts"]["pr-head-quality.json"]["tests"]["count"], 11)
        self.assertEqual(evidence["sources"]["perf"]["state"], "pending")
        self.assertIn("/attempts/1", evidence["sources"]["quality"]["url"])

    def test_old_attempt_and_changed_base_are_not_reused(self):
        class API:
            repository = "owner/repo"
            reads = 0

            def pages(self, path, field=None, max_pages=10):
                if "workflows/" in path:
                    return [{"id": 1, "head_sha": "h", "status": "completed", "conclusion": "success", "run_started_at": "2026-02-01"}]
                if "/jobs" in path:
                    return []
                return [{"id": 1, "name": "perf-ab-smoke", "created_at": "2026-01-01"},
                        {"id": 2, "name": "pr-quality-report", "created_at": "2026-03-01"}]

            def request(self, path, **kwargs):
                self.reads += 1
                return archive({"quality-meta.json": {"base": "old", "head": "m"}, "pr-head-quality.json": {}})

        api = API()
        pr = {"number": 4, "base": {"sha": "b"}, "head": {"sha": "h"}, "merge_commit_sha": "m"}
        evidence = self.m.collect(api, pr)
        self.assertEqual(evidence["sources"]["quality"]["artifacts"], {})
        self.assertEqual(evidence["sources"]["perf"]["artifacts"], {})
        self.assertEqual(api.reads, 1)

    def test_publication_does_not_write_after_head_changes(self):
        sys.path.insert(0, str(PATH.parent))
        class API:
            def request(self, path, *args):
                self_path = path
                if self_path != "pulls/4":
                    raise AssertionError("unexpected write")
                return {"state": "open", "base": {"sha": "b"}, "head": {"sha": "new"}}

        pr = {"number": 4, "base": {"sha": "b"}, "head": {"sha": "h"}}
        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(self.m.publish(API(), pr, {"sources": {"quality": {}}}, str(Path(tmp) / "out.md")))

    def test_malformed_nested_payload_is_omitted_without_hiding_failed_checks(self):
        sys.path.insert(0, str(PATH.parent))
        pr = {"number": 4, "state": "open", "base": {"sha": "b"}, "head": {"sha": "h"}}
        class API:
            def request(self, path, *args):
                return pr
        evidence = {"sources": {"quality": {"state": "completed", "conclusion": "failure", "artifacts": {"pr-head-quality.json": {"tests": []}}},
                                "ci": {"state": "completed", "conclusion": "failure", "jobs": [{"name": "test", "conclusion": "failure"}], "artifacts": {}}}}
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "report.md"
            self.assertTrue(self.m.publish(API(), pr, evidence, str(output), dry_run=True))
            self.assertIn("Malformed evidence omitted", output.read_text())
            self.assertIn("failure", output.read_text())

    def test_redirect_strips_credentials_and_rejects_external_hosts(self):
        redirect = self.m.SafeRedirect()
        request = urllib.request.Request("https://api.github.com/repos/o/r/artifact", headers={"Authorization": "Bearer example"})
        target = redirect.redirect_request(request, None, 302, "", {}, "https://productionresultssa0.blob.core.windows.net/data")
        self.assertIsNone(target.get_header("Authorization"))
        for url in ("http://api.github.com/plain", "https://evil.invalid/data", "https://api.github.com.evil.invalid/data"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                redirect.redirect_request(request, None, 302, "", {}, url)

    def test_provenance_cli_works_with_shallow_checkout(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo, clone = Path(tmp) / "repo", Path(tmp) / "clone"
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            for name in ("first", "second"):
                subprocess.run(["git", "-C", str(repo), "-c", "commit.gpgsign=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-qm", name], check=True)
            subprocess.run(["git", "clone", "-q", "--depth=1", repo.as_uri(), str(clone)], check=True)
            output = Path(tmp) / "meta.json"
            subprocess.run([sys.executable, str(PATH), "--provenance", str(output)], cwd=clone, check=True)
            meta = json.loads(output.read_text())
            self.assertEqual(len(meta["base"]), 40)
            self.assertNotEqual(meta["base"], meta["head"])


if __name__ == "__main__":
    unittest.main()
