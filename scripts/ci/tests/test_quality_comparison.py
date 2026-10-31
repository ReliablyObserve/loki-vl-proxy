"""Compare existing evidence by exact case identity, without running workloads."""
import unittest

from scripts.ci.quality_comparison import analyze


def inventory(**changes):
    value = {"state": "available", "surface_version": "v3.7.7",
             "endpoints": {"total": 50, "implemented": 42, "differential": 30, "declared_proven": 25},
             "tracks": {"behaviours": {"proven": 8, "partial": 2, "gap": 1},
                        "translations": {"proven": 5, "partial": 1, "gap": 2}},
             "gaps": {"open": 3, "fixed": 10, "documented": 2, "owner-kept": 1, "upstream": 1},
             "error_layer": {"cells": 2, "links": 3}, "items": {}}
    value.update(changes)
    return value


class ComparisonTests(unittest.TestCase):
    def text(self, base, head):
        result = analyze(base, head)
        self.assertEqual(set(result), {"signals", "details"})
        return "\n".join(result["signals"]), "\n".join(result["details"])

    def test_exact_id_outcome_changes_and_top_level_subtests_remain_separate(self):
        base = {"tests": {"outcomes": {"pkg/a\tTestA": "pass", "pkg/a\tTestA/edge": "pass",
                 "pkg/b\tTestA": "pass", "pkg/a\tTestRemoved": "pass"}}}
        head = {"tests": {"outcomes": {"pkg/a\tTestA": "pass", "pkg/a\tTestA/edge": "fail",
                 "pkg/b\tTestA": "skip", "pkg/a\tTestAdded": "pass"}}}
        signals, details = self.text(base, head)
        for expected in ("newly failing", "newly skipped", "removed", "new passing"):
            self.assertIn(expected, signals)
        self.assertIn("pkg/a", details)
        self.assertIn("pkg/b", details)
        self.assertIn("top-level", details)
        self.assertIn("subtests", details)
        self.assertIn("TestA/edge", details)

    def test_absent_base_never_invents_new_passes_or_fixes(self):
        signals, details = self.text({}, {"tests": {"outcomes": {"p\tTestA": "pass", "p\tTestB/x": "fail"}}})
        self.assertNotIn("new passing", signals)
        self.assertNotIn("recovered", signals)
        self.assertIn("base unavailable", details.lower())
        self.assertIn("TestB/x", details)
        self.assertIn("current failing", signals)

    def test_missing_candidate_outcomes_cannot_be_called_removed(self):
        signals, details = self.text({"tests": {"outcomes": {"p\tTestA": "pass"}}}, {"tests": {"count": 20}})
        self.assertNotIn("removed", signals)
        self.assertIn("candidate unavailable", details.lower())

    def test_partial_candidate_does_not_remove_unobserved_cases_but_reports_new_failure(self):
        base = {"tests": {"outcomes": {"p\tTestUnfinished": "pass", "p\tTestA": "pass"}}}
        head = {"tests": {"execution_state": "failed", "test_exit_code": 42,
                          "outcomes": {"p\tTestA": "fail", "p\tTestNewFailure": "fail", "p\tTestNewPass": "pass"}}}
        signals, details = self.text(base, head)
        self.assertNotIn("removed", signals)
        self.assertNotIn("new passing", signals)
        self.assertIn("2 newly failing", signals)
        self.assertIn("Incomplete observed population", details)
        self.assertIn("candidate", details)
        self.assertIn("42", details)
        self.assertIn("observed outcomes only", details)

    def test_partial_base_does_not_infer_new_passes_or_new_failure_from_absence(self):
        base = {"tests": {"execution_state": "failed", "outcomes": {"p\tTestA": "pass"}}}
        head = {"tests": {"outcomes": {"p\tTestA": "pass", "p\tTestUnknown": "pass", "p\tTestUnknownFailure": "fail"}}}
        signals, details = self.text(base, head)
        self.assertNotIn("new passing", signals)
        self.assertNotIn("newly failing", signals)
        self.assertIn("current failing", signals)
        self.assertIn("base", details)

    def test_shared_observed_transitions_remain_comparable_after_failed_runs(self):
        base = {"tests": {"execution_state": "failed", "outcomes": {
            "p\tTestRecovered": "fail", "p\tTestBroken": "pass", "p\tTestSkipped": "pass"}}}
        head = {"tests": {"execution_state": "failed", "outcomes": {
            "p\tTestRecovered": "pass", "p\tTestBroken": "fail", "p\tTestSkipped": "skip"}}}
        signals, details = self.text(base, head)
        for label in ("1 recovered", "1 newly failing", "1 newly skipped"):
            self.assertIn(label, signals)
        self.assertIn("Incomplete observed population", details)

    def test_package_coverage_shows_statement_scope_and_point_drop(self):
        base = {"tests": {"package_coverage": {"pkg/cache": {"covered": 90, "total": 100, "pct": 90}}}}
        head = {"tests": {"package_coverage": {"pkg/cache": {"covered": 80, "total": 100, "pct": 80},
                                                               "pkg/new": {"covered": 3, "total": 4, "pct": 75}}}}
        signals, details = self.text(base, head)
        self.assertIn("10.00 percentage points", signals)
        self.assertIn("pkg/cache", signals)
        self.assertIn("80/100", details)
        self.assertIn("pkg/new", details)
        self.assertIn("unavailable", details)

    def test_inventory_counts_and_declared_state_regressions(self):
        base = inventory(items={"endpoint:query": {"state": "proven", "kind": "endpoint"},
                                "case:foo": {"state": "fixed", "kind": "case"}})
        head = inventory(gaps={"open": 4, "fixed": 9}, items={
            "endpoint:query": {"state": "partial", "kind": "endpoint"},
            "case:foo": {"state": "open", "kind": "case"}})
        signals, details = self.text({"quality_inventory": base}, {"quality_inventory": head})
        self.assertIn("Open declared gaps increased", signals)
        self.assertIn("degraded", signals)
        for expected in ("Endpoints", "behaviours", "translations", "error", "declared", "not full runtime compatibility", "case:foo"):
            self.assertIn(expected, details)

    def test_missing_inventory_is_unavailable_without_zero_deltas(self):
        signals, details = self.text({}, {"quality_inventory": inventory()})
        self.assertNotIn("increased", signals)
        self.assertIn("unavailable", details)
        self.assertNotIn("| 0 | 50 |", details)

    def test_lost_declared_proof_becomes_unknown_without_ranking_scope_decisions(self):
        base = inventory(items={"behaviour:a": {"state": "proven"}, "case:b": {"state": "documented"}})
        head = inventory(items={"behaviour:a": {"state": "unknown"}, "case:b": {"state": "upstream"}})
        signals, details = self.text({"quality_inventory": base}, {"quality_inventory": head})
        self.assertIn("states degraded: 1", signals)
        self.assertIn("behaviour:a", signals)
        self.assertNotIn("case:b", signals)
        self.assertIn("upstream", details)

    def test_inventory_revision_changes_are_visible(self):
        _, details = self.text({"quality_inventory": inventory(surface_version="v3.7.6")},
                               {"quality_inventory": inventory(surface_version="v3.7.7")})
        self.assertIn("v3.7.6", details)
        self.assertIn("v3.7.7", details)

    def test_declared_endpoint_proof_losses_and_error_leak_growth_are_actionable(self):
        base = inventory()
        head = inventory(endpoints={"total": 50, "implemented": 41, "differential": 29, "declared_proven": 24},
                         error_layer={"cells": 3, "links": 4})
        signals, _ = self.text({"quality_inventory": base}, {"quality_inventory": head})
        for label in ("implemented", "differential", "proven"):
            self.assertIn(label + " endpoints decreased", signals)
        self.assertIn("Known error-layer leak cells increased", signals)

    def test_positive_declared_inventory_trends_do_not_claim_runtime_improvement(self):
        base = inventory()
        head = inventory(endpoints={"total": 50, "implemented": 42, "differential": 30, "declared_proven": 26},
                         gaps={"open": 2})
        signals, _ = self.text({"quality_inventory": base}, {"quality_inventory": head})
        self.assertIn("proven endpoints increased", signals)
        self.assertIn("Open declared gaps decreased", signals)
        self.assertIn("inventory trend", signals)
        self.assertIn("not runtime proof", signals)

    def test_count_signals_flag_changed_loki_surface_scope(self):
        base = inventory(surface_version="v3.7.6")
        head = inventory(surface_version="v3.7.7", gaps={"open": 4},
                         endpoints={"total": 51, "implemented": 42, "differential": 30, "declared_proven": 24},
                         error_layer={"cells": 3, "links": 4})
        result = analyze({"quality_inventory": base}, {"quality_inventory": head})
        for signal in result["signals"]:
            self.assertIn("Loki surface changed", signal)
            self.assertIn("v3.7.6", signal)
            self.assertIn("v3.7.7", signal)
        self.assertIn("different declared inventories", "\n".join(result["details"]))

    def test_shown_row_count_is_a_paragraph_outside_gfm_table(self):
        _, details = self.text({"tests": {"outcomes": {}}}, {"tests": {"outcomes": {}}})
        self.assertIn("|\n\nShowing ", details)
        self.assertNotIn("|\nShowing ", details)

    def test_selected_smoke_components_show_candidate_backend_checks_only(self):
        base = {"compatibility": {"loki": {"components": {"labels": {"passed": 2, "total": 2, "pct": 100}}}}}
        head = {"compatibility": {"loki": {"components": {"labels": {"passed": 1, "total": 2, "pct": 50}}}}}
        signals, details = self.text(base, head)
        self.assertIn("labels", details)
        self.assertIn("1/2", details)
        self.assertIn("selected smoke", details.lower())
        self.assertNotIn("11/11", signals)
        smoke = details.split("Selected smoke component checks", 1)[1]
        self.assertIn("candidate backend", smoke)
        self.assertIn("A/B", smoke)
        self.assertIn("| Track | Component | PR passing |", smoke)
        self.assertNotIn("Base", smoke)
        self.assertNotIn("Change", smoke)
        self.assertNotIn("2/2", smoke)

    def test_old_base_smoke_results_do_not_imply_runtime_improvements(self):
        base = {"compatibility": {"loki": {"components": {
            "labels": {"passed": 0, "total": 2, "pct": 0},
            "base-only": {"passed": 0, "total": 3, "pct": 0}}}}}
        head = {"compatibility": {"loki": {"components": {"labels": {"passed": 2, "total": 2, "pct": 100}}}}}
        signals, details = self.text(base, head)
        smoke = details.split("Selected smoke component checks", 1)[1]
        self.assertNotIn("base-only", smoke)
        self.assertNotIn("0/2", smoke)
        self.assertNotIn("+100", smoke)
        self.assertNotIn("smoke", signals)

    def test_tables_are_bounded_and_failures_precede_new_passes(self):
        base = {"tests": {"outcomes": {}}}
        head = {"tests": {"outcomes": {**{f"p\tTestAdded{i:03}": "pass" for i in range(100)},
                                         "z\tTestFailure": "fail"}}}
        _, details = self.text(base, head)
        self.assertIn("TestFailure", details)
        self.assertNotIn("TestAdded099", details)
        self.assertIn("20/101", details)

    def test_untrusted_strings_are_escaped_in_signals_and_details(self):
        key = "pkg/@owner|<script>\tTestA/[bad]"
        signals, details = self.text({"tests": {"outcomes": {key: "pass"}}},
                                     {"tests": {"outcomes": {key: "fail"}}})
        for output in (signals, details):
            self.assertNotIn("<script>", output)
            self.assertNotIn("@owner", output)
        self.assertIn("&lt;script&gt;", signals + details)

    def test_malformed_and_nonfinite_structures_do_not_raise(self):
        malformed = [None, [], "bad", {"tests": []}, {"tests": {"outcomes": {"bad": [], "p\tA": "unknown"}}},
                     {"tests": {"package_coverage": {"x": {"covered": 10**400, "total": 1, "pct": float("inf")}}}},
                     {"quality_inventory": {"state": "available", "tracks": [], "items": {"x": []}}},
                     {"compatibility": {"loki": {"components": {"x": []}}}}]
        for base in malformed:
            for head in malformed:
                with self.subTest(base=type(base).__name__, head=type(head).__name__):
                    result = analyze(base, head)
                    self.assertTrue(all(isinstance(line, str) for key in ("signals", "details") for line in result[key]))


if __name__ == "__main__":
    unittest.main()
