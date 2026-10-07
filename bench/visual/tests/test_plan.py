"""Unit tests for bench/visual/plan.py (python3 -m unittest discover -s bench/visual/tests)."""
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import plan  # noqa: E402

SPEC = {
    "ranges": {"15m": 900, "1h": 3600, "6h": 21600, "live": 300},
    "ci_ranges": ["15m", "1h", "6h"],
    "core_range": "1h",
    "pages": [
        {"id": "core-logs", "kind": "explore", "core": True, "covers": ["loki_api_v1_query_range"]},
        {"id": "json-volume", "kind": "explore", "covers": ["parser-json", "loki_api_v1_query_range"]},
        {"id": "fix-page", "kind": "explore", "covers": ["case-only"], "fixes": ["semantics/case-a"]},
        {"id": "fix-page-2", "kind": "explore", "covers": ["case-only"], "fixes": ["semantics/case-a", "semantics/case-b"]},
        {"id": "labels", "kind": "label-browser", "covers": ["loki_api_v1_labels"]},
        {"id": "tail", "kind": "tail", "covers": ["loki_api_v1_tail"]},
    ],
}
ITEMS = {
    "loki_api_v1_query_range": ({"internal/proxy/proxy.go"}, set()),
    "loki_api_v1_labels": ({"internal/proxy/label_handlers.go"}, set()),
    "loki_api_v1_tail": ({"internal/proxy/tail.go"}, set()),
    "parser-json": ({"internal/proxy/ordered_json_metric.go"}, set()),
    "case-only": ({"internal/proxy/case_only.go"}, set()),
}


GAPS = {
    "semantics/case-a": {"status": "fixed", "impact": "explore-visible"},
    "semantics/case-b": {"status": "fixed", "impact": "drilldown-visible"},
    "semantics/case-c": {"status": "fixed", "impact": "explore-visible", "visual": "none", "visual_reason": "api-only"},
}


def run(files, **kw):
    return plan.plan(files, spec=SPEC, items=ITEMS, **kw)


class PlanTest(unittest.TestCase):
    def test_docs_ci_tests_run_nothing(self):
        out = run(["docs/testing.md", ".github/workflows/ci.yaml", "internal/proxy/x_test.go", "CHANGELOG.md"])
        self.assertFalse(out["run"])
        self.assertEqual(out["entries"], {})

    def test_tooling_change_runs_the_core_set_only(self):
        out = run(["bench/visual/compare.py"])
        self.assertTrue(out["run"])
        self.assertEqual({k: v["ranges"] for k, v in out["entries"].items()}, {"core-logs": ["1h"]})
        self.assertEqual(out["captures"], 1)

    def test_code_selects_the_entries_that_cover_it(self):
        out = run(["internal/proxy/ordered_json_metric.go"])
        self.assertEqual(out["entries"]["json-volume"]["ranges"], ["15m", "1h", "6h"])
        self.assertEqual(out["entries"]["core-logs"]["ranges"], ["1h"])  # core stays at one range
        self.assertNotIn("labels", out["entries"])
        self.assertEqual(out["entries"]["json-volume"]["why"], ["internal/proxy/ordered_json_metric.go"])

    def test_endpoint_handler_selects_its_entries_but_query_range_does_not_select_all(self):
        self.assertIn("labels", run(["internal/proxy/label_handlers.go"])["entries"])
        out = run(["internal/proxy/proxy.go"])  # the query_range handler: every page reads it
        self.assertEqual(list(out["entries"]), ["core-logs"])

    def test_tail_entry_is_one_live_capture(self):
        out = run(["internal/proxy/tail.go"])
        self.assertEqual(out["entries"]["tail"]["ranges"], ["live"])
        self.assertEqual(out["captures"], 2)  # core 1h + the live pass

    def test_core_set_is_never_trimmed_and_budget_narrows_then_drops(self):
        out = run(["internal/proxy/ordered_json_metric.go", "internal/proxy/label_handlers.go"], max_captures=4)
        self.assertEqual(out["entries"]["core-logs"]["ranges"], ["1h"])
        self.assertEqual(set(out["trimmed"]), {"json-volume", "labels"})
        self.assertEqual(out["dropped"], [])
        out = run(["internal/proxy/ordered_json_metric.go", "internal/proxy/label_handlers.go"], max_captures=2)
        self.assertEqual(out["dropped"], ["labels"])
        self.assertIn("core-logs", out["entries"])

    def test_fix_proof_is_selected_first_at_all_ranges_when_a_case_changes(self):
        out = run(["conformance/registry/cases/semantics/case-a.yaml"], cases=["semantics/case-a"], case_gaps=GAPS)
        self.assertTrue(out["run"])  # a registry-only change runs nothing else, but a fix proof runs
        self.assertEqual(list(out["entries"])[:2], ["fix-page", "fix-page-2"])
        self.assertEqual(out["entries"]["fix-page"]["ranges"], ["15m", "1h"])  # fix proof: ranges Loki holds
        self.assertEqual(out["entries"]["fix-page"]["why"], ["fix proof: semantics/case-a"])
        self.assertEqual(out["entries"]["fix-page"]["fixes"], ["semantics/case-a"])
        self.assertEqual(out["fix_cases"]["semantics/case-a"]["entries"], ["fix-page", "fix-page-2"])

    def test_untouched_cases_are_not_selected(self):
        out = run(["conformance/registry/cases/semantics/case-b.yaml"], cases=["semantics/case-b"], case_gaps=GAPS)
        self.assertEqual(list(out["entries"]), ["fix-page-2", "core-logs"])  # fix proof first, then the core set
        self.assertNotIn("fix-page", out["entries"])
        out = run(["internal/proxy/ordered_json_metric.go"])  # code change, no case: pages are selected by code only
        self.assertEqual(out["fix_cases"], {})
        self.assertNotIn("fix-proof", " ".join(w for e in out["entries"].values() for w in e["why"]))

    def test_fix_proof_is_never_trimmed_or_dropped(self):
        files = ["internal/proxy/ordered_json_metric.go", "internal/proxy/label_handlers.go"]
        out = run(files, cases=["semantics/case-a"], case_gaps=GAPS, max_captures=4)
        self.assertEqual(out["entries"]["fix-page"]["ranges"], ["15m", "1h"])  # fix proof: ranges Loki holds
        self.assertNotIn("fix-page", out["trimmed"])
        out = run(files, cases=["semantics/case-a"], case_gaps=GAPS, max_captures=1)
        self.assertIn("fix-page-2", out["entries"])
        self.assertNotIn("fix-page", out["dropped"])
        self.assertEqual(out["entries"]["core-logs"]["ranges"], ["1h"])

    def test_new_open_case_without_an_entry_is_not_reported(self):
        gaps = {"semantics/case-z": {"status": "open", "impact": "explore-visible"}}
        out = run(["conformance/registry/cases/semantics/case-z.yaml"], cases=["semantics/case-z"], case_gaps=gaps)
        self.assertEqual(out["fix_cases"], {})
        self.assertFalse(out["run"])

    def test_exempt_or_unlisted_case_is_reported_without_entries(self):
        out = run(["conformance/registry/cases/semantics/case-c.yaml"], cases=["semantics/case-c"], case_gaps=GAPS)
        self.assertFalse(out["run"])
        self.assertEqual(out["fix_cases"]["semantics/case-c"]["exempt"], "api-only")

    def test_check_resolves_covers(self):
        self.assertEqual(plan.check(SPEC, ITEMS, GAPS), [])
        bad = {**SPEC, "pages": SPEC["pages"] + [{"id": "x", "kind": "explore", "covers": ["nope"]},
                                                   {"id": "y", "kind": "explore", "covers": ["loki_api_v1_query_range"]},
                                                   {"id": "z", "kind": "explore"}]}
        problems = "\n".join(plan.check(bad, ITEMS, {}))
        self.assertIn("x covers unknown registry id nope", problems)
        self.assertIn("y covers no registry item that names code", problems)
        self.assertIn("z has no covers", problems)

    def test_check_requires_a_fix_proof_or_an_exemption(self):
        gaps = {**GAPS, "semantics/case-d": {"status": "fixed", "impact": "explore-visible"},
                "semantics/case-e": {"status": "fixed", "impact": "api-only"},
                "semantics/case-f": {"status": "open", "impact": "explore-visible"},
                "semantics/case-g": {"status": "fixed", "impact": "explore-visible", "visual": "none"}}
        problems = "\n".join(plan.check(SPEC, ITEMS, gaps))
        self.assertIn("semantics/case-d is fixed with impact explore-visible but no", problems)
        self.assertIn("semantics/case-g has `visual: none` without", problems)
        self.assertNotIn("case-a is fixed", problems)  # listed in fixes
        self.assertNotIn("case-c", problems)           # exempt with a reason
        self.assertNotIn("case-e", problems)           # api-only: nothing to see
        self.assertNotIn("case-f", problems)           # not fixed yet
        bad = {**SPEC, "pages": SPEC["pages"] + [{"id": "q", "kind": "explore", "covers": ["case-only"], "fixes": ["nope"]}]}
        self.assertIn("q fixes unknown registry case nope", "\n".join(plan.check(bad, ITEMS, GAPS)))

    def test_case_gap_reads_the_gap_block(self):
        text = ("id: x/y\ntitle: t\ngap:\n  status: fixed\n  impact: explore-visible\n  visual: none\n"
                "  visual_reason: 'it''s no panel'\n  area: a\n")
        self.assertEqual(plan.case_gap(text), {"status": "fixed", "impact": "explore-visible", "visual": "none",
                                               "visual_reason": "it's no panel"})
        self.assertEqual(plan.case_gap("id: x\n"), {})

    def test_grafana_datasource_impact_needs_a_proof_or_an_exemption(self):
        gaps = {"semantics/case-h": {"status": "fixed", "impact": "grafana-datasource"}}
        self.assertIn("case-h is fixed", "\n".join(plan.check(SPEC, ITEMS, gaps)))

    def test_fix_captures_beyond_the_budget_narrow_to_the_core_range_and_are_never_dropped(self):
        spec = {**SPEC, "pages": SPEC["pages"] + [
            {"id": f"extra-{n}", "kind": "explore", "covers": ["case-only"], "fixes": ["semantics/case-a"]} for n in range(10)]}
        out = plan.plan(["conformance/registry/cases/semantics/case-a.yaml"], spec=spec, items=ITEMS,
                        cases=["semantics/case-a"], case_gaps=GAPS, max_captures=1)
        self.assertEqual(len(out["entries"]) - 1, 12)  # every fix entry still there, plus the core set
        # a fix-only entry is captured at the ranges Loki holds (15m, 1h): 9 entries fill the 18-capture budget
        full = [p for p, e in out["entries"].items() if e.get("fixes") and e["ranges"] == ["15m", "1h"]]
        self.assertEqual(len(full), 9)
        self.assertEqual(len(out["fix_trimmed"]), 3)
        self.assertEqual(out["dropped"], [])


def git(repo, *a):
    return subprocess.run(["git", "-C", repo, *a], check=True, capture_output=True, text=True).stdout.strip()


class ChangedCasesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = self.tmp.name
        git(self.repo, "init", "-q", "-b", "main")
        git(self.repo, "config", "user.email", "t@example.test")
        git(self.repo, "config", "user.name", "t")
        git(self.repo, "config", "commit.gpgsign", "false")
        self.write("a/open-one", "open")
        self.write("a/fixed-one", "fixed", extra="  area: 'x'\n")
        self.write("a/rename-me", "open")
        self.write("a/delete-me", "open")
        self.commit("base")
        git(self.repo, "checkout", "-q", "-b", "pr")

    def tearDown(self):
        self.tmp.cleanup()

    def path(self, cid):
        return os.path.join(self.repo, "conformance/registry/cases", cid + ".yaml")

    def write(self, cid, status, extra=""):
        os.makedirs(os.path.dirname(self.path(cid)), exist_ok=True)
        with open(self.path(cid), "w") as f:
            f.write(f"id: {cid}\ngap:\n  status: {status}\n  impact: explore-visible\n{extra}")

    def commit(self, msg):
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", msg)

    def changed(self):
        return sorted(plan.changed_cases("main", "HEAD", root=self.repo))

    def test_new_case_and_open_to_fixed_trigger(self):
        self.write("a/new-one", "open")
        self.write("a/open-one", "fixed")
        self.commit("pr")
        self.assertEqual(self.changed(), ["a/new-one", "a/open-one"])

    def test_edit_of_a_fixed_case_in_another_field_and_a_delete_trigger_nothing(self):
        self.write("a/fixed-one", "fixed", extra="  area: 'changed'\n")
        os.remove(self.path("a/delete-me"))
        self.commit("pr")
        self.assertEqual(self.changed(), [])

    def test_rename_follows_the_old_path(self):
        os.rename(self.path("a/rename-me"), self.path("a/renamed"))
        self.commit("rename only")
        self.assertEqual(self.changed(), [])
        self.write("a/renamed", "fixed")
        self.commit("and fixed")
        self.assertEqual(self.changed(), ["a/renamed"])

    def test_reads_head_from_git_not_the_working_tree_and_uses_the_merge_base(self):
        git(self.repo, "checkout", "-q", "main")
        self.write("a/main-only", "open")  # main moved on after the branch point
        self.commit("main moved")
        git(self.repo, "checkout", "-q", "pr")
        self.write("a/open-one", "fixed")
        self.commit("pr")
        self.write("a/open-one", "open")  # uncommitted edit must not count
        self.assertEqual(self.changed(), ["a/open-one"])  # "main" is a branch tip, not the merge base

    def test_repository_spec_is_consistent(self):
        self.assertEqual(plan.check(), [])


if __name__ == "__main__":
    unittest.main()


class ChangedEntriesTest(unittest.TestCase):
    """An edit that only links an entry to the registry (covers, fixes) does not re-select it at every range."""

    def test_link_only_edits_are_not_capture_changes(self):
        base = {"pages": [
            {"id": "a", "kind": "explore", "query": "{a=\"1\"}", "covers": ["x"]},
            {"id": "b", "kind": "explore", "query": "{b=\"1\"}", "covers": ["x"]},
        ]}
        head = {"pages": [
            {"id": "a", "kind": "explore", "query": "{a=\"1\"}", "covers": ["x", "y"], "fixes": ["semantics/z"]},
            {"id": "b", "kind": "explore", "query": "{b=\"2\"}", "covers": ["x"]},
            {"id": "c", "kind": "explore", "query": "{c=\"1\"}", "covers": ["x"]},
        ]}
        saved = plan.spec_at
        plan.spec_at = lambda ref: base
        try:
            self.assertEqual(plan.changed_entries(head, "base"), ["b", "c"])
        finally:
            plan.spec_at = saved
