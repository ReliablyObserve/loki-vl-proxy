"""conformance/scripts/parity_gaps.py: registration rules for differential-run clusters."""
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "conformance", "scripts"))
import parity_gaps  # noqa: E402


def case(status="open", clusters=(), fixed_by="", impact="explore-visible"):
    lines = ["id: x", "title: T", "gap:", f"  status: {status}", f"  impact: {impact}", "  effort: S",
             "  area: 'a'", "  loki: 'l'", "  proxy: 'p'", "  planned_test: 't'"]
    if fixed_by:
        lines.append(f"  fixed_by: '{fixed_by}'")
    lines.append("  clusters:" + ("" if clusters else " []"))
    lines += [f"    - {c}" for c in clusters]
    return "\n".join(lines) + "\n"


def cluster(cid, documented=None, queries=2):
    return {"id": cid, "signature": ["query_range", "logs", "labels", "x"], "documented": documented,
            "queries": queries, "requests": queries * 2}


class ParityGapsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.dir.name, "registry")
        os.makedirs(os.path.join(self.root, "cases", "t"))
        self.discovery = os.path.join(self.dir.name, "discovery.json")

    def tearDown(self):
        self.dir.cleanup()

    def write_case(self, name, text):
        with open(os.path.join(self.root, "cases", "t", name + ".yaml"), "w") as handle:
            handle.write(text)

    def write_discovery(self, clusters):
        with open(self.discovery, "w") as handle:
            json.dump({"clusters": clusters, "summary": {}}, handle)

    def analyse(self):
        return parity_gaps.analyse(self.root, self.discovery)

    def test_absent_discovery_file_is_no_problem(self):
        self.write_case("a", case(clusters=()))
        found, discovery, problems = self.analyse()
        self.assertEqual(problems, [])
        self.assertIn("No differential run is recorded.", parity_gaps.render(found, discovery))

    def test_unclaimed_and_double_claimed_clusters_fail(self):
        self.write_case("a", case(clusters=("c1",)))
        self.write_case("b", case(clusters=("c1",)))
        self.write_discovery([cluster("c1"), cluster("c2")])
        problems = self.analyse()[2]
        self.assertTrue(any("c1 is claimed by 2 cases" in p for p in problems))
        self.assertTrue(any("c2" in p and "no registry case" in p for p in problems))

    def test_claim_of_a_cluster_the_run_does_not_hold_fails(self):
        self.write_case("a", case(clusters=("gone",)))
        self.write_discovery([])
        self.assertTrue(any("gone is not in" in p for p in self.analyse()[2]))

    def test_documented_cluster_must_name_a_recorded_case(self):
        self.write_case("doc", case(status="documented"))
        self.write_case("open", case(status="open"))
        self.write_discovery([cluster("c1", documented="t/doc"), cluster("c2", documented="t/unknown"),
                              cluster("c3", documented="t/open")])
        found, _, problems = self.analyse()
        self.assertFalse(any("c1" in p for p in problems))
        self.assertTrue(any("c2" in p and "no registry case" in p for p in problems))
        self.assertTrue(any("c3" in p and "gap status" in p for p in problems))
        self.assertEqual(next(c for c in found if c["id"] == "t/doc")["queries"], 2)

    def test_fixed_needs_fixed_by_and_is_listed_apart(self):
        self.write_case("done", case(status="fixed", clusters=("c1",), fixed_by="#700"))
        self.write_case("bad", case(status="fixed"))
        self.write_discovery([cluster("c1")])
        found, discovery, problems = self.analyse()
        self.assertEqual(problems, ["t/bad: status fixed needs fixed_by (the PR number or commit that fixed it)"])
        report = parity_gaps.render(found, discovery)
        self.assertIn("## Fixed, awaiting the next run", report)
        self.assertIn("#700", report)

    def test_ranking_open_first_then_impact_then_queries(self):
        self.write_case("api", case(clusters=("c1",), impact="api-only"))
        self.write_case("few", case(clusters=("c2",)))
        self.write_case("many", case(clusters=("c3",)))
        self.write_case("kept", case(status="owner-kept", clusters=("c4",)))
        self.write_discovery([cluster("c1", queries=50), cluster("c2", queries=1), cluster("c3", queries=9),
                              cluster("c4", queries=99)])
        found = self.analyse()[0]
        self.assertEqual([c["id"] for c in found], ["t/many", "t/few", "t/api", "t/kept"])

    def test_unknown_status_and_impact_fail(self):
        self.write_case("a", case(status="maybe", impact="somewhere"))
        problems = self.analyse()[2]
        self.assertEqual(len(problems), 2)


ALL_SOURCES = {name: {"requests": 10, "same": 5, "diff": 5, "vacuous": 0, "blocked": 0}
               for name in parity_gaps.SOURCES}


class PruneTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = os.path.join(self.dir.name, "registry")
        os.makedirs(os.path.join(self.root, "cases", "t"))
        self.discovery = os.path.join(self.dir.name, "discovery.json")

    def tearDown(self):
        self.dir.cleanup()

    def write_case(self, name, text):
        with open(os.path.join(self.root, "cases", "t", name + ".yaml"), "w") as handle:
            handle.write(text)

    def read_case(self, name):
        with open(os.path.join(self.root, "cases", "t", name + ".yaml")) as handle:
            return handle.read()

    def write_discovery(self, clusters, **extra):
        doc = {"clusters": clusters, "summary": ALL_SOURCES}
        doc.update(extra)
        with open(self.discovery, "w") as handle:
            json.dump(doc, handle)

    def prune(self):
        return parity_gaps.prune(self.root, self.discovery)

    def analyse(self):
        return parity_gaps.analyse(self.root, self.discovery)

    def test_a_vanished_cluster_is_dropped_from_a_fixed_case_and_check_passes(self):
        self.write_case("done", case(status="fixed", clusters=("c1", "c2"), fixed_by="#700"))
        self.write_discovery([cluster("c1")])
        self.assertTrue(any("c2 is not in" in p for p in self.analyse()[2]))
        changes, refusal = self.prune()
        self.assertEqual(refusal, "")
        self.assertEqual(changes, [{"id": "t/done", "status": "fixed", "dropped": ["c2"], "left": 1}])
        self.assertEqual(self.analyse()[2], [])
        self.assertIn("    - c1\n", self.read_case("done"))
        self.assertNotIn("c2", self.read_case("done"))

    def test_an_open_case_left_without_a_cluster_is_reported_not_changed(self):
        self.write_case("open", case(status="open", clusters=("gone",)))
        self.write_discovery([cluster("c1", documented="t/doc")])
        self.write_case("doc", case(status="documented"))
        changes, refusal = self.prune()
        self.assertEqual(changes, [{"id": "t/open", "status": "open", "dropped": ["gone"], "left": 0}])
        text = self.read_case("open")
        self.assertIn("  status: open\n", text)
        self.assertIn("  clusters: []\n", text)
        self.assertEqual(self.analyse()[2], [])
        listing = parity_gaps.prune_report(changes)
        self.assertIn("Not fixed in the registry, yet left with no cluster", listing)
        self.assertIn("`t/open` (status open", listing)

    def test_a_fixed_case_left_without_a_cluster_is_reported_as_confirmed(self):
        self.write_case("done", case(status="fixed", clusters=("c1",), fixed_by="#700"))
        self.write_discovery([cluster("other", documented="t/done")])
        listing = parity_gaps.prune_report(self.prune()[0])
        self.assertIn("the fix is confirmed", listing)
        self.assertNotIn("Not fixed in the registry", listing)

    def test_flow_lists_and_quoted_ids_keep_their_form(self):
        quoted = case(status="open", clusters=()).replace("  clusters: []", "  clusters: [aa11bb22, '92699938', cc33dd44]")
        self.write_case("flow", quoted)
        block = case(status="open", clusters=("aa11bb22", "'92699938'", "cc33dd44"))
        self.write_case("block", block)
        self.write_discovery([cluster("aa11bb22"), cluster("92699938")])
        self.prune()
        self.assertIn("  clusters: [aa11bb22, '92699938']\n", self.read_case("flow"))
        self.assertIn("    - aa11bb22\n    - '92699938'\n", self.read_case("block"))
        self.assertNotIn("cc33dd44", self.read_case("block"))

    def test_only_the_gap_block_is_touched(self):
        text = "id: x\ntitle: T\nnotes:\n  clusters:\n    - c9\n" + case(clusters=("c9", "c1")) + "after: 1\n"
        self.write_case("a", text)
        self.write_discovery([cluster("c1")])
        self.prune()
        out = self.read_case("a")
        self.assertIn("notes:\n  clusters:\n    - c9\n", out)
        self.assertTrue(out.endswith("after: 1\n"))
        self.assertEqual(parity_gaps.cases(self.root)[0][0]["clusters"], ["c1"])

    def test_a_case_with_nothing_vanished_is_not_rewritten(self):
        self.write_case("a", case(clusters=("c1",)))
        self.write_case("planned", case(clusters=()))
        self.write_discovery([cluster("c1")])
        before = self.read_case("a"), self.read_case("planned")
        changes, _ = self.prune()
        self.assertEqual(changes, [])
        self.assertEqual((self.read_case("a"), self.read_case("planned")), before)
        self.assertIn("nothing dropped", parity_gaps.prune_report(changes))

    def test_prune_is_idempotent(self):
        self.write_case("a", case(status="fixed", clusters=("c1", "c2"), fixed_by="#1"))
        self.write_discovery([cluster("c1")])
        self.prune()
        once = self.read_case("a")
        self.assertEqual(self.prune()[0], [])
        self.assertEqual(self.read_case("a"), once)

    def test_unclaimed_new_clusters_stay_a_problem(self):
        self.write_case("a", case(clusters=("c1", "gone")))
        self.write_discovery([cluster("c1"), cluster("new")])
        self.prune()
        problems = self.analyse()[2]
        self.assertEqual(len(problems), 1)
        self.assertIn("new", problems[0])
        self.assertIn("no registry case", problems[0])

    def test_a_run_that_cannot_speak_for_the_corpus_is_refused_and_nothing_changes(self):
        self.write_case("a", case(clusters=("c1", "c2")))
        before = self.read_case("a")
        for label, extra, wanted in (
            ("partial", {"partial": True}, "partial"),
            ("no cluster", {"clusters": []}, "no cluster"),
        ):
            with self.subTest(label):
                self.write_discovery(extra.get("clusters", [cluster("c1")]), **{k: v for k, v in extra.items() if k != "clusters"})
                changes, refusal = self.prune()
                self.assertEqual(changes, [])
                self.assertIn(wanted, refusal)
                self.assertEqual(self.read_case("a"), before)
        silent = dict(ALL_SOURCES, seeded={"requests": 0})
        self.write_discovery([cluster("c1")], summary=silent)
        self.assertIn("seeded", self.prune()[1])
        os.remove(self.discovery)
        self.assertIn("no cluster", self.prune()[1])
        self.assertEqual(self.read_case("a"), before)


if __name__ == "__main__":
    unittest.main()
