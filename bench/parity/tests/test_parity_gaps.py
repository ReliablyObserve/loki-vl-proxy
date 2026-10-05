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


if __name__ == "__main__":
    unittest.main()
