"""The grammar-sweep source of the parity corpus (corpus.py --sweep): its size and the query shapes it reaches, held
against the run of 2026-11-07, the last whose corpus read Loki's test files (fixtures/run-2026-11-07-breadth.json)."""
import collections
import json
import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import cluster  # noqa: E402
import corpus  # noqa: E402

with open(os.path.join(HERE, "fixtures", "run-2026-11-07-breadth.json")) as handle:
    BEFORE = json.load(handle)


def queries(entries):
    return {e["params"]["query"] for e in entries}


class SweepSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = corpus.sweep()

    def test_the_order_and_the_content_are_fixed(self):
        again = corpus.sweep()
        self.assertEqual([e["id"] for e in self.entries], [e["id"] for e in again])
        self.assertEqual(len({e["id"] for e in self.entries}), len(self.entries))

    def test_every_entry_is_a_sweep_entry_with_a_known_origin(self):
        origins = collections.Counter(e["origin"] for e in self.entries)
        self.assertEqual(set(origins), {"sweep: accepted", "sweep: rejected", "sweep: composed"})
        self.assertEqual({e["source"] for e in self.entries}, {"sweep"})

    def test_accepted_and_composed_queries_sit_on_the_seeded_data(self):
        seeded = set(corpus.SELECTORS.values())
        for entry in self.entries:
            if entry["origin"] == "sweep: rejected":
                continue
            query = entry["params"]["query"]
            selectors = [query[a:b] for a, b in corpus.scan_selectors(query)]
            self.assertTrue(selectors, query)
            for selector in selectors:
                self.assertIn(selector, seeded, query)

    def test_rejected_queries_are_sent_as_the_sweep_wrote_them(self):
        rows = {row["q"].strip() for row in corpus.sweep_rows() if not row.get("a")}
        rejected = [e["params"]["query"] for e in self.entries if e["origin"] == "sweep: rejected"]
        self.assertTrue(rejected)
        for query in rejected:
            self.assertIn(query, rows)

    def test_each_error_text_of_the_sweep_is_asked_for(self):
        answers = {cluster.error_template(row.get("e", "")) for row in corpus.sweep_rows() if not row.get("a")}
        asked = {cluster.error_template(row.get("e", "")) for row in corpus.sweep_rows()
                 if not row.get("a") and row["q"].strip() in queries(
                     e for e in self.entries if e["origin"] == "sweep: rejected")}
        self.assertEqual(answers, asked)

    def test_the_corpus_is_as_large_as_the_one_read_from_loki_s_tests(self):
        # The run's "requests" are rows: a query endpoint is sent twice (run.py: both response encodings).
        sent = lambda entries: sum(2 if e["endpoint"] in ("query_range", "query", "tail") else 1 for e in entries)
        before = BEFORE["requests"]
        self.assertGreaterEqual(sent(self.entries), before["loki-tests"])
        self.assertGreaterEqual(sent(self.entries) + sent(corpus.repo_queries()), before["loki-tests"] + before["repo"])

    def test_every_shape_the_loki_tests_reached_is_reached_again(self):
        reached = {cluster.shape(q) for q in queries(self.entries)}
        reached |= {cluster.shape(q) for q in queries(corpus.repo_queries())}
        missing = sorted(set(BEFORE["shapes_of_loki_tests_clusters"]) - reached)
        self.assertEqual(missing, [])

    def test_every_other_shape_of_the_old_run_is_reached_by_the_offline_sources(self):
        reached = {cluster.shape(q) for q in queries(self.entries)} | {cluster.shape(q) for q in queries(corpus.repo_queries())}
        missing = sorted(set(BEFORE["shapes_of_all_clusters"]) - reached)
        self.assertEqual(missing, ["-"])  # "-": a Grafana resource call, sent by the grafana source

    def test_the_readme_table_states_what_the_sources_send(self):
        sent = lambda entries: sum(2 if e["endpoint"] in ("query_range", "query", "tail") else 1 for e in entries)
        with open(os.path.join(HERE, "..", "README.md")) as handle:
            readme = handle.read()
        for label, entries in (("grammar sweep", self.entries), ("repository cases", corpus.repo_queries())):
            row = re.search(rf"\| {label} \| [\d,]+ \| ([\d,]+) \(([\d,]+) entries\) \|", readme)
            self.assertIsNotNone(row, label)
            self.assertEqual((row.group(1), row.group(2)), (f"{sent(entries):,}", f"{len(entries):,}"), label)
        # The sweep's composed entries are named in the text above the table.
        composed = sum(1 for e in self.entries if e["origin"] == "sweep: composed")
        self.assertIn(f"{composed:,} entries composed", readme)


if __name__ == "__main__":
    unittest.main()
