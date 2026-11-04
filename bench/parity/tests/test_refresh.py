"""bench/parity/refresh.py: the corpus slice, the generator order, the summary and the guards of a refresh."""
import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import refresh  # noqa: E402


def entries(source, n):
    return [{"id": f"{source}-{i}", "source": source} for i in range(n)]


class SliceTest(unittest.TestCase):
    def slice(self, doc, per_source):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "corpus.json")
            with open(path, "w") as handle:
                json.dump(doc, handle)
            sliced, kept, total = refresh.slice_corpus(path, per_source)
            with open(sliced) as handle:
                return json.load(handle), kept, total, os.path.basename(sliced)

    def test_every_source_keeps_evenly_spaced_entries(self):
        doc = {"entries": entries("loki-tests", 100) + entries("repo", 10) + entries("seeded", 7), "meta": 1}
        out, kept, total, name = self.slice(doc, 5)
        by_source = {}
        for e in out["entries"]:
            by_source.setdefault(e["source"], []).append(int(e["id"].rsplit("-", 1)[1]))
        self.assertEqual(by_source["loki-tests"], [0, 20, 40, 60, 80])  # spread over the source, not its head
        self.assertEqual(by_source["repo"], [0, 2, 4, 6, 8])
        self.assertEqual(by_source["seeded"], [0, 1, 2, 3, 4])
        self.assertEqual((kept, total, name), (15, 117, "corpus-slice.json"))
        self.assertEqual(out["meta"], 1)
        self.assertEqual(out["slice"], {"per_source": 5, "of": 117})

    def test_a_small_source_is_kept_whole(self):
        out, kept, _, _ = self.slice({"entries": entries("grafana", 3)}, 25)
        self.assertEqual(kept, 3)


class MetricWarmupTest(unittest.TestCase):
    def verdict(self, **before):
        base = {"loki_ready": 200, "proxy_ready": 200, "vl_ready": 200, "lines_loki": 5, "lines_vl": 5,
                "slices_differing": [], "services_listed": 20, "services_in_loki_metric": 16}
        base.update(before)
        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "health.json"), "w") as handle:
                json.dump({"before": base}, handle)
            return refresh.only_metric_path_warming(directory)

    def test_only_a_short_metric_answer_is_waited_for(self):
        self.assertTrue(self.verdict())
        self.assertFalse(self.verdict(services_in_loki_metric=20))
        self.assertFalse(self.verdict(lines_loki=4))
        self.assertFalse(self.verdict(loki_ready=503))
        self.assertFalse(self.verdict(slices_differing=[1]))

    def test_no_health_file_is_not_waited_for(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(refresh.only_metric_path_warming(directory))


class GeneratorOrderTest(unittest.TestCase):
    def test_the_ranked_differences_are_written_before_the_sections_that_read_them(self):
        names = [(script, target) for script, target, _ in refresh.check_conformance.REPORTS]
        parity = names.index(("parity_gaps.py", "conformance/reports/parity-gaps.md"))
        for later in (("perf_evidence.py", "README.md"), ("perf_evidence.py", "docs/real-window-compatibility-gaps.md")):
            self.assertGreater(names.index(later), parity)

    def test_the_loki_tag_is_the_one_the_stack_runs(self):
        self.assertRegex(refresh.loki_tag(), r"^v\d+\.\d+\.\d+$")


class GuardTest(unittest.TestCase):
    def test_a_partial_run_is_refused_whatever_else_it_holds(self):
        full = {"clusters": [{"id": "a"}], "summary": {s: {"requests": 1} for s in refresh.parity_gaps.SOURCES}}
        self.assertEqual(refresh.parity_gaps.prune_refusal(full), "")
        self.assertIn("partial", refresh.parity_gaps.prune_refusal(dict(full, partial=True)))

    def test_every_corpus_source_is_one_the_corpus_builder_writes(self):
        with open(os.path.join(HERE, "..", "corpus.py")) as handle:
            text = handle.read()
        for source in refresh.parity_gaps.SOURCES:
            self.assertIn(f'"{source}"', text)

    def test_the_validation_pages_exist_in_the_capture_spec(self):
        self.assertEqual(sorted(refresh.spec_pages(refresh.VALIDATE_PAGES)), sorted(refresh.VALIDATE_PAGES))


class SummaryTest(unittest.TestCase):
    discovery = {"window": [1, 2], "summary": {"repo": {"requests": 4, "same": 2, "diff": 1, "vacuous": 1, "blocked": 0}},
                 "health": {"ok": True, "ok_after": True, "lines_loki": 9, "lines_vl": 9, "loki_restarts": 0,
                            "loki_restarts_after": 0, "vl_restarts": 0, "vl_restarts_after": 0}}
    result = {"mode": "full", "ref": "abc12345", "loki": "v3.7.9", "clusters": 3, "requests": 4, "not_reproducible": 0,
              "elapsed_s": 60, "phases_s": {"run": 30}, "conformance_ok": False}

    def test_unclaimed_clusters_lead_the_summary_with_the_count(self):
        problems = [f"cluster c{i} (q) is accounted for by no registry case" for i in range(50)]
        text = refresh.summary_md(None, self.result, self.discovery, "Prune: nothing dropped.", problems)
        self.assertIn("reports 50 problem(s)", text)
        self.assertIn("cluster c0", text)
        self.assertNotIn("cluster c45", text)
        self.assertIn("10 more in `conformance.txt`", text)
        self.assertIn("| repo | 4 | 2 | 1 | 1 | 0 |", text)

    def test_a_slice_says_so_and_a_clean_gate_says_so(self):
        result = dict(self.result, mode="validate", conformance_ok=True,
                      slice={"per_source": 25, "of": 900}, prune_refused="the recorded run is partial")
        text = refresh.summary_md(None, result, self.discovery, "", [])
        self.assertIn("slice (25 per source of 900", text)
        self.assertIn("refused", text)
        self.assertIn("gate passes", text)


if __name__ == "__main__":
    unittest.main()
