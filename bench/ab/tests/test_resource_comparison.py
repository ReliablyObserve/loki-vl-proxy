"""Unit tests for conformance/scripts/resource_comparison.py: the table it renders from bench/resources results,
the refusal to print numbers without provenance, the warning for a failed request, and the committed README and
docs/benchmarks.md sections, which must be what the committed results render to."""
import copy
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(REPO, "conformance", "scripts"))
import resource_comparison as rc  # noqa: E402


def run(side, cpu, mem, statuses=None):
    names = ["e2e-loki"] if side.startswith("loki") else ["e2e-proxy-underscore", "e2e-victorialogs"]
    return {"side": side, "requests": 81, "seconds": 60.0, "statuses": statuses or {"200": 81},
            "containers": {n: {"cpu_s_workload": cpu / len(names), "mem_peak_mib": mem // len(names)} for n in names}}


RESULTS = {"date": "2026-11-18", "proxy_commit": "abcdef1234", "host": {"cpu": "8 CPUs", "ram": "14 GiB", "vm": "Colima"},
           "images": {"loki": "grafana/loki:3.7.9", "proxy": "loki-vl-proxy:e2e-local", "victorialogs": "victoria-logs:v1.53.0"},
           "method": {"workload": "9 queries", "idle_s": 30},
           "sides": [run("loki", 40.0, 2000), run("proxy+victorialogs", 10.0, 800),
                    run("loki-again", 44.0, 2200), run("proxy+victorialogs-again", 12.0, 1000)]}


class RenderTest(unittest.TestCase):
    def test_the_table_states_the_run_and_the_ratios(self):
        text = rc.render(RESULTS)
        self.assertIn("Measured on 2026-11-18 at commit `abcdef1234`, on one machine (8 CPUs, 14 GiB, Colima)", text)
        self.assertIn("A local single-machine test, not a production figure", text)
        self.assertIn("[The Cost Case](#the-cost-case)", text)
        self.assertIn("| Loki | 81 | 40.0 | 2000 (0) | 44.0 | 2200 (0) | 60 s / 60 s |", text)
        self.assertIn("| Proxy + VictoriaLogs | 81 | 10.0 | 800 (0) | 12.0 | 1000 (0) | 60 s / 60 s |", text)
        self.assertIn("on the first, cold run Loki used **4.0x** the CPU-seconds and peaked at **2.5x** the memory", text)
        self.assertIn("on the repeat of the identical requests (caches warm on both sides) **3.7x** the CPU-seconds and **2.2x**", text)
        self.assertNotIn("Not a valid comparison", text)

    def test_the_docs_page_links_the_cost_model(self):
        self.assertIn("cost-model.md", rc.render(RESULTS, "docs"))

    def test_no_numbers_without_provenance(self):
        for key in ("date", "proxy_commit", "images", "host"):
            bad = copy.deepcopy(RESULTS)
            del bad[key]
            with self.assertRaises(ValueError):
                rc.render(bad)

    def test_the_bare_commit_and_runs_keys_are_read_too(self):
        bare = copy.deepcopy(RESULTS)
        bare["commit"], bare["runs"] = bare.pop("proxy_commit"), bare.pop("sides")
        self.assertEqual(rc.render(bare), rc.render(RESULTS))

    def test_a_failed_request_is_flagged(self):
        bad = copy.deepcopy(RESULTS)
        bad["sides"][0] = run("loki", 5.0, 100, {"200": 70, "500": 11})
        self.assertIn("Not a valid comparison: Loki (loki): 11 of 81 requests did not answer 200", rc.render(bad))

    def test_unequal_workloads_and_unmeasured_containers_are_refused(self):
        bad = copy.deepcopy(RESULTS)
        bad["sides"][1]["requests"] = 80
        with self.assertRaises(ValueError):
            rc.render(bad)
        bad = copy.deepcopy(RESULTS)
        bad["sides"][0]["containers"]["e2e-loki"]["cpu_s_workload"] = None
        with self.assertRaises(ValueError):
            rc.render(bad)

    def test_a_run_with_no_cpu_above_idle_is_refused(self):
        bad = copy.deepcopy(RESULTS)
        bad["sides"][1] = run("proxy+victorialogs", -1.0, 800)
        with self.assertRaises(ValueError):
            rc.render(bad)

    def test_no_results_render_a_not_measured_note(self):
        self.assertEqual(rc.render(None), rc.NOT_MEASURED)

    def test_markers_are_required_once(self):
        with self.assertRaises(ValueError):
            rc.replace_section("no markers", "x")
        text = f"a\n{rc.START}\nold\n{rc.END}\nb"
        self.assertEqual(rc.replace_section(text, "new"), f"a\n{rc.START}\n\nnew\n\n{rc.END}\nb")


class HarnessTest(unittest.TestCase):
    def test_the_default_workload_text_is_the_harness_workload(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("compare", os.path.join(REPO, "bench", "resources", "compare.py"))
        compare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(compare)
        self.assertEqual(rc.WORKLOAD, compare.WORKLOAD)
        bare = copy.deepcopy(RESULTS)
        del bare["method"]
        self.assertIn(compare.WORKLOAD, rc.render(bare))


class CommittedSectionTest(unittest.TestCase):
    def test_the_pages_state_the_committed_results(self):
        path = os.path.join(REPO, rc.RESULTS)
        results = rc.load(path) if os.path.exists(path) else None
        for page, name in (("readme", "README.md"), ("docs", os.path.join("docs", "benchmarks.md"))):
            with open(os.path.join(REPO, name), encoding="utf-8") as f:
                text = f.read()
            self.assertEqual(rc.replace_section(text, rc.render(results, page)), text, name)


if __name__ == "__main__":
    unittest.main()
