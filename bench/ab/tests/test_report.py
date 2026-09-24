"""Unit tests for bench/ab/report.py summarize verdicts."""
import argparse
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import report  # noqa: E402


def raw_rows(statuses, signatures):
    rows = []
    for run in range(3):
        for target in ("main", "branch", "loki"):
            rows.append({"shape": "s", "range": "1h", "run": run, "target": target, "seconds": 0.1,
                         "status": statuses[target], "signature": signatures[target], "vl_cpu_max": 0,
                         "end": 1000 + run})
    return {"set": "x", "rows": rows}


def summarize(raw):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "raw.json")
        with open(path, "w") as f:
            json.dump(raw, f)
        args = argparse.Namespace(raw=path, baseline="main", candidate="branch", reference="loki", label="t",
                                  save=d, tolerance=0.01, cold=False, noise=0.25, min_delta=0.05)
        report.summarize(args)
        with open(os.path.join(d, [n for n in os.listdir(d) if n != "raw.json"][0])) as f:
            return json.load(f)["rows"][0]


class VerdictTest(unittest.TestCase):
    def test_rejecting_like_the_reference_is_a_fix(self):
        err = "ERR parse error at line 1, col 64: syntax error: unexpected ."
        row = summarize(raw_rows({"main": 200, "branch": 400, "loki": 400},
                                 {"main": "streams lines=5", "branch": err, "loki": err}))
        self.assertEqual(row["verdict"], "fixed")

    def test_failing_differently_from_the_reference_is_broken(self):
        row = summarize(raw_rows({"main": 200, "branch": 502, "loki": 200},
                                 {"main": "streams lines=5", "branch": "ERR boom", "loki": "streams lines=5"}))
        self.assertEqual(row["verdict"], "broken")

    def test_failing_with_other_text_than_the_reference_is_broken(self):
        row = summarize(raw_rows({"main": 200, "branch": 400, "loki": 400},
                                 {"main": "streams lines=5", "branch": "ERR other", "loki": "ERR parse error"}))
        self.assertEqual(row["verdict"], "broken")


if __name__ == "__main__":
    unittest.main()
