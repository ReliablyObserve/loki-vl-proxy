"""compare.py on answers the PR proxy gave at a raised step (registry case limits/range-metric-step-adapt).

Such an answer carries Loki's warning "step raised from R to E to fit the query limits" and passes only when it equals
Loki's answer to the same query at the effective step E (loki.json: step_references, with the proxy's own
X-Proxy-Step-Adjusted header), on the grid of E, on a page that declares the case. Every other path fails as before.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import compare  # noqa: E402

PAGE = "explore-step-adapt"  # declares limits/range-metric-step-adapt in spec.json (fixes)
FROM, TO = 1795873080000, 1795873380000
EXPR = 'sum by (service_name) (sum_over_time({env="production"} | json | unwrap duration_ms [1h]))'
NOTICE = "step raised from 200ms to 400ms to fit the query limits"


def answer(times, values, notice=None, interval=200):
    meta = {"type": "timeseries-multi", "typeVersion": [0, 1]}
    if notice:
        meta["notices"] = [{"severity": "warning", "text": notice}]
    frame = {"schema": {"refId": "A", "meta": meta, "fields": [
        {"name": "Time", "type": "time", "config": {"interval": interval}},
        {"name": "Value", "type": "number", "labels": {"service_name": "api-gateway"}}]},
        "data": {"values": [times, values]}}
    return {"status": 200, "frames": [frame]}


def capture(result, uid, step=""):
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200 if "error" not in result else 500,
           "request": {"from": str(FROM), "to": str(TO), "queries": [
               {"refId": "A", "expr": EXPR, "queryType": "range", "datasource": {"type": "loki", "uid": uid}, "step": step}]},
           "response": {"results": {"A": result}}}
    return {"settled": True, "settle_ms": 1000, "records": [rec], "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


EFF = list(range(FROM, TO + 1, 400))
FINE = list(range(FROM, TO + 1, 200))


def reference(times=EFF, values=None, marker="200ms;400ms", status=200, eff_ms=400, interval=400):
    values = values if values is not None else [float(i) for i in range(len(times))]
    return {"refId": "A", "expr": EXPR, "from": str(FROM), "to": str(TO), "notice": NOTICE, "requested_step_ms": 200,
            "effective_step_ms": eff_ms, "marker": marker, "status": status,
            "response": {"results": {"A": answer(times, values, interval=interval)}}}


def pr_answer(times=EFF, values=None, notice=NOTICE):
    values = values if values is not None else [float(i) for i in range(len(times))]
    return answer(times, values, notice)


KEY = ("query", "A", EXPR, "range", str(FROM), str(TO), "d")


class JudgeStepAdjustedTest(unittest.TestCase):
    def test_equal_to_loki_at_the_effective_step_passes(self):
        verdict, failure = compare.judge_step_adjusted(KEY, pr_answer(), [reference()], True)
        self.assertIsNone(failure)
        self.assertIn("equals Loki's answer at 400ms", verdict)

    def test_a_step_without_samples_on_both_sides_passes(self):
        sparse = EFF[:3] + EFF[4:]
        verdict, failure = compare.judge_step_adjusted(KEY, pr_answer(times=sparse), [reference(times=sparse)], True)
        self.assertIsNone(failure)
        self.assertTrue(verdict)

    def test_no_warning_is_not_judged(self):
        self.assertIsNone(compare.judge_step_adjusted(KEY, pr_answer(notice=None), [reference()], True))

    def test_every_failure_path(self):
        off_grid = [t + 200 for t in EFF[:-1]]
        cases = {
            "page does not declare": (pr_answer(), [reference()], False),
            "no reference": (pr_answer(), [], True),
            "reference of another request": (pr_answer(), [dict(reference(), to=str(TO + 1))], True),
            "marker names other steps": (pr_answer(), [reference(marker="200ms;800ms")], True),
            "no marker": (pr_answer(), [reference(marker="")], True),
            "reference failed": (pr_answer(), [reference(status=500)], True),
            "reference step other than the warning's": (pr_answer(), [reference(eff_ms=800)], True),
            "a value differs": (pr_answer(values=[1.0] * len(EFF)), [reference()], True),
            "fewer points than Loki": (pr_answer(times=EFF[:-1]), [reference()], True),
            "timestamps off the effective grid": (pr_answer(times=off_grid), [reference(times=off_grid)], True),
            "two different warnings": (self.two_warnings(), [reference()], True),
            "reference asked at another step": (pr_answer(), [reference(interval=800)], True),
        }
        for name, (ans, refs, declared) in cases.items():
            with self.subTest(name):
                verdict, failure = compare.judge_step_adjusted(KEY, ans, refs, declared)
                self.assertIsNone(verdict)
                self.assertTrue(failure)

    @staticmethod
    def two_warnings():
        a = pr_answer()
        a["frames"].append(deepcopy(a["frames"][0]))
        a["frames"][1]["schema"]["fields"][1]["labels"] = {"service_name": "x"}
        a["frames"][1]["schema"]["meta"]["notices"] = [{"severity": "warning", "text": "step raised from 200ms to 800ms to fit the query limits"}]
        return a


class CompareCliStepAdjustedTest(unittest.TestCase):
    def run_compare(self, page, main, pr, loki, runtime_required=False):
        with tempfile.TemporaryDirectory() as out:
            d = os.path.join(out, "data", page, "5m")
            os.makedirs(d)
            if runtime_required:
                with open(os.path.join(out, "plan.json"), "w", encoding="utf-8") as f:
                    json.dump({"runtime_proof": {"required": True},
                               "fix_cases": {compare.STEP_ADAPT_CASE: {"entries": [page], "status": "fixed"}},
                               "entries": {page: {"ranges": ["5m"], "runtime_required": True}}}, f)
            for name, cap in (("main", main), ("pr", pr), ("loki", loki)):
                with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as f:
                    json.dump(cap, f)
            proc = subprocess.run([sys.executable, os.path.join(HERE, "..", "compare.py"), out], capture_output=True, text=True)
            with open(os.path.join(out, "compare.json"), encoding="utf-8") as f:
                return proc.returncode, json.load(f)[0]

    def captures(self, refs):
        main = capture({"error": "rpc error: code = ResourceExhausted desc = grpc: trying to send message larger than max (67108865 vs. 67108864)",
                        "status": 500, "frames": []}, "vp-main")
        pr = capture(pr_answer(), "vp-pr")
        loki = capture(answer(FINE, [float(i) for i in range(len(FINE))]), "vp-loki")
        loki["step_references"] = refs
        return main, pr, loki

    def test_adjusted_answer_equal_to_loki_reference_is_improved(self):
        import visual_comment
        _, row = self.run_compare(PAGE, *self.captures([reference()]))
        self.assertEqual(row["loki_diffs"], [])
        self.assertTrue(any("raised step" in x for x in row["loki_explained"]))
        self.assertTrue(row["main_pr_diffs"])  # the base answered an error: a change, judged against Loki
        self.assertEqual(visual_comment.classify(row), "improved")
        self.assertEqual(visual_comment.assess(row, None)[0], [])

    def test_adjusted_answer_without_reference_still_fails(self):
        import visual_comment
        _, row = self.run_compare(PAGE, *self.captures([]))
        self.assertTrue(any("no Loki reference" in x for x in row["loki_diffs"]))
        self.assertNotEqual(visual_comment.classify(row), "improved")
        self.assertTrue(visual_comment.assess(row, None)[0])

    def test_adjusted_answer_on_an_undeclared_page_still_fails(self):
        import visual_comment
        _, row = self.run_compare("page-a", *self.captures([reference()]))
        self.assertTrue(any("does not declare" in x for x in row["loki_diffs"]))
        self.assertTrue(visual_comment.assess(row, None)[0])

    def test_runtime_proof_records_the_corrected_base_rejection(self):
        runtime = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2, js_heap_max_sampled_bytes=1000,
                       api_requests=1, api_encoded_bytes=100, page_encoded_bytes=200, first_api_data_ms=25)
        main, pr, loki = self.captures([reference()])
        for cap in (main, pr, loki):
            cap["runtime"] = dict(runtime)
        _, row = self.run_compare(PAGE, main, pr, loki, runtime_required=True)
        self.assertTrue(row.get("corrected_base_rejections"))
        self.assertFalse([x for x in row["runtime_problems"] if "rejection differs" in x])


if __name__ == "__main__":
    unittest.main()
