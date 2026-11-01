"""compare.py on synthetic captures: identical, different, empty, and Loki differences the base has too."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import compare  # noqa: E402


def capture(values, ui=None, settled=True, uid="ds1", fields=None, status=200, error=None):
    frame = {"schema": {"name": "", "fields": fields or [{"type": "time"}, {"type": "number", "labels": {"level": "info"}}]},
             "data": {"values": [list(range(len(values))), values]}}
    result = {"error": error} if error else {"frames": [frame]}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": status,
           "request": {"from": "1", "to": "2", "requestId": uid + "-1", "queries": [
               {"refId": "A", "expr": "sum(x)", "queryType": "range", "datasource": {"type": "loki", "uid": uid}, "maxLines": 1000}]},
           "response": {"results": {"A": result}}}
    return {"settled": settled, "settle_ms": 1000, "records": [rec], "ui": ui or {"noData": 0, "banners": [], "panelErrors": 0}}


class CompareTest(unittest.TestCase):
    def run_compare(self, main, pr, loki=None, rng="1h"):
        with tempfile.TemporaryDirectory() as out:
            d = os.path.join(out, "data", "page-a", rng)
            os.makedirs(d)
            for name, cap in (("main", main), ("pr", pr), ("loki", loki)):
                if cap is not None:
                    with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as f:
                        json.dump(cap, f)
            proc = subprocess.run([sys.executable, os.path.join(HERE, "..", "compare.py"), out], capture_output=True, text=True)
            with open(os.path.join(out, "compare.json"), encoding="utf-8") as f:
                return proc.returncode, json.load(f)[0]

    def test_show_context_queries_pair_up_despite_grafanas_random_ref_ids(self):
        def ctx(ref):
            cap = capture([1, 2])
            rec = cap["records"][0]
            rec["request"]["queries"][0].update(refId=ref, direction="forward")
            rec["response"]["results"] = {ref: rec["response"]["results"]["A"]}
            return cap
        code, row = self.run_compare(ctx("log-row-context-query-_0.43741"), ctx("log-row-context-query-_0.95931"),
                                     ctx("log-row-context-query-_0.11"))
        self.assertEqual((code, row["main_pr_diffs"], row["loki_diffs"]), (0, [], []))
        self.assertEqual(compare.stable_ref("log-row-context-query-_0.4374129728512264"), "log-row-context-query")
        self.assertEqual(compare.stable_ref("A"), "A")
    def test_an_error_loki_answers_too_is_lokis_answer_beyond_its_history(self):
        err = "pipeline error: 'SampleExtractionErr' for series: '{v=\"x\"}'."
        # 6h is beyond the history Loki holds on the CI stack: the data is not compared, the error still is.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err), capture([1], error=err), rng="6h")
        self.assertFalse(row["loki_compared"])
        self.assertEqual(row["errors_pr"], [])
        self.assertEqual(len(row["errors_loki"]), 1)
        # An error Loki does not answer stays an error of the PR.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err), capture([1, 2]), rng="6h")
        self.assertEqual(len(row["errors_pr"]), 1)

    def test_identical_counts_points_and_exits_zero(self):
        code, row = self.run_compare(capture([1, 2, 0]), capture([1, 2, 0]), capture([1, 2, 0]))
        self.assertEqual(code, 0)
        self.assertEqual((row["points_main"], row["points_pr"]), (2, 2))  # zero points are not data
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["loki_diffs"], [])

    def test_unreadable_success_body_is_capture_gap_not_empty_result(self):
        base = capture([1, 2])
        base["records"][0]["response"] = None  # response headers arrived, but Playwright could not read the body
        code, row = self.run_compare(base, capture([1, 2]), capture([1, 2]))
        self.assertEqual(code, 0)
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertTrue(any("capture body unreadable" in d for d in row["main_pr_nondet"]))
        self.assertFalse(row["settled"])
        self.assertTrue(row["settled_pr"])

        pr = capture([1, 2])
        pr["records"][0]["response"] = None
        _, row = self.run_compare(capture([1, 2]), pr, capture([1, 2]))
        self.assertFalse(row["settled_pr"])  # the verdict must still reject missing PR evidence

    def test_value_difference_is_reported_and_exits_one(self):
        code, row = self.run_compare(capture([1, 2]), capture([1, 3]), capture([1, 2]))
        self.assertEqual(code, 1)
        self.assertTrue(row["main_pr_diffs"])
        self.assertEqual(len(row["loki_new"]), 1)  # PR differs from Loki where the base does not

    def test_differences_carry_the_query_they_are_about(self):
        # the window and ids vary by range; the expression and query type do not
        _, row = self.run_compare(capture([1, 2]), capture([1, 3]), capture([1, 2]))
        self.assertEqual(row["main_pr_qids"], ["sum(x)|range"])
        self.assertEqual(row["loki_qids"], ["sum(x)|range"])
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), capture([1, 2]))
        self.assertEqual((row["main_pr_qids"], row["loki_qids"]), ([], []))

    def test_a_resource_difference_has_no_query(self):
        _, row = self.run_compare(resources(("index/stats?q=1", {"streams": 1})), resources(("index/stats?q=1", {"streams": 2})))
        self.assertEqual((len(row["main_pr_diffs"]), row["main_pr_qids"]), (1, [None]))

    def test_loki_difference_the_base_has_is_not_new(self):
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), capture([5, 5]))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row["loki_main_n"], 1)
        self.assertEqual(row["loki_new"], [])

    def test_ui_state_is_carried_per_side(self):
        bad = {"noData": 1, "banners": ["Plugin unavailable"], "panelErrors": 1}
        _, row = self.run_compare(capture([1]), capture([1], ui=bad))
        self.assertEqual(row["ui_pr"], bad)
        self.assertEqual(row["ui_main"]["noData"], 0)

    def test_datasource_uid_and_request_id_do_not_split_requests(self):
        _, row = self.run_compare(capture([1, 2], uid="vp-main"), capture([1, 2], uid="vp-pr"))
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["main_vs_pr"], "1/1")

    def test_other_request_fields_make_a_different_request(self):
        a, b = capture([1, 2]), capture([1, 2])
        b["records"][0]["request"]["queries"][0]["maxLines"] = 5000
        _, row = self.run_compare(a, b)
        self.assertEqual(len(row["main_pr_diffs"]), 2)  # one request on each side only

    def test_frame_schema_difference_is_a_difference(self):
        other = [{"type": "time"}, {"type": "number", "labels": {"level": "info"}, "config": {"interval": 60000}}]
        _, row = self.run_compare(capture([1, 2]), capture([1, 2], fields=other))
        self.assertIn("schema", row["main_pr_diffs"][0])

    def test_duplicate_answers_pair_by_content_not_arrival_order(self):
        a, b = capture([1, 2]), capture([3, 4])
        a["records"] = a["records"] + b["records"]
        b2, a2 = capture([3, 4]), capture([1, 2])
        b2["records"] = b2["records"] + a2["records"]  # same two answers, other order
        _, row = self.run_compare(a, b2)
        self.assertEqual(row["main_pr_diffs"], [])

    def test_errors_are_listed_per_side_minus_the_allow_list(self):
        bad = capture([1], error="plugin unavailable")
        _, row = self.run_compare(bad, bad)
        self.assertEqual(row["errors_pr"], ["plugin unavailable"])
        self.assertEqual(row["main_pr_diffs"], [])  # equal on both sides, and still reported
        self.assertEqual(compare.errors({"k": [{("A", "error"): ("error", "plugin unavailable")}]}, ["unavailable"]), [])
        _, row = self.run_compare(capture([1]), capture([1], status=500))
        self.assertTrue(row["errors_pr"])

    def test_an_error_the_pr_answers_as_loki_does_is_lokis_answer(self):
        # Loki's pipeline error names the first failing line its shards meet: the series is set aside.
        def err(series):
            return ("pipeline error: 'SampleExtractionErr' for series: '{__error__=\"SampleExtractionErr\", v=\"" + series +
                    "\"}'.\nUse a label filter to intentionally skip this error.")
        code, row = self.run_compare(capture([1, 2]), capture([1], error=err("x7")), capture([1], error=err("x3")))
        self.assertEqual(code, 1)  # base and PR differ; the gate decides against Loki
        self.assertEqual(row["errors_pr"], [])
        self.assertEqual(len(row["errors_loki"]), 1)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_main_n"], 1)
        # An error Loki does not answer stays an error of the PR.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err("x7")), capture([1, 2]))
        self.assertEqual(len(row["errors_pr"]), 1)
        self.assertEqual(compare.error_text(err("a")), compare.error_text(err("b")))
        self.assertNotEqual(compare.error_text(err("a")), compare.error_text("pipeline error: 'JSONParserErr' for series: '{}'."))

    def test_missing_loki_capture_within_its_window_is_flagged(self):
        _, row = self.run_compare(capture([1]), capture([1]))
        self.assertTrue(row["loki_missing"])
        _, row = self.run_compare(capture([1]), capture([1]), capture([1]))
        self.assertFalse(row["loki_missing"])

    def test_points(self):
        self.assertEqual(compare.points({"k": [{"a": ("metric", {1: 0, 2: 5}, "sig"), "b": ("other", "h", 7), "c": ("error", "x")}]}), 8)


def resources(*answers, uid="vp-pr"):
    """A capture of datasource resource answers: (path, body) pairs."""
    recs = [{"url": f"/api/datasources/uid/{uid}/resources/{path}", "method": "GET", "status": 200, "request": None, "response": body}
            for path, body in answers]
    return {"settled": True, "settle_ms": 1000, "records": recs, "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


def log_capture(labels, types, uid="ds1"):
    names = ["labels", "Time", "Line", "tsNs", "labelTypes", "id"]
    frame = {"schema": {"name": "", "fields": [{"name": n, "type": "other" if n in ("labels", "labelTypes") else "string"} for n in names]},
             "data": {"values": [[labels], [1], ["line"], ["1"], [types], [f"1_{hash(json.dumps(labels, sort_keys=True)) & 0xffff:x}"]]}}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200,
           "request": {"from": "1", "to": "2", "queries": [{"refId": "A", "expr": "{app=\"x\"} | json", "queryType": "range",
                                                           "datasource": {"type": "loki", "uid": uid}}]},
           "response": {"results": {"A": {"frames": [frame]}}}}
    return {"settled": True, "settle_ms": 1000, "records": [rec], "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


class ExplainedVsLokiTest(unittest.TestCase):
    """Differences from Loki by design are listed as explained, everything else stays a difference."""
    run_compare = CompareTest.run_compare

    def check(self, pr, loki):
        _, row = self.run_compare(pr, pr, loki)
        return row

    def test_detected_fields_envelope_and_extracted_suffix_are_explained(self):
        field = {"label": "pipeline", "type": "string", "cardinality": 3, "parsers": ["json"], "jsonPath": ["pipeline"]}
        meta = {"label": "trace_id", "type": "string", "cardinality": 9, "parsers": None}
        pr = resources(("detected_fields?q=1", {"status": "success", "data": [field, meta], "fields": [field, meta], "limit": 1000}))
        loki = resources(("detected_fields?q=1", {"fields": [meta, field, {"label": "level_extracted", "type": "string", "cardinality": 2,
                                                                       "parsers": ["json"], "jsonPath": ["level"]}], "limit": 1000}), uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(len(row["loki_explained"]), 1)
        self.assertIn("_extracted", row["loki_explained"][0])
        # structured metadata listed as a JSON key is a real difference
        bad = dict(meta, parsers=["json"], jsonPath=["trace_id"])
        row = self.check(resources(("detected_fields?q=1", {"fields": [field, bad], "limit": 1000})),
                         resources(("detected_fields?q=1", {"fields": [field, meta], "limit": 1000}), uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row["loki_explained"], [])

    def test_base_that_needs_an_explanation_the_pr_does_not_is_not_a_loki_match(self):
        # The base lacks Loki's level_extracted (explained by design), the PR returns it: improved, not unsettled.
        import visual_comment
        stream, types = {"app": "x", "level": "info"}, {"app": "I", "level": "I"}
        loki = log_capture(dict(stream, level_extracted="debug"), dict(types, level_extracted="P"), uid="vp-loki")
        pr = log_capture(dict(stream, level_extracted="debug"), dict(types, level_extracted="P"))
        _, row = self.run_compare(log_capture(stream, types), pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_main_n"], 1)
        self.assertEqual(visual_comment.classify(row), "improved")
        # a parse error both lack plus the suffix only the base lacks: the base needs one more reason
        loki_err = log_capture(dict(stream, level_extracted="debug", __error__="JSONParserErr"),
                               dict(types, level_extracted="P", __error__="P"), uid="vp-loki")
        _, row = self.run_compare(log_capture(stream, types), pr, loki_err)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(visual_comment.classify(row), "improved")
        # both builds needing the same explanation still both match Loki
        _, row = self.run_compare(log_capture(stream, types), log_capture(stream, types), loki)
        self.assertEqual(row["loki_main_n"], 0)

    def test_extracted_label_the_pr_returns_is_compared_not_set_aside(self):
        # Loki adds level_extracted and a parse error the proxy does not report; a PR that returns level_extracted
        # with Loki's value is explained only by the parse-error rule, a different value stays a difference.
        stream = {"app": "x", "level": "info"}
        loki = log_capture(dict(stream, level_extracted="debug", __error__="JSONParserErr"),
                           {"app": "I", "level": "I", "level_extracted": "P", "__error__": "P"}, uid="vp-loki")
        row = self.check(log_capture(dict(stream, level_extracted="debug"), {"app": "I", "level": "I", "level_extracted": "P"}), loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertNotIn("_extracted suffix", row["loki_explained"][0])
        row = self.check(log_capture(dict(stream, level_extracted="warn"), {"app": "I", "level": "I", "level_extracted": "P"}), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)
        # a base without the suffix is still explained by it
        row = self.check(log_capture(stream, {"app": "I", "level": "I"}), loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("_extracted suffix", row["loki_explained"][0])

    def test_detected_labels_cardinality_explained_but_not_an_empty_loki(self):
        pr = resources(("detected_labels?q=1", {"detectedLabels": [{"label": "pod", "cardinality": 45}, {"label": "level", "cardinality": 2}]}))
        loki = resources(("detected_labels?q=1", {"detectedLabels": [{"label": "level", "cardinality": 2}, {"label": "pod", "cardinality": 67}]}), uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("sampled cardinality", row["loki_explained"][0])
        row = self.check(pr, resources(("detected_labels?q=1", {}), uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)

    def test_index_stats_bytes_explained_entries_not(self):
        loki = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 62091264, "entries": 289279}), uid="vp-loki")
        row = self.check(resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 28927900, "entries": 289279})), loki)
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"])), ([], 1))
        row = self.check(resources(("index/stats?q=1", {"streams": 1, "chunks": 1, "bytes": 39508700, "entries": 395087})), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)

    def test_index_stats_of_a_recapture_follows_the_same_rules_as_the_first_pass(self):
        # ci_run recaptures a page and runs this same compare again. Loki's index/stats is an estimate from chunk
        # metadata: while the live generator writes into the stream, bytes and chunks move (explained) and so can
        # entries (a difference), which is why the recapture is taken before the Live tail pass.
        pr = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 28927900, "entries": 289279}))
        first = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 62091264, "entries": 289279}), uid="vp-loki")
        recapture = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13590, "bytes": 62917300, "entries": 289279}), uid="vp-loki")
        moved = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13590, "bytes": 62917300, "entries": 289954}), uid="vp-loki")
        for loki, n_diffs in ((first, 0), (recapture, 0), (moved, 1)):
            row = self.check(pr, loki)
            self.assertEqual((len(row["loki_diffs"]), len(row["loki_explained"])), (n_diffs, 1 - n_diffs))

    def test_patterns_are_history_dependent_not_counted(self):
        pr = resources(("patterns?q=1", {"status": "success", "data": [{"pattern": "a <_>"}]}))
        row = self.check(pr, resources(("patterns?q=1", {"status": "success", "data": []}), uid="vp-loki"))
        self.assertEqual((row["loki_diffs"], row["loki_new"]), ([], []))
        self.assertEqual(len(row["loki_nondet"]), 1)

    def test_log_labels_extracted_suffix_explained_parsed_extra_not(self):
        loki = log_capture({"level": "info", "level_extracted": "info", "service_name": "svc", "service_name_extracted": "svc"},
                           {"level": "I", "level_extracted": "P", "service_name": "I", "service_name_extracted": "S"}, uid="vp-loki")
        pr = log_capture({"level": "info", "service_name": "svc"}, {"level": "I", "service_name": "S"}, uid="vp-pr")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("_extracted", row["loki_explained"][0])
        pr = log_capture({"level": "info", "service_name": "svc", "method": "GET"}, {"level": "I", "service_name": "I", "method": "P"}, uid="vp-pr")
        row = self.check(pr, log_capture({"level": "info", "service_name": "svc"}, {"level": "I", "service_name": "I"}, uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)


    def test_log_labels_parse_error_and_parsed_collision_explained(self):
        # A line | json rejects: Loki adds __error__ / __error_details__; a JSON key named like a stream label is
        # Loki's <key>_extracted while the proxy types the stream label as parsed.
        loki = log_capture({"app": "a", "__error__": "JSONParserErr", "__error_details__": "x", "service_name": "svc",
                            "service_name_extracted": "svc"},
                           {"app": "I", "__error__": "P", "__error_details__": "P", "service_name": "I",
                            "service_name_extracted": "P"}, uid="vp-loki")
        pr = log_capture({"app": "a", "service_name": "svc"}, {"app": "I", "service_name": "P"}, uid="vp-pr")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("__error__", row["loki_explained"][0])
        self.assertIn("_extracted", row["loki_explained"][0])
        # a different value under the collision stays a difference
        pr = log_capture({"app": "a", "service_name": "other"}, {"app": "I", "service_name": "P"}, uid="vp-pr")
        self.assertEqual(len(self.check(pr, loki)["loki_diffs"]), 1)


def ranked_capture(series, expr='topk by (service_name) (2, sum by (service_name, level) (count_over_time({env="production"}[5s])))', uid="ds1"):
    """A capture of one topk query: {(service_name, level): {step: value}}, one frame per series."""
    frames = []
    for (svc, level), by_t in series.items():
        times = sorted(by_t)
        frames.append({"schema": {"name": "", "fields": [{"type": "time"}, {"type": "number", "labels": {"service_name": svc, "level": level}}]},
                       "data": {"values": [times, [by_t[t] for t in times]]}})
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200,
           "request": {"from": "1", "to": "2", "requestId": uid + "-1", "queries": [
               {"refId": "A", "expr": expr, "queryType": "range", "datasource": {"type": "loki", "uid": uid}}]},
           "response": {"results": {"A": {"frames": frames}}}}
    return {"settled": True, "settle_ms": 1000, "records": [rec], "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


class RankedAnswerTest(unittest.TestCase):
    """Loki keeps the tied series its map yields at the k-th place, so a topk answer is compared by its ranked values per step."""
    run_compare = CompareTest.run_compare

    # Step 10: pricing-svc ties error 5 = warn 5 behind info 20 (k=2). Loki may keep either; the proxy keeps error.
    PROXY = {("pricing-svc", "info"): {10: 20, 20: 20}, ("pricing-svc", "error"): {10: 5, 20: 4}}
    LOKI = {("pricing-svc", "info"): {10: 20, 20: 20}, ("pricing-svc", "warn"): {10: 5}, ("pricing-svc", "error"): {20: 4}}

    def test_a_different_tied_series_with_the_same_ranked_values_is_the_same_answer(self):
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(self.LOKI))
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_new"], [])
        self.assertEqual(row["pr_vs_loki"], "1/1 identical")

    def test_uppercase_topk_uses_the_same_ranked_comparison(self):
        expr = 'TOPK(2, SUM BY (service_name, level) (COUNT_OVER_TIME({env="production"}[5s])))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(row["loki_diffs"], [])

    def test_the_union_of_series_differing_by_one_is_still_a_difference_for_other_queries(self):
        expr = 'sum by (service_name, level) (count_over_time({env="production"}[5s]))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("series sets differ", row["loki_diffs"][0])

    def test_bottomk_is_ranked_too(self):
        expr = 'bottomk(2, sum by (service_name, level) (count_over_time({env="production"}[5s])))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(row["loki_diffs"], [])

    def test_a_wrong_ranked_value_differs(self):
        loki = {**self.LOKI, ("pricing-svc", "warn"): {10: 6}}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("ranked values at 10", row["loki_diffs"][0])

    def test_a_missing_step_differs(self):
        loki = {("pricing-svc", "info"): {10: 20}, ("pricing-svc", "warn"): {10: 5}}  # nothing at step 20
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("timestamps differ", row["loki_diffs"][0])

    def test_an_extra_ranked_series_at_a_step_differs(self):
        loki = {**self.LOKI, ("pricing-svc", "error"): {10: 5, 20: 4}}  # three series at step 10 against two
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("ranked series at 10", row["loki_diffs"][0])

    def test_the_base_and_the_pr_run_the_same_code_so_their_series_must_match(self):
        swapped = {**self.PROXY, ("pricing-svc", "warn"): self.PROXY[("pricing-svc", "error")]}
        del swapped[("pricing-svc", "error")]
        code, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(swapped))
        self.assertEqual(code, 1)
        self.assertIn("series sets differ", row["main_pr_diffs"][0])

    def test_a_dropped_label_differs(self):
        # without (level): the proxy must keep the labels the series carry, whichever tied series it picks
        no_level = ranked_capture(self.LOKI)
        for frame in no_level["records"][0]["response"]["results"]["A"]["frames"]:
            frame["schema"]["fields"][1]["labels"].pop("level")
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), no_level)
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("label names differ", row["loki_diffs"][0])

    def test_a_frame_meta_difference_of_a_series_both_sides_have_differs(self):
        loki = ranked_capture(self.LOKI)
        for frame in loki["records"][0]["response"]["results"]["A"]["frames"]:
            frame["schema"]["fields"][1]["config"] = {"interval": 60000}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("schema or meta", row["loki_diffs"][0])

    def test_an_error_answer_is_not_a_ranked_answer(self):
        err = ranked_capture(self.PROXY)
        err["records"][0]["response"] = {"results": {"A": {"error": "boom"}}}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), err)
        self.assertEqual(len(row["loki_diffs"]), 1)


class OneSidedTest(unittest.TestCase):
    def test_only_an_extracted_field_breakdown_is_explained(self):
        key = ("query", "A", 'sum by (service_name_extracted) (count_over_time({service_name="x"} | service_name_extracted!="" [$__auto]))')
        self.assertIn("_extracted", compare.explain_one_sided(key))
        self.assertIsNone(compare.explain_one_sided(("query", "A", 'sum by (k8s_pod_name) (count_over_time({service_name="x"} [$__auto]))')))
        self.assertIsNone(compare.explain_one_sided(("resource", "/api/datasources/uid/*/resources/labels")))


class NondeterministicTest(unittest.TestCase):
    def test_only_patterns_resources_are_history_dependent(self):
        import compare
        self.assertTrue(compare.nondeterministic("resource /api/datasources/uid/*/resources/patterns?x: body differs"))
        self.assertFalse(compare.nondeterministic("query ('A', 'sum(...)'): value differs"))
        self.assertFalse(compare.nondeterministic("resource /api/datasources/uid/*/resources/labels: body differs"))

class ConfigurationTest(unittest.TestCase):
    def test_only_drilldown_limits_is_configuration(self):
        import compare
        self.assertTrue(compare.configuration("resource /api/datasources/uid/*/resources/drilldown-limits: body differs"))
        self.assertFalse(compare.configuration("resource /api/datasources/uid/*/resources/detected_fields?x: body differs"))


if __name__ == "__main__":
    unittest.main()
