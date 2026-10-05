"""Unit tests for the differential runner: corpus rewriting, semantic diff, clustering."""
import json
import os
import socket
import sys
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import cluster  # noqa: E402
import corpus  # noqa: E402
import differ  # noqa: E402
import run  # noqa: E402


def streams(*rows, result_type="streams"):
    """rows: (labels, ts, line[, meta])."""
    result = []
    for row in rows:
        value = [row[1], row[2]] + ([row[3]] if len(row) > 3 else [])
        result.append({"stream": row[0], "values": [value]})
    return {"status": "success", "data": {"resultType": result_type, "result": result}}


def matrix(*series):
    return {"status": "success", "data": {"resultType": "matrix",
                                          "result": [{"metric": m, "values": v} for m, v in series]}}


class CorpusTest(unittest.TestCase):
    def test_selector_rewrite_keeps_pipeline_and_skips_strings(self):
        q = '{app="foo"} | json | line_format "{{.a}} {x=\\"y\\"}" | b="{c=\\"d\\"}"'
        out = corpus.rewrite(q, force=True)
        self.assertTrue(out.startswith('{service_name="api-gateway"} | json | line_format'))
        self.assertIn('{{.a}}', out)

    def test_selector_inside_metric_and_binary_rewritten_per_parser(self):
        out = corpus.rewrite('sum(rate({job="a"} | logfmt [1m])) / sum(rate({job="b"}[1m]))', force=True)
        self.assertEqual(out.count('{service_name="payment-service"}'), 2)

    def test_seeded_selector_kept_unless_forced(self):
        self.assertEqual(corpus.rewrite('{namespace="prod", level="error"}'), '{namespace="prod", level="error"}')
        self.assertEqual(corpus.rewrite('{app="compat-anchoring-123"}'), '{namespace="prod"}')
        self.assertEqual(corpus.rewrite('{namespace="tns"} | json'), '{service_name="api-gateway"} | json')

    def test_extract_skips_fixtures_and_format_strings(self, tmp=None):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix="_test.go", delete=False) as handle:
            handle.write('q := `sum(rate({app="foo"}[1m]))`\n'
                         'x := "downstream<count_over_time({a=\\"b\\"}[1m]), shard=<nil>>"\n'
                         'f := fmt.Sprintf(`{app="%s"}`, x)\n'
                         'j := `{"a": 1}`\n')
        try:
            found = corpus.extract_strings([handle.name])
        finally:
            os.unlink(handle.name)
        self.assertEqual(found, {'sum(rate({app="foo"}[1m]))'})

    def test_repo_queries_have_no_grafana_variables(self):
        self.assertFalse([e for e in corpus.repo_queries() if "$__" in e["params"]["query"]])

    def test_query_entries_metric_gets_range_and_instant(self):
        kinds = [e["endpoint"] for e in corpus.query_entries("x", 'sum(rate({a="b"}[1m]))')]
        self.assertEqual(kinds, ["query_range", "query"])
        self.assertEqual([e["endpoint"] for e in corpus.query_entries("x", '{a="b"} |= "x"')], ["query_range"])


class DifferTest(unittest.TestCase):
    def test_equal_streams_in_any_order_are_the_same(self):
        a = streams(({"app": "x"}, "1", "l1"), ({"app": "y"}, "2", "l2"))
        b = streams(({"app": "y"}, "2", "l2"), ({"app": "x"}, "1", "l1"))
        self.assertEqual(differ.diff("query_range", 200, a, 200, b), [])

    def test_missing_parse_error_labels_cluster_by_name(self):
        a = streams(({"app": "x", "__error__": "JSONParserErr", "__error_details__": "bad"}, "1", "l1"))
        b = streams(({"app": "x"}, "1", "l1"))
        facets = differ.diff("query_range", 200, a, 200, b, stream_labels=["app"])
        self.assertEqual([f["detail"] for f in facets], ["loki-only __error__,__error_details__"])

    def test_parsed_keys_generalise_to_one_class(self):
        a = streams(({"app": "x", "method": "GET"}, "1", "l1"))
        b = streams(({"app": "x", "status": "200"}, "1", "l1"))
        details = sorted(f["detail"] for f in differ.diff("query_range", 200, a, 200, b, ["app"]))
        self.assertEqual(details, ["loki-only <line-or-metadata-key>", "proxy-only <line-or-metadata-key>"])

    def test_limit_boundary_ties_are_ignored(self):
        a = streams(({"a": "1"}, "5", "new"), ({"a": "1"}, "3", "tie-a"))
        b = streams(({"a": "1"}, "5", "new"), ({"a": "1"}, "3", "tie-b"))
        self.assertEqual(differ.diff("query_range", 200, a, 200, b, limit=2), [])
        self.assertTrue(differ.diff("query_range", 200, a, 200, b, limit=100))

    def test_entry_count_classified(self):
        a = streams(({"a": "1"}, "5", "x"), ({"a": "1"}, "4", "y"))
        facets = differ.diff("query_range", 200, a, 200, streams())
        self.assertEqual(facets[0]["detail"], "proxy-empty")

    def test_categories_compared_per_entry(self):
        a = streams(({"a": "1"}, "5", "x", {"structuredMetadata": {"trace_id": "t"}, "parsed": {"k": "v"}}))
        b = streams(({"a": "1"}, "5", "x", {"structuredMetadata": {"trace_id": "t", "k": "v"}}))
        details = [f["detail"] for f in differ.diff("query_range", 200, a, 200, b, ["a"])]
        self.assertIn("loki-only parsed <line-or-metadata-key>", details)
        self.assertIn("proxy-only structuredMetadata <line-or-metadata-key>", details)

    def test_metric_values_and_series(self):
        a = matrix(({"level": "info"}, [[60, "2"], [120, "3"]]))
        b = matrix(({"level": "info"}, [[60, "2"], [120, "4"]]))
        facets = differ.diff("query_range", 200, a, 200, b)
        self.assertEqual(facets[0]["kind"], "values")
        self.assertIn("proxy higher", facets[0]["detail"])
        c = matrix(({"level": "info"}, [[60, "2"]]), ({"level": "warn"}, [[60, "1"]]))
        self.assertEqual(differ.diff("query_range", 200, c, 200, matrix(({"level": "info"}, [[60, "2"]])))[0]["detail"],
                         "proxy-fewer")

    def test_non_numeric_proxy_sample_is_a_shape_difference(self):
        a = matrix(({"l": "a"}, [[60, "1"]]))
        b = matrix(({"l": "a"}, [[60, ""]]))
        self.assertEqual(differ.diff("query_range", 200, a, 200, b)[0]["detail"], "proxy sample value is not a number")

    def test_clamp_ranges(self):
        self.assertEqual(corpus.clamp_ranges('rate({a="b"}[5h]) / rate({a="b"}[30m])'),
                         'rate({a="b"}[1h]) / rate({a="b"}[30m])')

    def test_status_and_error_text(self):
        self.assertEqual(differ.diff("query", 400, {"_text": "parse error"}, 200, {"data": {}})[0]["kind"], "status")
        self.assertEqual(differ.diff("query", 400, {"_text": "parse error at 1"}, 400, {"error": "parse error at 1"}), [])
        self.assertEqual(differ.diff("query", 400, {"_text": "a"}, 400, {"error": "b"})[0]["kind"], "error-text")

    def test_documented_deviations_marked(self):
        loki = {"fields": [{"label": "service_extracted", "type": "string"}, {"label": "x", "type": "int"}]}
        proxy = {"fields": [{"label": "x", "type": "int"}]}
        facets = differ.diff("detected_fields", 200, loki, 200, proxy)
        self.assertEqual(facets[0]["documented"], differ.DOC_DETECTED_FIELDS_SERVICE)
        stats = differ.diff("index_stats", 200, {"streams": 1, "entries": 2, "bytes": 3, "chunks": 1},
                            200, {"streams": 1, "entries": 2, "bytes": 4, "chunks": 0})
        self.assertTrue(all(f.get("documented") == differ.DOC_INDEX_STATS_BYTES for f in stats))

    def test_detected_fields_exemption_covers_only_the_service_name(self):
        loki = {"fields": [{"label": "service_name_extracted"}, {"label": "level_extracted"}]}
        facets = differ.diff("detected_fields", 200, loki, 200, {"fields": []})
        documented = [f for f in facets if f.get("documented")]
        plain = [f for f in facets if not f.get("documented")]
        self.assertEqual([f["example"] for f in documented], ["['service_name_extracted']"])
        self.assertEqual([f["detail"] for f in plain], ["loki-only *_extracted"])

    def test_volume_exemption_covers_bytes_only(self):
        a = matrix(({"service_name": "a"}, [[60, "100"], [120, "100"]]))
        b = matrix(({"service_name": "a"}, [[60, "90"]]))
        facets = {f["kind"]: f for f in differ.diff("volume_range", 200, a, 200, b)}
        self.assertEqual(facets["values"]["documented"], differ.DOC_VOLUME_BYTES)
        self.assertNotIn("documented", facets["points"])
        fewer = differ.diff("volume", 200, matrix(({"a": "1"}, [[60, "1"]]), ({"a": "2"}, [[60, "1"]])),
                            200, matrix(({"a": "1"}, [[60, "1"]])))
        self.assertFalse(any(f.get("documented") for f in fewer))

    def test_documented_ids_are_registry_cases(self):
        for case in differ.DOCUMENTED:
            self.assertTrue(os.path.exists(os.path.join(HERE, "..", "..", "..", "conformance", "registry", "cases",
                                                        case + ".yaml")), case)

    def test_duplicate_label_sets_are_counted(self):
        a = matrix(({"l": "a"}, [[60, "1"]]))
        b = matrix(({"l": "a"}, [[60, "1"]]), ({"l": "a"}, [[60, "1"]]))
        self.assertEqual(differ.diff("query_range", 200, a, 200, b)[0]["detail"],
                         "proxy returns one label set as several series")

    def test_entry_order_follows_direction(self):
        backward = streams(({"a": "1"}, "5", "x"), result_type="streams")
        backward["data"]["result"][0]["values"] = [["5", "x"], ["3", "y"]]
        forward = json.loads(json.dumps(backward))
        forward["data"]["result"][0]["values"] = [["3", "y"], ["5", "x"]]
        self.assertEqual(differ.diff("query_range", 200, backward, 200, backward), [])
        facets = differ.diff("query_range", 200, backward, 200, forward)
        self.assertEqual(facets[0]["kind"], "order")
        self.assertEqual(differ.diff("query_range", 200, forward, 200, forward, direction="forward"), [])
        self.assertEqual(differ.diff("tail", 200, backward, 200, forward), [])

    def test_scalar_compared_as_numbers(self):
        a = {"data": {"resultType": "scalar", "result": [1700000000, "1"]}}
        b = {"data": {"resultType": "scalar", "result": [1700000000.0, "1.0"]}}
        self.assertEqual(differ.diff("query", 200, a, 200, b), [])
        c = {"data": {"resultType": "scalar", "result": [1700000000, "2"]}}
        self.assertEqual(differ.diff("query", 200, a, 200, c)[0]["detail"], "scalar differs")

    def test_vacuous(self):
        self.assertTrue(differ.vacuous("query_range", 200, streams(), 200, streams()))
        self.assertFalse(differ.vacuous("query_range", 200, streams(({"a": "1"}, "1", "x")), 200, streams()))


class ClusterTest(unittest.TestCase):
    def row(self, query, detail, endpoint="query_range", encoding="categorize", source="seeded", documented=None):
        f = differ.facet("labels", detail, "ex", documented)
        return {"id": query, "source": source, "endpoint": endpoint, "encoding": encoding, "params": {"query": query},
                "verdict": "diff", "facets": [f]}

    def test_same_signature_clusters_and_ranks_by_impact_then_queries(self):
        rows = [self.row('{a="1"} | json', "loki-only __error__"), self.row('{a="2"} | json', "loki-only __error__"),
                self.row('{a="3"}', "x", endpoint="format_query"),
                self.row('{a="4"}', "y", endpoint="detected_fields", documented=differ.DOC_DETECTED_FIELDS_SERVICE)]
        clusters = cluster.cluster(rows)
        self.assertEqual(clusters[0]["queries"], 2)
        self.assertEqual(clusters[0]["impact"], "explore-visible")
        self.assertEqual(clusters[0]["rank"], 1)
        self.assertEqual(clusters[1]["impact"], "api-only")
        self.assertTrue(clusters[-1]["documented"])
        self.assertNotIn("rank", clusters[-1])

    def test_one_cluster_per_request_by_primary_facet(self):
        row = self.row('sum(rate({a="1"}[1m]))', "x")
        row["facets"] = [differ.facet("values", "values differ (proxy higher)", "v"),
                         differ.facet("status", "loki 200 proxy 400", "loki - | proxy cannot parse \"abc\" at 12")]
        clusters = cluster.cluster([row])
        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["signature"],
                         ["query_range", "sum", "status", "loki 200 proxy 400: cannot parse Q at N"])
        self.assertEqual(clusters[0]["also"], {"values: values differ (proxy higher)": 1})

    def test_documented_facet_is_primary_only_when_alone(self):
        f1 = differ.facet("stats", "bytes differ", "b", differ.DOC_INDEX_STATS_BYTES)
        f2 = differ.facet("stats", "entries differ", "e")
        self.assertEqual(cluster.primary([f1, f2])[0]["detail"], "entries differ")
        self.assertEqual(cluster.primary([f1])[0]["detail"], "bytes differ")

    def test_signature_separates_filter_kinds(self):
        def row(query):
            return {"id": query, "source": "s", "endpoint": "query_range", "params": {"query": query},
                    "verdict": "diff", "facets": [differ.facet("entries", "proxy-empty", "e")]}
        sigs = {cluster.signature_of(row(q))[1] for q in ('{a="1"} !> "<_> x"', '{a="1"} | b!=""', '{a="1"} |= "x"')}
        self.assertEqual(sigs, {"logs[pattern-filter]", "logs[label-filter]", "logs[line-filter]"})
        doc = row('{a="1"}')
        doc["facets"] = [differ.facet("stats", "bytes differ", "e", differ.DOC_INDEX_STATS_BYTES)]
        self.assertTrue(cluster.signature_of(doc)[3].endswith(f"[{differ.DOC_INDEX_STATS_BYTES}]"))

    def test_shape(self):
        self.assertEqual(cluster.shape('sum by (a) (count_over_time({a="1"} | json | b!="" [5m]))'),
                         "sum[json,label-filter]")
        self.assertEqual(cluster.shape('{a="1"} |= "x"'), "logs[line-filter]")


class ConfirmTest(unittest.TestCase):
    def test_signature_that_does_not_reproduce_is_blocked(self):
        def row(i, detail):
            return {"id": i, "source": "s", "endpoint": "query_range", "encoding": "plain", "params": {"query": "{a=\"1\"}"},
                    "verdict": "diff", "facets": [differ.facet("labels", detail, "e")]}
        rows = [row("a", "stable"), row("b", "flaky")]
        corpus = [{"id": "a"}, {"id": "b"}]
        again = {"a": [row("a", "stable")], "b": [dict(row("b", "flaky"), verdict="same", facets=[])]}
        original = run.run_one
        run.run_one = lambda item, cfg: again[item["id"]]
        try:
            blocked = run.confirm(rows, corpus, {}, workers=1)
        finally:
            run.run_one = original
        self.assertEqual(blocked, 1)
        self.assertTrue(rows[0]["confirmed"])
        self.assertEqual(rows[1]["verdict"], "blocked")


class RunTest(unittest.TestCase):
    def test_unknown_restart_count_is_not_proof(self):
        good = {"loki_ready": 200, "proxy_ready": 200, "vl_ready": 200, "lines_loki": 5, "lines_vl": 5,
                "slices_differing": [], "services_listed": 3, "services_in_loki_metric": 3,
                "vl_restarts": 0, "loki_restarts": 0}
        self.assertTrue(run.healthy(good))
        self.assertFalse(run.healthy(dict(good, vl_restarts=None)))
        self.assertFalse(run.healthy(dict(good, loki_restarts=None)))
        self.assertFalse(run.healthy(dict(good, services_in_loki_metric=2)))

    def test_log_scan_gates_only_on_backend_failures(self):
        import tempfile
        lines = [{"body": "upstream_request", "http.response.status_code": 200},
                 {"body": "upstream_request", "http.response.status_code": 422},
                 {"body": "upstream_request", "http.response.status_code": 499},
                 {"body": "unwrap stats fast path failed, falling back to full-fetch", "err": "stats_query_range 422: x"}]
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as handle:
            handle.write("\n".join(json.dumps(x) for x in lines) + "\nnot json\n")
        try:
            before = run.scan_log(handle.name)
            self.assertEqual((before["upstream_4xx"], before["upstream_499"], before["fallback_after_4xx"]), (1, 1, 1))
            with open(handle.name, "a") as more:
                more.write(json.dumps({"body": "upstream_request", "http.response.status_code": 503}) + "\n")
                more.write(json.dumps({"body": "x falling back", "err": "stats_query_range 502: down"}) + "\n")
            self.assertEqual(run.log_rose(before, run.scan_log(handle.name)),
                             {"upstream_5xx": 1, "fallback_after_5xx": 1})
        finally:
            os.unlink(handle.name)

    def test_read_frame_gives_up_at_the_deadline(self):
        left, right = socket.socketpair()
        try:
            left.sendall(bytes([0x81, 10]) + b"abc")  # a 10-byte text frame of which 3 bytes arrive
            right.settimeout(0.05)
            t0 = time.time()
            frame, _ = run.read_frame(right, b"", time.time() + 0.3)
            self.assertIsNone(frame)
            self.assertLess(time.time() - t0, 2)
        finally:
            left.close()
            right.close()

    def test_rediff_recomputes_a_stored_pair(self):
        item = {"id": "x", "source": "s", "endpoint": "query", "transport": "api", "params": {}}
        record = {"id": "x", "encoding": "plain", "endpoint": "query", "params": {"query": "q"},
                  "loki": [200, {"data": {"resultType": "scalar", "result": [1, "1"]}}, {}],
                  "proxy": [200, {"data": {"resultType": "scalar", "result": [1.0, "1.0"]}}, {}]}
        row = run.rediff(item, record, {"stream_labels": []})
        self.assertEqual(row["verdict"], "same")


class FramesTest(unittest.TestCase):
    def test_log_frame_becomes_categorised_streams(self):
        result = {"frames": [{"schema": {"fields": [{"name": "labels"}, {"name": "Time"}, {"name": "Line"},
                                                    {"name": "tsNs"}, {"name": "labelTypes"}]},
                              "data": {"values": [[{"app": "x", "k": "v"}], [1], ["line"], ["1000000"],
                                                  [{"app": "I", "k": "P"}]]}}]}
        body = run.frames_body(result)
        row = body["data"]["result"][0]
        self.assertEqual(row["stream"], {"app": "x"})
        self.assertEqual(row["values"][0][2]["parsed"], {"k": "v"})

    def test_metric_frame_becomes_matrix(self):
        result = {"frames": [{"schema": {"fields": [{"type": "time"}, {"type": "number", "labels": {"l": "a"}}]},
                              "data": {"values": [[60000, 120000], [1, None]]}}]}
        body = run.frames_body(result)
        self.assertEqual(body["data"]["result"], [{"metric": {"l": "a"}, "values": [[60.0, "1"]]}])

    def test_resource_endpoint(self):
        self.assertEqual(run.resource_endpoint("detected_field/x/values?query=a"), "detected_field_values")
        self.assertEqual(run.resource_endpoint("index/volume_range?x"), "volume_range")
        self.assertEqual(run.resource_endpoint("labels?start=1"), "labels")


if __name__ == "__main__":
    unittest.main()
