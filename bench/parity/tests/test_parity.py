"""Unit tests for the differential runner: corpus rewriting, semantic diff, clustering."""
import copy
import gzip
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

    def test_non_success_error_type_and_presence_are_part_of_the_answer(self):
        for status in (400, 422, 500):
            for left, right in (({"errorType": "bad_data"}, {"errorType": "wrong"}),
                                ({"errorType": "bad_data"}, {}), ({}, {"errorType": "bad_data"}),
                                ({"errorType": None}, {}), ({}, {"errorType": None})):
                with self.subTest(status=status, left=left, right=right):
                    facets = differ.diff("query", status, {"error": "parse error", **left},
                                         status, {"error": "parse error", **right})
                    self.assertEqual([facet["kind"] for facet in facets], ["error-type"])
        facets = differ.diff("query", 400, {"errorType": "bad_data", "error": "one"},
                             400, {"errorType": "wrong", "error": "two"})
        self.assertEqual([facet["kind"] for facet in facets], ["error-text", "error-type"])

    def test_error_type_comparison_retains_plain_errors_and_text_normalization(self):
        for left, right in (("parse error.\n", "parse error"),
                            ({"_text": "parse  error."}, {"error": "parse error"}),
                            ({"errorType": "bad_data", "error": "parse  error."},
                             {"errorType": "bad_data", "error": "parse error"})):
            with self.subTest(left=left, right=right):
                self.assertEqual(differ.diff("query", 400, left, 400, right), [])

    def test_documented_deviations_marked(self):
        loki = {"fields": [{"label": "service_extracted", "type": "string"}, {"label": "x", "type": "int"}]}
        proxy = {"fields": [{"label": "service", "type": "string"}, {"label": "x", "type": "int"}]}
        facets = differ.diff("detected_fields", 200, loki, 200, proxy)
        self.assertEqual(facets[0]["documented"], differ.DOC_DETECTED_FIELDS_SERVICE)
        stats = differ.diff("index_stats", 200, {"streams": 1, "entries": 2, "bytes": 3, "chunks": 1},
                            200, {"streams": 1, "entries": 2, "bytes": 4, "chunks": 0})
        self.assertTrue(all(f.get("documented") == differ.DOC_INDEX_STATS_BYTES for f in stats))

    def test_detected_fields_exemption_covers_only_the_service_name(self):
        loki = {"fields": [{"label": "service_name_extracted"}, {"label": "level_extracted"}]}
        facets = differ.diff("detected_fields", 200, loki, 200, {"fields": [{"label": "service.name"}]})
        documented = [f for f in facets if f.get("documented")]
        plain = [f for f in facets if not f.get("documented")]
        self.assertEqual([f["example"] for f in documented], ["['service_name_extracted']"])
        self.assertEqual([f["detail"] for f in plain], ["loki-only *_extracted"])

    def actual_metadata(self, endpoint):
        path = os.path.join(HERE, "..", "..", "ab", "tests", "fixtures", "loki-metadata-protocol.json")
        with open(path) as source:
            rows = json.load(source)["responses"]
        return json.loads(next(row["body"] for row in rows if row["path"].endswith("/" + endpoint)))

    def test_recorded_empty_detected_fields_object_is_preserved_without_unwrapping_other_shapes(self):
        # semantics/detected-fields-empty-answer records Loki's exact {} reply.
        for right in ({}, {"fields": []}):
            self.assertEqual(differ.diff("detected_fields", 200, {}, 200, right), [])
        for wrong in ({"data": []}, {"fields": None}, {"fields": {}}, {"unexpected": []}):
            with self.subTest(wrong=wrong):
                self.assertTrue(differ.diff("detected_fields", 200, {}, 200, wrong))

    def test_detected_fields_limit_presence_value_and_type_remain_part_of_the_contract(self):
        old_proxy = {"data": [], "fields": [], "limit": 1000, "status": "success"}
        self.assertTrue(differ.diff("detected_fields", 200, {}, 200, old_proxy))
        self.assertTrue(differ.diff("detected_fields", 200, old_proxy, 200, {}))
        self.assertTrue(differ.diff("detected_fields", 200, {"fields": [], "limit": 1000},
                                   200, {"fields": [], "limit": 999}))
        self.assertEqual(differ.diff("detected_fields", 200, old_proxy, 200, copy.deepcopy(old_proxy)), [])
        for value in (None, True, "1000", -1, 1.5):
            bad = {"fields": [], "limit": value}
            with self.subTest(value=value):
                facets = differ.diff("detected_fields", 200, bad, 200, copy.deepcopy(bad))
                self.assertTrue(facets)
                self.assertFalse(any(f.get("documented") for f in facets))

    def test_actual_detected_field_attributes_are_compared(self):
        answer = self.actual_metadata("detected_fields")
        for key, value in (("jsonPath", ["wrong_path"]), ("jsonPath", None), ("jsonPath", "wrong_path"),
                           ("parsers", ["logfmt"]), ("parsers", "json"), ("type", "int"), ("cardinality", 999)):
            changed = copy.deepcopy(answer)
            changed["fields"][0][key] = value
            with self.subTest(key=key, value=value):
                facets = differ.diff("detected_fields", 200, answer, 200, changed)
                self.assertTrue(any(not f.get("documented") for f in facets))
        changed = copy.deepcopy(answer)
        changed["fields"][0].pop("jsonPath")
        self.assertTrue(differ.diff("detected_fields", 200, answer, 200, changed))
        answer["fields"][0]["cardinality"] = 0
        changed = copy.deepcopy(answer)
        changed["fields"][0]["cardinality"] = 999
        self.assertTrue(differ.diff("detected_fields", 200, answer, 200, changed))

    def test_service_owner_policy_requires_exact_counterparts_and_compares_attributes(self):
        field = copy.deepcopy(self.actual_metadata("detected_fields")["fields"][0])
        field.update(label="service_name_extracted", jsonPath=["service", "name"])
        answer = {"fields": [field]}
        for label in ("service", "service.name"):
            counterpart = copy.deepcopy(field)
            counterpart["label"] = label
            candidate = {"fields": [counterpart]}
            with self.subTest(label=label):
                facets = differ.diff("detected_fields", 200, answer, 200, candidate)
                self.assertTrue(facets)
                self.assertTrue(all(f.get("documented") == differ.DOC_DETECTED_FIELDS_SERVICE for f in facets))
            for key, value in (("jsonPath", ["service"]), ("parsers", ["logfmt"]),
                               ("type", "int"), ("cardinality", 999)):
                changed = copy.deepcopy(candidate)
                changed["fields"][0][key] = value
                with self.subTest(label=label, key=key):
                    self.assertTrue(any(not f.get("documented")
                                        for f in differ.diff("detected_fields", 200, answer, 200, changed)))
        for left, right in ((answer, {"fields": []}), ({"fields": []}, {"fields": [counterpart]}),
                            (answer, {"fields": [{**field, "label": "service.other"}]}),
                            (answer, {"fields": [{**field, "label": "service"}, {**field, "label": "service.name"}]}),
                            ({"fields": [{**field, "label": "version_extracted"}]},
                             {"fields": [{**field, "label": "version"}]})):
            facets = differ.diff("detected_fields", 200, left, 200, right)
            self.assertTrue(any(not f.get("documented") for f in facets))

    def test_duplicate_metadata_identities_fail_closed_on_either_side(self):
        for endpoint, key in (("labels", "data"), ("detected_fields", "fields"),
                              ("detected_labels", "detectedLabels")):
            answer = self.actual_metadata(endpoint)
            changed = copy.deepcopy(answer)
            changed[key].append(copy.deepcopy(changed[key][0]))
            for left, right in ((answer, changed), (changed, answer), (changed, copy.deepcopy(changed))):
                with self.subTest(endpoint=endpoint):
                    facets = differ.diff(endpoint, 200, left, 200, right)
                    self.assertTrue(facets)
                    self.assertFalse(any(f.get("documented") for f in facets))

    def test_volume_shape_and_values_cannot_use_byte_accounting_to_hide_invalid_data(self):
        answer = self.actual_metadata("index/volume")
        for value in ("-1", "-0.5", "1_000", " 1000", "0x1p2"):
            changed = copy.deepcopy(answer)
            changed["data"]["result"][0]["value"][1] = value
            with self.subTest(value=value):
                facets = differ.diff("volume", 200, answer, 200, changed)
                self.assertEqual([f["kind"] for f in facets], ["shape"])
                self.assertFalse(any(f.get("documented") for f in facets))
        changed = copy.deepcopy(answer)
        changed["data"]["resultType"] = "matrix"
        for row in changed["data"]["result"]:
            row["values"] = [row.pop("value")]
        self.assertEqual([f["kind"] for f in differ.diff("volume", 200, answer, 200, changed)], ["result-type"])

    def test_shared_numeric_sample_grammar_keeps_decimal_equivalences(self):
        for value in ("1", "+1000", "01000", "1e+3", "-.5", "1.", "NaN", "+Inf", "-Inf"):
            with self.subTest(value=value):
                self.assertTrue(differ.valid_sample_value(value))
        for value in ("1_000", " 1000", "1000\n", "0x1p2", "1e999", "Infinity", 1, True, None):
            with self.subTest(value=value):
                self.assertFalse(differ.valid_sample_value(value))
        for value in ("-1", "NaN", "+Inf", "-Inf"):
            self.assertFalse(differ.valid_sample_value(value, nonnegative=True))

    def test_metric_schema_rejects_duplicate_identities_and_points_without_changing_query_special_values(self):
        answer = matrix(({"app": "api"}, [[1000, "10"], [1060, "20"]]))
        for duplicate_series in (True, False):
            changed = copy.deepcopy(answer)
            if duplicate_series:
                changed["data"]["result"].append(copy.deepcopy(changed["data"]["result"][0]))
            else:
                changed["data"]["result"][0]["values"].append([1000, "10"])
            with self.subTest(duplicate_series=duplicate_series):
                self.assertFalse(differ.valid_metric_result(changed))
                for left, right in ((answer, changed), (changed, answer), (changed, copy.deepcopy(changed))):
                    self.assertTrue(differ.diff("volume_range", 200, left, 200, right))
        for value in ("NaN", "+Inf", "-Inf"):
            special = matrix(({"app": "api"}, [[1000, value]]))
            self.assertTrue(differ.valid_metric_result(special))
            self.assertFalse(differ.valid_volume_result(special))

    def test_detected_field_cardinality_keeps_existing_sketch_tolerance(self):
        answer = self.actual_metadata("detected_fields")
        answer["fields"][0]["cardinality"] = 100
        changed = copy.deepcopy(answer)
        changed["fields"][0]["cardinality"] = 104
        self.assertEqual(differ.diff("detected_fields", 200, answer, 200, changed), [])
        changed["fields"][0]["cardinality"] = 106
        self.assertTrue(differ.diff("detected_fields", 200, answer, 200, changed))

    def test_volume_exemption_covers_bytes_only(self):
        a = matrix(({"service_name": "a"}, [[60, "100"], [120, "100"]]))
        b = matrix(({"service_name": "a"}, [[60, "90"]]))
        facets = {f["kind"]: f for f in differ.diff("volume_range", 200, a, 200, b)}
        self.assertEqual(facets["values"]["documented"], differ.DOC_VOLUME_BYTES)
        self.assertNotIn("documented", facets["points"])
        fewer = differ.diff("volume", 200, matrix(({"a": "1"}, [[60, "1"]]), ({"a": "2"}, [[60, "1"]])),
                            200, matrix(({"a": "1"}, [[60, "1"]])))
        self.assertFalse(any(f.get("documented") for f in fewer))

    def test_volume_requires_a_metric_result_instead_of_raw_volume_rows(self):
        vector = {"status": "success", "data": {"resultType": "vector", "result": [
            {"metric": {"app": "api"}, "value": [1795378740, "984064"]}], "stats": {"summary": {}}}}
        invalid = [{"data": {"volumes": [{"name": "api", "volume": 1}]}},
                   {"data": {"volumes": [{"name": "other", "volume": 999}]}},
                   {"data": {"volumes": []}}, {}, {"data": {}},
                   {"data": {"resultType": "vector", "result": {}}},
                   {"data": {"resultType": "streams", "result": []}}]
        for value in ([], [1], ["bad-time", "1"], [1, "bad-value"]):
            invalid.append({"data": {"resultType": "vector", "result": [{"metric": {}, "value": value}]}})
        for endpoint in ("volume", "volume_range"):
            for bad in invalid:
                for left, right in ((vector, bad), (bad, vector), (bad, bad)):
                    with self.subTest(endpoint=endpoint, left=left, right=right):
                        facets = differ.diff(endpoint, 200, left, 200, right)
                        self.assertEqual([f["kind"] for f in facets], ["shape"])
                        self.assertFalse(any(f.get("documented") for f in facets))
        self.assertEqual(differ.diff("volume", 200, vector, 200, copy.deepcopy(vector)), [])
        changed = copy.deepcopy(vector)
        changed["data"]["result"][0]["value"][1] = "1"
        self.assertEqual(differ.diff("volume", 200, vector, 200, changed)[0]["documented"], differ.DOC_VOLUME_BYTES)
        changed["data"]["result"][0]["value"][0] += 1
        self.assertTrue(any(f["kind"] == "points" and not f.get("documented")
                            for f in differ.diff("volume", 200, vector, 200, changed)))

    def recorded_patterns(self):
        path = os.path.join(HERE, "fixtures", "loki-pattern-samples-3.7.10.json.gz")
        with gzip.open(path, "rt") as source:
            return json.load(source)["response"]

    def test_recorded_pattern_samples_are_order_independent_but_counts_and_times_are_not(self):
        answer = self.recorded_patterns()
        reordered = copy.deepcopy(answer)
        reordered["data"].reverse()
        for entry in reordered["data"]:
            entry["samples"].reverse()
        self.assertEqual(differ.diff("patterns", 200, answer, 200, reordered), [])
        for mutation in ("count", "timestamp", "text", "level", "duplicate-entry", "duplicate-timestamp"):
            changed = copy.deepcopy(answer)
            if mutation == "count":
                sample = next(s for row in changed["data"] for s in row["samples"] if s[1] == 1)
                sample[1] = 999
            if mutation == "timestamp": changed["data"][0]["samples"][0][0] += 1
            if mutation == "text": changed["data"][0]["pattern"] += " changed"
            if mutation == "level": changed["data"][0]["level"] += "-wrong"
            if mutation == "duplicate-entry": changed["data"].append(copy.deepcopy(changed["data"][0]))
            if mutation == "duplicate-timestamp": changed["data"][0]["samples"].append(changed["data"][0]["samples"][0][:])
            with self.subTest(mutation=mutation):
                facets = differ.diff("patterns", 200, answer, 200, changed)
                self.assertTrue(facets)
                self.assertTrue(all(f["kind"] == "patterns" and not f.get("documented") for f in facets))

    def test_malformed_pattern_answers_cannot_match_as_empty_sets(self):
        for bad in ({}, {"data": {}}, {"data": [None]}, {"data": [{"pattern": "a", "level": "info"}]}):
            with self.subTest(bad=bad):
                facets = differ.diff("patterns", 200, bad, 200, copy.deepcopy(bad))
                self.assertTrue(facets)
                self.assertTrue(all(f["kind"] == "patterns" and not f.get("documented") for f in facets))

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
