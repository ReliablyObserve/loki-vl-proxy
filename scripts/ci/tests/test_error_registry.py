"""conformance/scripts/error_registry.py: one registry item per Loki error message, the route x error kind matrix and the
scores, built from internal/lokierr, the recorded Loki answers and the error-layer gates."""
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import error_registry  # noqa: E402


class InRepo(unittest.TestCase):
    """The script reads repository-relative paths."""

    def setUp(self):
        self._cwd = os.getcwd()
        os.chdir(REPO)
        self.addCleanup(os.chdir, self._cwd)
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def patched_json(self, path, change):
        data = json.loads(Path(path).read_text())
        change(data)
        out = Path(self._tmp.name) / (Path(path).name + str(len(os.listdir(self._tmp.name))))
        out.write_text(json.dumps(data))
        return str(out)


class TemplateTest(unittest.TestCase):
    TEXTS = {"textFixed": "fixed text"}

    def test_go_expressions_render_as_templates(self):
        for expr, want in (
            ('"a"+name+"b"', "a<name>b"),
            ('"cannot parse "+strconv.Quote(raw)+" to a valid duration"', 'cannot parse "<raw>" to a valid duration'),
            ('"n="+strconv.Itoa(limit)', "n=<limit>"),
            ("textFixed", "fixed text"),
            ('err.Error()', "<err>"),
            ('"limit: "+FormatDuration(limit)+")"', "limit: <limit>)"),
            ('"x"+d.String()', "x<d>"),
        ):
            self.assertEqual(error_registry.template_of(expr, self.TEXTS), want, expr)

    def test_an_expression_that_is_not_a_concatenation_has_no_template(self):
        self.assertIsNone(error_registry.template_of("b.String(1, 2)", self.TEXTS))
        self.assertIsNone(error_registry.template_of("fmt.Sprintf(x)", self.TEXTS))


class MessagesTest(InRepo):
    def test_every_typed_message_is_parsed_with_its_constructor_and_status(self):
        messages = error_registry.parse_messages(Path(error_registry.MESSAGES_GO).read_text(), Path(error_registry.KINDS_GO).read_text())
        go = Path(error_registry.MESSAGES_GO).read_text()
        self.assertEqual(len(messages), len(re.findall(r'^\s*Msg\w+:\s*\{"', go, re.M)))
        by_id = {m["id"]: m for m in messages.values()}
        self.assertEqual(by_id["step-not-positive"]["status"], 400)
        self.assertEqual(by_id["canceled"]["status"], 499)
        self.assertEqual(by_id["method-not-allowed"]["status"], 405)
        self.assertEqual(by_id["method-not-allowed"]["shape"], "bodyless")
        self.assertEqual(by_id["limit-index-series"]["status"], 500)
        self.assertEqual(by_id["param-number"]["constructors"], ["ParamNumber"])
        self.assertIn("could not parse '<name>' parameter: <err>", by_id["param-number"]["templates"])
        self.assertIn("parse error : <detail>", by_id["parse"]["templates"])
        for message in messages.values():
            self.assertTrue(message["constructors"], message["id"] + " has no constructor")

    def test_every_recorded_body_fits_the_template_of_its_message(self):
        messages = error_registry.parse_messages(Path(error_registry.MESSAGES_GO).read_text(), Path(error_registry.KINDS_GO).read_text())
        by_id = {m["id"]: m for m in messages.values()}
        _, rows = error_registry.load_rows(error_registry.ANSWERS)
        checked = 0
        for row in rows:
            message = by_id.get(row.get("msg"))
            if not message or not message["templates"]:
                continue
            body = row["loki"]["body"]
            patterns = [re.escape(t).replace(re.escape("<"), "<").replace(">", ">") for t in message["templates"]]
            ok = any(re.fullmatch(re.sub(r"<\w+>", ".*", p), body.rstrip("\n"), re.S) for p in patterns)
            self.assertTrue(ok, f"{row['id']} ({message['id']}): {body!r} fits none of {message['templates']}")
            checked += 1
        self.assertGreater(checked, 150)


class GeneratedFilesTest(InRepo):
    def test_the_committed_outputs_are_what_the_generator_writes(self):
        files, items, doc, problems, spec = error_registry.outputs()
        self.assertEqual(problems, [])
        self.assertEqual(error_registry.status_problems(spec), [])
        for path, text in files.items():
            self.assertEqual(Path(path).read_text(), text, f"{path} is stale; run python3 conformance/scripts/error_registry.py")

    def test_every_message_has_exactly_one_item_and_no_item_is_orphaned(self):
        _, items, doc, _, _ = error_registry.outputs()
        messages = error_registry.parse_messages(Path(error_registry.MESSAGES_GO).read_text(), Path(error_registry.KINDS_GO).read_text())
        ids = {m["id"] for m in messages.values()}
        on_disk = {p.name[len("msg-"):-len(".yaml")] for p in Path(error_registry.ITEMS).glob("msg-*.yaml")}
        self.assertEqual(ids, on_disk)
        self.assertEqual({i for i in items if not i.startswith("unmapped-")}, ids)
        for item in items.values():
            self.assertIn(item["state"], ("matches", "differs", "documented", "recorded-not-comparable"), item["id"])

    def test_scores_follow_the_recorded_rows_and_the_known_differences(self):
        _, _, doc, _, _ = error_registry.outputs()
        s = doc["summary"]
        known = json.loads(Path(error_registry.KNOWN_DIFFS).read_text())
        self.assertEqual(s["matching_rows"], s["comparable_rows"] - len(known["rows"]))
        self.assertEqual(s["match_ratio"], round(s["matching_rows"] / s["comparable_rows"], 4))
        self.assertEqual(s["coverage_ratio"], round(s["covered_cells"] / s["cells"], 4))
        self.assertEqual(s["cells"], len(doc["cells"]))
        self.assertTrue(set(doc["e2e_rows"]) <= {r["id"] for r in error_registry.load_rows(error_registry.ANSWERS)[1]})

    def test_a_cell_is_covered_only_with_a_unit_gate_and_an_e2e_check(self):
        _, _, doc, _, _ = error_registry.outputs()
        for cell in doc["cells"]:
            if cell["covered"]:
                self.assertTrue(cell["unit_gate"] and cell["e2e"], cell)
                self.assertTrue(cell["recorded"] or cell["reason"], cell)
        tenant = [c for c in doc["cells"] if c["kind"] == "tenant" and c["route"] == "query_range"][0]
        self.assertFalse(tenant["covered"], "tenant answers are not recorded, so the cell may not count as covered")

    def test_every_combined_error_row_agrees_with_the_stage_ranks(self):
        _, _, _, problems, _ = error_registry.outputs()
        self.assertEqual([p for p in problems if "answered before" in p or "none of the request" in p], [])


class GateTest(InRepo):
    def run_build(self, **paths):
        with mock.patch.multiple(error_registry, **paths):
            return error_registry.build()

    def test_a_message_without_a_recording_or_a_reason_fails(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["no_recorded_answer"].pop("canceled"))
        problems = self.run_build(SPEC=spec)[3]
        self.assertTrue(any("message canceled has no recorded Loki answer and no entry" in p for p in problems), problems)

    def test_a_recorded_message_may_not_claim_a_reason(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["no_recorded_answer"].update({"parse": {"reason": "x", "tests": []}}))
        problems = self.run_build(SPEC=spec)[3]
        self.assertTrue(any("message parse has recorded rows" in p for p in problems), problems)

    def test_a_message_the_spec_does_not_stage_fails(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["message_stage"].pop("direction"))
        problems = self.run_build(SPEC=spec)[3]
        self.assertTrue(any("message direction has no stage" in p for p in problems), problems)

    def test_a_named_test_that_does_not_exist_fails(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["no_recorded_answer"]["canceled"].update({"tests": ["TestNoSuchGate"]}))
        problems = self.run_build(SPEC=spec)[3]
        self.assertTrue(any("TestNoSuchGate" in p for p in problems), problems)

    def test_a_known_difference_must_be_a_comparable_recorded_row(self):
        known = self.patched_json(error_registry.KNOWN_DIFFS, lambda d: d["rows"].update({"r9999": {}}))
        problems = self.run_build(KNOWN_DIFFS=known)[3]
        self.assertTrue(any("r9999 is not a recorded row" in p for p in problems), problems)

    def test_a_route_or_failure_mode_the_spec_does_not_classify_stops_the_generator(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["gate_mode_kinds"].pop("conn_reset"))
        with self.assertRaises(SystemExit) as raised:
            self.run_build(SPEC=spec)
        self.assertIn("conn_reset", str(raised.exception))
        spec = self.patched_json(error_registry.SPEC, lambda d: d["gate_route_map"].pop("index_stats"))
        with self.assertRaises(SystemExit) as raised:
            self.run_build(SPEC=spec)
        self.assertIn("index_stats", str(raised.exception))

    def test_a_recorded_error_with_no_message_and_no_rule_stops_the_generator(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["unmapped_messages"].pop(0))
        with self.assertRaises(SystemExit) as raised:
            self.run_build(SPEC=spec)
        self.assertIn("unmapped_messages", str(raised.exception))

    def test_a_trigger_no_rule_classifies_stops_the_generator(self):
        spec = self.patched_json(error_registry.SPEC, lambda d: d["trigger_kinds"].pop(0))
        with self.assertRaises(SystemExit) as raised:
            self.run_build(SPEC=spec)
        self.assertIn("trigger_kinds", str(raised.exception))

    def test_a_row_naming_an_unknown_message_fails(self):
        text = Path(error_registry.ANSWERS).read_text().split("\n")
        row = json.loads(text[1])
        row["msg"] = "no-such-message"
        path = Path(self._tmp.name) / "answers.jsonl"
        path.write_text("\n".join([text[0], json.dumps(row)] + text[2:]))
        problems = self.run_build(ANSWERS=str(path))[3]
        self.assertTrue(any("unknown message 'no-such-message'" in p for p in problems), problems)

    def test_a_combined_row_against_the_stage_ranks_fails(self):
        text = Path(error_registry.ORDER).read_text().split("\n")
        row = json.loads(text[1])
        row["wins"] = row["issues"][1]
        path = Path(self._tmp.name) / "order.jsonl"
        path.write_text("\n".join([text[0], json.dumps(row)] + text[2:]))
        problems = self.run_build(ORDER=str(path))[3]
        self.assertTrue(any("answered before" in p for p in problems), problems)


class RegressionTest(unittest.TestCase):
    @staticmethod
    def doc(cells, match=0.5, coverage=0.5):
        return {"cells": cells, "summary": {"match_ratio": match, "coverage_ratio": coverage}}

    def cell(self, **flags):
        base = {"route": "query", "kind": "parameters", "recorded": True, "proxy_matches": True, "unit_gate": True, "e2e": True, "covered": True}
        base.update(flags)
        return base

    def test_a_cell_that_loses_a_check_or_disappears_is_a_regression(self):
        base = json.dumps(self.doc([self.cell()]))
        self.assertEqual(error_registry.regressions(base, self.doc([self.cell()])), [])
        self.assertEqual(error_registry.regressions(base, self.doc([self.cell(proxy_matches=False)])), ["cell query x parameters lost proxy_matches"])
        self.assertEqual(error_registry.regressions(base, self.doc([self.cell(e2e=False, covered=False)])),
                         ["cell query x parameters lost e2e", "cell query x parameters lost covered"])
        self.assertEqual(error_registry.regressions(base, self.doc([])), ["cell query x parameters is gone"])

    def test_a_cell_that_gains_checks_and_a_new_cell_are_not_regressions(self):
        base = json.dumps(self.doc([self.cell(e2e=False, covered=False, proxy_matches=False)]))
        self.assertEqual(error_registry.regressions(base, self.doc([self.cell(), self.cell(kind="tenant")])), [])

    def test_a_fall_in_a_score_is_reported(self):
        base = json.dumps(self.doc([], match=0.5, coverage=0.5))
        out = error_registry.regressions(base, self.doc([], match=0.4, coverage=0.5))
        self.assertEqual(len(out), 1)
        self.assertIn("match_ratio fell from 0.5000 to 0.4000", out[0])


if __name__ == "__main__":
    unittest.main()
