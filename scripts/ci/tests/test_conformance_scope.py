"""Method-level scope of the conformance registry.

The proxy serves Loki's read API. A write method (POST, PUT, PATCH, DELETE) of an endpoint is
either listed in its state file's `out_of_scope_methods` with the owner decision, or is the
delete endpoint, which stays in scope. These tests build a tiny registry in a temp directory
and assert the strict checks and the helpers every generator uses.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import registry_io  # noqa: E402
from sync_proxy import proxy_routes  # noqa: E402

DECISION = '"do not list write APIs as gap" (owner 2026-10-27)'


class ScopeTest(unittest.TestCase):
    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)
        for directory in (registry_io.ENDPOINT_DIR, registry_io.STATE_DIR):
            os.makedirs(directory)

    def tearDown(self):
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def add(self, identifier, methods, state="partial", out=None, decision=True):
        Path(registry_io.ENDPOINT_DIR, identifier + ".yaml").write_text(
            f"id: {identifier}\nmethods: [{', '.join(methods)}]\n")
        text = f"id: {identifier}\nstate: {state}\n"
        if decision:
            text += f"owner_decision: |\n  {DECISION}\n"
        if out is not None:
            text += f"out_of_scope_methods: [{', '.join(out)}]\n"
        Path(registry_io.STATE_DIR, identifier + ".yaml").write_text(text)

    def test_form_encoded_read_post_needs_no_decision(self):
        self.add("query", ["GET", "POST"], "proven")
        self.assertEqual(registry_io.scope_problems(), [])
        self.assertEqual(registry_io.read_methods("query"), ["GET", "POST"])

    def test_ruler_write_methods_are_out_of_scope(self):
        self.add("rules_namespace", ["DELETE", "GET", "POST"], out=["DELETE", "POST"])
        self.assertEqual(registry_io.scope_problems(), [])
        self.assertEqual(registry_io.read_methods("rules_namespace"), ["GET"])
        self.assertNotIn("rules_namespace", registry_io.out_of_scope())

    def test_unlisted_write_method_fails(self):
        self.add("rules_namespace", ["DELETE", "GET", "POST"], out=["DELETE"])
        self.assertTrue(any("POST" in p for p in registry_io.scope_problems()))
        self.add("rules_group", ["DELETE", "GET"])
        self.assertTrue(any("rules_group" in p for p in registry_io.scope_problems()))

    def test_delete_endpoint_stays_in_scope(self):
        self.add("loki_api_v1_delete", ["DELETE", "GET", "POST", "PUT"])
        self.assertEqual(registry_io.scope_problems(), [])
        self.add("loki_api_v1_delete", ["DELETE", "GET", "POST", "PUT"], out=["DELETE"])
        self.assertTrue(any("delete stays in scope" in p for p in registry_io.scope_problems()))

    def test_listed_method_must_exist_and_be_a_write(self):
        self.add("a", ["GET"], out=["POST"])
        self.add("b", ["GET", "POST"], out=["GET"])
        problems = "\n".join(registry_io.scope_problems())
        self.assertIn("a: out_of_scope_methods lists POST, which the endpoint does not have", problems)
        self.assertIn("b: out_of_scope_methods lists GET, which is not a write method", problems)

    def test_owner_decision_is_required(self):
        self.add("rules_namespace", ["DELETE", "GET"], out=["DELETE"], decision=False)
        self.assertTrue(any("owner decision" in p for p in registry_io.scope_problems()))

    def test_write_only_endpoint_is_not_applicable(self):
        self.add("push", ["POST"], "partial", out=["POST"])
        self.assertTrue(any("must be not-applicable" in p for p in registry_io.scope_problems()))
        self.add("push", ["POST"], "not-applicable", out=["POST"])
        self.assertEqual(registry_io.scope_problems(), [])
        self.assertEqual(registry_io.out_of_scope(), {"push"})

    def test_not_applicable_needs_every_method_listed(self):
        self.add("push", ["POST"], "not-applicable")
        self.assertTrue(any("requires out_of_scope_methods" in p for p in registry_io.scope_problems()))

    def deprecated(self, identifier="api_prom_query", path="/api/prom/query", reason=True, decision=True):
        self.add(identifier, ["GET", "POST"], "not-applicable", out=["GET", "POST"], decision=False)
        definition = Path(registry_io.ENDPOINT_DIR, identifier + ".yaml")
        definition.write_text(definition.read_text() + f'path: "{path}"\n')
        state = Path(registry_io.STATE_DIR, identifier + ".yaml")
        state.write_text(state.read_text() + "scope_policy: deprecated-read-alias\n"
                         + ("reason: |\n  Deprecated alias; use /loki/api/v1/query.\n" if reason else "")
                         + ("owner_decision: |\n  Exclude deprecated read aliases (owner 2026-10-31).\n" if decision else ""))

    def test_deprecated_read_alias_is_excluded_with_explicit_policy(self):
        self.deprecated()
        self.assertEqual(registry_io.scope_problems(), [])
        self.assertEqual(registry_io.read_methods("api_prom_query"), [])
        self.assertEqual(registry_io.out_of_scope(), {"api_prom_query"})

    def test_deprecated_read_policy_requires_the_decision_and_reason(self):
        self.deprecated(reason=False, decision=False)
        problems = "\n".join(registry_io.scope_problems())
        self.assertIn("reason", problems)
        self.assertIn("owner 2026-10-31", problems)

    def test_deprecated_read_policy_cannot_exclude_rules_or_other_routes(self):
        self.deprecated("api_prom_rules", "/api/prom/rules")
        self.deprecated("api_prom_query", "/loki/api/v1/query")
        problems = "\n".join(registry_io.scope_problems())
        self.assertIn("api_prom_rules", problems)
        self.assertIn("api_prom_query", problems)

    def test_deprecated_policy_does_not_bypass_write_restrictions(self):
        self.deprecated()
        definition = Path(registry_io.ENDPOINT_DIR, "api_prom_query.yaml")
        definition.write_text(definition.read_text().replace("[GET, POST]", "[GET, POST, DELETE]"))
        self.assertTrue(registry_io.scope_problems())


class RepositoryTest(unittest.TestCase):
    def test_repository_registry_is_consistent(self):
        cwd = os.getcwd()
        os.chdir(REPO)
        try:
            self.assertEqual(registry_io.scope_problems(), [])
            self.assertEqual(registry_io.read_methods("loki_api_v1_rules_namespace"), ["GET"])
            self.assertEqual(registry_io.read_methods("loki_api_v1_rules_namespace_groupname"), ["GET"])
            self.assertEqual(registry_io.read_methods("api_prom_rules_namespace"), ["GET"])
            self.assertEqual(registry_io.read_methods("api_prom_rules_namespace_groupname"), ["GET"])
            self.assertIn("DELETE", registry_io.read_methods("loki_api_v1_delete"))
            self.assertEqual(registry_io.out_of_scope(), {"loki_api_v1_push", "api_prom_push", "api_prom_label",
                             "api_prom_label_name_values", "api_prom_query", "api_prom_series", "api_prom_tail"})
        finally:
            os.chdir(cwd)


LOCKED = {**registry_io.PERF_DEFAULTS, "locked": True, "locked_since": "2026-10-27"}


class PerfBlockTest(unittest.TestCase):
    """The perf: acceptance block (owner 2026-10-28): its fixed form, its schema and the gate's checks."""

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def item(self, path, identifier, block=None, extra=""):
        full = Path(registry_io.REGISTRY_ROOT, path)
        full.parent.mkdir(parents=True, exist_ok=True)
        text = f"id: {identifier}\ntitle: t\n" + (registry_io.render_perf(block) if block else "") + extra
        full.write_text(text)

    def test_render_and_parse_round_trip_with_an_exception(self):
        block = {**LOCKED, "max_ratio": 1.5, "modes": ["cold", "warm"],
                 "effort_exception": {"reason": "VictoriaLogs' scan can't be skipped", "follow_up": "native pipe"}}
        parsed, problems = registry_io.parse_perf("id: x\n" + registry_io.render_perf(block) + "why: |\n  text\n")
        self.assertEqual((parsed, problems), (block, []))
        self.assertEqual(registry_io.perf_schema_problems(parsed), [])

    def test_missing_keys_take_the_global_rule(self):
        parsed, problems = registry_io.parse_perf("id: x\nperf:\n  locked: false  # not yet\n")
        self.assertEqual(problems, [])
        self.assertEqual(parsed, registry_io.PERF_DEFAULTS)
        self.assertEqual((parsed["max_ratio"], parsed["target_ratio"]), (2.0, 1.0))

    def test_no_block_and_bad_forms(self):
        self.assertEqual(registry_io.parse_perf("id: x\n"), (None, []))
        self.assertIn("more than one perf: block", registry_io.parse_perf("perf:\n  locked: false\nperf:\n")[1])
        self.assertTrue(registry_io.parse_perf("perf: {locked: true}\n")[1])
        self.assertIn("perf: unknown key(s) bound", registry_io.parse_perf("perf:\n  bound: 3\n")[1])
        self.assertIn("perf.locked is given twice",
                      registry_io.parse_perf("perf:\n  locked: true\n  locked: false\n")[1])

    def test_schema(self):
        def problems(**kw):
            return registry_io.perf_schema_problems({**registry_io.PERF_DEFAULTS, **kw})

        self.assertEqual(problems(), [])
        self.assertIn("the owner's hard max", problems(max_ratio=2.5)[0])  # never looser than 2×
        self.assertTrue(problems(max_ratio=0.5))
        self.assertIn("no larger than max_ratio", problems(max_ratio=1.5, target_ratio=1.8)[0])
        self.assertTrue(problems(modes=[]))
        self.assertTrue(problems(modes=["cold", "hot"]))
        self.assertTrue(problems(modes=["cold", "cold"]))
        self.assertIn("locked_since must be the date", problems(locked=True)[0])
        self.assertIn("locked_since must be the date", problems(locked=True, locked_since="yesterday")[0])
        self.assertIn("not locked", problems(locked_since="2026-10-27")[0])
        self.assertTrue(problems(locked="yes"))
        self.assertTrue(problems(effort_exception={"reason": "slow"}))
        self.assertTrue(problems(effort_exception={"reason": "", "follow_up": "x"}))
        self.assertEqual(problems(effort_exception={"reason": "r", "follow_up": "f"}), [])

    def test_with_perf_replaces_the_block_and_keeps_the_rest(self):
        text = "id: x\n" + registry_io.render_perf(registry_io.PERF_DEFAULTS) + "victorialogs_candidates:\n  []\n"
        changed = registry_io.with_perf(text, LOCKED)
        self.assertEqual(changed, "id: x\n" + registry_io.render_perf(LOCKED) + "victorialogs_candidates:\n  []\n")
        self.assertEqual(registry_io.with_perf("id: y", LOCKED), "id: y\n" + registry_io.render_perf(LOCKED))

    def test_gate_checks(self):
        self.item("cases/semantics/a.yaml", "semantics/a", LOCKED)
        self.item("cases/semantics/b.yaml", "semantics/b")  # covered, no block
        self.item("behaviours/c.yaml", "c", registry_io.PERF_DEFAULTS)  # a block, not covered
        self.item("behaviours/d.yaml", "d", {**registry_io.PERF_DEFAULTS, "max_ratio": 3.0})
        # A state file is not the item: its block is not read.
        self.item("state/behaviours/d.yaml", "d", LOCKED)
        over = [{"set": "s", "shape": "x", "range": "1h", "mode": "warm"},
                {"set": "s", "shape": "y", "range": "1h", "mode": "warm"}]
        covers = {("s", "x"): ["semantics/a"], ("s", "y"): ["d"]}
        found = registry_io.perf_problems({"semantics/a", "semantics/b", "d"}, over, covers)
        self.assertEqual(len(found), 4, found)
        self.assertIn("semantics/b: an A/B shape covers it", found[0])
        self.assertTrue(any("behaviours/c.yaml: perf: block on an item no A/B shape covers" in p for p in found))
        self.assertTrue(any("behaviours/d.yaml: perf.max_ratio" in p for p in found))
        self.assertTrue(any(p.startswith("semantics/a: locked, but covers s / x / 1h / warm") for p in found))
        # The same row on a mode the lock does not cover is not a contradiction.
        self.item("cases/semantics/a.yaml", "semantics/a", {**LOCKED, "modes": ["cold"]})
        self.assertFalse(any("locked, but" in p for p in registry_io.perf_problems({"semantics/a"}, over, covers)))


class PerfRepositoryTest(unittest.TestCase):
    def test_repository_perf_blocks_are_valid(self):
        cwd = os.getcwd()
        os.chdir(REPO)
        try:
            sys.path.insert(0, str(REPO / "scripts" / "ci"))
            import check_conformance  # noqa: E402
            self.assertEqual(check_conformance.perf_acceptance_problems(), [])
        finally:
            os.chdir(cwd)


class RouteTests(unittest.TestCase):
    def test_rules_subtree_expands_only_namespace_and_group_templates(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory, "internal/proxy/proxy.go")
            source.parent.mkdir(parents=True)
            source.write_text('''
mux.Handle("/api/prom/rules/", alertRead("rules_prom_nested", "/api/prom/rules/{namespace}", p.handleRules))
mux.Handle("/loki/api/v1/rules/", alertRead("rules_nested", "/loki/api/v1/rules/{namespace}", p.handleRules))
mux.Handle("/loki/api/v1/label/", p.routeHandler("label_values", "/loki/api/v1/label/{name}/values", p.handleLabelValues))
''')
            routes = proxy_routes(directory)
            self.assertEqual(set(routes), {
                "/api/prom/rules/{namespace}", "/api/prom/rules/{namespace}/{groupName}",
                "/loki/api/v1/rules/{namespace}", "/loki/api/v1/rules/{namespace}/{groupName}",
                "/loki/api/v1/label/{name}/values"})
            self.assertEqual(routes["/api/prom/rules/{namespace}/{groupName}"]["handler"], "handleRules")

    def test_expansion_requires_the_real_subtree_and_rules_handler(self):
        for pattern, template, handler in (
                ("/other/", "/other/{namespace}", "handleRules"),
                ("/api/prom/rules/", "/api/prom/rules/{namespace}", "handleOther"),
                ("/api/prom/rules", "/api/prom/rules/{namespace}", "handleRules")):
            with self.subTest(pattern=pattern, handler=handler), tempfile.TemporaryDirectory() as directory:
                source = Path(directory, "internal/proxy/proxy.go")
                source.parent.mkdir(parents=True)
                source.write_text(f'mux.Handle("{pattern}", alertRead("rules", "{template}", p.{handler}))')
                self.assertEqual(set(proxy_routes(directory)), {template})

    def test_repository_rule_groups_are_implemented_but_deprecated_reads_are_not(self):
        routes = proxy_routes(REPO)
        self.assertIn("/api/prom/rules/{namespace}/{groupName}", routes)
        self.assertIn("/loki/api/v1/rules/{namespace}/{groupName}", routes)
        for path in ("/api/prom/label", "/api/prom/label/{name}/values", "/api/prom/query", "/api/prom/series", "/api/prom/tail"):
            self.assertNotIn(path, routes)

class PriorityTests(unittest.TestCase):
    def test_optional_gap_follows_last_priority_and_stays_in_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            tree = Path(directory)
            registry = tree / "conformance/registry"

            def write(path, text):
                target = registry / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text)

            endpoints = []
            for identifier, path, priority in (("query", "/query", "normal"),
                    ("rules", "/rules", "last"), ("shards", "/loki/api/v1/index/shards", "optional")):
                endpoints.append({"id": identifier, "path": path, "methods": ["GET"],
                                  "tests": {"unit": []}, "differential_tests": {"unit": []}})
                write("loki/endpoints/" + identifier + ".yaml", f"id: {identifier}\nmethods: [GET]\n")
                write("state/loki/" + identifier + ".yaml", f"id: {identifier}\nstate: gap\npriority: {priority}\n"
                      "reason: |\n  Optional planner compatibility only; no pinned UI or peer lookup dependency.\n")
            write("generated/proxy/coverage.json", json.dumps({"loki_version": "v3.7.7", "endpoints": endpoints}))
            write("generated/proxy/implementation.json", '{"endpoints": []}')
            write("generated/proxy/errors.json", '{"summary": {"by_status": {}}}')
            write("generated/wiring.json", '{"unwired_items": [], "item_to_tests": {}}')
            (registry / "behaviours").mkdir()
            write("generated/vl/usage.json", '{"endpoints_used": [], "pipes_used": [], "stats_functions_used": [], "version_gated": {}}')
            for script in ("gaps.py", "coverage_map.py"):
                subprocess.run([sys.executable, str(REPO / "conformance/scripts" / script)], cwd=tree,
                               capture_output=True, text=True, check=True)
            gaps = (tree / "conformance/reports/gaps.md").read_text()
            self.assertLess(gaps.index("`/query`"), gaps.index("`/rules`"))
            self.assertLess(gaps.index("`/rules`"), gaps.index("`/loki/api/v1/index/shards`"))
            self.assertIn("| optional | `/loki/api/v1/index/shards`", gaps)
            self.assertIn("Optional planner compatibility only", gaps)
            coverage = (tree / "conformance/reports/coverage-map.md").read_text()
            self.assertIn("`/loki/api/v1/index/shards` | GET | gap (optional)", coverage)
            self.assertIn("Optional planner compatibility only", coverage)
            # MDX treats bare {name} as a JavaScript expression. The shared
            # scope text reaches both published pages and repository reports.
            for rendered in (coverage, gaps):
                for api_path in ("/api/prom/rules*", "/api/prom/label", "/api/prom/label/{name}/values",
                                 "/api/prom/query", "/api/prom/series", "/api/prom/tail", "/loki/api/v1"):
                    self.assertIn("`" + api_path + "`", rendered)
            summary = json.loads((registry / "generated/summary.json").read_text())
            self.assertEqual(summary["endpoints"], 3)
            self.assertEqual(summary["state"]["gap"], 3)


if __name__ == "__main__":
    unittest.main()
