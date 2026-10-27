"""Method-level scope of the conformance registry.

The proxy serves Loki's read API. A write method (POST, PUT, PATCH, DELETE) of an endpoint is
either listed in its state file's `out_of_scope_methods` with the owner decision, or is the
delete endpoint, which stays in scope. These tests build a tiny registry in a temp directory
and assert the strict checks and the helpers every generator uses.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import registry_io  # noqa: E402

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
            self.assertEqual(registry_io.out_of_scope(), {"loki_api_v1_push", "api_prom_push"})
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
