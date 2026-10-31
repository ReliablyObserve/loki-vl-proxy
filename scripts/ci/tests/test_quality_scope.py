"""Registry snapshots read repository data without executing repository code."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.ci.quality_scope import registry_snapshot

ROOT = Path(__file__).resolve().parents[3]


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.registry = self.root / "conformance/registry"
        for directory in ("generated", "state/loki", "state/behaviours", "state/translations", "loki/endpoints", "cases"):
            (self.registry / directory).mkdir(parents=True)
        self.write("generated/summary.json", json.dumps({"loki_version": "v3.7.7", "endpoints": 1,
                   "implemented": 1, "with_differential_tests": 1, "proven": 1}))
        self.write("state/loki/query.yaml", "id: query\nstate: proven\n")
        self.write("loki/endpoints/query.yaml", "id: query\npath: /loki/api/v1/query\nmethods: [GET, POST]\n")
        self.write("state/behaviours/identity.yaml", "id: identity\nstate: partial\n")
        self.write("state/translations/identity.yaml", "id: identity\nstate: gap\n")
        self.write("cases/alpha/same.yaml", "id: alpha/same\ntitle: Example\ngap:\n  status: open\n  impact: explore-visible\n")
        self.write("cases/beta/same.yaml", "id: beta/same\ngap:\n  status: fixed\n  fixed_by: '#10'\n")
        self.write("error-layer-known-leaks.json", json.dumps({"ceiling": 2, "leaks": {"query malformed": ["alpha/same", "beta/same"]}}))

    def write(self, path, text):
        target = self.registry / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def test_declared_inventory_and_nested_cases_preserve_qualified_ids(self):
        result = registry_snapshot(self.root)
        self.assertEqual(result["state"], "available", result)
        self.assertEqual(result["endpoints"], {"total": 1, "implemented": 1, "differential": 1, "declared_proven": 1})
        self.assertEqual(result["tracks"]["behaviours"]["partial"], 1)
        self.assertEqual(result["tracks"]["translations"]["gap"], 1)
        self.assertEqual(result["gaps"]["open"], 1)
        self.assertEqual(result["gaps"]["fixed"], 1)
        self.assertEqual(result["error_layer"], {"cells": 1, "links": 2})
        for key in ("behaviour:identity", "translation:identity", "case:alpha/same", "case:beta/same", "endpoint:query"):
            self.assertIn(key, result["items"])

    def test_out_of_scope_endpoints_do_not_inflate_inventory(self):
        self.write("state/loki/push.yaml", "id: push\nstate: not-applicable\nout_of_scope_methods: [POST]\n")
        self.write("loki/endpoints/push.yaml", "id: push\npath: /loki/api/v1/push\nmethods: [POST]\n")
        self.write("state/loki/write.yaml", "id: write\nstate: gap\nout_of_scope_methods: [DELETE]\n")
        self.write("loki/endpoints/write.yaml", "id: write\npath: /write\nmethods: [DELETE]\n")
        result = registry_snapshot(self.root)
        self.assertEqual(result["state"], "available", result)
        self.assertNotIn("endpoint:push", result["items"])
        self.assertNotIn("endpoint:write", result["items"])
        self.assertEqual(result["endpoints"]["total"], 1)

    def test_missing_and_malformed_data_are_unavailable_not_zero(self):
        (self.registry / "generated/summary.json").unlink()
        result = registry_snapshot(self.root)
        self.assertEqual(result["state"], "unavailable")
        self.assertNotIn("endpoints", result)
        self.write("generated/summary.json", "{not json}")
        self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")

    def test_read_scope_separates_deletion_and_keeps_post_queries(self):
        self.write("generated/summary.json", json.dumps({"loki_version": "v3.7.7", "endpoints": 2,
                   "implemented": 2, "with_differential_tests": 1, "proven": 1}))
        self.write("state/loki/delete.yaml", "id: delete\nstate: partial\n")
        self.write("loki/endpoints/delete.yaml", "id: delete\npath: /loki/api/v1/delete\nmethods: [DELETE, GET, POST, PUT]\n")
        self.write("generated/proxy/implementation.json", json.dumps({"endpoints": [
            {"id": "delete", "implemented": True}]}))
        self.write("generated/proxy/coverage.json", json.dumps({"endpoints": [
            {"id": "delete", "differential_tests": {"unit": [], "e2e": [], "ui": []}}]}))
        result = registry_snapshot(self.root)
        self.assertEqual(result["state"], "available", result)
        self.assertEqual(result["endpoint_scope"], "loki-read-api")
        self.assertEqual(result["endpoints"], {"total": 1, "implemented": 1, "differential": 1, "declared_proven": 1})
        self.assertEqual(result["deletion_management"], {"total": 1, "implemented": 1, "differential": 0, "declared_proven": 0})
        self.assertEqual(result["items"]["endpoint:query"]["methods"], ["GET", "POST"])
        self.assertEqual(result["items"]["endpoint:delete"]["kind"], "deletion-management")

        # Proofs for the optional administration route cannot inflate read proof.
        self.write("state/loki/delete.yaml", "id: delete\nstate: proven\n")
        self.write("generated/summary.json", json.dumps({"loki_version": "v3.7.7", "endpoints": 2,
                   "implemented": 2, "with_differential_tests": 2, "proven": 2}))
        self.write("generated/proxy/coverage.json", json.dumps({"endpoints": [
            {"id": "delete", "differential_tests": {"unit": ["test_delete"], "e2e": [], "ui": []}}]}))
        result = registry_snapshot(self.root)
        self.assertEqual(result["state"], "available", result)
        self.assertEqual(result["endpoints"]["declared_proven"], 1)
        self.assertEqual(result["endpoints"]["differential"], 1)
        self.assertEqual(result["deletion_management"]["declared_proven"], 1)
        self.assertEqual(result["deletion_management"]["differential"], 1)

        for path in ("generated/proxy/implementation.json", "generated/proxy/coverage.json"):
            original = (self.registry / path).read_text()
            self.write(path, "[]")
            self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")
            self.write(path, original)

        self.write("generated/proxy/coverage.json", '{"endpoints": [{"id":"delete", "differential_tests":{}}]}')
        self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")
        (self.registry / "generated/proxy/implementation.json").unlink()
        self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")

    def test_read_scope_matches_real_registry_and_excludes_push(self):
        result = registry_snapshot(ROOT)
        self.assertEqual(result["state"], "available", result)
        summary = json.loads((ROOT / "conformance/registry/generated/summary.json").read_text())
        self.assertEqual(result["endpoints"]["total"], summary["endpoints"] - result["deletion_management"]["total"])
        read_paths = {item["path"] for item in result["items"].values() if item["kind"] == "endpoint"}
        self.assertNotIn("/loki/api/v1/delete", read_paths)
        self.assertEqual(result["excluded_write_endpoints"], 2)
        self.assertNotIn("endpoint:loki_api_v1_push", result["items"])
        self.assertNotIn("endpoint:api_prom_push", result["items"])

    def test_malformed_states_counts_and_gap_status_fail_closed(self):
        for path, text in (("state/behaviours/identity.yaml", "id: identity\nstate: maybe\n"),
                           ("cases/alpha/same.yaml", "id: alpha/same\ngap:\n  status: imaginary\n"),
                           ("cases/alpha/same.yaml", "id: alpha/same\ngap: []\n"),
                           ("error-layer-known-leaks.json", '{"ceiling":8,"leaks":{"a":[]}}')):
            original = (self.registry / path).read_text()
            self.write(path, text)
            self.assertEqual(registry_snapshot(self.root)["state"], "unavailable", path)
            self.write(path, original)

    def test_missing_track_directory_and_summary_disagreement_are_unavailable(self):
        track = self.registry / "state/translations/identity.yaml"
        track.unlink()
        track.parent.rmdir()
        self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")
        track.parent.mkdir()
        track.write_text("id: identity\nstate: partial\n")
        summary = json.loads((self.registry / "generated/summary.json").read_text())
        summary["endpoints"] = 4
        self.write("generated/summary.json", json.dumps(summary))
        self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")

    def test_symlink_cannot_read_outside_measured_tree(self):
        with tempfile.TemporaryDirectory() as outside:
            target = Path(outside) / "state.yaml"
            target.write_text("id: identity\nstate: proven\n")
            state = self.registry / "state/behaviours/identity.yaml"
            state.unlink()
            state.symlink_to(target)
            self.assertEqual(registry_snapshot(self.root)["state"], "unavailable")

    def test_snapshot_cli_reads_data_without_loading_repository_modules(self):
        self.write("generated/summary.json", json.dumps({"loki_version": "v3.7.7", "endpoints": 1,
                   "implemented": 1, "with_differential_tests": 1, "proven": 1}))
        sentinel = self.root / "executed"
        self.write("scripts/coverage_map.py", f"open({str(sentinel)!r}, 'w').write('unsafe')")
        result = subprocess.run([sys.executable, str(ROOT / "scripts/ci/quality_scope.py"), "--root", str(self.root)],
                                capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout)["state"], "available")
        self.assertFalse(sentinel.exists())


if __name__ == "__main__":
    unittest.main()
