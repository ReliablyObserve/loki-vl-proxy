"""scripts/ci/matrix_pins.py: every pin in the repository equals test/e2e-compat/compatibility-matrix.json."""
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/ci"))
import matrix_pins  # noqa: E402

MATRIX = {"stack": {
    "loki": {"image_repo": "grafana/loki", "pinned_version": "3.7.9"},
    "victorialogs": {"image_repo": "victoriametrics/victoria-logs", "pinned_version": "v1.52.0"},
    "grafana": {"image_repo": "grafana/grafana", "pinned_version": "13.2.3", "plugins": {
        "victoriametrics-logs-datasource": {"pinned_version": "0.32.0"},
        "grafana-lokiexplore-app": {"pinned_version": "2.6.0"}}},
    "logs_drilldown_contract": {"pinned_version": "2.6.0"}}}
COMPOSE = """services:
  loki:
    image: ${LOKI_IMAGE:-grafana/loki:3.7.9}
  victorialogs:
    image: victoriametrics/victoria-logs:v1.52.0
  grafana:
    image: ${GRAFANA_IMAGE:-grafana/grafana:13.2.3}
    environment:
      - GF_PLUGINS_PREINSTALL=${GRAFANA_PLUGINS_PREINSTALL:-victoriametrics-logs-datasource@0.32.0,grafana-lokiexplore-app@2.6.0}
"""
BENCH = '{"loki": {"image": "grafana/loki:3.7.9"}, "vl": {"image": "victoriametrics/victoria-logs:v1.52.0"}}\n'
DEPENDABOT = """updates:
  - package-ecosystem: docker-compose
    directories:
      - /test/e2e-compat
    ignore:
      - dependency-name: "grafana/*"
      - dependency-name: "victoriametrics/victoria-logs"
"""
WORKFLOW = "jobs:\n  a:\n    env:\n      LOKI_IMAGE: ${{ needs.src.outputs.pinned_loki_image }}\n"
MUST = {"docker-compose.yml": ("loki", "victorialogs", "grafana", "plugins"), "bench/compose.json": ("loki", "victorialogs")}
BENCH_FILES = ("bench/compose.json",)


class MatrixPinsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        self.write(matrix_pins.MATRIX, json.dumps(MATRIX))
        self.write("docker-compose.yml", COMPOSE)
        self.write("examples/docker-compose.yml", "services:\n  vl:\n    image: victoriametrics/victoria-logs:v1.52.0\n")
        self.write("bench/compose.json", BENCH)
        self.write(".github/dependabot.yml", DEPENDABOT)
        self.write(".github/workflows/compat.yaml", WORKFLOW)

    def write(self, rel, text):
        path = Path(self.dir) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def edit(self, rel, old, new):
        path = Path(self.dir) / rel
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new))

    def problems(self):
        return matrix_pins.problems(self.dir, MUST, BENCH_FILES)

    def test_consistent_tree_passes(self):
        self.assertEqual([], self.problems())

    def test_the_repository_matches_its_matrix(self):
        self.assertEqual([], matrix_pins.problems(str(REPO)))

    def test_compose_default_that_drifted_is_reported_with_its_line(self):
        self.edit("docker-compose.yml", "grafana/loki:3.7.9", "grafana/loki:3.7.8")
        self.assertEqual(["docker-compose.yml:3: grafana/loki:3.7.8 but the matrix pins grafana/loki:3.7.9"], self.problems())

    def test_example_compose_drift_is_reported(self):
        self.edit("examples/docker-compose.yml", "v1.52.0", "v1.53.0")
        self.assertEqual(1, len(self.problems()))
        self.assertIn("examples/docker-compose.yml:3", self.problems()[0])

    def test_plugin_drift_is_reported(self):
        self.edit("docker-compose.yml", "victoriametrics-logs-datasource@0.32.0", "victoriametrics-logs-datasource@0.33.1")
        found = self.problems()
        self.assertTrue(any("0.33.1" in line for line in found), found)

    def test_bench_pin_drift_is_reported(self):
        self.edit("bench/compose.json", "v1.52.0", "v1.51.1")
        self.assertTrue(any(line.startswith("bench/compose.json") for line in self.problems()))

    def test_a_pin_that_fell_back_to_latest_is_reported(self):
        self.edit("docker-compose.yml", "${GRAFANA_IMAGE:-grafana/grafana:13.2.3}", "grafana/grafana")
        found = self.problems()
        self.assertTrue(any("must pin grafana/grafana:13.2.3" in line for line in found), found)

    def test_a_missing_file_of_the_list_is_reported(self):
        (Path(self.dir) / "bench/compose.json").unlink()
        self.assertTrue(any("bench/compose.json: expected to hold a stack pin" in line for line in self.problems()))

    def test_workflow_image_or_plugin_literal_is_reported_even_when_equal(self):
        self.edit(".github/workflows/compat.yaml", "${{ needs.src.outputs.pinned_loki_image }}", "grafana/loki:3.7.9")
        found = self.problems()
        self.assertEqual(1, len(found), found)
        self.assertIn(".github/workflows/compat.yaml:4: a pin literal", found[0])
        self.edit(".github/workflows/compat.yaml", "grafana/loki:3.7.9", "x@y")
        self.write(".github/workflows/p.yaml", "env:\n  P: victoriametrics-logs-datasource@0.33.1,grafana-lokiexplore-app@3.1.0\n")
        self.assertEqual(2, len(self.problems()))

    def test_dependabot_must_ignore_every_matrix_image(self):
        self.edit(".github/dependabot.yml", '      - dependency-name: "victoriametrics/victoria-logs"\n', "")
        found = self.problems()
        self.assertEqual(1, len(found), found)
        self.assertIn("ignore list does not cover victoriametrics/victoria-logs", found[0])

    def test_dependabot_other_ecosystem_does_not_count(self):
        self.edit(".github/dependabot.yml", '      - dependency-name: "victoriametrics/victoria-logs"\n', "")
        self.write(".github/dependabot.yml", Path(self.dir, ".github/dependabot.yml").read_text() +
                   '  - package-ecosystem: npm\n    ignore:\n      - dependency-name: "victoriametrics/victoria-logs"\n')
        self.assertEqual(1, len(self.problems()))

    def test_drilldown_plugin_pin_must_equal_the_drilldown_contract_pin(self):
        matrix = json.loads(json.dumps(MATRIX))
        matrix["stack"]["logs_drilldown_contract"]["pinned_version"] = "3.1.0"
        self.write(matrix_pins.MATRIX, json.dumps(matrix))
        self.assertTrue(any("logs_drilldown_contract.pinned_version is 3.1.0" in line for line in self.problems()))

    def test_matrix_without_plugins_is_reported(self):
        matrix = json.loads(json.dumps(MATRIX))
        del matrix["stack"]["grafana"]["plugins"]
        self.write(matrix_pins.MATRIX, json.dumps(matrix))
        self.assertTrue(any("stack.grafana.plugins is missing" in line for line in self.problems()))


if __name__ == "__main__":
    unittest.main()
