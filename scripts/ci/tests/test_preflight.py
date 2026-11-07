"""preflight.py and preflight.sh: the diff maps to the packages and tests CI would run, and the workflow lists are read, not copied."""
import json
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.ci import preflight as pf

REPO = Path(__file__).resolve().parents[3]
CI = (REPO / ".github" / "workflows" / "ci.yaml").read_text()

WORKFLOW = """\
jobs:
  lint:
    steps:
      - name: golangci-lint
        uses: golangci/golangci-lint-action@abc  # v9
        with:
          version: v2.14.0
          args: --timeout=5m
      - name: Unit tests (quality gate policy)
        run: python3 -m unittest scripts.ci.tests.test_a scripts.ci.tests.test_b
      - name: A/B
        run: python3 -m unittest discover -s bench/ab/tests
      - name: again
        run: python3 -m unittest discover -s bench/ab/tests
      - name: Concurrent regression tests (verbose, race)
        run: |
          go test -race -count=1 -v -timeout=2m \\
            -run '^Test(One_|Two)' \\
            ./internal/proxy/
      - name: other
        uses: actions/setup-go@abc
        with:
          go-version: "1.27.3"
"""


class Tree:
    """A throwaway module tree: write(path, text) creates files."""

    def __init__(self):
        self.dir = tempfile.mkdtemp()
        self.root = Path(self.dir)

    def write(self, path, text="package x\n"):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        return path

    def close(self):
        shutil.rmtree(self.dir, True)


class WorkflowTest(unittest.TestCase):
    def test_unittest_lines_are_read_in_order_without_repeats(self):
        self.assertEqual(pf.workflow_unittests(WORKFLOW),
                         ["scripts.ci.tests.test_a scripts.ci.tests.test_b", "discover -s bench/ab/tests"])

    def test_golangci_version_comes_from_its_own_step_not_a_go_version(self):
        self.assertEqual(pf.golangci_version(WORKFLOW), "v2.14.0")
        self.assertEqual(pf.golangci_version("steps:\n  - name: x\n    with:\n      go-version: '1.27.3'\n"), "")

    def test_regression_pattern_is_read_from_its_step(self):
        self.assertEqual(pf.regression_run(WORKFLOW), "^Test(One_|Two)")

    def test_the_real_workflow_still_has_every_piece(self):
        self.assertRegex(pf.golangci_version(CI), r"^v\d+\.\d+\.\d+$")
        self.assertTrue(pf.regression_run(CI).startswith("^Test("))
        lines = pf.workflow_unittests(CI)
        self.assertTrue(any(line.startswith("scripts.ci.tests.") for line in lines))
        self.assertIn("discover -s bench/ab/tests", lines)
        # Every module of the long list exists, so a rename cannot leave the preflight running nothing.
        for line in lines:
            for module in re.findall(r"scripts\.ci\.tests\.\w+", line):
                self.assertTrue((REPO / (module.replace(".", "/") + ".py")).exists(), module)


class UnittestCommandsTest(unittest.TestCase):
    def test_a_module_list_is_one_interpreter_unless_parallel_and_discover_stays_whole(self):
        modules = "scripts.ci.tests.test_a scripts.ci.tests.test_b"
        self.assertEqual([c[2:] for c in pf.unittest_commands(modules)], [["unittest", "scripts.ci.tests.test_a", "scripts.ci.tests.test_b"]])
        self.assertEqual([c[2:] for c in pf.unittest_commands(modules, parallel=True)],
                         [["unittest", "scripts.ci.tests.test_a"], ["unittest", "scripts.ci.tests.test_b"]])
        discover = "discover -s bench/drilldown-memory -p 'test_*.py'"
        for parallel in (False, True):
            self.assertEqual(pf.unittest_commands(discover, parallel)[0][3:],
                             ["discover", "-s", "bench/drilldown-memory", "-p", "test_*.py"])
            self.assertEqual(len(pf.unittest_commands(discover, parallel)), 1)


class PackageMappingTest(unittest.TestCase):
    def setUp(self):
        self.t = Tree()
        self.addCleanup(self.t.close)
        self.t.write("go.mod", "module m\n")
        self.t.write("internal/cache/cache.go")
        self.t.write("internal/cache/testdata/x.json", "{}")
        self.t.write("docs/a.md", "x")
        self.t.write("bench/go.mod", "module b\n")
        self.t.write("bench/main.go")
        self.t.write("test/oracle/loki/go.mod", "module o\n")
        self.t.write("test/oracle/loki/o.go")

    def pkg(self, path):
        return pf.package_dir(path, self.t.root)

    def test_go_and_testdata_files_belong_to_the_package(self):
        self.assertEqual(self.pkg("internal/cache/cache.go"), "internal/cache")
        self.assertEqual(self.pkg("internal/cache/testdata/x.json"), "internal/cache")
        self.assertEqual(self.pkg("internal/cache/testdata/deep/y.json"), "internal/cache")

    def test_deleted_files_still_name_their_package_but_deleted_packages_name_none(self):
        self.assertEqual(self.pkg("internal/cache/gone.go"), "internal/cache")
        self.assertIsNone(self.pkg("internal/gone/gone.go"))

    def test_nested_modules_and_non_packages_belong_to_none(self):
        for path in ("bench/main.go", "test/oracle/loki/o.go", "docs/a.md", "go.mod"):
            self.assertIsNone(self.pkg(path), path)

    def test_plan_flags(self):
        plan = pf.plan("base", root=self.t.root, files=["internal/cache/cache.go", "bench/main.go", "docker-compose.yml"])
        self.assertEqual((plan["packages"], plan["cache"], plan["bench"], plan["compose"], plan["all"], plan["proxy"]),
                         ("./internal/cache", "1", "1", "1", "0", "0"))
        self.assertEqual(pf.plan("base", root=self.t.root, files=["go.sum"])["all"], "1")
        self.assertEqual(pf.plan("base", root=self.t.root, files=["docs/a.md", "bench/README.md"])["packages"], "")
        self.assertEqual(pf.plan("base", root=self.t.root, files=["bench/README.md"])["bench"], "0")


class ProxySelectionTest(unittest.TestCase):
    def setUp(self):
        self.t = Tree()
        self.addCleanup(self.t.close)
        p = "internal/proxy/"
        self.t.write(p + "alpha.go")
        self.t.write(p + "alpha_test.go", "func TestAlpha(t *testing.T) {}\nfunc helper(t *testing.T) {}\nfunc FuzzAlpha(f *testing.F) {}\n")
        self.t.write(p + "alpha_extra_test.go", "func TestAlphaExtra(t *testing.T) {}\n")
        self.t.write(p + "alphabet_test.go", "func TestAlphabet(t *testing.T) {}\n")
        self.t.write(p + "beta.go")
        self.t.write(p + "gamma_test.go", "func TestGamma(t *testing.T) {}\nfunc BenchmarkGamma(b *testing.B) {}\n")
        self.t.write(p + "testdata/x.json", "{}")

    def select(self, *files):
        return pf.proxy_selection(list(files), self.t.root)

    def test_a_changed_test_file_contributes_its_tests(self):
        self.assertEqual(self.select("internal/proxy/gamma_test.go"), (["TestGamma"], []))

    def test_a_changed_source_file_contributes_the_tests_of_its_stem(self):
        tests, unmatched = self.select("internal/proxy/alpha.go")
        self.assertEqual(tests, ["FuzzAlpha", "TestAlpha", "TestAlphaExtra"])
        self.assertEqual(unmatched, [])

    def test_a_source_file_without_tests_and_testdata_are_reported_not_dropped(self):
        self.assertEqual(self.select("internal/proxy/beta.go", "internal/proxy/testdata/x.json"),
                         ([], ["internal/proxy/beta.go", "internal/proxy/testdata/x.json"]))

    def test_other_packages_and_deleted_test_files_select_nothing(self):
        self.assertEqual(self.select("internal/cache/a.go", "internal/proxy/gone_test.go"), ([], []))

    def test_run_pattern_is_anchored(self):
        self.assertEqual(pf.run_pattern(["TestA", "TestB"]), "^(TestA|TestB)$")
        self.assertEqual(pf.run_pattern([]), "")


class DependentsTest(unittest.TestCase):
    def test_reverse_closure_follows_importers_transitively(self):
        imports = {"a": set(), "b": {"a"}, "c": {"b"}, "d": {"x"}}
        self.assertEqual(pf.reverse_closure(imports, {"a"}), {"a", "b", "c"})
        self.assertEqual(pf.reverse_closure(imports, {"d"}), {"d"})


class GoJsonTest(unittest.TestCase):
    def test_failures_come_with_their_output_and_passes_are_dropped(self):
        events = [
            {"Action": "output", "Package": "p", "Test": "TestA", "Output": "boom\n"},
            {"Action": "fail", "Package": "p", "Test": "TestA"},
            {"Action": "pass", "Package": "p", "Test": "TestB"},
            {"Action": "output", "Package": "p", "Output": "FAIL\tp\n"},
            {"Action": "fail", "Package": "p"},
        ]
        lines = [json.dumps(e) for e in events] + ["not json"]
        self.assertEqual(pf.failing_tests(lines), {("p", ""): ["FAIL\tp"], ("p", "TestA"): ["boom"]})
        self.assertEqual(pf.failing_tests([json.dumps(events[2])]), {})


@unittest.skipUnless(shutil.which("git"), "git is not installed")
class SnapshotTest(unittest.TestCase):
    def test_copy_has_tracked_and_untracked_files_but_not_ignored_or_deleted_ones(self):
        t = Tree()
        self.addCleanup(t.close)
        dest = Tree()
        self.addCleanup(dest.close)

        def git(*args):
            subprocess.run(["git", "-c", "user.email=a@b", "-c", "user.name=n", *args], cwd=t.root, check=True, capture_output=True)

        git("init", "-q")
        t.write(".gitignore", "ignored.txt\n")
        t.write("kept.txt", "kept")
        t.write("gone.txt", "gone")
        git("add", "-A")
        git("commit", "-qm", "x")
        (t.root / "gone.txt").unlink()
        (t.root / "kept.txt").write_text("edited")
        t.write("new.txt", "new")
        t.write("ignored.txt", "ignored")
        pf.snapshot(dest.root, root=t.root)
        copied = sorted(p.relative_to(dest.root).as_posix() for p in dest.root.rglob("*") if p.is_file())
        self.assertEqual(copied, [".gitignore", "kept.txt", "new.txt"])
        self.assertEqual((dest.root / "kept.txt").read_text(), "edited")


@unittest.skipUnless(shutil.which("go") and shutil.which("git"), "go and git are needed")
class ScriptTest(unittest.TestCase):
    def run_script(self, *args):
        return subprocess.run([str(REPO / "scripts/ci/preflight.sh"), *args], cwd=REPO, capture_output=True, text=True)

    def test_help_and_usage_errors(self):
        helped = self.run_script("--help")
        self.assertEqual(helped.returncode, 0)
        self.assertIn("--quick", helped.stdout)
        bad = self.run_script("--nope")
        self.assertEqual(bad.returncode, 2)
        self.assertIn("unknown option", bad.stderr)
        self.assertEqual(self.run_script("--quick", "--race-shard", "1").returncode, 2)
        self.assertEqual(self.run_script("--race-shard", "6").returncode, 2)

    def test_dry_run_plans_every_suite_of_the_workflow_and_runs_nothing(self):
        out = self.run_script("--quick", "--dry-run", "--base", "HEAD")
        self.assertEqual(out.returncode, 0, out.stderr)
        steps = [line for line in out.stdout.splitlines() if line.startswith("WOULD ")]
        self.assertIn("WOULD gofmt -s", steps)
        self.assertTrue(any(s.startswith("WOULD golangci-lint v") for s in steps))
        self.assertTrue(any(s.startswith("WOULD conformance gate") for s in steps))
        suites = pf.workflow_unittests(CI)
        self.assertEqual(len([s for s in steps if s.startswith("WOULD unittest ")]), len(suites))
        self.assertFalse([s for s in steps if "go test" in s], "--quick runs no package tests")

    def test_a_missing_base_is_a_usage_error_with_the_fix(self):
        out = self.run_script("--quick", "--dry-run", "--base", "origin/does-not-exist")
        self.assertEqual(out.returncode, 2)
        self.assertIn("git fetch", out.stderr)


if __name__ == "__main__":
    unittest.main()
