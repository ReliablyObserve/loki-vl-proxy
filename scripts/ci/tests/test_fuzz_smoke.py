"""fuzz_smoke.py: the target list covers the tree, shards partition it, and the skip decision is conservative."""
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.ci import fuzz_smoke as fs

REPO = Path(__file__).resolve().parents[3]
CI = (REPO / ".github" / "workflows" / "ci.yaml").read_text()


def target(name, package="internal/proxy", status="smoke", fuzztime="12s", timeout="-"):
    return fs.Target(status, package, name, fuzztime, timeout, name)


class RegistryTest(unittest.TestCase):
    def test_every_fuzz_function_in_the_tree_is_accounted_for_and_sharded_once(self):
        self.assertEqual(fs.check(fs.load_registry(), fs.discover(), fs.shard_count()), [])

    def test_every_smoke_row_keeps_its_fuzz_time(self):
        for t in fs.smoke_targets(fs.load_registry()):
            self.assertRegex(t.fuzztime, r"^\d+s$", t.name)
            self.assertIn(f"-fuzz=^{t.name}$", t.command())

    def test_check_names_each_kind_of_violation(self):
        tree = {("p", "FuzzA"), ("p", "FuzzB"), ("q", "FuzzC")}
        rows = [target("FuzzA", "p"), target("FuzzGone", "p"), target("FuzzA", "p"), target("FuzzC", "q", status="unscheduled", fuzztime="-")]
        problems = "\n".join(fs.check(rows, tree, 2))
        self.assertIn("p FuzzGone: dead row", problems)
        self.assertIn("p FuzzA: listed twice", problems)
        self.assertIn("p FuzzB: in the tree but not in", problems)
        self.assertNotIn("q FuzzC", problems)

    def test_unscheduled_list_may_only_shrink(self):
        rows = [target(f"Fuzz{i}", "p", status="unscheduled", fuzztime="-") for i in range(fs.MAX_UNSCHEDULED + 1)]
        tree = {(t.package, t.name) for t in rows}
        self.assertTrue(any("may only shrink" in p for p in fs.check(rows, tree, 1)))

    def test_an_unscheduled_row_must_say_why(self):
        row = fs.Target("unscheduled", "p", "FuzzA", "-", "-", "-")
        self.assertTrue(any("needs the reason" in p for p in fs.check([row], {("p", "FuzzA")}, 1)))

    def test_shards_partition_the_smoke_targets_and_differ_by_at_most_one(self):
        rows = [target(f"Fuzz{i}") for i in range(42)] + [target("FuzzU", status="unscheduled", fuzztime="-")]
        for shards in range(1, 9):
            with self.subTest(shards=shards):
                parts = [fs.shard_targets(rows, k, shards) for k in range(1, shards + 1)]
                flat = [t.ident for part in parts for t in part]
                self.assertEqual(sorted(flat), sorted(t.ident for t in fs.smoke_targets(rows)))
                self.assertEqual(len(flat), len(set(flat)))
                self.assertLessEqual(max(map(len, parts)) - min(map(len, parts)), 1)


class WorkflowTest(unittest.TestCase):
    def test_matrix_and_run_step_agree_on_the_shard_count(self):
        shards = fs.shard_count()
        self.assertGreaterEqual(shards, 3)
        self.assertLessEqual(shards, 4)
        self.assertIn(f'--of {shards} ', CI)
        self.assertIn(f"}}of{shards}-go", CI)

    def test_skipping_is_limited_to_pull_requests_without_ci_full(self):
        expr = re.search(r"ALLOW_SKIP: (\$\{\{.*?\}\})", CI).group(1)
        self.assertIn("github.event_name == 'pull_request'", expr)
        self.assertIn("!contains(github.event.pull_request.labels.*.name, 'ci-full')", expr)
        self.assertEqual(CI.count("--allow-skip"), 1)

    def test_ledger_is_saved_only_after_a_green_shard(self):
        save = CI[CI.index("name: Save the green-run ledger"):]
        self.assertIn("if: success()", save.split("\n\n")[0])
        self.assertLess(CI.index("scripts/ci/fuzz_smoke.py run"), CI.index("name: Save the green-run ledger"))

    def test_the_required_check_name_is_an_aggregate_over_the_shards(self):
        block = CI[CI.index("\n  fuzz-smoke:\n"):]
        block = block[:block.index("\n  race-stress-shard:")]
        self.assertIn("if: always()", block)
        self.assertIn("needs: [scope, fuzz-smoke-shard]", block)
        self.assertIn("needs.fuzz-smoke-shard.result", block)


class DecisionTest(unittest.TestCase):
    def test_skip_needs_permission_a_key_and_a_green_record(self):
        ledger = {"k1"}
        self.assertTrue(fs.should_skip("k1", ledger, True))
        self.assertFalse(fs.should_skip("k1", ledger, False), "main, ci-full and dispatch never skip")
        self.assertFalse(fs.should_skip("k2", ledger, True), "changed inputs run")
        self.assertFalse(fs.should_skip(None, ledger, True), "a key that cannot be computed runs")
        self.assertFalse(fs.should_skip("", ledger, True))
        self.assertFalse(fs.should_skip("k1", set(), True), "no ledger runs everything")

    def run_shard(self, rows, ledger_text, allow_skip, fail=(), key_for=None):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        ledger = tmp / "ledger.txt"
        if ledger_text is not None:
            ledger.write_text(ledger_text)
        ran = []

        def runner(cmd, cwd):
            ran.append(cmd[5].removeprefix("-fuzz=^").removesuffix("$"))
            return 1 if ran[-1] in fail else 0

        code = fs.run_shard(rows, 1, 1, allow_skip, ledger, ledger, root=tmp, runner=runner,
                            key_for=key_for or (lambda t, root: f"key-{t.name}"))
        return code, ran, ledger.read_text().split() if ledger.exists() else None

    def test_pull_request_skips_recorded_targets_only(self):
        rows = [target("FuzzA"), target("FuzzB"), target("FuzzC")]
        code, ran, ledger = self.run_shard(rows, "key-FuzzA\nkey-FuzzC\nkey-Old\n", True)
        self.assertEqual((code, ran), (0, ["FuzzB"]))
        self.assertEqual(ledger, ["key-FuzzA", "key-FuzzB", "key-FuzzC"], "the new ledger is this run's keys, not a growing union")

    def test_main_runs_everything_and_still_records_the_ledger(self):
        rows = [target("FuzzA"), target("FuzzB")]
        code, ran, ledger = self.run_shard(rows, "key-FuzzA\nkey-FuzzB\n", False)
        self.assertEqual((code, ran), (0, ["FuzzA", "FuzzB"]))
        self.assertEqual(ledger, ["key-FuzzA", "key-FuzzB"])

    def test_a_failed_target_fails_the_shard_and_leaves_the_ledger_alone(self):
        rows = [target("FuzzA"), target("FuzzB"), target("FuzzC")]
        code, ran, ledger = self.run_shard(rows, "key-Old\n", False, fail={"FuzzB"})
        self.assertEqual((code, ran), (1, ["FuzzA", "FuzzB", "FuzzC"]))
        self.assertEqual(ledger, ["key-Old"])

    def test_a_target_without_a_key_runs_and_is_not_recorded(self):
        rows = [target("FuzzA"), target("FuzzB")]
        code, ran, ledger = self.run_shard(rows, "key-FuzzA\n", True, key_for=lambda t, root: None if t.name == "FuzzA" else "kb")
        self.assertEqual((code, ran, ledger), (0, ["FuzzA", "FuzzB"], ["kb"]))

    def test_an_unreadable_ledger_means_nothing_is_skipped(self):
        rows = [target("FuzzA")]
        code, ran, _ = self.run_shard(rows, None, True)
        self.assertEqual((code, ran), (0, ["FuzzA"]))


@unittest.skipUnless(shutil.which("go") and shutil.which("git"), "needs go and git")
class KeyTest(unittest.TestCase):
    """target_key on a throwaway module: a imports b, c is independent, e reads outside its directory."""

    FILES = {
        "go.mod": "module example.test\n\ngo 1.21\n",
        "go.sum": "",
        "b/b.go": "package b\n\nfunc B() int { return 1 }\n",
        "a/a.go": 'package a\n\nimport "example.test/b"\n\nfunc A() int { return b.B() }\n',
        "a/a_test.go": 'package a\n\nimport "testing"\n\nfunc FuzzA(f *testing.F) { f.Fuzz(func(t *testing.T, s string) { _ = A() }) }\n',
        "a/testdata/fuzz/FuzzA/seed": "go test fuzz v1\nstring(\"x\")\n",
        "c/c.go": "package c\n",
        "c/c_test.go": 'package c\n\nimport "testing"\n\nfunc FuzzC(f *testing.F) { f.Fuzz(func(t *testing.T, s string) {}) }\n',
        "e/e.go": "package e\n",
        "e/e_test.go": 'package e\n\nimport (\n\t"os"\n\t"testing"\n)\n\nfunc FuzzE(f *testing.F) {\n\t_, _ = os.ReadFile("../docs/x.txt")\n\tf.Fuzz(func(t *testing.T, s string) {})\n}\n',
        "docs/x.txt": "one\n",
        "other/readme.txt": "one\n",
    }

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        for name, text in self.FILES.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)

    def key(self, package, name, **kw):
        return fs.target_key(target(name, package, **kw), self.root)

    def change(self, name, text):
        (self.root / name).write_text(text)

    def test_the_key_is_stable_and_present(self):
        self.assertTrue(self.key("a", "FuzzA"))
        self.assertEqual(self.key("a", "FuzzA"), self.key("a", "FuzzA"))

    def test_a_change_in_the_package_or_an_import_changes_the_key_and_an_unrelated_package_does_not(self):
        a, c = self.key("a", "FuzzA"), self.key("c", "FuzzC")
        self.change("b/b.go", "package b\n\nfunc B() int { return 2 }\n")
        self.assertNotEqual(self.key("a", "FuzzA"), a, "a dependency of the package")
        self.assertEqual(self.key("c", "FuzzC"), c, "a package that does not import it")
        a = self.key("a", "FuzzA")
        self.change("other/readme.txt", "two\n")
        self.assertEqual(self.key("a", "FuzzA"), a, "a file outside the package closure")

    def test_the_corpus_the_flags_and_the_module_files_are_inputs(self):
        a = self.key("a", "FuzzA")
        self.assertNotEqual(self.key("a", "FuzzA", fuzztime="20s"), a)
        self.assertNotEqual(self.key("a", "FuzzA", timeout="60s"), a)
        self.change("a/testdata/fuzz/FuzzA/seed", "go test fuzz v1\nstring(\"y\")\n")
        changed = self.key("a", "FuzzA")
        self.assertNotEqual(changed, a, "the fuzz corpus")
        c = self.key("c", "FuzzC")
        self.change("go.sum", "example.org/m v1.0.0 h1:abc=\n")
        self.assertNotEqual(self.key("a", "FuzzA"), changed, "go.sum")
        self.assertNotEqual(self.key("c", "FuzzC"), c, "go.sum reaches every package")

    def test_the_go_version_is_an_input(self):
        a = self.key("a", "FuzzA")
        real = fs._go

        def other_go(args, root):
            return "go9.99.9\nlinux\namd64\n1\n" if args[:2] == ["env", "GOVERSION"] else real(args, root)

        fs._go = other_go
        self.addCleanup(setattr, fs, "_go", real)
        self.assertNotEqual(self.key("a", "FuzzA"), a)

    def test_code_that_reads_outside_its_directory_is_keyed_on_the_whole_tree(self):
        e = self.key("e", "FuzzE")
        self.change("docs/x.txt", "two\n")
        self.assertNotEqual(self.key("e", "FuzzE"), e, "a file the test may read")
        self.change("other/readme.txt", "two\n")
        a = self.key("a", "FuzzA")
        self.change("other/readme.txt", "three\n")
        self.assertEqual(self.key("a", "FuzzA"), a, "a package without the escape stays narrow")

    def test_a_dependency_outside_the_module_and_the_module_cache_is_doubt(self):
        self.assertIsNone(fs.target_key(target("FuzzA", "a"), self.root, dirs_for=lambda package, root: None))

    def test_a_package_that_does_not_build_is_doubt(self):
        self.change("a/a.go", "package a\n\nimport \"example.test/missing\"\n")
        self.assertIsNone(self.key("a", "FuzzA"))


if __name__ == "__main__":
    unittest.main()
