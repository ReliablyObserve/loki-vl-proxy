"""The unit tests run as parallel shards: every test in exactly one, merged back into what one run produced."""
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
CI = REPO / "scripts" / "ci"
sys.path.insert(0, str(CI))

import merge_coverprofile  # noqa: E402
import merge_unit_shards  # noqa: E402
import test_shards  # noqa: E402

WORKFLOW = (REPO / ".github" / "workflows" / "ci.yaml").read_text()
PKG = "github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"


def names(n, prefix="TestCase"):
    return [f"{prefix}{i:04d}" for i in range(n)]


class PartitionTest(unittest.TestCase):
    def test_every_test_is_in_exactly_one_shard(self):
        tests = names(500) + ["FuzzThing", "ExampleThing"]
        weights = {"TestCase0003": 300.0, "TestCase0004": 200.0, "TestCase0005": 5.0, "TestGone": 90.0}
        for shards in (1, 2, 3, 4, 7):
            parts = test_shards.partition(tests, shards, weights)
            self.assertEqual(test_shards.check(tests, parts), [])
            self.assertEqual(sorted(n for p in parts for n in p), sorted(tests))

    def test_a_new_test_lands_in_exactly_one_shard_without_a_weight(self):
        tests = names(300)
        before = test_shards.assign(tests, 4, {"TestCase0001": 400.0})
        added = tests + ["TestBrandNew", "TestAnotherNew"]
        after = test_shards.assign(added, 4, {"TestCase0001": 400.0})
        for new in ("TestBrandNew", "TestAnotherNew"):
            self.assertIn(after[new], range(4))
        # the light tests keep their shard when others are added
        for name in tests:
            if name != "TestCase0001":
                self.assertEqual(before[name], after[name])

    def test_the_result_does_not_depend_on_the_order_of_the_list(self):
        tests = names(200)
        weights = {n: 6.0 + i for i, n in enumerate(tests[:20])}
        self.assertEqual(test_shards.assign(tests, 4, weights), test_shards.assign(list(reversed(tests)), 4, weights))

    def test_heavy_tests_are_balanced_longest_first(self):
        weights = {f"TestHeavy{i:02d}": 100.0 - i for i in range(40)}
        tests = list(weights) + names(100)
        parts = test_shards.partition(tests, 4, weights)
        loads = [sum(weights.get(n, 0) for n in p) for p in parts]
        self.assertLess(max(loads) - min(loads), 100.0)
        # the two slowest tests are apart
        placed = test_shards.assign(tests, 4, weights)
        self.assertNotEqual(placed["TestHeavy00"], placed["TestHeavy01"])

    def test_check_names_a_missing_a_doubled_and_an_unknown_test(self):
        tests = ["TestA", "TestB", "TestC"]
        problems = test_shards.check(tests, [["TestA", "TestB"], ["TestB", "TestX"]])
        self.assertEqual(sorted(problems), sorted([
            "TestB is in shard 1 and shard 2", "TestC is in no shard", "TestX is in shard 2 but not in the list"]))

    def test_the_run_pattern_selects_the_shard_and_refuses_unsafe_names(self):
        self.assertEqual(test_shards.pattern(["TestA", "TestB"]), "^(TestA|TestB)$")
        with self.assertRaises(SystemExit):
            test_shards.pattern(["TestA|TestB.*"])
        with self.assertRaises(SystemExit):
            test_shards.pattern([])

    def test_the_list_is_the_tests_go_runs_not_benchmarks_or_the_ok_line(self):
        text = "TestA\nFuzzB\nExampleC\nBenchmarkD\nok  \tgithub.com/x/y\t0.01s\nTestA\n"
        self.assertEqual(test_shards.parse_list(text), ["ExampleC", "FuzzB", "TestA"])

    def test_the_checked_in_weights_parse_and_the_shards_are_balanced(self):
        weights = test_shards.parse_weights((CI / "test-shard-weights.tsv").read_text())
        self.assertGreater(len(weights), 20)
        tests = list(weights) + names(2000)
        loads = [sum(weights.get(n, 0) for n in p) for p in test_shards.partition(tests, 4, weights)]
        self.assertLess((max(loads) - min(loads)) / (sum(loads) / 4), 0.05, loads)

    def test_weights_are_regenerated_from_events(self):
        events = [
            {"Action": "pass", "Package": PKG, "Test": "TestSlow", "Elapsed": 12.0},
            {"Action": "pass", "Package": PKG, "Test": "TestSlow/sub", "Elapsed": 11.0},
            {"Action": "pass", "Package": PKG, "Test": "TestFast", "Elapsed": 0.2},
            {"Action": "pass", "Package": "other/pkg", "Test": "TestOther", "Elapsed": 99.0},
        ]
        seconds = test_shards.weights_from_events([json.dumps(e) for e in events], "/internal/proxy")
        text = test_shards.render_weights(seconds)
        self.assertEqual(test_shards.parse_weights(text), {"TestSlow": 12.0})


class CoverageMergeTest(unittest.TestCase):
    def test_blocks_covered_by_several_shards_are_summed_and_the_rest_copied(self):
        a = "mode: atomic\nf.go:1.1,2.2 1 3\nf.go:3.1,4.2 2 0\n"
        b = "mode: atomic\nf.go:1.1,2.2 1 4\nf.go:10.1,11.2 1 1\n"
        self.assertEqual(merge_coverprofile.merge([a, b]),
                         "mode: atomic\nf.go:1.1,2.2 1 7\nf.go:3.1,4.2 2 0\nf.go:10.1,11.2 1 1\n")

    def test_different_modes_are_refused(self):
        with self.assertRaises(SystemExit):
            merge_coverprofile.merge(["mode: atomic\n", "mode: set\n"])


def write_shard(root, shard, events=(), profile="mode: atomic\n", assigned=None, listed=None, cache=None):
    d = Path(root) / f"unit-shard-{shard}"
    d.mkdir(parents=True)
    (d / "unit-results.json").write_text("".join(json.dumps(e) + "\n" for e in events))
    (d / "coverage.out").write_text(profile)
    if assigned is not None:
        (d / "assigned.txt").write_text("".join(n + "\n" for n in assigned))
        (d / "all-tests.txt").write_text("".join(n + "\n" for n in listed))
    if cache is not None:
        (d / "coverage-cache.out").write_text(cache)


def passed(test, package=PKG):
    return {"Action": "pass", "Package": package, "Test": test, "Elapsed": 0.1}


class MergeShardsTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.shards, self.out = self.dir / "shards", self.dir / "merged"
        self.listed = ["TestA", "TestB", "TestC", "TestD"]

    def build(self, assigned, events=None, drop=None):
        parts = {"1": ["TestA", "TestB"], "2": ["TestC"], "3": ["TestD"], "4": []}
        parts.update(assigned or {})
        for shard, tests in parts.items():
            if shard == drop:
                continue
            ev = [passed(t) for t in tests] if events is None else events.get(shard, [])
            write_shard(self.shards, shard, ev, "mode: atomic\nf.go:1.1,2.2 1 1\n", tests, self.listed)
        write_shard(self.shards, "rest", [passed("TestRest", "x/internal/cache")], "mode: atomic\ng.go:1.1,2.2 1 1\n",
                    cache="mode: atomic\nh.go:1.1,2.2 1 1\n")

    def run_merge(self, complete=True):
        return merge_unit_shards.merge(self.shards, self.out, ["rest", "1", "2", "3", "4"], complete)

    def test_the_merge_holds_every_event_and_the_summed_coverage(self):
        self.build(None)
        problems, count = self.run_merge()
        self.assertEqual(problems, [])
        self.assertEqual(count, 5)
        got = [json.loads(line) for line in (self.out / "unit-results.json").read_text().splitlines()]
        self.assertEqual(sorted(e["Test"] for e in got), ["TestA", "TestB", "TestC", "TestD", "TestRest"])
        self.assertIn("f.go:1.1,2.2 1 4", (self.out / "coverage.out").read_text())
        self.assertTrue((self.out / "coverage-cache.out").is_file())

    def test_a_test_in_two_shards_or_in_none_fails_the_merge(self):
        self.build({"2": ["TestC", "TestB"]})
        problems, _ = self.run_merge()
        self.assertTrue(any("TestB is in shard 1 and shard 2" in p for p in problems), problems)
        shutil.rmtree(self.dir / "shards")
        self.build({"3": []})
        problems, _ = self.run_merge()
        self.assertTrue(any("TestD is in no shard" in p for p in problems), problems)

    def test_an_assigned_test_without_an_outcome_fails_the_merge(self):
        self.build(None, events={"1": [passed("TestA"), passed("TestB")], "2": [passed("TestC")]})
        problems, _ = self.run_merge()
        self.assertEqual(problems, ["TestD was assigned to a shard and has no outcome in the merged results"])

    def test_a_shard_that_uploaded_nothing_fails_a_complete_merge_only(self):
        self.build(None, drop="3")
        problems, _ = self.run_merge()
        self.assertTrue(any("shard 3" in p for p in problems), problems)
        problems, _ = self.run_merge(complete=False)
        self.assertEqual(problems, [])

    def test_the_proxy_shards_must_agree_on_the_list(self):
        self.build(None)
        (self.shards / "unit-shard-2" / "all-tests.txt").write_text("TestA\nTestB\nTestC\n")
        problems, _ = self.run_merge()
        self.assertTrue(any("listed different tests" in p for p in problems), problems)

    def test_the_cli_exit_status(self):
        self.build(None)
        run = subprocess.run([sys.executable, str(CI / "merge_unit_shards.py"), "--shards-dir", str(self.shards),
                              "--out-dir", str(self.out), "--require-complete"], capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        (self.shards / "unit-shard-1" / "assigned.txt").write_text("TestA\n")
        run = subprocess.run([sys.executable, str(CI / "merge_unit_shards.py"), "--shards-dir", str(self.shards),
                              "--out-dir", str(self.out), "--require-complete"], capture_output=True, text=True)
        self.assertEqual(run.returncode, 1)
        self.assertIn("TestB is in no shard", run.stderr)


GO_STUB = """\
#!/bin/bash
echo "go $*" >> "$STUB_LOG"
if [ "$1" = "clean" ]; then exit 0; fi
if [ "$1" = "list" ] && [ "$2" = "-m" ]; then echo stub/mod; exit 0; fi
if [ "$1" = "list" ]; then
  for p in cmd/proxy internal/cache internal/memlimit internal/metrics internal/proxy; do echo "stub/mod/$p"; done
  exit 0
fi
for a in "$@"; do
  case "$a" in
    -list) cat "$STUB_TESTS"; echo "ok  	stub/mod/internal/proxy	0.01s"; exit 0 ;;
    -coverprofile=*) printf 'mode: atomic\\nf.go:1.1,2.2 1 1\\n' > "${a#-coverprofile=}" ;;
  esac
done
echo '{"Action":"pass","Package":"p","Test":"T"}'
"""


@unittest.skipUnless(shutil.which("bash") and shutil.which("python3"), "needs bash and python3")
class RunnerShardTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        (self.dir / "bin").mkdir()
        go = self.dir / "bin" / "go"
        go.write_text(GO_STUB)
        go.chmod(go.stat().st_mode | stat.S_IXUSR)
        self.tests = self.dir / "tests.txt"
        self.tests.write_text("".join(n + "\n" for n in names(300) + ["FuzzSeed"]))
        self.log = self.dir / "calls.log"

    def run_script(self, shard, fresh=False):
        self.log.write_text("")
        out = self.dir / "out"
        env = {**os.environ, "PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}", "STUB_LOG": str(self.log),
               "STUB_TESTS": str(self.tests), "COVERPROFILE": str(self.dir / "cov.out"), "UNIT_SHARD": str(shard),
               "UNIT_SHARDS": "4", "UNIT_SHARD_DIR": str(out)}
        env["UNIT_FRESH"] = "1" if fresh else ""
        done = subprocess.run(["bash", str(CI / "go_test_unit.sh")], capture_output=True, text=True, env=env, cwd=self.dir)
        calls = [c for c in self.log.read_text().splitlines() if c.startswith("go test") and " -list " not in c]
        return done, calls, out

    def test_a_proxy_shard_runs_only_its_tests_of_internal_proxy(self):
        listed = test_shards.parse_list(self.tests.read_text())
        parts = test_shards.partition(listed, 4, test_shards.parse_weights((CI / "test-shard-weights.tsv").read_text()))
        seen = []
        for shard in (1, 2, 3, 4):
            done, calls, out = self.run_script(shard)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(len(calls), 1, calls)
            call = calls[0]
            self.assertIn("./internal/proxy/", call)
            for flag in ("-race", "-timeout=50m", "-json", "-coverprofile="):
                self.assertIn(flag, call)
            self.assertNotIn("-count=1", call)
            run = re.search(r"-run (\S+)", call).group(1)
            self.assertEqual(run, test_shards.pattern(parts[shard - 1]))
            assigned = (out / "assigned.txt").read_text().split()
            self.assertEqual(assigned, sorted(parts[shard - 1]))
            self.assertEqual((out / "all-tests.txt").read_text().split(), sorted(listed))
            seen += assigned
        self.assertEqual(sorted(seen), sorted(listed))

    def test_fresh_expires_stored_results_then_runs_normally_so_they_are_stored(self):
        for shard in (1, "rest"):
            _, calls, _ = self.run_script(shard, fresh=True)
            everything = self.log.read_text().splitlines()
            self.assertIn("go clean -testcache", everything)
            # before the first test run, and it is the only thing that forgets results
            self.assertLess(everything.index("go clean -testcache"),
                            min(i for i, c in enumerate(everything) if c.startswith("go test")))
            # -count=1 would run the tests but store nothing: only the always-uncached packages keep it
            counted = [c for c in calls if " -count=1 " in c]
            if shard == 1:
                self.assertEqual(counted, [], calls)
            else:
                self.assertEqual(len(counted), 1, calls)
                self.assertIn("cmd/proxy", counted[0])
                self.assertNotIn("internal/cache", counted[0])
        self.run_script(1, fresh=False)
        self.assertNotIn("go clean -testcache", self.log.read_text())

    def test_rest_runs_every_package_but_internal_proxy(self):
        done, calls, _ = self.run_script("rest")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(len(calls), 2)
        self.assertFalse(any("internal/proxy" in c for c in calls), calls)
        self.assertTrue(any("internal/cache" in c and " -count=1 " not in c for c in calls))
        self.assertTrue(any("cmd/proxy" in c and " -count=1 " in c for c in calls))

    def test_a_failing_run_fails_the_shard(self):
        self.tests.write_text("")
        done, _, _ = self.run_script(1)
        self.assertNotEqual(done.returncode, 0)


GH_STUB = """\\
#!/bin/bash
echo "gh $*" >> "$STUB_LOG"
[ -n "${STUB_GH_FAIL:-}" ] && exit 1
printf '%s\\n' $STUB_LABELS
"""


@unittest.skipUnless(shutil.which("bash"), "needs bash")
class ModeTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        (self.dir / "bin").mkdir()
        gh = self.dir / "bin" / "gh"
        gh.write_text(GH_STUB.replace("\\\\", "\\"))
        gh.chmod(gh.stat().st_mode | stat.S_IXUSR)

    def mode(self, event="pull_request", pr="12", labels="bug", fail=False):
        env = {**os.environ, "PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}", "STUB_LOG": str(self.dir / "log"),
               "EVENT_NAME": event, "REPO": "o/r", "PR_NUMBER": pr, "STUB_LABELS": labels}
        if fail:
            env["STUB_GH_FAIL"] = "1"
        done = subprocess.run(["bash", str(CI / "unit_test_mode.sh")], capture_output=True, text=True, env=env)
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout.strip()

    def test_a_pull_request_replays(self):
        self.assertEqual(self.mode(), "fresh=false")

    def test_main_dispatch_schedule_and_the_ci_full_label_run_fresh(self):
        for event in ("push", "workflow_dispatch", "schedule"):
            self.assertEqual(self.mode(event=event, pr=""), "fresh=true", event)
        self.assertEqual(self.mode(labels="bug ci-full"), "fresh=true")

    def test_doubt_runs_fresh(self):
        self.assertEqual(self.mode(pr=""), "fresh=true")
        self.assertEqual(self.mode(fail=True), "fresh=true")


class WorkflowWiringTest(unittest.TestCase):
    def job(self, name):
        m = re.search(r"(?ms)^  " + re.escape(name) + r":\n(.*?)(?=^  [a-z0-9-]+:\n)", WORKFLOW)
        self.assertIsNotNone(m, name)
        return m.group(1)

    def test_the_matrix_and_the_shard_count_agree(self):
        shard = self.job("test-shard")
        matrix = re.search(r"shard: \[([^\]]*)\]", shard).group(1).replace(" ", "").split(",")
        numbered = [m for m in matrix if m != "rest"]
        self.assertEqual(numbered, [str(i) for i in range(1, len(numbered) + 1)])
        self.assertIn("rest", matrix)
        self.assertEqual(re.search(r'UNIT_SHARDS: "(\d+)"', shard).group(1), str(len(numbered)))
        merge = self.job("test")
        listed = re.search(r'SHARDS: "([^"]*)"', merge).group(1).split()
        self.assertEqual(sorted(listed), sorted(matrix))

    def test_the_race_shards_use_the_weighted_split_with_their_matrix_count(self):
        # race-stress-shard splits internal/proxy with the same weights as the
        # test job; a checksum split put the heaviest -race tests together.
        race = self.job("race-stress-shard")
        matrix = re.search(r"shard: \[([^\]]*)\]", race).group(1).replace(" ", "").split(",")
        numbered = [m for m in matrix if m != "other"]
        self.assertEqual(numbered, [str(i) for i in range(1, len(numbered) + 1)])
        self.assertIn("other", matrix)
        counts = set(re.findall(r"scripts/ci/test_shards\.py (?:check|regex) --shards (\d+)", race))
        self.assertEqual(counts, {str(len(numbered))})
        self.assertNotIn("cksum", race)

    def test_the_check_named_test_fans_in_and_fails_unless_every_shard_passed(self):
        test = self.job("test")
        self.assertIn("needs: [scope, test-shard]", test)
        self.assertIn("needs.scope.outputs.product != 'false'", test)
        self.assertNotIn("name:", test.split("steps:")[0])  # the check keeps the job id as its name
        self.assertRegex(test, r"SHARDS_RESULT: \$\{\{ needs\.test-shard\.result \}\}")
        self.assertIn('!= success', test)
        for artifact in ("name: unit-results", "name: coverage", "quality-meta.json", "--require-complete"):
            self.assertIn(artifact, test)

    def test_nothing_else_in_the_workflow_started_running_the_unit_suite(self):
        self.assertEqual(WORKFLOW.count("scripts/ci/go_test_unit.sh"), 1)

    def test_every_downstream_job_still_needs_the_test_check(self):
        for name in ("conformance", "tuple-contract", "fuzz-smoke-shard", "race-stress-shard"):
            self.assertRegex(self.job(name), r"needs: \[[^\]]*\btest\b[^\]]*\]", name)


if __name__ == "__main__":
    unittest.main()
