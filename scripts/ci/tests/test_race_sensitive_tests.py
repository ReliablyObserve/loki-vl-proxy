"""race_sensitive_tests.sh: every test of a file that reads the race detector's state is run once without it."""
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "race_sensitive_tests.sh"
NAMES = re.compile(r"raceDetectorEnabled|skipSimulationUnderRace|requireBoundedAlloc")
TEST_FUNC = re.compile(r"^func (Test[A-Za-z0-9_]+)", re.M)

GO_STUB = """\
#!/bin/bash
echo "go $*" >> "$STUB_LOG"
pkg="$2"
echo "{\\"Action\\":\\"pass\\",\\"Package\\":\\"p/${pkg#./}\\",\\"Test\\":\\"TestStub\\",\\"Elapsed\\":0.01}"
if [ -n "${STUB_FAIL_PKG:-}" ] && [ "$pkg" = "$STUB_FAIL_PKG" ]; then
  echo "{\\"Action\\":\\"fail\\",\\"Package\\":\\"p/${pkg#./}\\",\\"Test\\":\\"TestBroken\\"}"
  exit 1
fi
"""


def expected():
    """dir -> the tests of the files in it that use the names, found independently of the script."""
    found = {}
    for path in sorted((REPO / "internal").rglob("*_test.go")):
        text = path.read_text(errors="ignore")
        if NAMES.search(text):
            found.setdefault(str(path.parent.relative_to(REPO)), set()).update(TEST_FUNC.findall(text))
    return {d: tests for d, tests in found.items() if tests}


class RaceSensitiveTestsTest(unittest.TestCase):
    def setUp(self):
        if shutil.which("jq") is None:
            self.skipTest("jq is not installed")
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        (self.dir / "bin").mkdir()
        go = self.dir / "bin" / "go"
        go.write_text(GO_STUB)
        go.chmod(go.stat().st_mode | stat.S_IXUSR)
        self.log = self.dir / "calls.log"
        self.out = self.dir / "out.json"

    def run_script(self, **env):
        self.log.write_text("")
        return subprocess.run(
            ["bash", str(SCRIPT), str(self.out)], cwd=self.dir, capture_output=True, text=True, timeout=60,
            env={**os.environ, "PATH": f"{self.dir / 'bin'}{os.pathsep}{os.environ['PATH']}",
                 "STUB_LOG": str(self.log), **env})

    def calls(self):
        out = {}
        for line in self.log.read_text().splitlines():
            m = re.match(r"go test \./(\S+) -count=1 -json -run \^\((.*)\)\$$", line)
            self.assertIsNotNone(m, line)
            out[m.group(1)] = set(m.group(2).split("|"))
        return out

    def test_one_run_per_directory_with_every_test_of_the_files_that_use_the_names(self):
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        want = expected()
        self.assertEqual(self.calls(), want)
        # the ones this exists for
        self.assertIn("TestMetadataScanSim_LimitConverges", want["internal/proxy"])
        self.assertIn("TestBufferedBackendReads_ErrorBodiesAreBounded", want["internal/proxy"])
        self.assertIn("TestDecodePeerResponseBodyRejectsOversizedZstdWindow", want["internal/cache"])

    def test_the_events_of_every_package_are_written_for_the_gate(self):
        self.run_script()
        events = [json.loads(line) for line in self.out.read_text().splitlines()]
        self.assertEqual(len(events), len(expected()))
        self.assertTrue(all(e["Action"] == "pass" for e in events))

    def test_a_failing_package_fails_the_script_after_the_others_ran(self):
        done = self.run_script(STUB_FAIL_PKG="./internal/cache")
        self.assertEqual(done.returncode, 1)
        self.assertEqual(set(self.calls()), set(expected()))
        self.assertIn("TestBroken", done.stderr)


if __name__ == "__main__":
    unittest.main()
