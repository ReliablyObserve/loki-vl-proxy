import base64
import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "commit_via_api.sh"

# A stand-in for the gh CLI: records every call (argv, and the --input body for
# graphql) in calls.jsonl and answers the way GitHub would. BRANCH_EXISTS decides
# whether the branch ref lookup succeeds; OID is what createCommitOnBranch returns.
FAKE_GH = r"""#!/usr/bin/env python3
import json, os, sys
log = os.path.join(os.environ["FAKE_DIR"], "calls.jsonl")
args = sys.argv[1:]
entry = {"argv": args}
if "--input" in args:
    with open(args[args.index("--input") + 1]) as f:
        entry["input"] = json.load(f)
with open(log, "a") as f:
    f.write(json.dumps(entry) + "\n")
if args[:2] == ["api", "graphql"]:
    print(os.environ.get("OID", ""))
elif len(args) >= 2 and args[0] == "api" and "/git/ref/heads/" in args[1]:
    sys.exit(0 if os.environ.get("BRANCH_EXISTS") == "1" else 1)
"""


class CommitViaAPITests(unittest.TestCase):
    def run_script(self, files, branch_exists, oid="abc123", setup=None):
        with tempfile.TemporaryDirectory() as tmp:
            fake = Path(tmp) / "bin"
            fake.mkdir()
            gh = fake / "gh"
            gh.write_text(FAKE_GH)
            gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
            work = Path(tmp) / "work"
            work.mkdir()
            if setup:
                setup(work)
            env = {**os.environ, "PATH": f"{fake}:{os.environ['PATH']}", "FAKE_DIR": tmp,
                   "BRANCH_EXISTS": "1" if branch_exists else "0", "OID": oid}
            proc = subprocess.run(["bash", str(SCRIPT), "acme/repo", "release/metadata-v1.2.3", "base999",
                                   "docs: sync release metadata for v1.2.3", *files],
                                  cwd=work, env=env, capture_output=True, text=True)
            log = Path(tmp) / "calls.jsonl"
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return proc, calls

    @staticmethod
    def write_files(work):
        (work / "CHANGELOG.md").write_text("## [1.2.3]\n")
        (work / "charts").mkdir()
        (work / "charts" / "Chart.yaml").write_text("version: 1.2.3\n")

    def test_new_branch_is_created_at_base_and_files_are_committed_through_the_api(self):
        proc, calls = self.run_script(["CHANGELOG.md", "charts/Chart.yaml"], branch_exists=False, setup=self.write_files)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), "abc123")
        create = [c for c in calls if c["argv"][:4] == ["api", "-X", "POST", "repos/acme/repo/git/refs"]]
        self.assertEqual(len(create), 1, calls)
        self.assertIn("ref=refs/heads/release/metadata-v1.2.3", create[0]["argv"])
        self.assertIn("sha=base999", create[0]["argv"])
        graphql = [c for c in calls if c["argv"][:2] == ["api", "graphql"]]
        self.assertEqual(len(graphql), 1)
        body = graphql[0]["input"]
        self.assertIn("createCommitOnBranch", body["query"])
        inp = body["variables"]["input"]
        self.assertEqual(inp["branch"], {"repositoryNameWithOwner": "acme/repo", "branchName": "release/metadata-v1.2.3"})
        self.assertEqual(inp["expectedHeadOid"], "base999")
        self.assertEqual(inp["message"], {"headline": "docs: sync release metadata for v1.2.3"})
        added = {a["path"]: base64.b64decode(a["contents"]).decode() for a in inp["fileChanges"]["additions"]}
        self.assertEqual(added, {"CHANGELOG.md": "## [1.2.3]\n", "charts/Chart.yaml": "version: 1.2.3\n"})
        self.assertEqual(inp["fileChanges"]["deletions"], [])

    def test_existing_branch_is_forced_back_to_base_and_a_missing_file_is_deleted(self):
        proc, calls = self.run_script(["CHANGELOG.md", "docs/gone.md"], branch_exists=True, setup=self.write_files)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        reset = [c for c in calls if c["argv"][:3] == ["api", "-X", "PATCH"]]
        self.assertEqual(len(reset), 1, calls)
        self.assertEqual(reset[0]["argv"][3], "repos/acme/repo/git/refs/heads/release/metadata-v1.2.3")
        self.assertIn("sha=base999", reset[0]["argv"])
        self.assertIn("force=true", reset[0]["argv"])
        self.assertFalse(any(c["argv"][:3] == ["api", "-X", "POST"] for c in calls))
        inp = [c for c in calls if c["argv"][:2] == ["api", "graphql"]][0]["input"]["variables"]["input"]
        self.assertEqual(inp["fileChanges"]["deletions"], [{"path": "docs/gone.md"}])

    def test_no_commit_returned_fails(self):
        proc, _ = self.run_script(["CHANGELOG.md"], branch_exists=False, oid="", setup=self.write_files)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("returned no commit", proc.stderr)

    def test_usage_without_files(self):
        proc = subprocess.run(["bash", str(SCRIPT), "acme/repo", "b", "base"], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 2)
        self.assertIn("usage", proc.stderr)


if __name__ == "__main__":
    unittest.main()
