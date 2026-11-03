import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from scripts.ci import release_metadata as rm
from scripts.ci.tests.release_repo import Repo, sh

# Stands in for commit_via_api.sh: the same contract (REPO BRANCH BASE MESSAGE
# FILE...), without GitHub. It commits the working-tree FILEs on top of BASE and
# force-pushes that commit to BRANCH of the local origin. When MOVE_ONCE names a
# clone, the first call first lands a commit on main from it, as a merge would
# while the job was running.
FAKE_COMMIT = r"""#!/usr/bin/env python3
import json, os, subprocess, sys
repo, branch, base, message, *files = sys.argv[1:]
d = os.environ["FAKE_DIR"]
def sh(*a, env=None):
    return subprocess.run(a, check=True, capture_output=True, text=True, env=env).stdout.strip()
log = os.path.join(d, "commits.jsonl")
n = len(open(log).read().splitlines()) if os.path.exists(log) else 0
move = os.environ.get("MOVE_ONCE")
if n >= int(os.environ.get("FAIL_AFTER", "1000")):
    sys.exit("commit refused")
if move and n == 0:
    sh("git", "-C", move, "commit", "-q", "--allow-empty", "-m", "fix: landed meanwhile")
    sh("git", "-C", move, "push", "-q", "origin", "HEAD:main")
idx = os.path.join(d, "index")
env = {**os.environ, "GIT_INDEX_FILE": idx}
if os.path.exists(idx): os.remove(idx)
sh("git", "read-tree", base, env=env)
for f in files:
    blob = sh("git", "hash-object", "-w", f)
    sh("git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{f}", env=env)
tree = sh("git", "write-tree", env=env)
commit = sh("git", "commit-tree", tree, "-p", base, "-m", message)
sh("git", "push", "-q", "--force", "origin", f"{commit}:refs/heads/{branch}")
with open(log, "a") as fh:
    fh.write(json.dumps({"branch": branch, "base": base, "files": files, "commit": commit}) + "\n")
print(commit)
"""

FAKE_WRAPPER = '#!/usr/bin/env bash\nexec python3 "$(dirname "$0")/commit.py" "$@"\n'

FAKE_GH = r"""#!/usr/bin/env python3
import json, os, sys
d = os.environ["FAKE_DIR"]
a = sys.argv[1:]
if a[:2] == ["pr", "list"]:
    print(open(os.path.join(d, "prs.json")).read())
elif a[:2] == ["pr", "close"]:
    with open(os.path.join(d, "closed.txt"), "a") as fh:
        fh.write(a[2] + "\n")
else:
    sys.exit(1)
"""


class MetadataCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.main = Repo()
        self.addCleanup(self.main.close)
        self.bare = self.dir / "origin.git"
        r = self.main
        r.commit("fix: a (#1)", files={"internal/proxy/a.go": "a"}, entry="- A.")
        r.tag("v2.62.1")
        r.commit("fix: b (#2)", files={"internal/proxy/b.go": "b"}, entry="- B.")
        sh(r.path, "git", "clone", "-q", "--bare", r.path, str(self.bare))
        sh(r.path, "git", "remote", "add", "origin", str(self.bare))
        sh(r.path, "git", "push", "-q", "origin", "main", "--tags")
        # the job's checkout: the tagged commit, with every tag
        self.work = self.dir / "work"
        sh(str(self.dir), "git", "clone", "-q", str(self.bare), str(self.work))
        sh(str(self.work), "git", "checkout", "-q", "--detach", "v2.62.1")
        for cfg in (("user.email", "t@example.com"), ("user.name", "t")):
            sh(str(self.work), "git", "config", *cfg)
        fake = self.dir / "bin"
        fake.mkdir()
        for name, body in (("commit.py", FAKE_COMMIT), ("commit_via_api.sh", FAKE_WRAPPER), ("gh", FAKE_GH)):
            path = fake / name
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        (self.dir / "fake").mkdir()
        os.environ_backup = dict(os.environ)
        patch = {"COMMIT_VIA_API": str(fake / "commit_via_api.sh"), "FAKE_DIR": str(self.dir / "fake"),
                 "PATH": f"{fake}:{os.environ['PATH']}"}
        for k, v in patch.items():
            old = os.environ.get(k)
            os.environ[k] = v
            self.addCleanup(lambda k=k, old=old: os.environ.pop(k, None) if old is None else os.environ.__setitem__(k, old))
        self.addCleanup(lambda: os.environ.pop("MOVE_ONCE", None))

    def commits(self):
        log = self.dir / "fake" / "commits.jsonl"
        return [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []

    def show(self, branch, path):
        return sh(str(self.work), "git", "show", f"origin/{branch}:{path}")

    def fetch_branch(self, branch):
        sh(str(self.work), "git", "fetch", "-q", "origin", f"{branch}:refs/remotes/origin/{branch}")


class PrepareTests(MetadataCase):
    def test_entries_merged_after_the_tag_stay_in_unreleased(self):
        changes, branch = rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "321", "81.5", "12345")
        self.assertTrue(changes)
        self.assertEqual(branch, "release/metadata-v2.62.1")
        self.fetch_branch(branch)
        changelog = self.show(branch, "CHANGELOG.md")
        self.assertIn("## [2.62.1] - 2026-11-03\n\n### Fixed\n\n- A.\n", changelog)
        self.assertEqual(changelog.split("## [2.62.1]")[0].count("- B."), 1)  # B is still under [Unreleased]
        self.assertEqual(changelog.split("## [2.62.1]")[1].count("- B."), 0)
        self.assertIn("version: 2.62.1", self.show(branch, "charts/loki-vl-proxy/Chart.yaml"))
        self.assertIn("321%20passed", self.show(branch, "README.md"))

    def test_the_branch_is_made_on_the_current_main_not_the_tagged_commit(self):
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok")
        (call,) = self.commits()
        self.assertEqual(call["base"], sh(str(self.bare), "git", "rev-parse", "main"))
        self.assertNotEqual(call["base"], sh(str(self.work), "git", "rev-parse", "v2.62.1"))

    def test_main_moving_while_the_job_runs_makes_it_generate_again(self):
        mover = self.dir / "mover"
        sh(str(self.dir), "git", "clone", "-q", str(self.bare), str(mover))
        for cfg in (("user.email", "t@example.com"), ("user.name", "t")):
            sh(str(mover), "git", "config", *cfg)
        os.environ["MOVE_ONCE"] = str(mover)
        changes, branch = rm.prepare(str(self.work), "2.62.1", "o/r", "tok")
        first, second = self.commits()
        self.assertTrue(changes)
        self.assertNotEqual(first["base"], second["base"])
        self.assertEqual(second["base"], sh(str(self.bare), "git", "rev-parse", "main"))
        # the branch tip is the second one, one commit on top of the new main
        self.assertEqual(sh(str(self.bare), "git", "rev-parse", branch), second["commit"])
        self.assertEqual(sh(str(self.bare), "git", "rev-parse", f"{branch}^"), second["base"])

    def test_a_failure_after_a_branch_was_pushed_keeps_the_branch_and_its_pull_request(self):
        # Main moves, the second attempt cannot commit: the first branch stays,
        # and the step opens its pull request (the refresh job fixes the rest).
        mover = self.dir / "mover"
        sh(str(self.dir), "git", "clone", "-q", str(self.bare), str(mover))
        for cfg in (("user.email", "t@example.com"), ("user.name", "t")):
            sh(str(mover), "git", "config", *cfg)
        os.environ["MOVE_ONCE"] = str(mover)
        os.environ["FAIL_AFTER"] = "1"
        self.addCleanup(lambda: os.environ.pop("FAIL_AFTER", None))
        changes, branch = rm.prepare(str(self.work), "2.62.1", "o/r", "tok")
        self.assertEqual((changes, len(self.commits())), (True, 1))
        self.assertEqual(sh(str(self.bare), "git", "rev-parse", branch), self.commits()[0]["commit"])

    def test_a_failure_before_any_branch_was_pushed_is_an_error(self):
        os.environ["FAIL_AFTER"] = "0"
        self.addCleanup(lambda: os.environ.pop("FAIL_AFTER", None))
        with self.assertRaises(RuntimeError):
            rm.prepare(str(self.work), "2.62.1", "o/r", "tok")

    def test_nothing_to_do_when_main_already_has_the_release(self):
        main = self.main
        main.merge_metadata("2.62.1")
        sh(main.path, "git", "push", "-q", "origin", "main")
        changes, _ = rm.prepare(str(self.work), "2.62.1", "o/r", "tok")
        self.assertFalse(changes)
        self.assertEqual(self.commits(), [])


class RefreshTests(MetadataCase):
    def prs(self, *versions):
        data = [{"number": 100 + i, "headRefName": f"release/metadata-v{v}"} for i, v in enumerate(versions)]
        (self.dir / "fake" / "prs.json").write_text(json.dumps(data))

    def closed(self):
        path = self.dir / "fake" / "closed.txt"
        return path.read_text().split() if path.exists() else []

    def test_a_branch_behind_main_is_regenerated_and_older_pull_requests_close(self):
        self.prs("2.62.0", "2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.assertEqual(len(self.commits()), 1)
        # main moves on after the branch was made
        self.main.commit("fix: c (#3)", files={"internal/proxy/c.go": "c"}, entry="- C.")
        sh(self.main.path, "git", "push", "-q", "origin", "main")
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.closed(), ["100"])
        self.assertEqual(len(self.commits()), 2)
        self.fetch_branch("release/metadata-v2.62.1")
        changelog = self.show("release/metadata-v2.62.1", "CHANGELOG.md")
        self.assertEqual(changelog.split("## [2.62.1]")[0].count("- C."), 1)
        self.assertEqual(changelog.split("## [2.62.1]")[1].count("- C."), 0)
        # the badge numbers from the release run survive the regeneration
        self.assertIn("7%20passed", self.show("release/metadata-v2.62.1", "README.md"))

    def test_a_branch_that_is_on_main_and_matches_is_left_alone(self):
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(len(self.commits()), 1)
        self.assertEqual(self.closed(), [])

    def test_a_pull_request_whose_content_main_already_has_is_closed(self):
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        # the pull request merged: its files are on main
        self.fetch_branch("release/metadata-v2.62.1")
        self.main.write("README.md", self.show("release/metadata-v2.62.1", "README.md") + "\n")
        self.main.merge_metadata("2.62.1")
        sh(self.main.path, "git", "push", "-q", "origin", "main")
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.closed(), ["100"])
        self.assertEqual(len(self.commits()), 1)

    def test_no_open_pull_request_is_a_no_op(self):
        self.prs()
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.commits(), [])


class BadgeTests(unittest.TestCase):
    def test_only_the_badge_lines_are_copied(self):
        src = "[![Tests](a)](#t)\n[![Coverage](b)](#t)\n[![Lines of Code](c)](u)\nother\n"
        dst = "x\n[![Tests](old)](#t)\ny\n[![Lines of Code](older)](u)\n"
        self.assertEqual(rm.copy_badges(src, dst), "x\n[![Tests](a)](#t)\ny\n[![Lines of Code](c)](u)\n")


if __name__ == "__main__":
    unittest.main()
