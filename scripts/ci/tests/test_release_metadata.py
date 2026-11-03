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
# A stand-in for gh: git refs of the local origin (BARE), and pull requests kept
# in FAKE_DIR/prs.json. Deleting a branch closes its pull request, as GitHub does.
import json, os, subprocess, sys
d, bare = os.environ["FAKE_DIR"], os.environ["BARE"]
a = sys.argv[1:]
path = os.path.join(d, "prs.json")
prs = json.load(open(path)) if os.path.exists(path) else []
def save(): json.dump(prs, open(path, "w"))
def log(**kw):
    with open(os.path.join(d, "gh.jsonl"), "a") as fh: fh.write(json.dumps(kw) + "\n")
def git(*x, check=True):
    return subprocess.run(["git", "-C", bare, *x], capture_output=True, text=True, check=check)
def opt(name):
    return next((x.split("=", 1)[1] for x in a if x.startswith(name + "=")), None)
def flag(name): return a[a.index(name) + 1] if name in a else None
def branch_of(route): return route.split("/heads/", 1)[1]
if a[0] == "api":
    route = next(x for x in a if x.startswith("repos/"))
    method = flag("-X") or "GET"
    if method == "GET":
        sys.exit(0 if git("rev-parse", "-q", "--verify", "refs/heads/" + branch_of(route), check=False).returncode == 0 else 1)
    if method == "PATCH":
        log(op="patch", branch=branch_of(route), sha=opt("sha"))
        if os.environ.get("FAIL_PATCH"): sys.exit("HTTP 422: refusing to update workflow")
        git("update-ref", "refs/heads/" + branch_of(route), opt("sha"))
    elif method == "POST":
        ref = opt("ref"); log(op="post", branch=ref[len("refs/heads/"):], sha=opt("sha"))
        git("update-ref", ref, opt("sha"))
    elif method == "DELETE":
        b = branch_of(route); log(op="delete", branch=b)
        git("update-ref", "-d", "refs/heads/" + b, check=False)
        for p in prs:
            if p["headRefName"] == b and p["state"] == "OPEN": p["state"] = "CLOSED"
        save()
elif a[:2] == ["pr", "list"]:
    print(json.dumps([p for p in prs if p["state"] == "OPEN"]))
elif a[:2] == ["pr", "view"]:
    print(json.dumps({"state": next(p["state"] for p in prs if str(p["number"]) == a[2])}))
elif a[:2] == ["pr", "close"]:
    with open(os.path.join(d, "closed.txt"), "a") as fh: fh.write(a[2] + "\n")
    for p in prs:
        if str(p["number"]) == a[2]: p["state"] = "CLOSED"
    save()
elif a[:2] == ["pr", "reopen"]:
    for p in prs:
        if str(p["number"]) == a[2]:
            if git("rev-parse", "-q", "--verify", "refs/heads/" + p["headRefName"], check=False).returncode: sys.exit(1)
            p["state"] = "OPEN"
    log(op="reopen", number=a[2]); save()
elif a[:2] == ["pr", "create"]:
    prs.append({"number": 200 + len(prs), "headRefName": flag("--head"), "state": "OPEN",
                "author": {"login": "szibis"}, "isCrossRepository": False})
    log(op="create", branch=flag("--head")); save()
elif a[:2] in (["pr", "merge"], ["pr", "edit"]):
    log(op=a[1], number=a[2])
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
        patch = {"COMMIT_VIA_API": str(fake / "commit_via_api.sh"), "FAKE_DIR": str(self.dir / "fake"), "BARE": str(self.bare),
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
        sh(str(self.work), "git", "fetch", "-q", "origin", f"+{branch}:refs/remotes/origin/{branch}")


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

    def test_an_entry_edited_after_the_tag_moves_once_and_the_pull_request_says_so(self):
        main = self.main
        main.write("CHANGELOG.md", main.read("CHANGELOG.md").replace("- A.", "- A.\n  - a detail added after the tag"))
        main.commit("docs(changelog): detail")
        sh(main.path, "git", "push", "-q", "origin", "main")
        body = self.dir / "body.md"
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", body_out=str(body))
        branch = "release/metadata-v2.62.1"
        self.fetch_branch(branch)
        changelog = self.show(branch, "CHANGELOG.md")
        self.assertEqual(changelog.count("- A.\n"), 1)
        self.assertIn("a detail added after the tag", changelog.split("## [2.62.1]")[1].split("## [2.62.0]")[0])
        self.assertNotIn("a detail added after the tag", changelog.split("## [2.62.1]")[0])
        self.assertIn("reworded or extended", body.read_text())

    def test_nothing_to_do_when_main_already_has_the_release(self):
        main = self.main
        main.merge_metadata("2.62.1")
        sh(main.path, "git", "push", "-q", "origin", "main")
        changes, _ = rm.prepare(str(self.work), "2.62.1", "o/r", "tok")
        self.assertFalse(changes)
        self.assertEqual(self.commits(), [])


class RefreshTests(MetadataCase):
    def prs(self, *versions, author="szibis", cross=False, state="OPEN"):
        path = self.dir / "fake" / "prs.json"
        data = json.loads(path.read_text()) if path.exists() else []
        for v in versions:
            data.append({"number": 100 + len(data), "headRefName": f"release/metadata-v{v}", "state": state,
                         "author": {"login": author}, "isCrossRepository": cross})
        path.write_text(json.dumps(data))

    def state(self, number):
        return {p["number"]: p["state"] for p in json.loads((self.dir / "fake" / "prs.json").read_text())}[number]

    def closed(self):
        path = self.dir / "fake" / "closed.txt"
        return path.read_text().split() if path.exists() else []

    def ops(self):
        path = self.dir / "fake" / "gh.jsonl"
        return [json.loads(l) for l in path.read_text().splitlines()] if path.exists() else []

    def main_moves_on(self, path="internal/proxy/c.go", entry="- C."):
        self.main.commit("fix: c (#3)", files={path: "c"}, entry=entry)
        sh(self.main.path, "git", "push", "-q", "origin", "main")

    def test_a_branch_behind_main_is_regenerated_and_older_pull_requests_close(self):
        self.prs("2.62.0", "2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.assertEqual(len(self.commits()), 1)
        self.main_moves_on()  # a new [Unreleased] entry: the CHANGELOG the branch holds is stale
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.closed(), ["100"])
        self.assertEqual(len(self.commits()), 2)
        self.fetch_branch("release/metadata-v2.62.1")
        changelog = self.show("release/metadata-v2.62.1", "CHANGELOG.md")
        self.assertEqual(changelog.split("## [2.62.1]")[0].count("- C."), 1)
        self.assertEqual(changelog.split("## [2.62.1]")[1].count("- C."), 0)
        # the badge numbers from the release run survive the regeneration
        self.assertIn("7%20passed", self.show("release/metadata-v2.62.1", "README.md"))
        self.assertEqual(self.state(101), "OPEN")

    def test_the_pull_request_branch_never_goes_through_the_tip_of_main(self):
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.main_moves_on()
        rm.refresh(str(self.work), "o/r", "tok")
        tips = {sh(str(self.bare), "git", "rev-parse", "main"), *[c["base"] for c in self.commits()]}
        branch_ops = [o for o in self.ops() if o.get("branch", "").startswith("release/metadata-v")]
        self.assertTrue(branch_ops)
        for op in branch_ops:
            self.assertNotIn(op.get("sha"), tips)
        # the commit was built on a temporary branch, which is gone
        self.assertEqual(sh(str(self.bare), "git", "branch", "--list", "release/metadata-tmp-*"), "")

    def test_a_refused_move_recreates_the_branch_and_the_pull_request_stays_open(self):
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.main_moves_on()
        os.environ["FAIL_PATCH"] = "1"
        self.addCleanup(lambda: os.environ.pop("FAIL_PATCH", None))
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.state(100), "OPEN")
        self.assertIn("reopen", [o["op"] for o in self.ops()])
        self.assertIn("merge", [o["op"] for o in self.ops()])  # auto-merge armed again
        self.assertEqual(sh(str(self.bare), "git", "rev-parse", "release/metadata-v2.62.1"), self.commits()[-1]["commit"])

    def test_a_pull_request_that_cannot_be_brought_back_fails_loudly(self):
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.main_moves_on()
        os.environ["FAIL_PATCH"] = "1"
        self.addCleanup(lambda: os.environ.pop("FAIL_PATCH", None))
        # a gh that cannot reopen or create: the state stays closed
        path = self.dir / "bin" / "gh"
        path.write_text(path.read_text().replace('p["state"] = "OPEN"\n    log(op="reopen"', 'pass\n    log(op="reopen"')
                        .replace('prs.append({"number": 200', 'sys.exit(1); prs.append({"number": 200'))
        with self.assertRaises(RuntimeError):
            rm.refresh(str(self.work), "o/r", "tok")

    def test_a_branch_that_holds_what_main_needs_is_left_alone_even_when_main_moved(self):
        # The owner re-signed the commit; main moved on in files the metadata does not touch.
        self.prs("2.62.1")
        rm.prepare(str(self.work), "2.62.1", "o/r", "tok", "7", "80", "1000")
        self.main_moves_on(path="docs/other.md", entry=None)
        (self.dir / "fake" / "gh.jsonl").unlink()
        rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(len(self.commits()), 1)
        self.assertEqual(self.closed(), [])
        self.assertEqual([o for o in self.ops() if o["op"] in ("patch", "delete", "reopen")], [])

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

    def test_only_pull_requests_of_the_bot_or_the_owner_from_this_repository_count(self):
        self.prs("2.62.1", author="someone-else")
        self.prs("2.62.1", cross=True)
        self.assertEqual(rm.open_metadata_prs("o/r", "tok"), [])
        self.prs("2.62.1", author="ReliablyObserve-Bot")
        self.assertEqual([n for _, n, _ in rm.open_metadata_prs("o/r", "tok")], [102])

    def test_older_pull_requests_stay_open_when_the_newest_cannot_be_regenerated(self):
        self.prs("2.62.1", "2.99.0")  # v2.99.0 has no tag: the CHANGELOG move cannot be made
        with self.assertRaises(RuntimeError):
            rm.refresh(str(self.work), "o/r", "tok")
        self.assertEqual(self.closed(), [])

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
