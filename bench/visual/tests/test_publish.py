"""publish.py against a local bare repository standing in for GitHub."""
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import publish  # noqa: E402


def git(repo, *a):
    return subprocess.run(["git", "-C", repo, *a], check=True, capture_output=True, text=True).stdout.strip()


class PublishTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.remote = os.path.join(self.tmp.name, "remote.git")
        subprocess.run(["git", "init", "-q", "--bare", self.remote], check=True)

    def tearDown(self):
        self.tmp.cleanup()

    def montage(self, **files):
        d = tempfile.mkdtemp(dir=self.tmp.name)
        for name, size in files.items():
            with open(os.path.join(d, name + ".png"), "wb") as f:
                f.write(b"x" * size)
        return d

    def tree(self):
        return git(self.remote, "ls-tree", "-r", "--name-only", "pr-visuals").splitlines()

    def test_creates_orphan_branch_prunes_own_folder_keeps_others(self):
        sha1 = publish.publish(self.montage(a=10, b=10), 5, self.remote, "pr-visuals")
        publish.publish(self.montage(c=10), 6, self.remote, "pr-visuals")
        self.assertEqual(self.tree(), ["pr-5/a.png", "pr-5/b.png", "pr-6/c.png"])
        publish.publish(self.montage(a=11), 5, self.remote, "pr-visuals")  # b is pruned, a rewritten
        self.assertEqual(self.tree(), ["pr-5/a.png", "pr-6/c.png"])
        self.assertEqual(git(self.remote, "rev-list", "--max-parents=0", "pr-visuals"), sha1)  # one root: orphan
        self.assertEqual(git(self.remote, "log", "-1", "--format=%an <%ae>", "pr-visuals"),
                         "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>")

    def test_unchanged_run_makes_no_commit(self):
        m = self.montage(a=10)
        publish.publish(m, 5, self.remote, "pr-visuals")
        self.assertEqual(publish.publish(m, 5, self.remote, "pr-visuals"), "unchanged")
        self.assertEqual(git(self.remote, "rev-list", "--count", "pr-visuals"), "1")

    def test_refuses_oversized_and_oddly_named_files(self):
        with self.assertRaises(SystemExit):
            publish.publish(self.montage(big=publish.MAX_BYTES + 1), 5, self.remote, "pr-visuals")
        d = self.montage(ok=1)
        open(os.path.join(d, "..evil"), "w").close()
        with self.assertRaises(SystemExit):
            publish.publish(d, 5, self.remote, "pr-visuals")


if __name__ == "__main__":
    unittest.main()
