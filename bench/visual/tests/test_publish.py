"""publish.py against a local bare repository standing in for GitHub."""
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import publish  # noqa: E402

BOT = "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>"


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

    def pub(self, pr, **files):
        return publish.publish(self.montage(**files), pr, self.remote, "pr-visuals")

    def test_every_publish_is_one_parentless_commit_that_keeps_other_folders(self):
        self.pub(5, a=10, b=10)
        self.pub(6, c=10)
        self.pub(5, a=11)  # b is pruned, a rewritten, pr-6 untouched
        self.assertEqual(self.tree(), ["pr-5/a.png", "pr-6/c.png"])
        self.assertEqual(git(self.remote, "rev-list", "--count", "pr-visuals"), "1")  # no history grows
        self.assertEqual(git(self.remote, "log", "-1", "--format=%an <%ae>", "pr-visuals"), BOT)

    def test_unchanged_run_pushes_nothing(self):
        m = self.montage(a=10)
        first = publish.publish(m, 5, self.remote, "pr-visuals")
        self.assertEqual(publish.publish(m, 5, self.remote, "pr-visuals"), "unchanged")
        self.assertEqual(git(self.remote, "rev-parse", "pr-visuals"), first)

    def test_remove_and_prune(self):
        for pr in (1, 2, 3):
            self.pub(pr, a=5)
        publish.remove({"2"}, self.remote, "pr-visuals")
        self.assertEqual(self.tree(), ["pr-1/a.png", "pr-3/a.png"])
        publish.remove(set(), self.remote, "pr-visuals", keep={"3"})
        self.assertEqual(self.tree(), ["pr-3/a.png"])
        self.assertEqual(git(self.remote, "rev-list", "--count", "pr-visuals"), "1")

    def test_remove_on_a_missing_branch_creates_nothing(self):
        self.assertEqual(publish.remove({"9"}, self.remote, "pr-visuals"), "unchanged")
        self.assertEqual(subprocess.run(["git", "-C", self.remote, "branch", "--list"], capture_output=True, text=True).stdout.strip(), "")

    def test_a_lost_race_is_rebuilt_on_the_new_tip(self):
        self.pub(1, a=5)
        other = self.montage(z=5)
        raced = []

        def change(repo):
            if not raced:  # another job pushes between this one's fetch and push
                raced.append(publish.publish(other, 2, self.remote, "pr-visuals"))
            publish_change(repo)

        def publish_change(repo):
            os.makedirs(os.path.join(repo, "pr-3"), exist_ok=True)
            with open(os.path.join(repo, "pr-3", "b.png"), "wb") as f:
                f.write(b"x")
        publish.run_change(change, self.remote, "pr-visuals")
        self.assertEqual(self.tree(), ["pr-1/a.png", "pr-2/z.png", "pr-3/b.png"])
        self.assertEqual(git(self.remote, "rev-list", "--count", "pr-visuals"), "1")

    def test_refuses_oversized_oddly_named_symlinked_and_nested_entries(self):
        with self.assertRaises(SystemExit):
            publish.montages(self.montage(big=publish.MAX_BYTES + 1))
        d = self.montage(ok=1)
        open(os.path.join(d, "..evil"), "w").close()
        with self.assertRaises(SystemExit):
            publish.montages(d)
        d = self.montage(ok=1)
        os.symlink("/etc/passwd", os.path.join(d, "link.png"))
        with self.assertRaises(SystemExit):
            publish.montages(d)
        d = self.montage(ok=1)
        os.mkdir(os.path.join(d, "dir.png"))
        with self.assertRaises(SystemExit):
            publish.montages(d)
        with self.assertRaises(SystemExit):
            publish.montages(self.montage(**{f"f{i}": 1 for i in range(publish.MAX_FILES + 1)}))


if __name__ == "__main__":
    unittest.main()
