import unittest

from scripts.ci import release_plan
from scripts.ci.tests.release_repo import Repo


def pr(*labels, files=3, churn=50):
    return {"labels": [{"name": l} for l in labels], "changed_files": files, "additions": churn, "deletions": 0}


class PlanCase(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)
        self.prs = {}

    def fix(self, number, name, entry="- Entry.", labels=("bugfix",), **pr_kwargs):
        if number:
            self.prs[number] = pr(*labels, **pr_kwargs)
        return self.repo.commit(f"fix: {name} (#{number})", files={f"internal/proxy/{name}.go": name}, entry=entry)

    def docs(self, name="d"):
        return self.repo.commit(f"docs: {name} (#99)", files={f"docs/{name}.md": name})

    def plan(self, tip="HEAD"):
        return release_plan.plan(self.repo.path, tip, self.prs.get)

    def release(self, tip="HEAD"):
        result = self.plan(tip)
        self.assertEqual(result["should_release"], "true", result["reason"])
        self.repo.tag(f"v{result['next_version']}", tip)
        return result


class ShippedChangesSinceTheTag(PlanCase):
    def test_several_commits_with_cancelled_runs_in_between_make_one_release(self):
        # Pushes A and B started runs that were cancelled; the run for C, a
        # docs-only push, is the only one that happens.
        self.fix(1, "a", "- A.")
        self.fix(2, "b", "- B.", labels=("feature",))
        self.docs()
        result = self.plan()
        self.assertEqual((result["should_release"], result["next_version"], result["bump"]), ("true", "2.63.0", "minor"))

    def test_a_docs_only_trigger_still_releases_earlier_untagged_shipped_changes(self):
        self.fix(1, "a", "- A.")
        self.docs()
        self.assertEqual(self.plan()["next_version"], "2.62.1")

    def test_a_metadata_merge_trigger_still_releases_earlier_untagged_shipped_changes(self):
        r = self.repo
        self.fix(1, "a", "- A.")
        r.tag("v2.62.1")
        self.fix(2, "b", "- B.")
        r.merge_metadata("2.62.1")
        result = self.plan()
        self.assertEqual((result["should_release"], result["next_version"]), ("true", "2.62.2"))

    def test_nothing_shipped_since_the_tag_is_no_release(self):
        self.docs()
        self.repo.commit("ci: x (#5)", files={".github/workflows/ci.yaml": "x"})
        result = self.plan()
        self.assertEqual(result["should_release"], "false")
        self.assertIn("nothing that ships", result["reason"])

    def test_shipped_files_without_a_new_changelog_entry_are_no_release(self):
        self.repo.commit("build(deps): bump (#7)", files={"go.mod": "module x\n"})
        result = self.plan()
        self.assertEqual(result["should_release"], "false")
        self.assertIn("holds no entry", result["reason"])

    def test_the_chart_version_the_sync_rewrites_is_not_a_change(self):
        r = self.repo
        self.fix(1, "a", "- A.")
        r.tag("v2.62.1")
        r.merge_metadata("2.62.1")
        self.assertEqual(self.plan()["should_release"], "false")

    def test_a_shipped_pull_request_after_an_open_metadata_pull_request_releases_only_itself(self):
        # v2.62.1 is tagged and its metadata pull request is still open: its entry
        # sits in [Unreleased]. The next release must not repeat it.
        r = self.repo
        self.fix(1, "a", "- A.")
        r.tag("v2.62.1")
        self.fix(2, "b", "- B.")
        notes = release_plan.changelog_release.release_entries(r.path, "HEAD", "v2.62.1")
        self.assertEqual([e.key[0] for e in notes], ["- B."])
        self.assertEqual(self.plan()["next_version"], "2.62.2")

    def test_a_second_run_over_the_same_commit_finds_the_version_taken(self):
        self.fix(1, "a", "- A.")
        self.release()
        self.assertEqual(self.plan()["should_release"], "false")


class BumpFromEveryShippingPullRequest(PlanCase):
    def test_the_largest_bump_wins_whichever_pull_request_started_the_run(self):
        self.fix(1, "a", "- A.", labels=("bugfix",))
        self.fix(2, "b", "- B.", labels=("feature",))
        self.fix(3, "c", "- C.", labels=("bugfix",))
        self.assertEqual(self.plan()["bump"], "minor")

    def test_a_large_ci_only_pull_request_does_not_promote_a_fix_release(self):
        self.fix(1, "a", "- A.", labels=("bugfix", "size/S"))
        self.prs[2] = pr("maintenance", "size/XL", "scope/ci", files=30, churn=2000)
        self.repo.commit("ci: shard (#2)", files={".github/workflows/ci.yaml": "x", "scripts/ci/x.py": "x"})
        result = self.plan()
        self.assertEqual((result["bump"], result["next_version"]), ("patch", "2.62.1"))

    def test_a_large_runtime_pull_request_is_promoted_to_minor(self):
        self.fix(1, "a", "- A.", labels=("performance", "size/XL", "scope/proxy"), files=18, churn=1477)
        self.assertEqual(self.plan()["bump"], "minor")

    def test_a_no_release_pull_request_does_not_stop_the_others(self):
        self.fix(1, "a", "- A.", labels=("no-release",))
        self.fix(2, "b", "- B.")
        self.assertEqual(self.plan()["should_release"], "true")

    def test_only_no_release_pull_requests_is_no_release(self):
        self.fix(1, "a", "- A.", labels=("no-release",))
        self.assertEqual(self.plan()["should_release"], "false")

    def test_without_labels_the_commit_subjects_decide(self):
        r = self.repo
        r.commit("feat: new thing", files={"internal/proxy/n.go": "n"}, entry="- New.")
        self.assertEqual(self.plan()["next_version"], "2.63.0")

    def test_an_unreadable_pull_request_falls_back_to_the_subject(self):
        r = self.repo
        r.commit("fix: x (#404)", files={"internal/proxy/n.go": "n"}, entry="- X.")
        self.assertEqual(self.plan()["bump"], "patch")


class ChartAhead(PlanCase):
    def test_a_chart_ahead_of_the_tag_is_a_release_request(self):
        r = self.repo
        self.fix(1, "a", "- A.")
        r.commit("chart: bump (#3)", files={"charts/loki-vl-proxy/Chart.yaml": 'apiVersion: v2\nversion: 3.0.0\nappVersion: "3.0.0"\n'})
        result = self.plan()
        self.assertEqual((result["next_version"], result["bump"]), ("3.0.0", "chart"))


if __name__ == "__main__":
    unittest.main()
