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

    def test_a_breaking_change_moves_the_minor_to_the_next_multiple_of_ten(self):
        self.fix(1, "a", "- A.", labels=("breaking-change",))
        self.fix(2, "b", "- B.", labels=("feature",))
        result = self.release()
        self.assertEqual((result["next_version"], result["bump"]), ("2.70.0", "breaking"))
        self.fix(3, "c", "- C.", labels=("breaking-change",))
        self.assertEqual(self.release()["next_version"], "2.80.0")
        self.fix(4, "d", "- D.", labels=("release:major",))
        self.assertEqual(self.plan()["next_version"], "3.0.0")

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


class LabelPrecedence(unittest.TestCase):
    # The order the bot has always applied; the first label present decides.
    ORDER = [("release:major", "major"), ("release:minor", "minor"), ("release:patch", "patch"),
             ("breaking-change", "breaking"), ("feature", "minor"), ("bugfix", "patch"), ("performance", "patch")]

    def test_every_label_alone(self):
        for label, bump in self.ORDER:
            self.assertEqual(release_plan.pr_bump(pr(label))[0], bump, label)

    def test_for_every_pair_the_earlier_label_wins(self):
        for i, (first, first_bump) in enumerate(self.ORDER):
            for second, _ in self.ORDER[i + 1 :]:
                self.assertEqual(release_plan.pr_bump(pr(second, first))[0], first_bump, (first, second))

    def test_an_explicit_release_label_caps_what_the_labeler_added(self):
        # The labeler adds feature / breaking-change from the title.
        self.assertEqual(release_plan.pr_bump(pr("release:patch", "feature"))[0], "patch")
        self.assertEqual(release_plan.pr_bump(pr("release:minor", "breaking-change"))[0], "minor")
        self.assertEqual(release_plan.pr_bump(pr("release:patch", "breaking-change", "feature"))[0], "patch")

    def test_a_breaking_change_is_below_release_major_and_above_minor(self):
        self.assertEqual(release_plan.pr_bump(pr("breaking-change"))[0], "breaking")
        self.assertEqual(release_plan.pr_bump(pr("breaking-change", "size/XL", "scope/proxy"))[0], "breaking")
        self.assertEqual(release_plan.pr_bump(pr("release:major", "breaking-change"))[0], "major")
        for subject in ("feat!: drop a flag", "fix(api)!: change a status", "feat: x\n\nBREAKING CHANGE: y"):
            self.assertEqual(release_plan.subject_bump(subject), "breaking", subject)

    def test_no_release_wins_over_everything(self):
        self.assertEqual(release_plan.pr_bump(pr("no-release", "release:major")), ("none", True))

    def test_the_size_rule_promotes_patch_to_minor_and_never_touches_major(self):
        self.assertEqual(release_plan.pr_bump(pr("release:patch", "size/XL", "scope/proxy"))[0], "minor")
        self.assertEqual(release_plan.pr_bump(pr("release:major", "size/XL", "scope/proxy"))[0], "major")
        self.assertEqual(release_plan.pr_bump(pr("release:minor", files=30))[0], "minor")
        self.assertEqual(release_plan.pr_bump(pr("size/XL", "scope/ci"))[0], "none")

    def test_labels_are_matched_in_lower_case(self):
        self.assertEqual(release_plan.pr_bump(pr("Release:Minor", "Bugfix"))[0], "minor")


class ChartAhead(PlanCase):
    def test_a_chart_ahead_of_the_tag_is_a_release_request(self):
        r = self.repo
        self.fix(1, "a", "- A.")
        r.commit("chart: bump (#3)", files={"charts/loki-vl-proxy/Chart.yaml": 'apiVersion: v2\nversion: 3.0.0\nappVersion: "3.0.0"\n'})
        result = self.plan()
        self.assertEqual((result["next_version"], result["bump"]), ("3.0.0", "chart"))

    def test_the_label_no_release_on_the_pull_request_that_moved_the_chart_withdraws_it(self):
        r = self.repo
        self.fix(1, "a", "- A.")
        self.prs[3] = pr("no-release")
        r.commit("chart: bump (#3)", files={"charts/loki-vl-proxy/Chart.yaml": 'apiVersion: v2\nversion: 3.0.0\nappVersion: "3.0.0"\n'})
        result = self.plan()
        self.assertEqual(result["should_release"], "false")
        self.assertIn("no-release", result["reason"])


if __name__ == "__main__":
    unittest.main()
