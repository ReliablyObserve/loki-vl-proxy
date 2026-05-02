import unittest

from scripts.ci.check_changelog_pr import (
    extract_unreleased_section,
    has_genuinely_new_unreleased_entries,
    has_meaningful_changelog_content,
    is_dependency_only_pr,
    is_release_metadata_sync,
    should_require_changelog,
)


class CheckChangelogPRTests(unittest.TestCase):
    def test_extract_unreleased_section(self):
        text = """# Changelog

## [Unreleased]

### Features

- add thing

## [0.1.0] - 2026-01-01
"""
        self.assertEqual(
            extract_unreleased_section(text),
            "### Features\n\n- add thing",
        )

    def test_has_meaningful_changelog_content(self):
        self.assertFalse(has_meaningful_changelog_content(""))
        self.assertFalse(has_meaningful_changelog_content("### Features"))
        self.assertTrue(has_meaningful_changelog_content("### Features\n\n- add thing"))

    def test_should_require_changelog_for_release_commits(self):
        self.assertTrue(should_require_changelog(["feat: add tail mode"], ["docs/readme.md"]))
        self.assertTrue(should_require_changelog(["fix(proxy): handle tail fallback"], ["README.md"]))

    def test_should_require_changelog_for_impactful_paths(self):
        self.assertTrue(should_require_changelog(["test: add coverage"], ["internal/proxy/proxy.go"]))
        self.assertTrue(should_require_changelog(["docs: mention thing"], ["go.mod"]))

    def test_should_skip_for_unit_test_only_changes(self):
        self.assertFalse(
            should_require_changelog(
                ["test: add coverage"],
                ["internal/metrics/metrics_test.go", "pkg/cache/cache_test.go"],
            )
        )

    def test_should_skip_for_docs_only(self):
        self.assertFalse(should_require_changelog(["docs: update guide"], ["docs/getting-started.md"]))

    def test_should_require_for_ci_and_tests_changes(self):
        self.assertTrue(should_require_changelog(["ci: tune workflow"], [".github/workflows/ci.yaml"]))
        self.assertTrue(should_require_changelog(["test: add coverage"], ["test/e2e-compat/features_test.go"]))

    def test_should_skip_for_changelog_gate_policy_only_changes(self):
        self.assertFalse(
            should_require_changelog(
                ["test: refine changelog gate"],
                [
                    "scripts/ci/check_changelog_pr.py",
                    "scripts/ci/tests/test_check_changelog_pr.py",
                ],
            )
        )

    def test_has_genuinely_new_unreleased_entries_detects_new(self):
        base_changelog = "## [1.0.0]\n\n- fix: old bug\n"
        head_unreleased = "- fix: brand new fix\n"
        self.assertTrue(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_has_genuinely_new_unreleased_entries_rejects_stale_branch(self):
        # Simulates a feature branch that still has entries from before the last release.
        # Those same bullets now appear in [1.15.0] on main — must NOT count as new.
        base_changelog = (
            "## [Unreleased]\n\n"
            "## [1.15.0] - 2026-04-25\n\n"
            "- feat(otel): hierarchical OTel detection\n"
            "- fix(otel): service_name suppression\n"
        )
        head_unreleased = (
            "### Added\n\n"
            "- feat(otel): hierarchical OTel detection\n"
            "- fix(otel): service_name suppression\n"
        )
        self.assertFalse(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_has_genuinely_new_unreleased_entries_mixed(self):
        # One stale entry + one genuinely new one → should pass.
        base_changelog = (
            "## [1.15.0] - 2026-04-25\n\n"
            "- feat(otel): old feature\n"
        )
        head_unreleased = (
            "- feat(otel): old feature\n"
            "- fix: new fix added in this PR\n"
        )
        self.assertTrue(has_genuinely_new_unreleased_entries(head_unreleased, base_changelog))

    def test_dependency_only_pr_go_modules(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump github.com/klauspost/compress from 1.18.5 to 1.18.6 in the go-minor group"],
                ["go.mod", "go.sum"],
            )
        )

    def test_dependency_only_pr_github_actions(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump the actions-minor group with 16 updates"],
                [".github/workflows/ci.yaml", ".github/workflows/release.yaml"],
            )
        )

    def test_dependency_only_pr_rejects_mixed_commits(self):
        self.assertFalse(
            is_dependency_only_pr(
                ["build(deps): bump X", "feat: add new feature"],
                ["go.mod", "go.sum"],
            )
        )

    def test_dependency_only_pr_rejects_app_code(self):
        self.assertFalse(
            is_dependency_only_pr(
                ["build(deps): bump X"],
                ["go.mod", "go.sum", "internal/proxy/proxy.go"],
            )
        )

    def test_dependency_only_pr_rejects_empty(self):
        self.assertFalse(is_dependency_only_pr([], ["go.mod"]))
        self.assertFalse(is_dependency_only_pr(["build(deps): bump X"], []))

    def test_release_metadata_sync_detection(self):
        self.assertTrue(
            is_release_metadata_sync(
                [
                    "CHANGELOG.md",
                    "README.md",
                    "docs/observability.md",
                    "charts/loki-vl-proxy/Chart.yaml",
                ]
            )
        )
        self.assertFalse(is_release_metadata_sync(["README.md", "docs/observability.md"]))
        self.assertFalse(is_release_metadata_sync(["CHANGELOG.md", "internal/proxy/proxy.go"]))


if __name__ == "__main__":
    unittest.main()
