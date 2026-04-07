import unittest

from scripts.ci.check_changelog_pr import (
    extract_unreleased_section,
    has_meaningful_changelog_content,
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

    def test_should_skip_for_docs_only(self):
        self.assertFalse(should_require_changelog(["docs: update guide"], ["docs/getting-started.md"]))

    def test_should_require_for_ci_and_tests_changes(self):
        self.assertTrue(should_require_changelog(["ci: tune workflow"], [".github/workflows/ci.yaml"]))
        self.assertTrue(should_require_changelog(["test: add coverage"], ["test/e2e-compat/features_test.go"]))

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
