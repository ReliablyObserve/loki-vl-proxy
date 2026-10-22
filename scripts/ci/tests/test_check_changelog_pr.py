import unittest

from scripts.ci.check_changelog_pr import (
    extract_unreleased_section,
    has_genuinely_new_unreleased_entries,
    has_meaningful_changelog_content,
    has_new_version_sections,
    extract_released_sections,
    is_changelog_history_fix,
    is_dependency_only_pr,
    is_non_release_path,
    released_history_changed,
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
        self.assertTrue(should_require_changelog(["feat: add tail mode"], ["docs/readme.md", "charts/loki-vl-proxy/values.yaml"]))
        self.assertTrue(should_require_changelog(["fix(proxy): handle tail fallback"], ["README.md", "bench/ab/daily.py"]))
        # Nothing shipped changes, so the subject does not matter.
        self.assertFalse(should_require_changelog(["feat: add tail mode"], ["docs/readme.md"]))
        self.assertFalse(should_require_changelog(["fix(proxy): handle tail fallback"], ["README.md"]))

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
        # A workflow edit alone ships nothing, so it needs no entry. A change
        # that also touches product code still does.
        self.assertFalse(should_require_changelog(["ci: tune workflow"], [".github/workflows/ci.yaml"]))
        self.assertTrue(
            should_require_changelog(
                ["ci: tune workflow"],
                [".github/workflows/ci.yaml", "internal/proxy/proxy.go"],
            )
        )
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

    def test_extract_released_sections_starts_at_first_version(self):
        text = """# Changelog

## [Unreleased]

### Fixed

- something new

## [1.2.0] - 2026-01-01

- released entry
"""
        released = extract_released_sections(text)
        self.assertTrue(released.startswith("## [1.2.0]"))
        self.assertNotIn("something new", released)

    def test_extract_released_sections_empty_when_nothing_released(self):
        self.assertEqual(extract_released_sections("# Changelog\n\n## [Unreleased]\n"), "")

    def test_changelog_history_fix_detects_docs_changelog_commits(self):
        self.assertTrue(is_changelog_history_fix(["docs(changelog): move entry back to Unreleased"]))
        self.assertFalse(is_changelog_history_fix(["fix: something"]))
        self.assertFalse(is_changelog_history_fix([]))

    RELEASED = """# Changelog

## [Unreleased]

### Fixed

- pending entry

## [1.2.0] - 2026-01-02

### Fixed

- second release

## [1.1.0] - 2026-01-01

- first release
"""

    def test_released_history_unchanged_by_an_unreleased_entry(self):
        head = self.RELEASED.replace("- pending entry", "- pending entry\n- another")
        self.assertFalse(released_history_changed(head, self.RELEASED))

    def test_released_history_allows_a_new_version_section(self):
        head = self.RELEASED.replace(
            "## [Unreleased]\n\n### Fixed\n\n- pending entry\n",
            "## [Unreleased]\n\n## [1.3.0] - 2026-01-03\n\n### Fixed\n\n- pending entry\n",
        )
        self.assertFalse(released_history_changed(head, self.RELEASED))

    def test_released_history_rejects_an_entry_moved_into_a_release(self):
        head = self.RELEASED.replace("- second release", "- second release\n- rebased entry")
        self.assertTrue(released_history_changed(head, self.RELEASED))

    def test_released_history_rejects_a_removed_release(self):
        head = self.RELEASED.split("## [1.1.0]")[0]
        self.assertTrue(released_history_changed(head, self.RELEASED))

    def test_dependency_only_pr_survives_a_branch_update(self):
        """Bringing a dependency branch up to date must not make it releasable."""
        self.assertTrue(
            is_dependency_only_pr(
                [
                    "build(deps): bump the actions-minor group with 5 updates",
                    "Merge branch 'main' into dependabot/github_actions/actions-minor",
                ],
                [".github/workflows/ci.yaml", ".github/workflows/release.yaml"],
            )
        )

    def test_dependency_only_pr_rejects_merge_commits_alone(self):
        self.assertFalse(
            is_dependency_only_pr(
                ["Merge branch 'main' into some-branch"],
                [".github/workflows/ci.yaml"],
            )
        )

    def test_dependency_only_pr_github_actions(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump the actions-minor group with 16 updates"],
                [".github/workflows/ci.yaml", ".github/workflows/release.yaml"],
            )
        )

    def test_dependency_only_pr_website_lockfile(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump postcss from 8.5.12 to 8.5.25 in /website"],
                ["website/package.json", "website/package-lock.json"],
            )
        )

    def test_website_only_changes_do_not_require_changelog(self):
        self.assertFalse(
            should_require_changelog(
                ["docs(website): refresh landing page copy"],
                ["website/src/pages/index.tsx", "website/package-lock.json"],
            )
        )

    def test_daily_performance_report_does_not_require_changelog(self):
        self.assertFalse(
            should_require_changelog(
                [
                    "bench: daily performance report 2026-10-22",
                    "Merge branch 'main' into bench/daily-perf",
                ],
                [
                    "bench/ab/history/control.jsonl",
                    "bench/ab/history/ratchet.json",
                    "bench/ab/history/trend.md",
                    "bench/ab/results/daily-control.json",
                    "conformance/registry/generated/perf-evidence.json",
                    "conformance/reports/gaps.md",
                    "conformance/reports/performance.md",
                ],
            )
        )

    def test_benchmark_harness_changes_still_require_changelog(self):
        self.assertTrue(
            should_require_changelog(
                ["bench: the daily run measures one more shape"],
                ["bench/ab/daily.py", "bench/ab/history/control.jsonl"],
            )
        )
        self.assertTrue(
            should_require_changelog(
                ["bench: add a registry row"],
                ["conformance/registry/baseline.json", "conformance/reports/gaps.md"],
            )
        )
        self.assertTrue(
            should_require_changelog(
                ["bench: regenerate the limits export"],
                ["conformance/registry/generated/proxy/limits.json"],
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

    def test_has_new_version_sections_detects_added_section(self):
        base = "## [Unreleased]\n\n## [1.33.0] - 2026-05-14\n\n- something\n"
        head = "## [Unreleased]\n\n## [1.33.0] - 2026-05-14\n\n## [1.32.4] - 2026-05-14\n\n- fix\n"
        self.assertTrue(has_new_version_sections(head, base))

    def test_has_new_version_sections_no_change(self):
        text = "## [Unreleased]\n\n## [1.33.0] - 2026-05-14\n\n- something\n"
        self.assertFalse(has_new_version_sections(text, text))

    def test_has_new_version_sections_ignores_unreleased(self):
        base = "## [Unreleased]\n\n- new thing\n\n## [1.33.0] - 2026-05-14\n"
        head = "## [Unreleased]\n\n- different thing\n\n## [1.33.0] - 2026-05-14\n"
        self.assertFalse(has_new_version_sections(head, base))


class InfraScopeTests(unittest.TestCase):
    # File lists are the real ones of the Dependabot pull requests named.
    DEPENDABOT = {
        "#814 semgrep": (
            "build(deps): bump semgrep from 1.161.0 to 1.181.0 in /security in the semgrep-deps group across 1 directory",
            ["security/requirements-semgrep.in", "security/requirements-semgrep.txt"],
        ),
        "#816 certifi": (
            "build(deps): bump certifi from 2026.7.22 to 2026.10.19 in /security",
            ["security/requirements-semgrep.txt"],
        ),
        "#813 bench/visual": (
            "build(deps-dev): bump @playwright/test from 1.59.1 to 1.64.1 in /bench/visual in the visual-deps group",
            ["bench/visual/package-lock.json", "bench/visual/package.json"],
        ),
        "#811 test/e2e-ui": (
            "build(deps-dev): bump @playwright/test from 1.59.1 to 1.64.1 in /test/e2e-ui in the e2e-ui-deps group",
            ["test/e2e-ui/package-lock.json", "test/e2e-ui/package.json"],
        ),
        "#810 test compose": (
            "build(deps): bump nginx from 1.27-alpine to 1.29-alpine in /test/e2e-compat in the test-stack-images group across 1 directory",
            ["test/e2e-compat/docker-compose.yml"],
        ),
        "#815 website": (
            "build(deps): bump the website-deps group in /website with 2 updates",
            ["website/package-lock.json", "website/package.json"],
        ),
    }

    def test_dependabot_pull_requests_pass_without_an_entry(self):
        for name, (subject, files) in self.DEPENDABOT.items():
            with self.subTest(name):
                self.assertTrue(is_dependency_only_pr([subject], files))

    def test_bench_manifests_are_dependency_only(self):
        # Product for CI, but a bump of them is not a release.
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump playwright from 1.59.1 to 1.64.1 in /bench/visual"],
                ["bench/visual/requirements.txt", "bench/visual/requirements.in"],
            )
        )

    def test_dependency_manifests_match_by_name(self):
        self.assertTrue(
            is_dependency_only_pr(
                ["build(deps): bump the image"],
                ["Dockerfile", "test/e2e-compat/Dockerfile", "docker-compose.yml", "test/x/docker-compose.override.yml"],
            )
        )
        self.assertFalse(
            is_dependency_only_pr(
                ["build(deps): bump X"],
                ["bench/visual/package.json", "bench/visual/run.py"],
            )
        )

    def test_dependency_only_pr_still_rejects_a_release_commit(self):
        self.assertFalse(
            is_dependency_only_pr(
                ["build(deps): bump X", "fix: handle the new API"],
                ["bench/visual/package.json"],
            )
        )

    def test_infra_only_fix_needs_no_entry(self):
        self.assertFalse(
            should_require_changelog(
                ["fix(ci): pin the runner image", "feat(ci): add a lint step"],
                [".github/workflows/ci.yaml", "scripts/ci/run_lint.sh", "security/semgrep.yml"],
            )
        )

    def test_product_fix_still_needs_an_entry(self):
        self.assertTrue(should_require_changelog(["fix: handle empty range"], ["internal/proxy/range.go"]))
        self.assertTrue(
            should_require_changelog(
                ["fix(ci): pin the runner image"],
                [".github/workflows/ci.yaml", "charts/loki-vl-proxy/values.yaml"],
            )
        )

    def test_unit_test_only_fix_still_needs_an_entry(self):
        self.assertTrue(should_require_changelog(["fix: cover the empty range"], ["internal/x/y_test.go"]))

    def test_classifier_only_change_needs_no_entry(self):
        for files in (
            ["scripts/ci/change_scope.py"],
            [".github/workflows/change-scope.yaml"],
            ["scripts/ci/change_scope.py", "scripts/ci/tests/test_change_scope.py", "docs/ci.md"],
        ):
            with self.subTest(files):
                self.assertFalse(should_require_changelog(["fix(ci): refine the classifier"], files))
                self.assertTrue(all(is_non_release_path(f) for f in files))

    def test_every_legacy_non_release_path_is_still_non_release(self):
        legacy = [
            "docs/getting-started.md",
            "docs/observability.md",
            "scripts/ci/tests/test_check_changelog_pr.py",
            "website/package.json",
            "website/src/pages/index.tsx",
            "bench/ab/history/control.jsonl",
            "bench/ab/results/daily-control.json",
            "README.md",
            "CHANGELOG.md",
            "LICENSE",
            "scripts/ci/check_changelog_pr.py",
            "conformance/registry/generated/perf-evidence.json",
            "conformance/reports/performance.md",
            "conformance/reports/gaps.md",
        ]
        for path in legacy:
            with self.subTest(path):
                self.assertTrue(is_non_release_path(path))

    def test_product_paths_are_not_non_release(self):
        for path in (
            "internal/proxy/proxy.go",
            "cmd/proxy/main.go",
            "charts/loki-vl-proxy/values.yaml",
            "go.mod",
            "Dockerfile",
            "bench/ab/daily.py",
            "bench/visual/package.json",
            "conformance/registry/baseline.json",
        ):
            with self.subTest(path):
                self.assertFalse(is_non_release_path(path))

    def test_unit_test_files_stay_non_release_paths(self):
        self.assertTrue(is_non_release_path("internal/x/y_test.go"))
        self.assertTrue(is_non_release_path("pkg/cache/cache_test.go"))


if __name__ == "__main__":
    unittest.main()
