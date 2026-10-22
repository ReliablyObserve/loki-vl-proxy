import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.ci import change_scope
from scripts.ci.change_scope import (
    NON_PRODUCT,
    NOT_SHIPPED_GO,
    PRODUCT_OVERRIDES,
    SHIPPED,
    any_ships,
    classify,
    glob_match,
    scope,
    ships,
)

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "ci" / "change_scope.py"


def tracked_files():
    try:
        out = subprocess.run(
            ["git", "ls-files"], cwd=REPO, check=True, capture_output=True, text=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return []
    return [line for line in out.splitlines() if line]


TRACKED = tracked_files()

# File lists of real pull requests, as the GitHub API reports them.

# PR #807: workflows, scripts/ci and CHANGELOG
PR_807 = [
    ".github/workflows/auto-release.yaml",
    ".github/workflows/pr-labeler.yaml",
    "CHANGELOG.md",
    "scripts/ci/check_changelog_pr.py",
    "scripts/ci/tests/test_check_changelog_pr.py",
]

# PR #810: test compose file
PR_810 = [
    "test/e2e-compat/docker-compose.yml",
]

# PR #811: Playwright manifests under test/e2e-ui
PR_811 = [
    "test/e2e-ui/package-lock.json",
    "test/e2e-ui/package.json",
]

# PR #814: semgrep requirements
PR_814 = [
    "security/requirements-semgrep.in",
    "security/requirements-semgrep.txt",
]

# PR #815: website npm manifests
PR_815 = [
    "website/package-lock.json",
    "website/package.json",
]

# PR #816: certifi in the semgrep requirements
PR_816 = [
    "security/requirements-semgrep.txt",
]

# PR #817: three security workflows
PR_817 = [
    ".github/workflows/codeql.yaml",
    ".github/workflows/security-heavy.yaml",
    ".github/workflows/security-pr.yaml",
]

# PR #805: daily performance report data
PR_805 = [
    "bench/ab/history/control.jsonl",
    "bench/ab/history/drilldown-breakdown-exact.jsonl",
    "bench/ab/history/extracted-pushdown.jsonl",
    "bench/ab/history/extracted-suffix.jsonl",
    "bench/ab/history/json-detected-level.jsonl",
    "bench/ab/history/json-filter-pushdown.jsonl",
    "bench/ab/history/loki-profile-otel-metadata.jsonl",
    "bench/ab/history/metadata-freshness.jsonl",
    "bench/ab/history/metadata-inventory.jsonl",
    "bench/ab/history/metric-latency-gap.jsonl",
    "bench/ab/history/parenthesized-range-or-filters.jsonl",
    "bench/ab/history/pattern-line-filter-anchoring.jsonl",
    "bench/ab/history/pattern-line-filter.jsonl",
    "bench/ab/history/plain-name-after-parser.jsonl",
    "bench/ab/history/ratchet.json",
    "bench/ab/history/stage-field-exposure.jsonl",
    "bench/ab/history/trend.md",
    "bench/ab/history/unwrap-conversion-error.jsonl",
    "bench/ab/history/unwrap-duration-seconds.jsonl",
    "bench/ab/history/unwrap-sum-without.jsonl",
    "bench/ab/history/unwrap-ungrouped-sum.jsonl",
    "bench/ab/history/unwrap-validity.jsonl",
    "bench/ab/results/daily-control.json",
    "bench/ab/results/daily-drilldown-breakdown-exact.json",
    "bench/ab/results/daily-extracted-pushdown.json",
    "bench/ab/results/daily-extracted-suffix.json",
    "bench/ab/results/daily-json-detected-level.json",
    "bench/ab/results/daily-json-filter-pushdown.json",
    "bench/ab/results/daily-loki-profile-otel-metadata.json",
    "bench/ab/results/daily-metadata-freshness.json",
    "bench/ab/results/daily-metadata-inventory.json",
    "bench/ab/results/daily-metric-latency-gap.json",
    "bench/ab/results/daily-parenthesized-range-or-filters.json",
    "bench/ab/results/daily-pattern-line-filter-anchoring.json",
    "bench/ab/results/daily-pattern-line-filter.json",
    "bench/ab/results/daily-plain-name-after-parser.json",
    "bench/ab/results/daily-stage-field-exposure.json",
    "bench/ab/results/daily-unwrap-conversion-error.json",
    "bench/ab/results/daily-unwrap-duration-seconds.json",
    "bench/ab/results/daily-unwrap-sum-without.json",
    "bench/ab/results/daily-unwrap-ungrouped-sum.json",
    "bench/ab/results/daily-unwrap-validity.json",
    "conformance/registry/generated/perf-evidence.json",
    "conformance/reports/gaps.md",
    "conformance/reports/performance.md",
]

# PR #808: tool pin bumps that also touch bench/visual and the Helm chart
PR_808 = [
    ".github/dependabot.yml",
    ".github/workflows/ci.yaml",
    ".github/workflows/security-heavy.yaml",
    ".github/workflows/security-pr.yaml",
    ".github/workflows/visual-smoke.yaml",
    "CHANGELOG.md",
    "bench/visual/requirements.txt",
    "charts/loki-vl-proxy/templates/tests/test-connection.yaml",
    "scripts/ci/run_nuclei_scan.sh",
    "scripts/ci/run_zap_scan.sh",
    "scripts/ci/tests/test_tool_pins.py",
    "security/requirements-semgrep.in",
    "security/requirements-semgrep.txt",
    "test/e2e-compat/docker-compose.yml",
    "test/e2e-fleet/docker-compose.http-discovery.yml",
]

# PR #812: Go module files
PR_812 = [
    "go.mod",
    "go.sum",
]

# PR #813: bench/visual manifests
PR_813 = [
    "bench/visual/package-lock.json",
    "bench/visual/package.json",
]

# PR #804: workflows plus Go source
PR_804 = [
    ".github/workflows/auto-release.yaml",
    ".github/workflows/perf-daily.yaml",
    ".github/workflows/release.yaml",
    "CHANGELOG.md",
    "internal/proxy/unwrap_conversion_error.go",
    "internal/proxy/unwrap_conversion_error_test.go",
]

# PR #582: Go source, registry and e2e tests
PR_582 = [
    "CHANGELOG.md",
    "conformance/registry/behaviours/metric-series-identity.yaml",
    "conformance/registry/generated/wiring.json",
    "conformance/registry/state/behaviours/metric-series-identity.yaml",
    "conformance/reports/gaps.md",
    "conformance/reports/roadmap.md",
    "internal/proxy/metric_agg.go",
    "internal/proxy/proxy.go",
    "internal/proxy/utility_coverage_test.go",
    "internal/proxy/vector_matching.go",
    "internal/proxy/without_test.go",
    "internal/translator/series_aggregation_test.go",
    "internal/translator/translator.go",
    "test/e2e-compat/series_count_aggregation_test.go",
]

# PR #819: Dockerfile with workflows, examples, docs and the website
PR_819 = [
    ".github/workflows/auto-release.yaml",
    ".github/workflows/release-pr-validation.yaml",
    ".github/workflows/release.yaml",
    "CHANGELOG.md",
    "Dockerfile",
    "README.md",
    "docs/getting-started.md",
    "examples/docker-compose.yml",
    "examples/peers/consul-catalog/README.md",
    "examples/peers/consul-catalog/docker-compose.yml",
    "examples/peers/haproxy-dns/README.md",
    "examples/peers/haproxy-dns/docker-compose.yml",
    "examples/peers/prometheus-sd/README.md",
    "examples/peers/prometheus-sd/docker-compose.yml",
    "examples/peers/static-list/README.md",
    "examples/peers/static-list/docker-compose.yml",
    "scripts/ci/sync_release_metadata.sh",
    "website/src/pages/install-loki-vl-proxy-with-helm.tsx",
]

# PR #820: classifier and its workflows (rule P4)
PR_820 = [
    ".github/workflows/change-scope.yaml",
    ".github/workflows/changelog-pr.yaml",
    "scripts/ci/change_scope.py",
    "scripts/ci/tests/test_change_scope.py",
]

# PR #821: release workflow and changelog gate
PR_821 = [
    ".github/workflows/auto-release.yaml",
    "scripts/ci/check_changelog_pr.py",
    "scripts/ci/tests/test_check_changelog_pr.py",
]

# PR #822: CI workflow and compose validation
PR_822 = [
    ".github/workflows/ci.yaml",
    "scripts/ci/tests/test_scope_wiring.py",
    "scripts/ci/validate_compose.sh",
]

# PR #823: Go version in seven workflows
PR_823 = [
    ".github/workflows/compat-drilldown.yaml",
    ".github/workflows/compat-loki.yaml",
    ".github/workflows/compat-vl.yaml",
    ".github/workflows/e2e-pipeline.yml",
    ".github/workflows/perf-ab.yaml",
    ".github/workflows/pr-quality-report.yaml",
    ".github/workflows/security-pr.yaml",
    ".github/workflows/visual-smoke.yaml",
]

# PR #826: compose validation script
PR_826 = [
    "scripts/ci/validate_compose.sh",
]

INFRA_PRS = {
    807: PR_807,
    810: PR_810,
    811: PR_811,
    814: PR_814,
    815: PR_815,
    816: PR_816,
    817: PR_817,
    805: PR_805,
}
PRODUCT_PRS = {
    808: PR_808,
    812: PR_812,
    813: PR_813,
    804: PR_804,
    582: PR_582,
}

# Pull requests that change nothing users receive, and ones that do.
NO_SHIP_PRS = {
    807: PR_807,
    810: PR_810,
    811: PR_811,
    813: PR_813,
    814: PR_814,
    815: PR_815,
    816: PR_816,
    817: PR_817,
    820: PR_820,
    821: PR_821,
    822: PR_822,
    823: PR_823,
    826: PR_826,
    805: PR_805,
}
SHIP_PRS = {
    808: PR_808,  # chart test template
    812: PR_812,  # go.mod
    819: PR_819,  # Dockerfile
    804: PR_804,  # internal/
    582: PR_582,  # internal/ and cmd/
}

# (path, expected is_product, expected rule id). At least one case per rule.
RULE_CASES = [
    # Product overrides beat the broad infra globs.
    ("scripts/ci/run_security_regressions.sh", True, "P1"),
    ("scripts/ci/run_zap_scan.sh", True, "P1"),
    ("security/zap/zap-baseline.conf", True, "P1"),
    ("scripts/ci/wait_e2e_stack.sh", True, "P2"),
    ("scripts/ci/wait_loki_track_stack.sh", True, "P2"),
    ("scripts/ci/wait_pipeline_stack.sh", True, "P2"),
    ("scripts/smoke-test.sh", True, "P2"),
    ("scripts/compat/anything.sh", True, "P2"),
    ("scripts/ci/collect_quality_metrics.sh", True, "P3"),
    ("scripts/ci/check_quality_gate.py", True, "P3"),
    ("scripts/ci/render_pr_report.sh", True, "P3"),
    ("scripts/ci/check_conformance.py", True, "P3"),
    ("scripts/ci/change_scope.py", True, "P4"),
    (".github/workflows/change-scope.yaml", True, "P4"),
    # Infra rules.
    (".github/workflows/ci.yaml", False, "I1"),
    (".github/dependabot.yml", False, "I1"),
    (".github/ISSUE_TEMPLATE/bug.md", False, "I1"),
    ("scripts/ci/check_changelog_pr.py", False, "I2"),
    ("scripts/ci/tests/test_change_scope.py", False, "I2"),
    ("scripts/ci/helm-env-alias-values.yaml", False, "I2"),
    ("scripts/sync/deep/nested/file.sh", False, "I2"),
    ("security/requirements-semgrep.txt", False, "I3"),
    ("security/nuclei/templates/a.yaml", False, "I3"),
    ("bench/ab/history/2026-10-20.json", False, "I4"),
    ("bench/ab/results/run-1/summary.json", False, "I4"),
    ("test/e2e-compat/docker-compose.yml", False, "I5"),
    ("test/e2e-compat/docker-compose.bench.yml", False, "I5"),
    ("test/e2e-fleet/docker-compose.http-discovery.yml", False, "I5"),
    ("test/e2e-ui/package.json", False, "I6"),
    ("test/e2e-ui/package-lock.json", False, "I6"),
    ("examples/docker-compose.yml", False, "I7"),
    ("examples/peers/static-list/docker-compose.yml", False, "I7"),
    ("docker-compose.yml", False, "I7"),
    ("website/package.json", False, "I8"),
    ("website/docs/intro.md", False, "I8"),
    ("docs/reference/config.md", False, "I9"),
    ("results/bench-2026-04-29T14-45-17.json", False, "I9"),
    ("README.md", False, "I10"),
    ("CHANGELOG.md", False, "I10"),
    ("LICENSE", False, "I10"),
    ("SECURITY.md", False, "I10"),
    ("CONTRIBUTING.md", False, "I10"),
    ("conformance/registry/generated/perf-evidence.json", False, "I11"),
    ("conformance/reports/performance.md", False, "I11"),
    ("conformance/reports/gaps.md", False, "I11"),
    (".gitignore", False, "I12"),
    (".gitleaks.toml", False, "I12"),
    (".trivyignore.yaml", False, "I12"),
    (".hadolint.yaml", False, "I12"),
    (".golangci.yml", False, "I12"),
    # Product by default.
    ("cmd/loki-vl-proxy/main.go", True, "P-default"),
    ("internal/proxy/proxy.go", True, "P-default"),
    ("go.mod", True, "P-default"),
    ("go.sum", True, "P-default"),
    ("Dockerfile", True, "P-default"),
    ("charts/loki-vl-proxy/values.yaml", True, "P-default"),
    ("conformance/registry/behaviours/x.yaml", True, "P-default"),
    ("conformance/reports/roadmap.md", True, "P-default"),
    ("test/e2e-compat/compatibility-matrix.json", True, "P-default"),
    ("test/e2e-compat/series_count_aggregation_test.go", True, "P-default"),
    ("test/e2e-ui/playwright.config.ts", True, "P-default"),
    ("test/e2e-ui/tests/drilldown.spec.ts", True, "P-default"),
    ("bench/visual/package.json", True, "P-default"),
    ("bench/visual/requirements.txt", True, "P-default"),
    ("bench/ab/compose.yml", True, "P-default"),
    ("bench/cmd/main.go", True, "P-default"),
]


class ClassifyTests(unittest.TestCase):
    def test_rule_table(self):
        for path, want_product, want_rule in RULE_CASES:
            with self.subTest(path=path):
                self.assertEqual(classify(path), (want_product, want_rule))

    def test_every_rule_has_a_case(self):
        covered = {rule for _, _, rule in RULE_CASES}
        declared = {rule for rule, _ in PRODUCT_OVERRIDES + NON_PRODUCT}
        self.assertEqual(declared - covered, set())

    def test_rule_ids_are_unique(self):
        ids = [rule for rule, _ in PRODUCT_OVERRIDES + NON_PRODUCT]
        self.assertEqual(len(ids), len(set(ids)))

    def test_overrides_beat_broad_infra_globs(self):
        self.assertTrue(glob_match("scripts/**", "scripts/ci/run_zap_scan.sh"))
        self.assertEqual(classify("scripts/ci/run_zap_scan.sh"), (True, "P1"))
        self.assertTrue(glob_match("security/**", "security/zap/rules.tsv"))
        self.assertEqual(classify("security/zap/rules.tsv"), (True, "P1"))
        self.assertTrue(glob_match(".github/**", ".github/workflows/change-scope.yaml"))
        self.assertEqual(classify(".github/workflows/change-scope.yaml"), (True, "P4"))

    def test_double_star_matches_nested_paths(self):
        self.assertTrue(glob_match(".github/**", ".github/workflows/ci.yaml"))
        self.assertTrue(glob_match(".github/**", ".github/a/b/c/d.yaml"))
        self.assertFalse(glob_match(".github/**", ".githubx/ci.yaml"))
        self.assertFalse(glob_match("docs/**", "internal/docs/x.go"))

    def test_single_star_stays_within_one_directory(self):
        self.assertTrue(glob_match("test/*/docker-compose*.yml", "test/e2e-compat/docker-compose.yml"))
        self.assertTrue(glob_match("test/*/docker-compose*.yml", "test/e2e-fleet/docker-compose.http-discovery.yml"))
        self.assertFalse(glob_match("test/*/docker-compose*.yml", "test/a/b/docker-compose.yml"))
        self.assertFalse(glob_match("test/*/docker-compose*.yml", "test/e2e-compat/sub/docker-compose.yml"))
        self.assertEqual(classify("test/a/b/docker-compose.yml"), (True, "P-default"))
        self.assertFalse(glob_match("README.md", "READMEXmd"))

    def test_root_only_files_do_not_match_nested_copies(self):
        self.assertEqual(classify("internal/foo/README.md"), (True, "P-default"))
        self.assertEqual(classify("internal/foo/.gitignore"), (True, "P-default"))
        self.assertEqual(classify("charts/loki-vl-proxy/docker-compose.yml"), (True, "P-default"))

    def test_matching_is_case_sensitive(self):
        self.assertEqual(classify("Docs/reference/config.md"), (True, "P-default"))
        self.assertEqual(classify("readme.md"), (True, "P-default"))

    def test_unknown_path_is_product(self):
        self.assertEqual(classify("something/new/file.txt"), (True, "P-default"))

    def test_untrusted_paths_are_product(self):
        for path in ["", "   ", "/etc/passwd", "../outside.md", "docs/../internal/x.go", "."]:
            with self.subTest(path=path):
                self.assertEqual(classify(path)[0], True)

    def test_leading_dot_slash_and_whitespace_are_ignored(self):
        self.assertEqual(classify("./docs/a.md"), (False, "I9"))
        self.assertEqual(classify("  docs/a.md\n"), (False, "I9"))

    def test_quoted_git_paths_are_product(self):
        # git quotes unusual names unless core.quotepath=false; never infra.
        self.assertEqual(classify('"docs/caf\\303\\251.md"')[0], True)


class ScopeTests(unittest.TestCase):
    def test_empty_list_is_product(self):
        self.assertTrue(scope([]))
        self.assertTrue(scope(["", "  "]))

    def test_all_infra_is_not_product(self):
        self.assertFalse(scope(["docs/a.md", ".github/workflows/ci.yaml", "scripts/x.sh"]))

    def test_one_product_path_makes_the_change_product(self):
        self.assertTrue(scope(["docs/a.md", "internal/proxy/proxy.go"]))
        self.assertTrue(scope(["docs/a.md", "scripts/ci/run_zap_scan.sh"]))
        self.assertTrue(scope(["docs/a.md", "something/unknown.txt"]))

    def test_a_deleted_product_path_still_counts(self):
        # A rename out of a product directory is reported with both paths.
        self.assertTrue(scope(["internal/proxy/old.go", "docs/old.go"]))

    def test_generators_are_accepted(self):
        self.assertFalse(scope(p for p in ["docs/a.md"]))


class RealPullRequestTests(unittest.TestCase):
    def test_infra_pull_requests(self):
        for number, files in INFRA_PRS.items():
            with self.subTest(pr=number):
                self.assertFalse(scope(files), [f for f in files if classify(f)[0]])

    def test_product_pull_requests(self):
        for number, files in PRODUCT_PRS.items():
            with self.subTest(pr=number):
                self.assertTrue(scope(files))

    def test_fixtures_are_not_empty(self):
        for files in list(INFRA_PRS.values()) + list(PRODUCT_PRS.values()):
            self.assertGreater(len(files), 0)


class ShipsTests(unittest.TestCase):
    def test_shipped_paths(self):
        for path in (
            "cmd/proxy/main.go",
            "cmd/healthcheck/main.go",
            "internal/proxy/proxy.go",
            "internal/translator/testutil.go",
            "internal/proxy/testdata_helpers.go",
            "pkg/cache/cache.go",
            "go.mod",
            "go.sum",
            "Dockerfile",
            ".dockerignore",
            "charts/loki-vl-proxy/Chart.yaml",
            "charts/loki-vl-proxy/values.yaml",
            "charts/loki-vl-proxy/README.md",
            "charts/loki-vl-proxy/templates/deployment.yaml",
            "charts/loki-vl-proxy/templates/tests/test-connection.yaml",
            "charts/loki-vl-proxy/dashboards/loki-vl-proxy.json",
            "charts/loki-vl-proxy/alerting/loki-vl-proxy-prometheusrule.yaml",
            "dashboard/loki-vl-proxy.json",
            "alerting/loki-vl-proxy-alerting-rules.yaml",
        ):
            with self.subTest(path):
                self.assertTrue(ships(path))

    def test_go_tests_and_testdata_do_not_ship(self):
        for path in (
            "internal/proxy/proxy_test.go",
            "internal/x/y_test.go",
            "cmd/proxy/main_test.go",
            "pkg/cache/cache_test.go",
            "internal/proxy/testdata/detected_level_golden.json",
            "internal/translator/testdata/fuzz/FuzzParseDropConditions/ba3bf070c9affc13",
            "internal/testdata/a.json",
            "pkg/a/b/testdata/c/d.txt",
        ):
            with self.subTest(path):
                self.assertFalse(ships(path))

    def test_lookalikes_still_ship(self):
        # Only a file name ending in _test.go and a testdata directory are
        # excluded; neighbours with similar names are compiled into the binary.
        for path in (
            "internal/proxy/test_helpers.go",
            "internal/proxy/testdata.go",
            "internal/proxy/mytestdata/a.go",
            "internal/proxy/proxy_test.go.bak",
        ):
            with self.subTest(path):
                self.assertTrue(ships(path))

    def test_nothing_else_ships(self):
        for path in (
            "bench/ab/daily.py",
            "bench/cmd/main.go",
            "bench/go.mod",
            "bench/visual/package.json",
            "test/e2e-compat/series_count_aggregation_test.go",
            "test/e2e-compat/compatibility-matrix.json",
            "test/e2e-compat/docker-compose.yml",
            "test/e2e-ui/tests/drilldown.spec.ts",
            "conformance/registry/behaviours/x.yaml",
            "conformance/registry/generated/wiring.json",
            "conformance/reports/roadmap.md",
            "scripts/ci/check_changelog_pr.py",
            "scripts/ci/change_scope.py",
            "scripts/ci/tests/test_change_scope.py",
            "scripts/smoke-test.sh",
            ".github/workflows/auto-release.yaml",
            ".github/dependabot.yml",
            "docs/getting-started.md",
            "website/src/pages/index.tsx",
            "examples/docker-compose.yml",
            "docker-compose.yml",
            "security/zap/zap-baseline.conf",
            "results/bench.json",
            "README.md",
            "CHANGELOG.md",
            "LICENSE",
            ".gitignore",
            ".golangci.yml",
            "Dockerfile.test",
        ):
            with self.subTest(path):
                self.assertFalse(ships(path))

    def test_root_only_files_do_not_match_nested_copies(self):
        for path in ("test/e2e-compat/Dockerfile", "bench/go.mod", "bench/go.sum", "test/x/go.mod"):
            with self.subTest(path):
                self.assertFalse(ships(path))

    def test_untrusted_paths_do_not_ship(self):
        # The opposite of classify(), which treats them as product.
        for path in ("", "  ", "/internal/proxy/proxy.go", "../internal/proxy/proxy.go",
                     "..", ".", '"internal/caf\\303\\251.go"'):
            with self.subTest(path):
                self.assertFalse(ships(path))

    def test_leading_dot_slash_and_whitespace_are_ignored(self):
        self.assertTrue(ships("./internal/proxy/proxy.go"))
        self.assertTrue(ships("  go.mod \n"))
        self.assertFalse(ships("./docs/a.md"))

    def test_matching_is_case_sensitive(self):
        self.assertFalse(ships("Internal/proxy/proxy.go"))
        self.assertFalse(ships("dockerfile"))

    def test_any_ships(self):
        self.assertTrue(any_ships(["docs/a.md", "internal/proxy/proxy.go"]))
        self.assertFalse(any_ships(["docs/a.md", "internal/proxy/proxy_test.go"]))
        self.assertFalse(any_ships(p for p in ["docs/a.md"]))

    def test_any_ships_empty_list_keeps_the_release_bot_meaning(self):
        self.assertTrue(any_ships([]))
        self.assertTrue(any_ships(["", "  "]))

    def test_a_classifier_change_does_not_ship(self):
        # P4 runs every check but is no release.
        self.assertTrue(classify("scripts/ci/change_scope.py")[0])
        self.assertFalse(ships("scripts/ci/change_scope.py"))
        self.assertFalse(ships(".github/workflows/change-scope.yaml"))

    def test_shipped_is_a_subset_of_product(self):
        for path in (
            "cmd/proxy/main.go", "internal/proxy/proxy.go", "pkg/a/b.go", "go.mod", "go.sum",
            "Dockerfile", ".dockerignore", "charts/loki-vl-proxy/values.yaml",
            "dashboard/loki-vl-proxy.json", "alerting/loki-vl-proxy-prometheusrule.yaml",
        ):
            with self.subTest(path):
                self.assertTrue(classify(path)[0])

    def test_pull_requests_that_ship_nothing(self):
        for number, files in NO_SHIP_PRS.items():
            with self.subTest(pr=number):
                self.assertFalse(any_ships(files), [f for f in files if ships(f)])

    def test_pull_requests_that_ship(self):
        for number, files in SHIP_PRS.items():
            with self.subTest(pr=number):
                self.assertTrue(any_ships(files))

    def test_ship_fixtures_are_not_empty(self):
        for files in list(NO_SHIP_PRS.values()) + list(SHIP_PRS.values()):
            self.assertGreater(len(files), 0)

    def test_the_shipping_file_of_each_pull_request(self):
        expected = {
            808: ["charts/loki-vl-proxy/templates/tests/test-connection.yaml"],
            812: ["go.mod", "go.sum"],
            819: ["Dockerfile"],
            804: ["internal/proxy/unwrap_conversion_error.go"],
        }
        for number, paths in expected.items():
            with self.subTest(pr=number):
                self.assertEqual([f for f in SHIP_PRS[number] if ships(f)], paths)


@unittest.skipUnless(TRACKED, "git ls-files is not available")
class ShippedRepositoryTests(unittest.TestCase):
    # Globs kept for a Go root that has no files yet: a package added there
    # later must ship without anyone remembering to edit the table. Remove the
    # entry here once the directory exists.
    RESERVED = {"pkg/**"}

    def test_no_shipped_glob_is_dead(self):
        dead = [g for g in SHIPPED
                if g not in self.RESERVED and not any(glob_match(g, path) for path in TRACKED)]
        self.assertEqual(dead, [], "shipped globs that match no tracked file")

    def test_reserved_globs_are_still_unused(self):
        used = [g for g in self.RESERVED if any(glob_match(g, path) for path in TRACKED)]
        self.assertEqual(used, [], "drop these from RESERVED, they match files now")

    def test_no_exclusion_is_dead(self):
        dead = [
            g for g in NOT_SHIPPED_GO
            if not any(glob_match(g, path) and ships(path) is False
                       and any(glob_match(r, path) for r in change_scope.GO_ROOTS)
                       for path in TRACKED)
        ]
        self.assertEqual(dead, [], "exclusions that match no tracked Go-root file")

    def test_every_tracked_file_has_an_answer(self):
        for path in TRACKED:
            self.assertIsInstance(ships(path), bool, path)

    def test_nothing_is_embedded_into_the_binary(self):
        # The exclusion of testdata is only safe while no package embeds it.
        offenders = []
        for path in TRACKED:
            if (path.endswith(".go") and not path.endswith("_test.go")
                    and any(glob_match(r, path) for r in change_scope.GO_ROOTS)):
                if "//go:embed" in (REPO / path).read_text(encoding="utf-8", errors="replace"):
                    offenders.append(path)
        self.assertEqual(offenders, [], "go:embed found: review SHIPPED / NOT_SHIPPED_GO")


@unittest.skipUnless(TRACKED, "git ls-files is not available")
class RepositoryTests(unittest.TestCase):
    def test_every_tracked_file_classifies(self):
        for path in TRACKED:
            result = classify(path)
            self.assertIsInstance(result[0], bool, path)
            self.assertTrue(result[1], path)

    def test_no_rule_is_dead(self):
        dead = []
        for rule, globs in PRODUCT_OVERRIDES + NON_PRODUCT:
            for glob in globs:
                if not any(glob_match(glob, path) for path in TRACKED):
                    dead.append("%s: %s" % (rule, glob))
        self.assertEqual(dead, [], "globs that match no tracked file")


class CLITests(unittest.TestCase):
    def run_cli(self, *args, stdin=None):
        return subprocess.run(
            [sys.executable, "-I", str(SCRIPT), *args],
            input=stdin, capture_output=True, text=True, cwd=REPO,
        )

    def test_stdin_infra_writes_github_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", "-", "--github-output", str(out),
                                  stdin="docs/a.md\nREADME.md\n")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(out.read_text(), "product=false\n")
            self.assertEqual(result.stdout, "")

    def test_github_output_is_appended(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.write_text("other=1\n")
            self.run_cli("--files-from", "-", "--github-output", str(out),
                         stdin="internal/proxy/proxy.go\n")
            self.assertEqual(out.read_text(), "other=1\nproduct=true\n")

    def test_stdout_without_github_output(self):
        result = self.run_cli("--files-from", "-", stdin="docs/a.md\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "product=false\n")

    def test_files_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            listing = Path(tmp) / "files.txt"
            listing.write_text("docs/a.md\ninternal/x.go\n")
            result = self.run_cli("--files-from", str(listing))
            self.assertEqual(result.stdout, "product=true\n")

    def test_force_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            summary = Path(tmp) / "summary.md"
            result = self.run_cli("--files-from", "-", "--force-product",
                                  "--github-output", str(out), "--summary", str(summary),
                                  stdin="docs/a.md\n")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), "product=true\n")
            self.assertIn("--force-product", summary.read_text())

    def test_empty_input_is_product(self):
        for stdin in ["", "\n\n  \n"]:
            with self.subTest(stdin=stdin), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                result = self.run_cli("--files-from", "-", "--github-output", str(out), stdin=stdin)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(out.read_text(), "product=true\n")

    def test_no_input_is_product(self):
        result = self.run_cli()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "product=true\n")

    def test_missing_file_is_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", str(Path(tmp) / "absent"),
                                  "--github-output", str(out))
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), "product=true\n")

    def test_unknown_argument_is_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--bogus", "--github-output", str(out))
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), "product=true\n")

    def test_unwritable_summary_does_not_change_the_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", "-", "--github-output", str(out),
                                  "--summary", str(Path(tmp) / "no" / "such" / "dir"),
                                  stdin="docs/a.md\n")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), "product=false\n")

    def test_ships_true(self):
        result = self.run_cli("--files-from", "-", "--ships", stdin="docs/a.md\ninternal/x.go\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "ships=true\n")

    def test_ships_false(self):
        result = self.run_cli("--files-from", "-", "--ships",
                              stdin="docs/a.md\ninternal/x_test.go\nscripts/ci/change_scope.py\n")
        self.assertEqual(result.stdout, "ships=false\n")

    def test_ships_writes_github_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.write_text("other=1\n")
            self.run_cli("--files-from", "-", "--ships", "--github-output", str(out),
                         stdin="bench/ab/daily.py\n")
            self.assertEqual(out.read_text(), "other=1\nships=false\n")

    def test_ships_empty_input_keeps_the_release_bot_meaning(self):
        for stdin in ["", "\n\n  \n"]:
            with self.subTest(stdin=stdin):
                result = self.run_cli("--files-from", "-", "--ships", stdin=stdin)
                self.assertEqual(result.stdout, "ships=true\n")
        self.assertEqual(self.run_cli("--ships").stdout, "ships=true\n")

    def test_ships_missing_file_reports_shipping(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = self.run_cli("--files-from", str(Path(tmp) / "absent"), "--ships")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "ships=true\n")

    def test_ships_unknown_argument_reports_shipping(self):
        result = self.run_cli("--ships", "--bogus")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "ships=true\n")

    def test_ships_untrusted_paths_do_not_ship(self):
        result = self.run_cli("--files-from", "-", "--ships", stdin="/etc/passwd\n../x.go\n")
        self.assertEqual(result.stdout, "ships=false\n")

    def test_product_output_is_unchanged_by_the_ships_flag_existing(self):
        result = self.run_cli("--files-from", "-", stdin="internal/x_test.go\n")
        self.assertEqual(result.stdout, "product=true\n")

    def test_summary_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "summary.md"
            self.run_cli("--files-from", "-", "--summary", str(summary),
                         stdin="docs/a.md\nscripts/ci/run_zap_scan.sh\ninternal/x.go\n")
            text = summary.read_text()
            self.assertIn("| Path | Class | Rule |", text)
            self.assertIn("| `docs/a.md` | infra | I9 |", text)
            self.assertIn("| `scripts/ci/run_zap_scan.sh` | product | P1 |", text)
            self.assertIn("| `internal/x.go` | product | P-default |", text)
            self.assertIn("`product=true`", text)

    def test_summary_escapes_pipes(self):
        text = change_scope.render_summary(["docs/a|b.md"], False)
        self.assertIn("docs/a\\|b.md", text)

    def test_summary_is_capped(self):
        paths = ["docs/%d.md" % i for i in range(change_scope.MAX_SUMMARY_ROWS + 7)]
        text = change_scope.render_summary(paths, False)
        self.assertIn("7 more paths not listed.", text)


if __name__ == "__main__":
    unittest.main()
