import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.ci import change_scope
from scripts.ci.change_scope import (
    BASELINE_BASE,
    BASELINE_ENTRY_LINES,
    BASELINE_ENTRY_POINTS,
    BASELINE_FILES,
    BASELINE_LABEL,
    BASELINE_LINES,
    LANES,
    NON_PRODUCT,
    NOT_SHIPPED_GO,
    PRODUCT_OVERRIDES,
    SHIPPED,
    any_ships,
    baseline,
    baseline_reasons,
    classify,
    glob_match,
    lane,
    lane_flags,
    parse_numstat,
    scope,
    scope_lane,
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

# The CI-only pull requests of 2026-10-28, which ran the whole product CI before
# the lanes existed (the file lists are the API's).
PR_888 = [  # gRPC limit of the CI Loki stacks
    "test/e2e-compat/loki-bench-config.yaml",
    "test/e2e-compat/loki-bench-nocache-config.yaml",
    "test/e2e-compat/loki-local-config.yaml",
]
PR_889 = [  # A/B matrix loop
    "bench/ab/README.md",
    "bench/ab/perf_matrix.py",
    "bench/ab/pr_smoke.py",
    "bench/ab/tests/fixtures/raw-unwrap-sum-without-w8-w9.json",
    "bench/ab/tests/test_matrix_loop.py",
    "bench/ab/tests/test_pr_smoke.py",
]
PR_890 = [  # conformance job reads the unit results
    ".github/workflows/ci.yaml",
    "scripts/ci/race_sensitive_tests.sh",
    "scripts/ci/tests/test_race_sensitive_tests.py",
    "scripts/ci/tests/test_scope_wiring.py",
]
PR_891 = [  # image built once, e2e-ui shards merged
    ".github/workflows/ci.yaml",
    "docs/testing-e2e-guide.md",
    "docs/testing.md",
    "test/e2e-ui/README.md",
]
PR_892 = [  # PR quality report reads the base commit back
    ".github/workflows/ci.yaml",
    ".github/workflows/pr-quality-report.yaml",
    "docs/testing.md",
    "scripts/ci/collect_quality_metrics.sh",
    "scripts/ci/tests/test_collect_quality_metrics.py",
]

INFRA_PRS = {
    807: PR_807,
    811: PR_811,
    814: PR_814,
    815: PR_815,
    816: PR_816,
    817: PR_817,
    805: PR_805,
    890: PR_890,
    891: PR_891,
}
STACK_PRS = {
    810: PR_810,  # the compose file CI starts
    888: PR_888,  # loki-local-config.yaml
}
PERF_PRS = {
    889: PR_889,
}
PRODUCT_PRS = {
    808: PR_808,
    812: PR_812,
    813: PR_813,
    804: PR_804,
    582: PR_582,
    892: PR_892,  # P3: the quality report's own scripts run the whole CI, by design
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
    ("bench/ab/selection.py", True, "P5"),
    ("bench/ab/stack.py", True, "P5"),
    ("bench/ab/shapes.json", True, "P5"),
    ("bench/ab/docker-compose.ab.yml", True, "P5"),
    ("bench/ab/docker-compose.ab-loki-uncached.yml", True, "P5"),
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
    ("test/e2e-compat/docker-compose.bench.yml", False, "I5"),
    ("test/e2e-compat/docker-compose.review.yml", False, "I5"),
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
    ("ROADMAP.md", False, "I13"),
    ("bench/ab/README.md", False, "I13"),
    ("bench/visual/README.md", False, "I13"),
    ("test/e2e-ui/README.md", False, "I13"),
    ("test/e2e-compat/results/bench-warm-v1.50.0.md", False, "I13"),
    ("test/e2e-compat/loki-bench-config.yaml", False, "I14"),
    ("test/e2e-compat/loki-bench-nocache-config.yaml", False, "I14"),
    # Lanes: not the full product, but not nothing.
    ("test/e2e-compat/docker-compose.yml", False, "S1"),
    ("test/e2e-fleet/docker-compose.yml", False, "S1"),
    ("test/e2e-pipeline/docker-compose.yml", False, "S1"),
    ("test/e2e-compat/loki-local-config.yaml", False, "S1"),
    ("test/e2e-compat/grafana-datasources.yaml", False, "S1"),
    ("test/e2e-compat/grafana-dashboards.yaml", False, "S1"),
    ("test/e2e-compat/vmauth.yml", False, "S1"),
    ("test/e2e-compat/vmauth-ring.yml", False, "S1"),
    ("test/e2e-compat/vmalert-rules.yml", False, "S1"),
    ("test/e2e-compat/vm-scrape.yml", False, "S1"),
    ("test/e2e-compat/nginx-tail.conf", False, "S1"),
    ("bench/ab/perf_matrix.py", False, "S2"),
    ("bench/ab/pr_smoke.py", False, "S2"),
    ("bench/ab/tests/test_pr_smoke.py", False, "S2"),
    ("bench/ab/tests/fixtures/raw-unwrap-sum-without-w8-w9.json", False, "S2"),
    # Product by default.
    ("cmd/loki-vl-proxy/main.go", True, "P-default"),
    ("internal/proxy/proxy.go", True, "P-default"),
    ("go.mod", True, "P-default"),
    ("go.sum", True, "P-default"),
    ("Dockerfile", True, "P-default"),
    ("charts/loki-vl-proxy/values.yaml", True, "P-default"),
    ("conformance/registry/behaviours/x.yaml", True, "P-default"),
    ("conformance/reports/roadmap.md", True, "P-default"),
    ("conformance/reports/coverage-map.md", True, "P-default"),
    ("conformance/README.md", True, "P-default"),
    ("charts/loki-vl-proxy/README.md", True, "P-default"),
    ("internal/proxy/README.md", True, "P-default"),
    ("cmd/loki-vl-proxy/README.md", True, "P-default"),
    ("test/e2e-compat/log-generator.py", True, "P-default"),
    ("test/e2e-compat/query-semantics-matrix.json", True, "P-default"),
    ("test/e2e-fleet/sd-peers.json", True, "P-default"),
    ("test/e2e-compat/dashboards/bench-resources.json", True, "P-default"),
    ("test/e2e-compat/compatibility-matrix.json", True, "P-default"),
    ("test/e2e-compat/series_count_aggregation_test.go", True, "P-default"),
    ("test/e2e-ui/playwright.config.ts", True, "P-default"),
    ("test/e2e-ui/tests/drilldown.spec.ts", True, "P-default"),
    ("bench/visual/package.json", True, "P-default"),
    ("bench/visual/requirements.txt", True, "P-default"),
    ("bench/parity/run.py", True, "P-default"),
    ("bench/visual/plan.py", True, "P-default"),
    ("bench/cmd/main.go", True, "P-default"),
]


class ClassifyTests(unittest.TestCase):
    def test_rule_table(self):
        for path, want_product, want_rule in RULE_CASES:
            with self.subTest(path=path):
                self.assertEqual(classify(path), (want_product, want_rule))

    def test_every_rule_has_a_case(self):
        covered = {rule for _, _, rule in RULE_CASES}
        declared = {rule for rule, _ in PRODUCT_OVERRIDES + NON_PRODUCT} | {rule for rule, _, _ in LANES}
        self.assertEqual(declared - covered, set())

    def test_rule_ids_are_unique(self):
        ids = [rule for rule, _ in PRODUCT_OVERRIDES + NON_PRODUCT] + [rule for rule, _, _ in LANES]
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
        self.assertEqual(classify("README.MD"), (True, "P-default"))
        self.assertEqual(classify("Test/e2e-ui/README.md"), (True, "P-default"))

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


class LaneTests(unittest.TestCase):
    def test_lane_flags_are_nested(self):
        self.assertEqual(lane_flags("infra"), {"product": False, "stack": False, "perf": False, "baseline": False})
        self.assertEqual(lane_flags("perf"), {"product": False, "stack": False, "perf": True, "baseline": False})
        self.assertEqual(lane_flags("stack"), {"product": False, "stack": True, "perf": True, "baseline": False})
        self.assertEqual(lane_flags("product"), {"product": True, "stack": True, "perf": True, "baseline": False})

    def test_the_highest_lane_wins(self):
        self.assertEqual(scope_lane(["docs/a.md"]), "infra")
        self.assertEqual(scope_lane(["docs/a.md", "bench/ab/perf_matrix.py"]), "perf")
        self.assertEqual(scope_lane(["bench/ab/perf_matrix.py", "test/e2e-compat/vmauth.yml"]), "stack")
        self.assertEqual(scope_lane(["test/e2e-compat/vmauth.yml", "internal/proxy/proxy.go"]), "product")
        self.assertEqual(scope_lane(["bench/ab/perf_matrix.py", "something/unknown.txt"]), "product")

    def test_empty_and_untrusted_lists_are_product(self):
        self.assertEqual(scope_lane([]), "product")
        self.assertEqual(scope_lane(["", "  "]), "product")
        self.assertEqual(scope_lane(["docs/a.md", "../x.md"]), "product")

    def test_a_lane_glob_never_beats_an_infra_rule_or_an_override(self):
        # bench/ab/** is the perf lane, but its history and results stay infra
        # and its shared modules stay product.
        self.assertEqual(lane("bench/ab/history/trend.md"), ("infra", "I4"))
        self.assertEqual(lane("bench/ab/results/daily-control.json"), ("infra", "I4"))
        self.assertEqual(lane("bench/ab/shapes.json"), ("product", "P5"))
        self.assertEqual(lane("bench/ab/README.md"), ("infra", "I13"))
        self.assertEqual(lane("bench/ab/daily.py"), ("perf", "S2"))

    def test_markdown_is_infra_only_where_nothing_reads_it(self):
        for path in ("ROADMAP.md", "bench/README.md", "bench/parity/README.md", "test/e2e-ui/README.md"):
            with self.subTest(path):
                self.assertEqual(lane(path)[0], "infra")
        for path in (
            "charts/loki-vl-proxy/README.md",       # shipped, and read by the flag documentation gate
            "conformance/README.md",
            "conformance/registry/README.md",
            "conformance/reports/coverage-map.md",  # generated and compared by the conformance gate
            "conformance/reports/roadmap.md",
            "cmd/loki-vl-proxy/README.md",
            "internal/proxy/README.md",
            "pkg/a/README.md",
            "dashboard/README.md",
            "alerting/README.md",
        ):
            with self.subTest(path):
                self.assertEqual(lane(path)[0], "product")

    def test_the_gate_data_stays_product(self):
        for path in (
            "conformance/registry/behaviours/x.yaml",
            "conformance/registry/generated/wiring.json",
            "bench/visual/plan.py",
            "bench/visual/spec.json",
            "bench/parity/run.py",
            "bench/parity/corpus.py",
            "bench/ab/selection.py",
            "bench/ab/stack.py",
            "bench/ab/shapes.json",
            "test/e2e-compat/log-generator.py",
            "test/e2e-compat/compatibility-matrix.json",
            "scripts/ci/check_conformance.py",
        ):
            with self.subTest(path):
                self.assertEqual(lane(path)[0], "product")

    def test_a_pure_go_change_is_the_full_product(self):
        for files in (
            ["internal/proxy/proxy.go"],
            ["internal/proxy/proxy.go", "internal/proxy/proxy_test.go", "docs/a.md"],
            ["cmd/loki-vl-proxy/main.go"],
            ["go.mod", "go.sum"],
            ["Dockerfile"],
            ["charts/loki-vl-proxy/values.yaml"],
            ["internal/translator/testdata/fuzz/FuzzX/abc"],
        ):
            with self.subTest(files=files):
                self.assertEqual(scope_lane(files), "product")
                self.assertTrue(scope(files))
                self.assertEqual(lane_flags(scope_lane(files)), {"product": True, "stack": True, "perf": True, "baseline": False})

    def test_the_ci_only_pull_requests_of_2026_10_28(self):
        # Each ran the whole product CI before the lanes. #892 still does, by design (P3).
        expected = {
            888: ("stack", {"test/e2e-compat/loki-local-config.yaml": "S1"}),
            889: ("perf", {"bench/ab/perf_matrix.py": "S2", "bench/ab/pr_smoke.py": "S2"}),
            890: ("infra", {}),
            891: ("infra", {"test/e2e-ui/README.md": "I13"}),
            892: ("product", {"scripts/ci/collect_quality_metrics.sh": "P3"}),
        }
        files = {888: PR_888, 889: PR_889, 890: PR_890, 891: PR_891, 892: PR_892}
        for number, (want, rules) in expected.items():
            with self.subTest(pr=number):
                self.assertEqual(scope_lane(files[number]), want)
                for path, rule in rules.items():
                    self.assertEqual(lane(path)[1], rule)

    def test_stack_pull_requests(self):
        for number, files in STACK_PRS.items():
            with self.subTest(pr=number):
                self.assertEqual(scope_lane(files), "stack")
                self.assertFalse(scope(files))

    def test_perf_pull_requests(self):
        for number, files in PERF_PRS.items():
            with self.subTest(pr=number):
                self.assertEqual(scope_lane(files), "perf")
                self.assertFalse(scope(files))

    def test_infra_pull_requests_are_infra_not_a_lane(self):
        for number, files in INFRA_PRS.items():
            with self.subTest(pr=number):
                self.assertEqual(scope_lane(files), "infra")


@unittest.skipUnless(TRACKED, "git ls-files is not available")
class LaneRepositoryTests(unittest.TestCase):
    # Every path a user receives, and every path the Go, chart and image jobs
    # read, must run the full product CI.
    PRODUCT_ROOTS = ("cmd/", "internal/", "pkg/", "charts/", "dashboard/", "alerting/")
    PRODUCT_FILES = ("go.mod", "go.sum", "Dockerfile", ".dockerignore")

    def test_everything_that_ships_is_the_full_product(self):
        for path in TRACKED:
            if ships(path):
                with self.subTest(path=path):
                    self.assertEqual(lane(path)[0], "product")

    def test_nothing_under_a_product_root_leaves_the_product_lane(self):
        for path in TRACKED:
            if path.startswith(self.PRODUCT_ROOTS) or path in self.PRODUCT_FILES:
                with self.subTest(path=path):
                    self.assertEqual(lane(path)[0], "product")

    def test_gate_inputs_are_product_except_the_declared_generated_reports(self):
        reports = {
            "conformance/registry/generated/perf-evidence.json",
            "conformance/reports/performance.md",
            "conformance/reports/gaps.md",
        }
        for path in TRACKED:
            if path.startswith("conformance/") and path not in reports:
                with self.subTest(path=path):
                    self.assertEqual(lane(path)[0], "product")

    def test_the_other_harnesses_and_their_shared_modules_are_product(self):
        for path in TRACKED:
            if path.startswith(("bench/visual/", "bench/parity/", "bench/cmd/", "bench/internal/")) \
                    and not path.endswith(".md"):
                with self.subTest(path=path):
                    self.assertEqual(lane(path)[0], "product")

    def test_the_perf_lane_holds_only_files_no_other_job_reads(self):
        for path in TRACKED:
            if lane(path)[0] == "perf":
                with self.subTest(path=path):
                    self.assertTrue(path.startswith("bench/ab/"), path)
                    self.assertNotIn(path, ("bench/ab/selection.py", "bench/ab/stack.py", "bench/ab/shapes.json"))

    def test_the_stack_lane_holds_only_stack_inputs(self):
        for path in TRACKED:
            if lane(path)[0] == "stack":
                with self.subTest(path=path):
                    self.assertTrue(path.startswith(("test/e2e-compat/", "test/e2e-fleet/", "test/e2e-pipeline/")), path)

    def test_the_stack_configs_are_mounted_by_a_ci_compose_file(self):
        mounted = (REPO / "test/e2e-compat/docker-compose.yml").read_text()
        for path in TRACKED:
            if lane(path) == ("stack", "S1") and path.startswith("test/e2e-compat/") and not path.endswith("docker-compose.yml"):
                with self.subTest(path=path):
                    self.assertIn(path.rsplit("/", 1)[1], mounted)

    def test_every_lane_rule_has_a_tracked_file(self):
        for rule, name, globs in LANES:
            with self.subTest(rule=rule):
                self.assertTrue(any(lane(path)[0] == name and lane(path)[1] == rule for path in TRACKED))


def numstat(*changes):
    """paths and changed-line counts as `git diff --numstat` prints them: (path, lines) with the lines split
    as added and deleted."""
    return [(path, lines) for path, lines in changes]


def many(count, lines_each=1, prefix="internal/proxy/f"):
    return numstat(*[(f"{prefix}{i}.go", lines_each) for i in range(count)])


def decide(changes, base="main", labels=()):
    paths = [path for path, _ in changes]
    return baseline(paths, changes, base, labels)[0]


# Recorded pull requests of #678 to #894 (shipped changed lines, shipped files, lines in the entry points) and
# whether the trigger selects them; the replay in the pull request description lists the 200.
RECORDED = [
    (862, 1739, 13, 222, True),   # metric query_range reuses buckets: both rules
    (846, 331, 7, 299, True),     # canonical tenants: the entry points
    (760, 1710, 8, 2, True),      # sum by (detected_level): lines
    (747, 335, 2, 333, True),     # pattern line filters: the entry points
    (739, 663, 9, 282, True),     # sum without over unwrap: the entry points
    (704, 1565, 9, 76, True),     # unwrap conversion error: lines
    (877, 510, 8, 17, False),     # near-now label lists
    (746, 1364, 10, 32, False),   # large, below the bar
    (758, 1090, 3, 0, False),
    (743, 86, 1, 86, False),      # an entry point, but a small edit
    (883, 50, 1, 0, False),       # a small Go fix
    (857, 31, 3, 16, False),
    (855, 22, 4, 15, False),
    (860, 7, 1, 0, False),
]


class BaselineTests(unittest.TestCase):
    def test_a_big_go_refactor_triggers(self):
        self.assertTrue(decide(numstat(("internal/proxy/postprocess.go", 900), ("internal/proxy/metric_agg.go", 800))))
        self.assertTrue(decide(many(BASELINE_FILES + 1)))

    def test_a_small_go_fix_does_not(self):
        self.assertFalse(decide(numstat(("internal/proxy/metric_agg.go", 30), ("internal/proxy/metric_agg_test.go", 90))))
        self.assertFalse(decide(numstat(("go.mod", 2), ("go.sum", 2))))

    def test_the_thresholds_are_strict(self):
        self.assertFalse(decide(numstat(("internal/proxy/a.go", BASELINE_LINES))))
        self.assertTrue(decide(numstat(("internal/proxy/a.go", BASELINE_LINES + 1))))
        self.assertFalse(decide(many(BASELINE_FILES)))
        self.assertTrue(decide(many(BASELINE_FILES + 1)))
        # The entry points: at least, not more than.
        self.assertFalse(decide(numstat(("internal/proxy/proxy.go", BASELINE_ENTRY_LINES - 1))))
        self.assertTrue(decide(numstat(("internal/proxy/proxy.go", BASELINE_ENTRY_LINES))))

    def test_entry_point_lines_add_up_across_files(self):
        half = BASELINE_ENTRY_LINES // 2
        self.assertTrue(decide(numstat(("internal/proxy/proxy.go", half), ("internal/translator/translator.go", half))))
        self.assertFalse(decide(numstat(("internal/proxy/proxy.go", half), ("internal/proxy/metric_agg.go", 500))))

    def test_each_entry_point_counts_and_others_do_not(self):
        for path in BASELINE_ENTRY_POINTS:
            with self.subTest(path):
                self.assertTrue(ships(path))
                self.assertTrue(decide(numstat((path, BASELINE_ENTRY_LINES))))
        for path in ("internal/proxy/postprocess.go", "internal/proxy/backend.go", "internal/cache/cache.go",
                     "internal/translator/normalize.go", "cmd/loki-vl-proxy/main.go"):
            with self.subTest(path):
                self.assertFalse(decide(numstat((path, BASELINE_ENTRY_LINES * 5))))

    def test_the_entry_points_are_real_files(self):
        for path in BASELINE_ENTRY_POINTS:
            with self.subTest(path):
                self.assertTrue((REPO / path).is_file(), path)

    def test_tests_testdata_and_non_shipped_paths_do_not_count(self):
        self.assertFalse(decide(numstat(("internal/proxy/big_test.go", 9000), ("internal/proxy/testdata/g.json", 9000),
                                        ("docs/a.md", 9000), ("bench/ab/perf_matrix.py", 9000),
                                        ("conformance/registry/behaviours/x.yaml", 9000))))
        self.assertFalse(decide(numstat(*[(f"internal/proxy/t{i}_test.go", 5) for i in range(BASELINE_FILES + 20)])))

    def test_charts_and_the_image_count_as_shipped(self):
        self.assertTrue(decide(numstat(("charts/loki-vl-proxy/templates/deployment.yaml", BASELINE_LINES + 1))))
        self.assertTrue(decide(numstat(("Dockerfile", BASELINE_LINES + 1))))

    def test_a_stacked_pull_request_never_gets_it(self):
        big = many(BASELINE_FILES + 5)
        for base in ("feature/parent", "release/1.x", "", "Main", "main2"):
            with self.subTest(base):
                self.assertFalse(decide(big, base=base))
                self.assertFalse(decide(big, base=base, labels=[BASELINE_LABEL]))
        self.assertTrue(decide(big, base=BASELINE_BASE))

    def test_the_label_forces_it(self):
        small = numstat(("internal/proxy/metric_agg.go", 3))
        self.assertTrue(decide(small, labels=[BASELINE_LABEL]))
        self.assertTrue(decide(small, labels=["bug", BASELINE_LABEL]))
        self.assertEqual(baseline(["internal/proxy/metric_agg.go"], small, "main", [BASELINE_LABEL])[1],
                         ["the `baseline` label is set"])

    def test_the_label_forces_it_on_an_infra_pull_request_too(self):
        infra = numstat(("docs/a.md", 3))
        self.assertFalse(decide(infra))
        self.assertTrue(decide(infra, labels=[BASELINE_LABEL]))

    def test_ci_full_forces_it(self):
        infra = numstat((".github/workflows/ci.yaml", 400))
        self.assertTrue(decide(infra, labels=["ci-full"]))
        self.assertTrue(decide(numstat(("internal/proxy/metric_agg.go", 3)), labels=["ci-full"]))

    def test_ci_full_on_a_stacked_pull_request_does_not_force_it(self):
        self.assertEqual(baseline(["internal/proxy/a.go"], [("internal/proxy/a.go", 3)], "perf/x", ["ci-full"]),
                         (False, []))

    def test_ci_full_does_not_stop_a_big_change_from_triggering(self):
        self.assertTrue(decide(many(BASELINE_FILES + 1), labels=["ci-full"]))

    def test_an_infra_lane_diff_never_triggers_by_size(self):
        self.assertFalse(decide(numstat(("test/e2e-compat/loki-local-config.yaml", 9000))))
        self.assertFalse(decide(numstat(("bench/ab/perf_matrix.py", 9000))))

    def test_no_diff_no_baseline(self):
        self.assertEqual(baseline([], [], "main", []), (False, []))
        self.assertEqual(baseline(["internal/proxy/a.go"], [], "main", []), (False, []))

    def test_reasons_name_what_tripped(self):
        reasons = baseline_reasons(numstat(*[(f"internal/proxy/f{i}.go", 40) for i in range(41)]))
        self.assertEqual(len(reasons), 2)
        self.assertIn("1640 changed lines", reasons[0])
        self.assertIn("41 shipped files", reasons[1])
        self.assertIn("changed lines in the files every query passes through",
                      baseline_reasons(numstat(("internal/proxy/proxy.go", 200)))[0])

    def test_numstat_parsing(self):
        text = "10\t5\tinternal/proxy/a.go\n-\t-\tcharts/loki-vl-proxy/logo.png\nbad line\n3\tx\tb.go\n0\t0\t\n"
        self.assertEqual(parse_numstat(text), [("internal/proxy/a.go", 15), ("charts/loki-vl-proxy/logo.png", 0)])
        self.assertEqual(parse_numstat(""), [])

    def test_a_binary_file_is_a_file_with_no_lines(self):
        changes = parse_numstat("".join(f"-\t-\tcharts/loki-vl-proxy/img{i}.png\n" for i in range(BASELINE_FILES + 1)))
        self.assertTrue(decide(changes))

    def test_recorded_pull_requests(self):
        for number, lines, files, entry, want in RECORDED:
            with self.subTest(pr=number):
                changes = [("internal/proxy/proxy.go", entry)] if entry else []
                rest, count = lines - entry, files - (1 if entry else 0)
                if rest:
                    changes += [(f"internal/proxy/x{i}.go", rest // count + (1 if i < rest % count else 0))
                                for i in range(count)]
                self.assertEqual(sum(n for _, n in changes), lines)
                self.assertEqual(decide(changes), want)

    def test_the_recorded_fixture_covers_both_rules(self):
        # 862 and 760 trip the line count, 846 and 747 only the entry points: neither rule alone is enough.
        by_lines = {n for n, lines, files, entry, _ in RECORDED if lines > BASELINE_LINES}
        by_entry = {n for n, lines, files, entry, _ in RECORDED if entry >= BASELINE_ENTRY_LINES}
        self.assertTrue(by_lines - by_entry and by_entry - by_lines)

    def test_the_constants(self):
        self.assertEqual((BASELINE_LINES, BASELINE_FILES, BASELINE_ENTRY_LINES), (1500, 40, 150))
        self.assertEqual(BASELINE_LABEL, "baseline")
        self.assertEqual(BASELINE_BASE, "main")
        self.assertEqual(len(BASELINE_ENTRY_POINTS), len(set(BASELINE_ENTRY_POINTS)))


class BaselineCLITests(unittest.TestCase):
    def run_cli(self, *args, stdin=None):
        return subprocess.run([sys.executable, "-I", str(SCRIPT), *args], input=stdin, capture_output=True,
                              text=True, cwd=REPO)

    def run_with(self, files, stat, *args):
        with tempfile.TemporaryDirectory() as tmp:
            listing = Path(tmp) / "numstat"
            listing.write_text("".join(f"{n}\t0\t{p}\n" for p, n in stat))
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", "-", "--numstat", str(listing), "--github-output", str(out), *args,
                                  stdin="".join(p + "\n" for p in files))
            self.assertEqual(result.returncode, 0, result.stderr)
            return out.read_text()

    def test_a_big_pull_request_into_main(self):
        stat = [(f"internal/proxy/f{i}.go", 1) for i in range(41)]
        self.assertEqual(self.run_with([p for p, _ in stat], stat, "--base-ref", "main"), BASELINE_FLAGS)

    def test_a_small_pull_request_into_main(self):
        stat = [("internal/proxy/f.go", 9)]
        self.assertEqual(self.run_with([p for p, _ in stat], stat, "--base-ref", "main"), ALL_FLAGS)

    def test_a_stacked_pull_request(self):
        stat = [(f"internal/proxy/f{i}.go", 1) for i in range(41)]
        self.assertEqual(self.run_with([p for p, _ in stat], stat, "--base-ref", "feature/x", "--labels", "baseline"),
                         ALL_FLAGS)

    def test_no_base_ref_means_not_a_pull_request(self):
        stat = [(f"internal/proxy/f{i}.go", 1) for i in range(41)]
        self.assertEqual(self.run_with([p for p, _ in stat], stat), ALL_FLAGS)

    def test_the_label_on_an_infra_pull_request_gets_the_full_product_ci_and_the_baseline(self):
        # The workflow passes --force-product for a ci-full or baseline label.
        stat = [("docs/a.md", 3)]
        self.assertEqual(self.run_with(["docs/a.md"], stat, "--base-ref", "main", "--labels", "bug,baseline",
                                       "--force-product"), BASELINE_FLAGS)

    def test_ci_full_on_an_infra_pull_request_gets_the_full_product_ci_and_the_baseline(self):
        stat = [(".github/workflows/ci.yaml", 400)]
        self.assertEqual(self.run_with([".github/workflows/ci.yaml"], stat, "--base-ref", "main", "--labels",
                                       "ci-full", "--force-product"), BASELINE_FLAGS)

    def test_an_unreadable_numstat_means_no_baseline_and_every_lane(self):
        out = self.run_cli("--files-from", "-", "--numstat", "/nonexistent/numstat", "--base-ref", "main",
                           stdin="internal/proxy/f.go\n").stdout
        self.assertEqual(out, ALL_FLAGS)

    def test_the_summary_says_why(self):
        with tempfile.TemporaryDirectory() as tmp:
            listing, summary = Path(tmp) / "n", Path(tmp) / "s.md"
            listing.write_text("2000\t0\tinternal/proxy/a.go\n")
            self.run_cli("--files-from", "-", "--numstat", str(listing), "--base-ref", "main",
                         "--summary", str(summary), stdin="internal/proxy/a.go\n")
            text = summary.read_text()
            self.assertIn("`baseline=true`", text)
            self.assertIn("Baseline run: 2000 changed lines in shipped paths", text)


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
        for rule, globs in PRODUCT_OVERRIDES + NON_PRODUCT + [(r, g) for r, _, g in LANES]:
            for glob in globs:
                if not any(glob_match(glob, path) for path in TRACKED):
                    dead.append("%s: %s" % (rule, glob))
        self.assertEqual(dead, [], "globs that match no tracked file")


ALL_FLAGS = "product=true\nstack=true\nperf=true\nbaseline=false\n"
NO_FLAGS = "product=false\nstack=false\nperf=false\nbaseline=false\n"
STACK_FLAGS = "product=false\nstack=true\nperf=true\nbaseline=false\n"
PERF_FLAGS = "product=false\nstack=false\nperf=true\nbaseline=false\n"
BASELINE_FLAGS = "product=true\nstack=true\nperf=true\nbaseline=true\n"


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
            self.assertEqual(out.read_text(), NO_FLAGS)
            self.assertEqual(result.stdout, "")

    def test_github_output_is_appended(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.write_text("other=1\n")
            self.run_cli("--files-from", "-", "--github-output", str(out),
                         stdin="internal/proxy/proxy.go\n")
            self.assertEqual(out.read_text(), "other=1\n" + ALL_FLAGS)

    def test_stdout_without_github_output(self):
        result = self.run_cli("--files-from", "-", stdin="docs/a.md\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, NO_FLAGS)

    def test_files_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            listing = Path(tmp) / "files.txt"
            listing.write_text("docs/a.md\ninternal/x.go\n")
            result = self.run_cli("--files-from", str(listing))
            self.assertEqual(result.stdout, ALL_FLAGS)

    def test_force_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            summary = Path(tmp) / "summary.md"
            result = self.run_cli("--files-from", "-", "--force-product",
                                  "--github-output", str(out), "--summary", str(summary),
                                  stdin="docs/a.md\n")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), ALL_FLAGS)
            self.assertIn("--force-product", summary.read_text())

    def test_empty_input_is_product(self):
        for stdin in ["", "\n\n  \n"]:
            with self.subTest(stdin=stdin), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                result = self.run_cli("--files-from", "-", "--github-output", str(out), stdin=stdin)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(out.read_text(), ALL_FLAGS)

    def test_no_input_is_product(self):
        result = self.run_cli()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, ALL_FLAGS)

    def test_missing_file_is_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", str(Path(tmp) / "absent"),
                                  "--github-output", str(out))
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), ALL_FLAGS)

    def test_unknown_argument_is_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--bogus", "--github-output", str(out))
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), ALL_FLAGS)

    def test_unwritable_summary_does_not_change_the_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = self.run_cli("--files-from", "-", "--github-output", str(out),
                                  "--summary", str(Path(tmp) / "no" / "such" / "dir"),
                                  stdin="docs/a.md\n")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(out.read_text(), NO_FLAGS)

    def test_stack_and_perf_lanes_report_their_flags(self):
        cases = {
            "test/e2e-compat/loki-local-config.yaml\ndocs/a.md\n": STACK_FLAGS,
            "bench/ab/perf_matrix.py\nbench/ab/README.md\n": PERF_FLAGS,
            "bench/ab/perf_matrix.py\ntest/e2e-compat/vmauth.yml\n": STACK_FLAGS,
            "bench/ab/perf_matrix.py\ninternal/proxy/proxy.go\n": ALL_FLAGS,
            "test/e2e-compat/vmauth.yml\nscripts/ci/change_scope.py\n": ALL_FLAGS,
        }
        for stdin, want in cases.items():
            with self.subTest(stdin=stdin), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                self.run_cli("--files-from", "-", "--github-output", str(out), stdin=stdin)
                self.assertEqual(out.read_text(), want)

    def test_unknown_argument_without_output_prints_every_lane(self):
        self.assertEqual(self.run_cli("--bogus").stdout, ALL_FLAGS)

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
        self.assertEqual(result.stdout, ALL_FLAGS)

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
            self.assertIn("`product=true`, `stack=true`, `perf=true`, `baseline=false`", text)

    def test_summary_names_the_lane_of_each_path(self):
        text = change_scope.render_summary(
            ["bench/ab/perf_matrix.py", "test/e2e-compat/vmauth.yml", "docs/a.md"], lane_flags("stack"))
        self.assertIn("| `bench/ab/perf_matrix.py` | perf | S2 |", text)
        self.assertIn("| `test/e2e-compat/vmauth.yml` | stack | S1 |", text)
        self.assertIn("`product=false`, `stack=true`, `perf=true`, `baseline=false`", text)

    def test_summary_escapes_pipes(self):
        text = change_scope.render_summary(["docs/a|b.md"], lane_flags("infra"))
        self.assertIn("docs/a\\|b.md", text)

    def test_summary_is_capped(self):
        paths = ["docs/%d.md" % i for i in range(change_scope.MAX_SUMMARY_ROWS + 7)]
        text = change_scope.render_summary(paths, lane_flags("infra"))
        self.assertIn("7 more paths not listed.", text)


if __name__ == "__main__":
    unittest.main()
