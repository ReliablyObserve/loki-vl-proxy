import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.ci import change_scope
from scripts.ci.change_scope import NON_PRODUCT, PRODUCT_OVERRIDES, classify, glob_match, scope

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
