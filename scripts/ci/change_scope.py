#!/usr/bin/env python3
"""Classify the paths a pull request changes as infra or product.

A path is "product" when it can change proxy behaviour, the shipped binary,
image or chart, or the compatibility evidence, or when only a product job
verifies it. A path is "infra" only when it matches an explicit rule below.
Anything that matches no rule is product, and so is an empty or unreadable
file list: the classifier can only ever skip checks on purpose.

Rules are ordered and the first match wins. PRODUCT_OVERRIDES is consulted
before NON_PRODUCT so a narrow product path beats a broad infra glob (for
example scripts/ci/run_zap_scan.sh beats scripts/**).

Globs are segment-aware: "*" and "?" never match "/", and "**" matches any
number of characters including "/". "dir/**" therefore matches everything
below dir/ at any depth, while "test/*/docker-compose*.yml" matches only a
compose file one directory below test/.

Between "everything" and "nothing" there are two lanes, for paths that only a
few product jobs verify. A change runs the jobs of the highest lane any of its
paths needs:

    infra    lint, helm, Security / static, CodeQL (always on, every lane)
    perf     + the performance lane (A/B separately requires evidence)
    stack    + every job that starts a compose stack (image, tuple-smoke,
               e2e-compat, e2e-fleet, e2e-ui, e2e-pipeline, the compat-*
               pinned jobs, Security / runtime, visual-smoke)
    product  + the Go unit tests, race, fuzz, bench, conformance, quality report

The lanes are cumulative, so the CLI reports three lane flags, each "false"
only when the lane's jobs may be skipped: product=false (no Go-level job),
stack=false (no stack job), perf=false (no performance-lane job). A lane rule sits after the infra
table, so a path an infra rule names stays infra, and every path that matches
no rule at all is product.

Baseline. Separately from the lanes, a very large change to the proxy re-measures
everything once, so the project gets a fresh baseline to compare later pull
requests with: every A/B set, every visual page and the whole VictoriaLogs
version matrix, instead of the shapes and pages a small change selects. The CLI
reports baseline=true only for a pull request whose base is main, and only when

  - the `baseline` label is set (it also forces the full product CI), or
  - the change is in the product lane and its shipped diff is big: more than
    BASELINE_LINES changed lines, or more than BASELINE_FILES files, in shipped
    paths (ships(): tests and testdata do not count), or at least
    BASELINE_ENTRY_LINES changed lines in the files every query passes through
    (BASELINE_ENTRY_POINTS).

`ci-full` forces the product lanes but not the baseline: it exists to run the
normal CI on a change that touches no product path, where a baseline would only
repeat main's. A pull request that does not target main (a stacked one) never
gets the baseline, label or not: it pays once, when the stack lands. Where the
lane flags fail towards running more, baseline fails towards running less: an
error, an empty diff or an unreadable label list reports baseline=false.

`evidence` is independent of the CI lanes and labels: it is true when runtime
Go code or the module graph changes, so visual, A/B and quality proofs can
measure the change. A forced product run on CI, release or docs files still has
evidence=false. Empty, invalid and unknown path lists fail toward evidence.

A second, narrower question is whether a change SHIPS: whether it alters
something users receive (the proxy binary, the container image, the Helm
chart, the dashboards and alert rules). Only a change that ships cuts a
release or needs a changelog entry. Its table is SHIPPED below and it fails
the other way round from the classifier: a path it cannot place does not
ship, because releasing on a guess is the mistake it exists to prevent.

Usage:
    git diff --name-only HEAD^1 HEAD | change_scope.py --files-from - \
        --github-output "$GITHUB_OUTPUT" --summary "$GITHUB_STEP_SUMMARY"
    git diff --name-only TAG..HEAD | change_scope.py --files-from - --ships

The CLI always exits 0 once it can report a result. On an empty list or any
internal error it reports product=true, stack=true, perf=true and evidence=true,
and baseline=false. With --ships it reports
ships=true|false instead: an empty list or an internal error reports
ships=true, as the release bot always has, and the bot's own checks (the
changelog and the release labels) stay the deciding gate in that case.
"""

import argparse
import posixpath
import re
import subprocess
import sys

DEFAULT_RULE = "P-default"

# (rule id, globs). Product paths that sit inside a broader infra glob.
PRODUCT_OVERRIDES = [
    # Only the Security / runtime job runs these.
    ("P1", (
        "scripts/ci/run_security_regressions.sh",
        "scripts/ci/run_zap_scan.sh",
        "security/zap/**",
    )),
    # Only the e2e, compat, tuple-smoke and runtime jobs run these.
    ("P2", (
        "scripts/ci/wait_e2e_stack.sh",
        "scripts/ci/wait_loki_track_stack.sh",
        "scripts/ci/wait_pipeline_stack.sh",
        "scripts/smoke-test.sh",
        "scripts/compat/**",
    )),
    # The report and conformance gates themselves.
    ("P3", (
        "scripts/ci/collect_quality_metrics.sh",
        "scripts/ci/check_quality_gate.py",
        "scripts/ci/load_abab.py",
        "scripts/ci/render_pr_report.sh",
        "scripts/ci/check_conformance.py",
    )),
    # The gating machinery. A pull request that edits the classifier or its
    # workflow runs every check. Release tooling treats this rule id as
    # non-release.
    ("P4", (
        "scripts/ci/change_scope.py",
        ".github/workflows/change-scope.yaml",
    )),
    # The parts of bench/ab that other harnesses and gates read: stack.py is
    # imported by bench/visual and bench/parity, selection.py by
    # bench/visual/plan.py, shapes.json by the conformance gate, the parity
    # corpus and perf_evidence.py, and the compose overlays are stack.py's.
    ("P5", (
        "bench/ab/selection.py",
        "bench/ab/stack.py",
        "bench/ab/shapes.json",
        "bench/ab/docker-compose*.yml",
    )),
]

# (rule id, globs). Paths verified by an always-on check, or by nothing that
# runs on a pull request.
NON_PRODUCT = [
    ("I1", (".github/**",)),
    ("I2", ("scripts/**",)),
    ("I3", ("security/**",)),
    # Daily performance report data only. The rest of bench/ is product.
    ("I4", ("bench/ab/history/**", "bench/ab/results/**")),
    # Compose files of the test stacks that no pull request job starts; the
    # ones CI does start are in LANES (S1). lint validates all of them.
    ("I5", (
        "test/e2e-compat/docker-compose.bench.yml",
        "test/e2e-compat/docker-compose.review.yml",
        "test/e2e-fleet/docker-compose.http-discovery.yml",
    )),
    ("I6", ("test/e2e-ui/package.json", "test/e2e-ui/package-lock.json")),
    ("I7", ("examples/**", "docker-compose.yml")),
    ("I8", ("website/**",)),
    ("I9", ("docs/**", "results/**")),
    ("I10", (
        "README.md",
        "CHANGELOG.md",
        "LICENSE",
        "SECURITY.md",
        "CONTRIBUTING.md",
    )),
    ("I11", (
        "conformance/registry/generated/perf-evidence.json",
        "conformance/reports/performance.md",
        "conformance/reports/gaps.md",
    )),
    ("I12", (
        ".gitignore",
        ".gitleaks.toml",
        ".trivyignore.yaml",
        ".hadolint.yaml",
        ".golangci.yml",
    )),
    # Markdown nobody reads as data. Listed by directory, not as **/*.md:
    # the Markdown below conformance/ is generated and compared by the
    # conformance gate, the chart README ships, and cmd/, internal/ and pkg/
    # stay product whatever they hold. Root-level files are *.md, so a
    # nested README does not match.
    ("I13", ("*.md", "bench/*.md", "bench/**/*.md", "test/**/*.md")),
    # The benchmark Loki configs are mounted only by the benchmark compose
    # file. Their one pull request check is the drift guard in the bench
    # module's tests (bench/cmd/seed), which lint runs.
    ("I14", (
        "test/e2e-compat/loki-bench-config.yaml",
        "test/e2e-compat/loki-bench-nocache-config.yaml",
    )),
]

# Lanes. (rule id, lane, globs). Paths verified by some product jobs but not
# the whole set. Consulted after NON_PRODUCT, so an infra rule wins over a
# lane glob (bench/ab/results/** stays infra below bench/ab/**), and after
# PRODUCT_OVERRIDES, so a shared module stays product.
PERF = "perf"
STACK = "stack"
INFRA = "infra"
PRODUCT = "product"
LANE_ORDER = (INFRA, PERF, STACK, PRODUCT)
LANES = [
    # The compose files CI starts, and the configs the e2e-compat stack
    # mounts (loki-local-config.yaml also feeds the bench drift guard, which
    # lint runs).
    ("S1", STACK, (
        "test/e2e-compat/docker-compose.yml",
        "test/e2e-fleet/docker-compose.yml",
        "test/e2e-pipeline/docker-compose.yml",
        "test/e2e-compat/loki-local-config.yaml",
        "test/e2e-compat/grafana-*.yaml",
        "test/e2e-compat/vm*.yml",
        "test/e2e-compat/nginx-tail.conf",
    )),
    # The A/B harness: perf-smoke is its proof, and the unit tests under
    # bench/ab/tests run in lint.
    ("S2", PERF, ("bench/ab/**",)),
]

# Globs of the paths that ship. Everything the binary, image, chart and
# release assets are built from, and nothing else: bench, test suites, the
# conformance registry, scripts, workflows, compose files, examples, docs and
# the website never ship, whatever the classifier says about them.
SHIPPED = (
    "cmd/**",
    "internal/**",
    "pkg/**",
    # Module files and the image build.
    "go.mod",
    "go.sum",
    "Dockerfile",
    ".dockerignore",
    # The chart is packaged whole, its test templates (templates/tests/**)
    # included. dashboard/ and alerting/ are the sources that
    # scripts/ci/sync_observability_assets.sh copies into the chart, and the
    # operations guide points users at them directly.
    "charts/**",
    "dashboard/**",
    "alerting/**",
)

# Go files under the Go roots that are compiled into no binary: unit tests and
# testdata (fuzz corpora, golden files). Nothing is embedded with go:embed,
# so every other file below cmd/, internal/ and pkg/ ships.
NOT_SHIPPED_GO = (
    "**_test.go",
    "**/testdata/**",
)
GO_ROOTS = ("cmd/**", "internal/**", "pkg/**")

# The baseline trigger (see the module docstring). Tuned on the 200 merged pull
# requests #678 to #894: of the 45 that changed shipped code (release and
# dependency bots left out) the lines/files rule alone selects 3 (7%), the
# entry-point rule adds 3 more (13% together); counting any edit of an entry
# point would select 44%, which is why it has a size.
BASELINE_LABEL = "baseline"
# ci-full forces the full product CI and the baseline run too (owner decision).
CI_FULL_LABEL = "ci-full"
BASELINE_BASE = "main"
BASELINE_LINES = 1500
BASELINE_FILES = 40
BASELINE_ENTRY_LINES = 150
# The files every query passes through, none of them specific to one endpoint:
#   proxy.go            route table (Init), New, the handlers of query_range and query
#   query_translation.go  requestLogger around every route and translateQuery
#   request_scope.go    tenant and routing scope applied to every request
#   cache_keys.go       the canonical read-cache key of every cached endpoint
#   middleware.go       tenant validation and the security headers of every request
#   translator.go       the LogQL to LogsQL translation every query goes through
BASELINE_ENTRY_POINTS = (
    "internal/proxy/proxy.go",
    "internal/proxy/query_translation.go",
    "internal/proxy/request_scope.go",
    "internal/proxy/cache_keys.go",
    "internal/proxy/middleware.go",
    "internal/translator/translator.go",
)

MAX_SUMMARY_ROWS = 500


_GLOB_CACHE = {}


def _glob_regex(glob):
    """Translate a rule glob into an anchored regular expression."""
    pattern = _GLOB_CACHE.get(glob)
    if pattern is None:
        parts = []
        i = 0
        while i < len(glob):
            if glob.startswith("**", i):
                parts.append(".*")
                i += 2
            elif glob[i] == "*":
                parts.append("[^/]*")
                i += 1
            elif glob[i] == "?":
                parts.append("[^/]")
                i += 1
            else:
                parts.append(re.escape(glob[i]))
                i += 1
        pattern = re.compile("".join(parts) + r"\Z")
        _GLOB_CACHE[glob] = pattern
    return pattern


def glob_match(glob, path):
    """Case-sensitive, segment-aware match of a rule glob against a path."""
    return _glob_regex(glob).match(path) is not None


def normalize(path):
    """Return a clean repo-relative path, or None when it cannot be trusted."""
    path = path.strip()
    if not path:
        return None
    while path.startswith("./"):
        path = path[2:]
    path = posixpath.normpath(path)
    if path in (".", "") or path.startswith("/") or path == ".." or path.startswith("../"):
        return None
    return path


def lane(path):
    """Return (lane, rule_id) for one repo-relative path."""
    clean = normalize(path)
    if clean is None:
        return PRODUCT, DEFAULT_RULE
    for rule_id, globs in PRODUCT_OVERRIDES:
        if any(glob_match(g, clean) for g in globs):
            return PRODUCT, rule_id
    for rule_id, globs in NON_PRODUCT:
        if any(glob_match(g, clean) for g in globs):
            return INFRA, rule_id
    for rule_id, name, globs in LANES:
        if any(glob_match(g, clean) for g in globs):
            return name, rule_id
    return PRODUCT, DEFAULT_RULE


def classify(path):
    """Return (is_product, rule_id) for one repo-relative path.

    is_product is true only for the full product lane; a stack or perf path
    is not product but still runs its own jobs (see lane()).
    """
    name, rule_id = lane(path)
    return name == PRODUCT, rule_id


def scope_lane(paths):
    """The highest lane any path needs; product when there are no paths."""
    paths = [p for p in paths if p and p.strip()]
    if not paths:
        return PRODUCT
    return max((lane(p)[0] for p in paths), key=LANE_ORDER.index)


def lane_flags(name):
    """The flags a lane switches on: product, stack, perf. A lane never implies the baseline."""
    rank = LANE_ORDER.index(name)
    return {
        "product": rank >= LANE_ORDER.index(PRODUCT),
        "stack": rank >= LANE_ORDER.index(STACK),
        "perf": rank >= LANE_ORDER.index(PERF),
        "baseline": False,
    }


def scope(paths):
    """True when the change is product: any product path, or no paths at all."""
    return scope_lane(paths) == PRODUCT


def product_evidence(paths):
    """Whether a PR changes runtime code the visual, A/B and quality jobs measure.

    This is deliberately independent of the CI lanes: gate machinery, labels
    and test-only changes can force product CI without changing the proxy. An
    empty, invalid or unrecognized path fails toward collecting evidence.
    """
    if not paths:
        return True
    for path in paths:
        clean = normalize(path)
        if clean is None:
            return True
        if clean in ("go.mod", "go.sum"):
            return True
        if any(glob_match(root, clean) for root in GO_ROOTS):
            if clean.endswith(".go") and ships(clean):
                return True
            continue
        # Other shipped files (chart, image, dashboard, alerts) are not inputs
        # to the host-built proxy in either evidence harness.
        if ships(clean):
            continue
        # These default to the product CI lane but are evidence/test inputs,
        # not a change to the runtime being measured.
        if clean.startswith(("bench/", "conformance/", "test/")):
            continue
        if lane(clean)[1] == DEFAULT_RULE:
            return True
    return False


def ships(path):
    """True when the path is part of something users receive.

    Unlike classify(), a path that cannot be trusted (empty, absolute, or
    leaving the repository) does not ship.
    """
    clean = normalize(path)
    if clean is None:
        return False
    if not any(glob_match(g, clean) for g in SHIPPED):
        return False
    if any(glob_match(g, clean) for g in GO_ROOTS):
        return not any(glob_match(g, clean) for g in NOT_SHIPPED_GO)
    return True


def any_ships(paths):
    """True when any path ships, or when there are no paths at all.

    The empty list keeps the release bot's long-standing meaning: with
    nothing to judge it does not skip the release on its own.
    """
    paths = [p for p in paths if p and p.strip()]
    if not paths:
        return True
    return any(ships(p) for p in paths)


# ---------------------------------------------------------------------------
# Job selection for a forced run (the `ci-full` label on a pull request, a push
# to main).
#
# Ids are "<workflow file stem>/<job id>": several workflows have a job named
# `scope`, and a workflow tests the list with contains(), where `ci/test` must
# not match `ci/test-shard`. `perf-ab/full` and `visual-smoke/full` are not jobs:
# they ask those workflows for their full run (every A/B set, every visual
# page). A change to the harness needs it, because the shapes and pages a run
# normally selects are chosen from the runtime code the change touches.

CI_WORKFLOW = ".github/workflows/ci.yaml"
# ci.yaml jobs that run on every change and are never selected.
ALWAYS_ON_CI = ("scope", "lint", "helm")
CI_JOBS = (
    "test-shard", "test", "tuple-contract", "fuzz-smoke-shard", "fuzz-smoke",
    "race-stress-shard", "race-stress", "image", "tuple-smoke", "bench",
    "conformance", "docker", "e2e-compat-group", "e2e-compat", "e2e-fleet",
    "e2e-ui", "stress-tests", "memory-leak-tests",
)
# A job and the aggregate that carries its required check name: one never runs without the other.
CI_PARTNERS = (
    ("test-shard", "test"),
    ("fuzz-smoke-shard", "fuzz-smoke"),
    ("race-stress-shard", "race-stress"),
    ("e2e-compat-group", "e2e-compat"),
)
# Jobs that cannot run without another job of the same run (an artifact they download).
CI_NEEDS_RUN = {
    "conformance": ("test",),
    "tuple-smoke": ("image",),
    "e2e-compat-group": ("image",),
    "e2e-fleet": ("image",),
    "e2e-ui": ("image",),
}
# Jobs that consume what a job produces: an edit to the producer runs them too.
CI_CONSUMERS = {
    "image": ("tuple-smoke", "e2e-compat-group", "e2e-compat", "e2e-fleet", "e2e-ui"),
    "test-shard": ("conformance",),
    "test": ("conformance",),
}
# The Go test that pins the shard wiring (test/testcache) runs in a unit test shard and reads both
# shard jobs, so an edit to the race shards runs it.
CI_ALSO_RUN = {
    "race-stress-shard": ("test-shard",),
    "race-stress": ("test-shard",),
}

# Workflow file -> (stem, its selectable jobs, extra ids). A change to the file runs the whole
# workflow, in full where the workflow has a full run.
WORKFLOW_JOBS = {
    ".github/workflows/compat-loki.yaml": ("compat-loki", ("loki-pinned",), ()),
    ".github/workflows/compat-drilldown.yaml": (
        "compat-drilldown", ("drilldown-pinned-runtime", "drilldown-grafana-pr-matrix"), ()),
    ".github/workflows/compat-vl.yaml": ("compat-vl", ("vl-pinned",), ()),
    ".github/workflows/e2e-pipeline.yml": ("e2e-pipeline", ("e2e-pipeline",), ()),
    ".github/workflows/security-pr.yaml": ("security-pr", ("security-runtime",), ()),
    ".github/workflows/perf-ab.yaml": ("perf-ab", ("perf-smoke",), ("full",)),
    ".github/workflows/visual-smoke.yaml": ("visual-smoke", ("visual-plan", "visual-smoke"), ("full",)),
    ".github/workflows/pr-quality-report.yaml": ("pr-quality-report", ("report",), ()),
}
# Workflows with no selectable job: they carry their own triggers (path filters, schedules, labels,
# releases), so a change to one needs nothing more from the pull request CI.
NO_JOB_WORKFLOWS = (
    "auto-release.yaml", "cancel-conflicted-pr-runs.yaml", "changelog-pr.yaml", "codeql.yaml",
    "docs-site.yaml", "ghcr-badges.yaml", "loc-badges.yaml", "parity-refresh.yaml", "perf-daily.yaml",
    "pr-evidence-summary.yaml", "pr-labeler.yaml", "query-corpus.yaml", "release-pr-validation.yaml",
    "release.yaml", "scorecard.yaml", "security-heavy.yaml", "visual-smoke-cleanup.yaml", "vl-ast-coverage.yml",
)


def _qualified(stem, names):
    return frozenset("%s/%s" % (stem, name) for name in names)


ALL_CI = _qualified("ci", CI_JOBS)
ALL_JOBS = ALL_CI.union(*(_qualified(stem, jobs + extra) for stem, jobs, extra in WORKFLOW_JOBS.values()))
STACK_JOBS = _qualified("ci", ("image", "tuple-smoke", "e2e-compat-group", "e2e-compat", "e2e-fleet", "e2e-ui")) | {
    "compat-loki/loki-pinned", "compat-drilldown/drilldown-pinned-runtime",
    "compat-drilldown/drilldown-grafana-pr-matrix", "compat-vl/vl-pinned",
    "e2e-pipeline/e2e-pipeline", "security-pr/security-runtime",
}
UNIT_JOBS = _qualified("ci", ("test-shard", "test"))
FUZZ_JOBS = _qualified("ci", ("fuzz-smoke-shard", "fuzz-smoke"))
CONFORMANCE_JOBS = _qualified("ci", ("conformance",))
E2E_UI_JOBS = _qualified("ci", ("image", "e2e-ui"))
VISUAL_JOBS = _qualified("visual-smoke", ("visual-plan", "visual-smoke", "full"))
PERF_AB_JOBS = _qualified("perf-ab", ("perf-smoke", "full"))
REPORT_JOBS = _qualified("pr-quality-report", ("report",))
NO_JOBS = frozenset()

# Paths below a JOB_MAP glob that must never narrow: files that other gates read as data.
# (PRODUCT_OVERRIDES and the shipped paths never narrow either; mapped_jobs() checks them first.)
NEVER_NARROW = (
    "bench/visual/spec.json",  # conformance/scripts/census.py reads it, like bench/ab/shapes.json
)

# (rule id, globs, jobs). First match wins. An empty job set means the path is read by nothing that
# runs on a pull request but the always-on checks (lint, helm, Security / static, CodeQL).
JOB_MAP = [
    ("J1", ("docs/**", "website/**", "results/**", "examples/**", "*.md", "LICENSE",
            "bench/*.md", "bench/**/*.md", "test/**/*.md"), NO_JOBS),
    ("J2", (".gitignore", ".gitleaks.toml", ".trivyignore.yaml", ".hadolint.yaml", ".golangci.yml",
            "docker-compose.yml"), NO_JOBS),
    ("J3", (".github/dependabot.yml", ".github/FUNDING.yml", ".github/ISSUE_TEMPLATE/**",
            ".github/pull_request_template.md", ".github/badges/**"), NO_JOBS),
    ("J4", tuple(".github/workflows/" + name for name in NO_JOB_WORKFLOWS), NO_JOBS),
    # Scripts the release, changelog, helm and security-scan workflows run, and the unit tests of
    # scripts/ci (lint runs them).
    ("J5", (
        "scripts/ci/tests/**", "scripts/ci/check_changelog_pr.py", "scripts/ci/cancel_conflicted_pr_runs.py",
        "scripts/ci/collect_release_assets.sh", "scripts/ci/commit_via_api.sh",
        "scripts/ci/extract_changelog_section.sh", "scripts/ci/fetch_ghcr_pulls.sh",
        "scripts/ci/changelog_release.py", "scripts/ci/release_plan.py", "scripts/ci/release_metadata.py",
        "scripts/ci/scan_release_image.sh",
        "scripts/ci/sync_release_metadata.sh", "scripts/ci/sync_observability_assets.sh",
        "scripts/ci/validate_compose.sh", "scripts/ci/validate_helm_flags.sh",
        "scripts/ci/validate_helm_secrets.sh", "scripts/ci/helm-*.yaml", "scripts/ci/check_scorecard.py",
        "scripts/ci/repro_dense_patterns.sh", "scripts/ci/run_nuclei_scan.sh",
        "scripts/check-vl-ast-coverage.py", "scripts/vl-ast-coverage.json",
        "scripts/bench-cache-tiers.sh", "scripts/test-short.sh",
        "bench/ab/history/**", "bench/ab/results/**", "bench/drilldown-memory/**",
        "conformance/registry/generated/perf-evidence.json", "conformance/reports/performance.md",
        "conformance/reports/gaps.md",
        "test/e2e-compat/docker-compose.bench.yml", "test/e2e-compat/docker-compose.review.yml",
        "test/e2e-fleet/docker-compose.http-discovery.yml",
        "test/e2e-compat/loki-bench-config.yaml", "test/e2e-compat/loki-bench-nocache-config.yaml",
    ), NO_JOBS),
    ("J6", ("scripts/ci/go_test_unit.sh", "scripts/ci/unit_test_mode.sh",
            "scripts/ci/merge_unit_shards.py", "scripts/ci/merge_coverprofile.py",
            "scripts/ci/test-cache-uncached.tsv"),
     UNIT_JOBS | CONFORMANCE_JOBS),
    # The test job's shards and the race-detector shards split internal/proxy with
    # the same script and weights.
    ("J6s", ("scripts/ci/test_shards.py", "scripts/ci/test-shard-weights.tsv"),
     UNIT_JOBS | CONFORMANCE_JOBS | _qualified("ci", ("race-stress-shard", "race-stress"))),
    ("J7", ("scripts/ci/fuzz_smoke.py", "scripts/ci/fuzz_smoke_targets.tsv"), FUZZ_JOBS),
    ("J8", ("scripts/ci/race_sensitive_tests.sh",), CONFORMANCE_JOBS),
    ("J9", ("scripts/ci/pr_quality_report.py", "scripts/ci/quality_scope.py",
            "scripts/ci/quality_comparison.py", "scripts/ci/query_semantics_comment.py"), REPORT_JOBS),
    ("J10", ("test/e2e-ui/package.json", "test/e2e-ui/package-lock.json"), E2E_UI_JOBS),
    # The compose files and configs the stack jobs start.
    ("J11", (
        "test/e2e-compat/docker-compose.yml", "test/e2e-fleet/docker-compose.yml",
        "test/e2e-pipeline/docker-compose.yml", "test/e2e-compat/loki-local-config.yaml",
        "test/e2e-compat/grafana-*.yaml", "test/e2e-compat/vm*.yml", "test/e2e-compat/nginx-tail.conf",
    ), STACK_JOBS),
    ("J12", ("bench/visual/**",), VISUAL_JOBS),
    ("J13", ("bench/ab/**",), PERF_AB_JOBS),
]


def parse_jobs(text):
    """Return {job id: {"needs", "if", "uses", "body"}} for a workflow's jobs.

    Reads the block layout the workflows use (two-space job keys, four-space
    fields) with the standard library only: the scope job has no YAML module.
    """
    jobs = {}
    lines = text.splitlines()
    in_jobs = False
    current = None
    for index, line in enumerate(lines):
        if not in_jobs:
            in_jobs = bool(_JOBS_KEY.match(line))
            continue
        if line and not line.startswith((" ", "#")):
            break  # next top-level key
        key = _JOB_KEY.match(line)
        if key:
            current = {"needs": [], "if": None, "uses": None, "body": []}
            jobs[key.group(1)] = current
            continue
        if current is None:
            continue
        current["body"].append(line)
        field = _JOB_FIELD.match(line)
        if not field:
            continue
        name, value = field.groups()
        value = re.sub(r"\s+#.*$", "", value)
        if name == "needs":
            if value.startswith("["):
                current["needs"] = [n.strip() for n in value.strip("[]").split(",") if n.strip()]
            elif value:
                current["needs"] = [value]
            else:
                for follow in lines[index + 1:]:
                    item = _LIST_ITEM.match(follow)
                    if not item:
                        break
                    current["needs"].append(item.group(1))
        elif name == "if":
            current["if"] = value.strip("\"'") if not value.startswith(("\"", "'")) else value[1:-1]
        else:
            current["uses"] = value
    for job in jobs.values():
        job["body"] = "\n".join(job["body"])
    return jobs


_JOBS_KEY = re.compile(r"^jobs:\s*$")
_JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):\s*(?:#.*)?$")
_JOB_FIELD = re.compile(r"^    (needs|if|uses):\s*(.*?)\s*$")
_LIST_ITEM = re.compile(r"^      -\s+(\S+)")


def workflow_blocks(text):
    """(everything outside the jobs, {job id: its lines}), without comments and blank lines.

    A comment-only edit is then no edit. Inline comments stay, so an edit to one is an edit.
    """
    header, jobs, current, in_jobs = [], {}, None, False
    for line in text.splitlines():
        if in_jobs and line and not line.startswith((" ", "#")):
            in_jobs = False
            current = None
        if not in_jobs:
            if _JOBS_KEY.match(line):
                in_jobs = True
                continue
            if line.strip() and not line.lstrip().startswith("#"):
                header.append(line.rstrip())
            continue
        key = _JOB_KEY.match(line)
        if key:
            current = jobs.setdefault(key.group(1), [])
            continue
        if current is not None and line.strip() and not line.lstrip().startswith("#"):
            current.append(line.rstrip())
    return header, jobs


def ci_closure(edited, edit=True):
    """The bare ci.yaml job ids to run for `edited`, with what they need.

    For an edit of a job (edit=True) the jobs that consume what it produces run too, but only for
    the jobs that were edited, not for the ones they pull in: an edit to e2e-ui runs the image it
    needs, not every other job that uses the image.
    """
    result = set(edited)
    if edit:
        for name in edited:
            result.update(CI_CONSUMERS.get(name, ()))
            result.update(CI_ALSO_RUN.get(name, ()))
    changed = True
    while changed:
        changed = False
        for name in list(result):
            extra = set(CI_NEEDS_RUN.get(name, ()))
            for left, right in CI_PARTNERS:
                if name == left:
                    extra.add(right)
                elif name == right:
                    extra.add(left)
            if not extra <= result:
                result |= extra
                changed = True
    return result


def expand(ids):
    """Add what the ci.yaml jobs among `ids` cannot run without (their aggregate, the job they download from)."""
    ci = {i.split("/", 1)[1] for i in ids if i.startswith("ci/")}
    return frozenset(i for i in ids if not i.startswith("ci/")) | _qualified("ci", ci_closure(ci, edit=False))


def ci_workflow_jobs(read_pair=None):
    """The ids to run for an edit of ci.yaml: the jobs whose block changed, with their closure.

    A change outside the job blocks (triggers, permissions, env, the scope job), a job this module
    does not know and any failure to read either side run every ci.yaml job.
    """
    try:
        pair = read_pair(CI_WORKFLOW) if read_pair else None
        if not pair or pair[0] is None or pair[1] is None:
            return ALL_CI
        base_header, base_jobs = workflow_blocks(pair[0])
        head_header, head_jobs = workflow_blocks(pair[1])
    except Exception:
        return ALL_CI
    if base_header != head_header or not base_jobs or not head_jobs:
        return ALL_CI
    edited = {name for name in set(base_jobs) | set(head_jobs) if base_jobs.get(name) != head_jobs.get(name)}
    # The scope job decides what runs, and a job this module does not know has no place in the tables.
    if "scope" in edited or any(name not in CI_JOBS and name not in ALWAYS_ON_CI for name in edited):
        return ALL_CI
    return _qualified("ci", ci_closure(edited & set(CI_JOBS)))


def mapped_jobs(path, read_pair=None):
    """The ids a change to `path` needs, or None when no entry maps it (then every lane runs).

    Only an explicit entry returns a set: a product override, a shipped path, anything in
    NEVER_NARROW and every unknown path return None.
    """
    clean = normalize(path)
    if clean is None:
        return None
    if any(glob_match(g, clean) for _, globs in PRODUCT_OVERRIDES for g in globs):
        return None
    if ships(clean) or any(glob_match(g, clean) for g in NEVER_NARROW):
        return None
    if clean == CI_WORKFLOW:
        return ci_workflow_jobs(read_pair)
    if clean in WORKFLOW_JOBS:
        stem, jobs, extra = WORKFLOW_JOBS[clean]
        return _qualified(stem, jobs + extra)
    for _, globs, jobs in JOB_MAP:
        if any(glob_match(g, clean) for g in globs):
            return jobs
    return None


def select_jobs(paths, read_pair=None):
    """The ids to run for a forced run of this change, or None when it cannot be narrowed.

    None (an empty list, or any path no entry maps) keeps the forced run of every lane.
    """
    paths = [p for p in paths if p and p.strip()]
    if not paths:
        return None
    selected = set()
    for path in paths:
        jobs = mapped_jobs(path, read_pair)
        if jobs is None:
            return None
        selected |= jobs
    return expand(selected)


def git_pair_reader(base_rev, root=None):
    """read_pair(path) -> (text at base_rev, text in the working tree); None where a side is missing."""
    def read(path):
        try:
            base = subprocess.run(["git", "show", "%s:%s" % (base_rev, path)], cwd=root, check=True,
                                  capture_output=True, text=True).stdout
            with open(posixpath.join(root, path) if root else path, encoding="utf-8") as handle:
                head = handle.read()
        except (OSError, subprocess.CalledProcessError):
            return None
        return base, head
    return read


def parse_numstat(text):
    """[(path, changed lines)] from `git diff --numstat --no-renames` output.

    A binary file (`-` counts) is one file of zero lines. Lines that do not
    parse are skipped: the baseline can only get smaller from bad input.
    """
    out = []
    for line in text.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3 or not parts[2].strip():
            continue
        try:
            lines = 0 if parts[0] == "-" or parts[1] == "-" else int(parts[0]) + int(parts[1])
        except ValueError:
            continue
        out.append((parts[2].strip(), lines))
    return out


def baseline_reasons(numstat):
    """Why the shipped diff is big: a list of sentences, empty when it is not."""
    shipped = [(path, lines) for path, lines in numstat if ships(path)]
    total = sum(lines for _, lines in shipped)
    entry = sum(lines for path, lines in shipped if normalize(path) in BASELINE_ENTRY_POINTS)
    reasons = []
    if total > BASELINE_LINES:
        reasons.append("%d changed lines in shipped paths (more than %d)" % (total, BASELINE_LINES))
    if len(shipped) > BASELINE_FILES:
        reasons.append("%d shipped files changed (more than %d)" % (len(shipped), BASELINE_FILES))
    if entry >= BASELINE_ENTRY_LINES:
        reasons.append("%d changed lines in the files every query passes through (at least %d)"
                       % (entry, BASELINE_ENTRY_LINES))
    return reasons


def baseline(paths, numstat, base_ref, labels=()):
    """(wanted, reasons): whether the pull request gets the baseline run.

    base_ref is the pull request's base branch ("" when it is not a pull
    request); labels are its label names. Only a pull request into main can get
    it, whatever the label says. The baseline or ci-full label forces it;
    otherwise the change must be in the product lane on its own paths and its
    shipped diff must be big.
    """
    if base_ref != BASELINE_BASE:
        return False, []
    for label in (BASELINE_LABEL, CI_FULL_LABEL):
        if label in labels:
            return True, ["the `%s` label is set" % label]
    if scope_lane(paths) != PRODUCT:
        return False, []
    reasons = baseline_reasons(numstat)
    return bool(reasons), reasons


def read_paths(source):
    if source == "-":
        text = sys.stdin.read()
    else:
        with open(source, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    return [line.strip() for line in text.splitlines() if line.strip()]


def _cell(text):
    return text.replace("|", "\\|").replace("`", "'")


def render_summary(paths, flags, forced=False, note="", reasons=()):
    lines = ["### Change scope", "", "Result: " + ", ".join(
        "`%s=%s`" % (key, str(flags[key]).lower()) for key in FLAG_NAMES if key in flags)]
    if flags.get("jobs") is not None:
        lines += ["", "Forced run, narrowed to the jobs the change can affect: " + (
            ", ".join("`%s`" % job for job in sorted(flags["jobs"])) or "none beyond the always-on checks") + "."]
    if flags.get("baseline"):
        lines += ["", "Baseline run: " + "; ".join(reasons) + "."]
    if forced:
        lines.append("")
        lines.append("Forced to every lane: `--force-product`, or `--narrow` with a path no JOB_MAP entry maps.")
    if note:
        lines.append("")
        lines.append(note)
    if paths:
        lines += ["", "| Path | Class | Rule |", "| --- | --- | --- |"]
        for path in paths[:MAX_SUMMARY_ROWS]:
            name, rule_id = lane(path)
            lines.append("| `%s` | %s | %s |" % (_cell(path), name, rule_id))
        if len(paths) > MAX_SUMMARY_ROWS:
            lines.append("")
            lines.append("%d more paths not listed." % (len(paths) - MAX_SUMMARY_ROWS))
    lines.append("")
    return "\n".join(lines)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def build_parser():
    parser = _Parser(description="Classify changed paths into the infra, perf, stack or product lane.")
    parser.add_argument("--files-from", metavar="FILE|-",
                        help="file with one changed path per line, or - for stdin")
    parser.add_argument("--force-product", action="store_true",
                        help="report every lane whatever the paths are")
    parser.add_argument("--github-output", metavar="FILE",
                        help="append product, stack, perf and baseline =true|false to this file")
    parser.add_argument("--narrow", action="store_true",
                        help="a forced run (the ci-full label, a push to main) that may run less: when every path "
                             "is mapped by JOB_MAP, report no lane and `jobs=` instead of forcing every lane")
    parser.add_argument("--git-base", default="", metavar="REV",
                        help="with --narrow: the revision to compare changed workflow files against")
    parser.add_argument("--ships", action="store_true",
                        help="report ships=true|false (does anything users receive change) "
                             "instead of product=true|false")
    parser.add_argument("--summary", metavar="FILE",
                        help="append a markdown table of path, class and rule")
    parser.add_argument("--numstat", metavar="FILE",
                        help="`git diff --numstat --no-renames` of the change; with --base-ref it decides "
                             "baseline=true|false")
    parser.add_argument("--base-ref", default="", metavar="BRANCH",
                        help="the pull request's base branch; empty (not a pull request) never gets the baseline")
    parser.add_argument("--labels", default="", metavar="A,B",
                        help="comma-separated pull request labels (`baseline` forces the baseline)")
    return parser


FLAG_NAMES = ("product", "stack", "perf", "baseline", "evidence")
# What an error reports: every lane on, the baseline off.
ALL_TRUE = {"product": True, "stack": True, "perf": True, "baseline": False, "evidence": True}


def format_jobs(jobs):
    """The `jobs=` value: ids between commas, so a workflow can test one with contains()."""
    return "," + "".join(job + "," for job in sorted(jobs))


def emit(value, github_output, name="product"):
    """Report one flag, or (name="lanes") all scope outputs of a dict."""
    if name == "lanes":
        lines = ["%s=%s" % (key, str(value.get(key, False)).lower()) for key in FLAG_NAMES]
        if value.get("jobs") is not None:
            lines.append("jobs=" + format_jobs(value["jobs"]))
    else:
        lines = ["%s=%s" % (name, str(value).lower())]
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write("".join(line + "\n" for line in lines))
    else:
        for line in lines:
            print(line)


def _fallback_output(argv):
    """Find --github-output in argv without argparse, for the error path."""
    for index, arg in enumerate(argv):
        if arg == "--github-output" and index + 1 < len(argv):
            return argv[index + 1]
        if arg.startswith("--github-output="):
            return arg.split("=", 1)[1]
    return None


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = build_parser().parse_args(argv)
    except SystemExit:
        raise
    except Exception as exc:  # unknown arguments: still report a result
        print("change_scope: %s; treating the change as product" % exc, file=sys.stderr)
        ships_mode = "--ships" in argv
        try:
            if ships_mode:
                emit(True, _fallback_output(argv), "ships")
            else:
                emit(ALL_TRUE, _fallback_output(argv), "lanes")
        except Exception:
            for name in (("ships",) if ships_mode else FLAG_NAMES):
                print("%s=%s" % (name, "false" if name == "baseline" else "true"))
        return 0

    if args.ships:
        return main_ships(args)

    paths = []
    note = ""
    reasons = []
    flags = dict(ALL_TRUE)
    try:
        if args.files_from:
            paths = read_paths(args.files_from)
        narrowed = None
        if args.narrow and not args.force_product and paths:
            read_pair = git_pair_reader(args.git_base) if args.git_base else None
            narrowed = select_jobs(paths, read_pair)
        # A forced run (--force-product, or --narrow when some path cannot be narrowed) reports every lane.
        forced = args.force_product or (args.narrow and narrowed is None and bool(paths))
        if forced:
            pass
        elif narrowed is not None:
            flags = {"product": False, "stack": False, "perf": False, "baseline": False, "jobs": narrowed}
        elif not paths:
            note = "No changed paths were provided, so the change counts as product."
        else:
            flags = lane_flags(scope_lane(paths))
        if paths:
            flags["evidence"] = product_evidence(paths)
        # Fails towards the cheaper answer: no paths, no diff or any error is no baseline.
        if paths and args.base_ref and narrowed is None:
            numstat = []
            if args.numstat:
                with open(args.numstat, encoding="utf-8", errors="replace") as handle:
                    numstat = parse_numstat(handle.read())
            labels = [name.strip() for name in args.labels.split(",") if name.strip()]
            flags["baseline"], reasons = baseline(paths, numstat, args.base_ref, labels)
    except Exception as exc:
        print("change_scope: %s; treating the change as product" % exc, file=sys.stderr)
        flags = dict(ALL_TRUE)
        reasons = []
        forced = False
        note = "The classifier failed, so the change counts as product."

    try:
        emit(flags, args.github_output, "lanes")
    except Exception as exc:
        print("change_scope: cannot write output: %s" % exc, file=sys.stderr)
        for name in FLAG_NAMES:
            print("%s=%s" % (name, "false" if name == "baseline" else "true"))
        return 0

    if args.summary:
        try:
            with open(args.summary, "a", encoding="utf-8") as handle:
                handle.write(render_summary(paths, flags, forced, note, reasons))
        except Exception as exc:
            print("change_scope: cannot write summary: %s" % exc, file=sys.stderr)
    return 0


def main_ships(args):
    """Report whether the change ships. Any failure reports ships=true."""
    result = True
    try:
        if args.files_from:
            result = any_ships(read_paths(args.files_from))
    except Exception as exc:
        print("change_scope: %s; treating the change as shipping" % exc, file=sys.stderr)
    try:
        emit(result, args.github_output, "ships")
    except Exception as exc:
        print("change_scope: cannot write output: %s" % exc, file=sys.stderr)
        print("ships=true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
