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
    perf     + perf-smoke
    stack    + every job that starts a compose stack (image, tuple-smoke,
               e2e-compat, e2e-fleet, e2e-ui, e2e-pipeline, the compat-*
               pinned jobs, Security / runtime, visual-smoke)
    product  + the Go unit tests, race, fuzz, bench, conformance, quality report

The lanes are cumulative, so the CLI reports three flags, each "false" only when
the lane's jobs may be skipped: product=false (no Go-level job), stack=false
(no stack job), perf=false (no perf-smoke). A lane rule sits after the infra
table, so a path an infra rule names stays infra, and every path that matches
no rule at all is product.

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
internal error it reports product=true, stack=true and perf=true. With --ships it reports
ships=true|false instead: an empty list or an internal error reports
ships=true, as the release bot always has, and the bot's own checks (the
changelog and the release labels) stay the deciding gate in that case.
"""

import argparse
import posixpath
import re
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
    """The three flags a lane switches on: product, stack, perf."""
    rank = LANE_ORDER.index(name)
    return {
        "product": rank >= LANE_ORDER.index(PRODUCT),
        "stack": rank >= LANE_ORDER.index(STACK),
        "perf": rank >= LANE_ORDER.index(PERF),
    }


def scope(paths):
    """True when the change is product: any product path, or no paths at all."""
    return scope_lane(paths) == PRODUCT


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


def read_paths(source):
    if source == "-":
        text = sys.stdin.read()
    else:
        with open(source, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    return [line.strip() for line in text.splitlines() if line.strip()]


def _cell(text):
    return text.replace("|", "\\|").replace("`", "'")


def render_summary(paths, flags, forced=False, note=""):
    lines = ["### Change scope", "", "Result: " + ", ".join(
        "`%s=%s`" % (key, str(flags[key]).lower()) for key in ("product", "stack", "perf"))]
    if forced:
        lines.append("")
        lines.append("Forced to product by `--force-product`.")
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
                        help="append product, stack and perf =true|false to this file")
    parser.add_argument("--ships", action="store_true",
                        help="report ships=true|false (does anything users receive change) "
                             "instead of product=true|false")
    parser.add_argument("--summary", metavar="FILE",
                        help="append a markdown table of path, class and rule")
    return parser


FLAG_NAMES = ("product", "stack", "perf")
ALL_TRUE = {name: True for name in FLAG_NAMES}


def emit(value, github_output, name="product"):
    """Report one flag, or (name="lanes") the three lane flags of a dict."""
    if name == "lanes":
        lines = ["%s=%s" % (key, str(value[key]).lower()) for key in FLAG_NAMES]
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
                print("%s=true" % name)
        return 0

    if args.ships:
        return main_ships(args)

    paths = []
    note = ""
    flags = dict(ALL_TRUE)
    try:
        if args.files_from:
            paths = read_paths(args.files_from)
        if args.force_product:
            pass
        elif not paths:
            note = "No changed paths were provided, so the change counts as product."
        else:
            flags = lane_flags(scope_lane(paths))
    except Exception as exc:
        print("change_scope: %s; treating the change as product" % exc, file=sys.stderr)
        flags = dict(ALL_TRUE)
        note = "The classifier failed, so the change counts as product."

    try:
        emit(flags, args.github_output, "lanes")
    except Exception as exc:
        print("change_scope: cannot write output: %s" % exc, file=sys.stderr)
        for name in FLAG_NAMES:
            print("%s=true" % name)
        return 0

    if args.summary:
        try:
            with open(args.summary, "a", encoding="utf-8") as handle:
                handle.write(render_summary(paths, flags, args.force_product, note))
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
