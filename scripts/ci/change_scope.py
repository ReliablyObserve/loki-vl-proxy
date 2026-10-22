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
internal error it reports product=true. With --ships it reports
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
]

# (rule id, globs). Paths verified by an always-on check, or by nothing that
# runs on a pull request.
NON_PRODUCT = [
    ("I1", (".github/**",)),
    ("I2", ("scripts/**",)),
    ("I3", ("security/**",)),
    # Daily performance report data only. The rest of bench/ is product.
    ("I4", ("bench/ab/history/**", "bench/ab/results/**")),
    # Compose files of the test stacks.
    ("I5", ("test/*/docker-compose*.yml",)),
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


def classify(path):
    """Return (is_product, rule_id) for one repo-relative path."""
    clean = normalize(path)
    if clean is None:
        return True, DEFAULT_RULE
    for rule_id, globs in PRODUCT_OVERRIDES:
        if any(glob_match(g, clean) for g in globs):
            return True, rule_id
    for rule_id, globs in NON_PRODUCT:
        if any(glob_match(g, clean) for g in globs):
            return False, rule_id
    return True, DEFAULT_RULE


def scope(paths):
    """True when the change is product: any product path, or no paths at all."""
    paths = [p for p in paths if p and p.strip()]
    if not paths:
        return True
    return any(classify(p)[0] for p in paths)


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


def render_summary(paths, product, forced=False, note=""):
    lines = ["### Change scope", "", "Result: `product=%s`" % str(product).lower()]
    if forced:
        lines.append("")
        lines.append("Forced to product by `--force-product`.")
    if note:
        lines.append("")
        lines.append(note)
    if paths:
        lines += ["", "| Path | Class | Rule |", "| --- | --- | --- |"]
        for path in paths[:MAX_SUMMARY_ROWS]:
            is_product, rule_id = classify(path)
            lines.append("| `%s` | %s | %s |" % (
                _cell(path), "product" if is_product else "infra", rule_id))
        if len(paths) > MAX_SUMMARY_ROWS:
            lines.append("")
            lines.append("%d more paths not listed." % (len(paths) - MAX_SUMMARY_ROWS))
    lines.append("")
    return "\n".join(lines)


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def build_parser():
    parser = _Parser(description="Classify changed paths as infra or product.")
    parser.add_argument("--files-from", metavar="FILE|-",
                        help="file with one changed path per line, or - for stdin")
    parser.add_argument("--force-product", action="store_true",
                        help="report product=true whatever the paths are")
    parser.add_argument("--github-output", metavar="FILE",
                        help="append product=true|false to this file")
    parser.add_argument("--ships", action="store_true",
                        help="report ships=true|false (does anything users receive change) "
                             "instead of product=true|false")
    parser.add_argument("--summary", metavar="FILE",
                        help="append a markdown table of path, class and rule")
    return parser


def emit(product, github_output, name="product"):
    line = "%s=%s" % (name, str(product).lower())
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    else:
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
        name = "ships" if "--ships" in argv else "product"
        try:
            emit(True, _fallback_output(argv), name)
        except Exception:
            print("%s=true" % name)
        return 0

    if args.ships:
        return main_ships(args)

    paths = []
    note = ""
    product = True
    try:
        if args.files_from:
            paths = read_paths(args.files_from)
        if args.force_product:
            product = True
        elif not paths:
            note = "No changed paths were provided, so the change counts as product."
        else:
            product = scope(paths)
    except Exception as exc:
        print("change_scope: %s; treating the change as product" % exc, file=sys.stderr)
        product = True
        note = "The classifier failed, so the change counts as product."

    try:
        emit(product, args.github_output)
    except Exception as exc:
        print("change_scope: cannot write output: %s" % exc, file=sys.stderr)
        print("product=true")
        return 0

    if args.summary:
        try:
            with open(args.summary, "a", encoding="utf-8") as handle:
                handle.write(render_summary(paths, product, args.force_product, note))
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
