#!/usr/bin/env python3
"""Split the top-level tests of one Go package across parallel CI shards.

The `test` job's time is the time of internal/proxy and internal/querycorpus (the
two packages that take minutes under -race), so their tests run in several jobs,
each package split on its own (--package, default internal/proxy). The split works
from the names `go test -list` prints:

  * the tests named in scripts/ci/test-shard-weights.tsv (the slow ones, with
    their measured seconds; an optional third column names the package, internal/proxy
    when it is left out) are placed longest first on the shard with the least
    work so far;
  * with --joint (the split packages together), the weighted tests of all of them
    are placed in one longest-first pass, so a shard never receives the slowest test
    of every package at once; the placement is a function of the weights file alone
    and the same in every invocation;
  * every other test, and any test added later, goes to the shard its name hashes
    to. A new test therefore always lands in exactly one shard without the weights
    file being edited, and a shard keeps the same tests when unrelated tests are
    added.

Commands (the list is read from --list or stdin):

  assign  --shards N          print "<shard>\\t<test>" for every test
  regex   --shards N --shard K  print the `go test -run` pattern of shard K
  check   --shards N          fail unless every test is in exactly one shard
  weights --results FILE [--package P ...]
                              print a weights file from `go test -json` events
  verify  --assigned F... --results FILE
                              fail unless every assigned test has an outcome

Weights are a balancing hint only: a wrong or stale entry costs time, never
coverage.
"""
import argparse
import json
import re
import sys
import zlib
from pathlib import Path

SALT = "ci-test-shard:"
HEAVY_SECONDS = 5.0
DEFAULT_PACKAGE = "internal/proxy"
NAME = re.compile(r"(?:Test|Fuzz|Example)\w*")
OUTCOMES = {"pass", "fail", "skip"}


def parse_list(text):
    """The tests `go test -list` printed: Test*, Fuzz* and Example* names, which `go test` runs."""
    names = sorted({line.strip() for line in text.splitlines() if NAME.fullmatch(line.strip())})
    if not names:
        raise SystemExit("test_shards: no tests in the list")
    return names


def parse_weights(text, package=DEFAULT_PACKAGE):
    """Seconds by test name for one package: the rows whose optional third column names it."""
    weights = {}
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        name, _, rest = line.partition("\t")
        columns = rest.split("\t")
        try:
            seconds = float(columns[0])
        except ValueError:
            raise SystemExit(f"test_shards: weights line {number}: want <test><tab><seconds>[<tab><package>]")
        if (columns[1].strip() if len(columns) > 1 else DEFAULT_PACKAGE) == package:
            weights[name.strip()] = seconds
    return weights


def place_heavy(weights_by_package, shards):
    """{(package, test): shard} (0-based) for every weighted test of the packages, longest first on the least loaded
    shard. Reads the weights only (a row naming a test that is gone still takes its place: harmless), so every
    invocation, and every package, sees the same placement."""
    if shards < 1:
        raise SystemExit("test_shards: --shards must be at least 1")
    loads = [0.0] * shards
    rows = sorted(((seconds, package, name) for package, weights in weights_by_package.items()
                   for name, seconds in weights.items() if seconds >= HEAVY_SECONDS),
                  key=lambda r: (-r[0], r[1], r[2]))
    placed = {}
    for seconds, package, name in rows:
        shard = min(range(shards), key=lambda i: (loads[i], i))
        loads[shard] += seconds
        placed[(package, name)] = shard
    return placed


def assign(names, shards, weights, package=DEFAULT_PACKAGE, joint=None):
    """test -> shard (0-based). Total, deterministic and independent of the order of names.

    joint, when given, is the {package: weights} of every split package: the heavy tests are placed with theirs
    (place_heavy) instead of on their own."""
    if shards < 1:
        raise SystemExit("test_shards: --shards must be at least 1")
    placed = {}
    if joint is not None:
        together = place_heavy({**joint, package: weights}, shards)
        for name in names:
            if (package, name) in together:
                placed[name] = together[(package, name)]
    else:
        listed = {n: weights[n] for n in names if n in weights}
        placed = {name: shard for (_, name), shard in place_heavy({package: listed}, shards).items()}
    for name in names:
        if name not in placed:
            placed[name] = zlib.crc32((SALT + name).encode()) % shards
    return placed


def partition(names, shards, weights, package=DEFAULT_PACKAGE, joint=None):
    placed = assign(names, shards, weights, package, joint)
    return [[n for n in names if placed[n] == i] for i in range(shards)]


def check(names, parts):
    """Every test in exactly one shard, nothing else in any shard."""
    problems = []
    seen = {}
    for index, part in enumerate(parts, 1):
        for name in part:
            if name in seen:
                problems.append(f"{name} is in shard {seen[name]} and shard {index}")
            seen[name] = index
    for name in names:
        if name not in seen:
            problems.append(f"{name} is in no shard")
    for name in seen:
        if name not in set(names):
            problems.append(f"{name} is in shard {seen[name]} but not in the list")
    return problems


def pattern(part):
    if not part:
        raise SystemExit("test_shards: the shard has no tests")
    for name in part:
        if not NAME.fullmatch(name):
            raise SystemExit(f"test_shards: {name!r} is not safe in a -run pattern")
    return "^(" + "|".join(part) + ")$"


def weights_from_events(lines, package_suffix):
    """Seconds of each top-level test of the package, from `go test -json` events. A test that appears more than
    once (the events of several runs, concatenated) gets the mean of its runs: one run's timing varies by up to
    2x with the runner."""
    samples = {}
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        test = event.get("Test")
        if (event.get("Action") in OUTCOMES and test and "/" not in test
                and event.get("Package", "").endswith(package_suffix)):
            samples.setdefault(test, []).append(float(event.get("Elapsed", 0.0)))
    return {test: sum(values) / len(values) for test, values in samples.items()}


def render_weights(seconds, minimum=HEAVY_SECONDS, package=DEFAULT_PACKAGE):
    """Rows of one package; the package column is left out for internal/proxy."""
    rows = sorted(((s, n) for n, s in seconds.items() if s >= minimum), key=lambda r: (-r[0], r[1]))
    tail = "" if package == DEFAULT_PACKAGE else f"\t{package}"
    return "".join(f"{name}\t{s:.1f}{tail}\n" for s, name in rows)


WEIGHTS_HEAD = ("# Seconds of the slow top-level tests under -race, one per line: test, a tab, seconds, and for a package\n"
                "# other than internal/proxy a tab and its directory (internal/querycorpus).\n"
                "# Only a balancing hint for scripts/ci/test_shards.py: a test missing here, or listed with a stale time,\n"
                "# still runs in exactly one shard. Regenerate from a unit-results.json artifact:\n"
                "#   scripts/ci/test_shards.py weights --package internal/proxy --package internal/querycorpus \\\n"
                "#       --results unit-results.json > scripts/ci/test-shard-weights.tsv\n")


def verify(assigned, events):
    """Every assigned test has a pass, fail or skip event; no event comes from an unassigned test."""
    problems = []
    outcomes = {}
    for event in events:
        test = event.get("Test")
        if test and "/" not in test and event.get("Action") in OUTCOMES:
            outcomes.setdefault(test, set()).add(event["Action"])
    for name in sorted(assigned):
        if name not in outcomes:
            problems.append(f"{name} was assigned to a shard and has no outcome in the merged results")
    return problems


def read_events(path, package_suffix):
    events = []
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("Package", "").endswith(package_suffix):
            events.append(event)
    return events


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["assign", "regex", "check", "weights", "verify"])
    parser.add_argument("--list", help="`go test -list` output (default: stdin)")
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--shard", type=int, help="1-based shard number (regex)")
    parser.add_argument("--weights-file", default=str(Path(__file__).with_name("test-shard-weights.tsv")))
    parser.add_argument("--results", help="go test -json events (weights, verify)")
    parser.add_argument("--package", action="append", help=f"directory of the package in the module (default {DEFAULT_PACKAGE}); "
                        "repeat for weights; selects the weight rows of assign, regex and check")
    parser.add_argument("--joint", action="append", help="directory of a split package whose heavy tests are placed together "
                        "with this one's (repeat; name every split package)")
    parser.add_argument("--assigned", nargs="*", default=[], help="files of assigned test names (verify)")
    args = parser.parse_args(argv)
    packages = [p.strip("/") for p in args.package or [DEFAULT_PACKAGE]]
    package = packages[0]

    if args.command == "weights":
        if not args.results:
            parser.error("weights needs --results")
        lines = Path(args.results).read_text().splitlines()
        sys.stdout.write(WEIGHTS_HEAD + "".join(
            render_weights(weights_from_events(lines, "/" + p), package=p) for p in packages))
        return 0
    if args.command == "verify":
        if not args.results or not args.assigned:
            parser.error("verify needs --results and --assigned")
        assigned = set()
        for path in args.assigned:
            assigned.update(line.strip() for line in Path(path).read_text().splitlines() if line.strip())
        problems = verify(assigned, read_events(args.results, "/" + package))
        for problem in problems:
            print(f"test_shards: {problem}", file=sys.stderr)
        print(f"test_shards: {len(assigned)} assigned tests, {len(problems)} without an outcome")
        return 1 if problems else 0

    names = parse_list(Path(args.list).read_text() if args.list else sys.stdin.read())
    weights_path = Path(args.weights_file)
    weights = parse_weights(weights_path.read_text(), package) if weights_path.exists() else {}
    joint = None
    if args.joint:
        joint = {p.strip("/"): parse_weights(weights_path.read_text(), p.strip("/")) if weights_path.exists() else {}
                 for p in args.joint if p.strip("/") != package}
    parts = partition(names, args.shards, weights, package, joint)
    if args.command == "assign":
        for index, part in enumerate(parts, 1):
            for name in part:
                print(f"{index}\t{name}")
    elif args.command == "regex":
        if not args.shard or not 1 <= args.shard <= args.shards:
            parser.error("regex needs --shard in 1..--shards")
        # A shard can hold none of a small package's tests: an empty pattern tells the runner to skip that package.
        print(pattern(parts[args.shard - 1]) if parts[args.shard - 1] else "")
    else:
        problems = check(names, parts)
        for problem in problems:
            print(f"test_shards: {problem}", file=sys.stderr)
        sizes = ", ".join(f"{i}: {len(p)} tests ~{sum(weights.get(n, 0.0) for n in p):.0f}s heavy" for i, p in enumerate(parts, 1))
        print(f"test_shards: {len(names)} tests in {args.shards} shards ({sizes})")
        stale = sorted(n for n in weights if n not in set(names))
        if stale:
            print(f"test_shards: {len(stale)} weight entries name no test (harmless): {', '.join(stale[:5])}", file=sys.stderr)
        return 1 if problems else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
