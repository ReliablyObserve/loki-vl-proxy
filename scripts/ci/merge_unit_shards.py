#!/usr/bin/env python3
"""Fan in the artifacts of the parallel `test-shard` jobs into what one `test` run produced.

Usage: merge_unit_shards.py --shards-dir DIR --out-dir DIR [--require-complete] [--packages "P ..."] [--no-cache-profile]

Each shard uploaded a directory DIR/<prefix><shard>/ (prefix unit-shard-) with
  unit-results.json    the outcome events of its tests under ./internal/...
  coverage.out         its coverage profile
  coverage-cache.out   (shard "rest") the internal/cache profile for the coverage guard (a caller without
                       that guard passes --no-cache-profile)
  assigned.txt, all-tests.txt   (numbered shards) its tests and the full `go test -list` of each split
                       package, one "<package directory>:<test>" line per test

Writes to the out directory
  unit-results.json    the events of every shard, which the conformance gate reads
  coverage.out         the profiles merged (a block several shards cover gets the sum)
  coverage-cache.out   copied from the shard that ran it

and, with --require-complete (every shard job succeeded), fails unless
  * every expected shard uploaded its files;
  * for each split package (--packages: internal/proxy and internal/querycorpus), the shards
    agree on the list of its tests, no test is in two shards, and together they hold the
    whole list;
  * every assigned test has an outcome in the merged events of its package.
A test that was added is in the list and so in exactly one shard; one that is
somehow in none, or ran in none, fails the job instead of passing unseen.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import merge_coverprofile  # noqa: E402
import test_shards  # noqa: E402

NUMBERED_SHARDS = tuple(str(i) for i in range(1, 13))  # keep equal to the test-shard matrix of ci.yaml
# The packages whose tests the numbered shards split; keep equal to UNIT_SHARD_PACKAGES in go_test_unit.sh.
PACKAGES = ("internal/proxy", "internal/querycorpus")
REST = "rest"


def read_names(path):
    """{package directory: [test]} from the "<package directory>:<test>" lines of a shard file."""
    by_package = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            package, _, test = line.partition(":")
            by_package.setdefault(package, []).append(test)
    return by_package


def merge(shards_dir, out_dir, shard_ids, require_complete, packages=PACKAGES, cache_profile_required=True, prefix="unit-shard-"):
    shards_dir, out_dir = Path(shards_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    problems = []
    events, profiles, assigned, lists = [], [], {}, {}
    cache_profile = None
    for shard in shard_ids:
        d = shards_dir / f"{prefix}{shard}"
        results, coverage = d / "unit-results.json", d / "coverage.out"
        if results.is_file():
            events.extend(line for line in results.read_text().splitlines() if line.strip())
        elif require_complete:
            problems.append(f"shard {shard}: no unit-results.json")
        if coverage.is_file() and coverage.stat().st_size:
            profiles.append(coverage.read_text())
        elif require_complete:
            problems.append(f"shard {shard}: no coverage.out")
        if (d / "coverage-cache.out").is_file():
            cache_profile = (d / "coverage-cache.out").read_text()
        if shard != REST:
            for name, store in (("assigned.txt", assigned), ("all-tests.txt", lists)):
                f = d / name
                if f.is_file():
                    store[shard] = read_names(f)
                elif require_complete:
                    problems.append(f"shard {shard}: no {name}")

    (out_dir / "unit-results.json").write_text("".join(line + "\n" for line in events))
    if profiles:
        (out_dir / "coverage.out").write_text(merge_coverprofile.merge(profiles))
    if cache_profile is not None:
        (out_dir / "coverage-cache.out").write_text(cache_profile)
    elif require_complete and cache_profile_required:
        problems.append("no coverage-cache.out from the rest shard")

    if require_complete:
        parsed = [json.loads(line) for line in events]
        for package in packages:
            problems.extend(f"{package}: {p}" for p in check_package(package, assigned, lists, parsed))
    return problems, len(events)


def check_package(package, assigned, lists, events):
    """The problems of one split package: lists that differ, a test in two shards or none, a test without an outcome."""
    problems = []
    listed = {shard: names.get(package, []) for shard, names in lists.items()}
    full = next((names for names in listed.values() if names), [])
    for shard, names in listed.items():
        if names != full:
            problems.append(f"shard {shard} listed different tests than the others")
    parts = [assigned[s].get(package, []) for s in sorted(assigned)]
    problems.extend(test_shards.check(full, parts))
    if not full:
        problems.append("no list of tests")
    suffix = "/" + package
    ours = [e for e in events if e.get("Package", "").endswith(suffix)]
    problems.extend(test_shards.verify({n for part in parts for n in part}, ours))
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--shards-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--shards", default=" ".join((REST,) + NUMBERED_SHARDS), help="space-separated shard ids")
    parser.add_argument("--packages", default=" ".join(PACKAGES), help="space-separated directories of the split packages")
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--no-cache-profile", action="store_true", help="the shards ran no internal/cache coverage guard")
    parser.add_argument("--dir-prefix", default="unit-shard-", help="the artifact directories are <prefix><shard>")
    args = parser.parse_args(argv)
    problems, count = merge(args.shards_dir, args.out_dir, args.shards.split(), args.require_complete, args.packages.split(),
                            not args.no_cache_profile, args.dir_prefix)
    for problem in problems:
        print(f"merge_unit_shards: {problem}", file=sys.stderr)
    print(f"merge_unit_shards: {count} events merged from {len(args.shards.split())} shards, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
