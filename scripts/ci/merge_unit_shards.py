#!/usr/bin/env python3
"""Fan in the artifacts of the parallel `test-shard` jobs into what one `test` run produced.

Usage: merge_unit_shards.py --shards-dir DIR --out-dir DIR [--require-complete]

Each shard uploaded a directory DIR/unit-shard-<shard>/ with
  unit-results.json    the outcome events of its tests under ./internal/...
  coverage.out         its coverage profile
  coverage-cache.out   (shard "rest") the internal/cache profile for the coverage guard
  assigned.txt, all-tests.txt   (internal/proxy shards) its tests and the full `go test -list`

Writes to the out directory
  unit-results.json    the events of every shard, which the conformance gate reads
  coverage.out         the profiles merged (a block several shards cover gets the sum)
  coverage-cache.out   copied from the shard that ran it

and, with --require-complete (every shard job succeeded), fails unless
  * every expected shard uploaded its files;
  * the internal/proxy shards agree on the list of tests, no test is in two
    shards, and together they hold the whole list;
  * every assigned test has an outcome in the merged events.
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

PROXY_SHARDS = ("1", "2", "3", "4")
REST = "rest"


def merge(shards_dir, out_dir, shard_ids, require_complete):
    shards_dir, out_dir = Path(shards_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    problems = []
    events, profiles, assigned, lists = [], [], {}, {}
    cache_profile = None
    for shard in shard_ids:
        d = shards_dir / f"unit-shard-{shard}"
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
                    store[shard] = [line.strip() for line in f.read_text().splitlines() if line.strip()]
                elif require_complete:
                    problems.append(f"shard {shard}: no {name}")

    (out_dir / "unit-results.json").write_text("".join(line + "\n" for line in events))
    if profiles:
        (out_dir / "coverage.out").write_text(merge_coverprofile.merge(profiles))
    if cache_profile is not None:
        (out_dir / "coverage-cache.out").write_text(cache_profile)
    elif require_complete:
        problems.append("no coverage-cache.out from the rest shard")

    if require_complete:
        full = next(iter(lists.values()), [])
        for shard, names in lists.items():
            if names != full:
                problems.append(f"shard {shard} listed different tests than the others")
        problems.extend(test_shards.check(full, [assigned[s] for s in sorted(assigned)]))
        if not full:
            problems.append("no list of internal/proxy tests")
        parsed = [json.loads(line) for line in events]
        proxy_events = [e for e in parsed if e.get("Package", "").endswith("/internal/proxy")]
        flat = {n for names in assigned.values() for n in names}
        problems.extend(test_shards.verify(flat, proxy_events))
    return problems, len(events)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--shards-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--shards", default=" ".join((REST,) + PROXY_SHARDS), help="space-separated shard ids")
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args(argv)
    problems, count = merge(args.shards_dir, args.out_dir, args.shards.split(), args.require_complete)
    for problem in problems:
        print(f"merge_unit_shards: {problem}", file=sys.stderr)
    print(f"merge_unit_shards: {count} events merged from {len(args.shards.split())} shards, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
