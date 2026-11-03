#!/usr/bin/env python3
"""Merge Go coverage profiles that cover the same code into one.

Usage: merge_coverprofile.py <out> <profile>...

The profiles must share one mode (-race writes "atomic"). A block that several
profiles list (internal/proxy runs as several shards, each over the whole
package) gets the sum of their counts, which is what one run of all the tests
would have counted; a block listed once is copied. The result has one "mode:"
line and its blocks sorted by file and position, so `go tool cover` reads it
like the profile of a single run.
"""
import re
import sys

BLOCK = re.compile(r"^(?P<key>.+:\d+\.\d+,\d+\.\d+ \d+) (?P<count>\d+)$")


def merge(texts):
    mode = None
    counts = {}
    for text in texts:
        lines = text.splitlines()
        if not lines or not lines[0].startswith("mode: "):
            raise SystemExit("merge_coverprofile: a profile without a mode line")
        if mode is None:
            mode = lines[0]
        elif lines[0] != mode:
            raise SystemExit(f"merge_coverprofile: modes differ: {mode!r} and {lines[0]!r}")
        for line in lines[1:]:
            if not line.strip():
                continue
            m = BLOCK.match(line)
            if not m:
                raise SystemExit(f"merge_coverprofile: not a coverage block: {line!r}")
            if mode == "mode: set":
                counts[m["key"]] = max(counts.get(m["key"], 0), int(m["count"]))
            else:
                counts[m["key"]] = counts.get(m["key"], 0) + int(m["count"])
    if mode is None:
        raise SystemExit("merge_coverprofile: no profiles")

    def position(key):
        file, _, rest = key.rpartition(":")
        span = re.findall(r"\d+", rest)
        return (file, *map(int, span))

    return mode + "\n" + "".join(f"{key} {counts[key]}\n" for key in sorted(counts, key=position))


def main(argv):
    if len(argv) < 3:
        raise SystemExit(__doc__)
    texts = [open(path).read() for path in argv[2:]]
    with open(argv[1], "w") as out:
        out.write(merge(texts))


if __name__ == "__main__":
    main(sys.argv)
