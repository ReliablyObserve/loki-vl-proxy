#!/usr/bin/env python3
"""
Check internal/logsql AST coverage against VictoriaLogs upstream.

Fetches the lib/logstorage file listing from GitHub and diffs against
docs/vl-ast-coverage.json to identify new upstream constructs not yet
implemented in the proxy.

Usage:
    python3 scripts/check-vl-ast-coverage.py [--token GITHUB_TOKEN]

Exit codes:
    0 — no gaps found
    1 — gaps found (new upstream constructs not in our registry)
    2 — API error (rate limit, network failure)
"""
import json
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path

VL_API_URL = (
    "https://api.github.com/repos/VictoriaMetrics/VictoriaMetrics"
    "/contents/lib/logstorage"
)
REGISTRY_PATH = Path(__file__).parent.parent / "docs" / "vl-ast-coverage.json"

CATEGORY_PATTERNS = {
    "pipes":   re.compile(r"^pipe_(.+)\.go$"),
    "stats":   re.compile(r"^stats_(.+)\.go$"),
    "filters": re.compile(r"^filter_(.+)\.go$"),
}

# Upstream file stems we intentionally ignore (internals, not user-facing constructs).
IGNORED = {
    "pipes":   {"local", "topk"},          # pipe_*_local, pipe_sort_topk are internal variants
    "stats":   set(),
    "filters": {"generic", "noop", "stream_id"},  # internal/structural nodes
}


def fetch_file_list(token: str | None) -> list[str]:
    req = urllib.request.Request(VL_API_URL)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"ERROR: GitHub API returned {e.code}: {e.reason}", file=sys.stderr)
        if e.code == 403:
            print("Tip: set GITHUB_TOKEN env var to avoid rate limiting", file=sys.stderr)
        sys.exit(2)
    except urllib.error.URLError as e:
        print(f"ERROR: Network failure: {e.reason}", file=sys.stderr)
        sys.exit(2)
    return [entry["name"] for entry in data if entry["type"] == "file"]


def parse_upstream(filenames: list[str]) -> dict[str, set[str]]:
    result = {"pipes": set(), "stats": set(), "filters": set()}
    for name in filenames:
        if name.endswith("_test.go"):
            continue
        for category, pattern in CATEGORY_PATTERNS.items():
            m = pattern.match(name)
            if m:
                stem = m.group(1)
                # Skip internal variants (e.g. pipe_field_values_local.go → local already stripped by pattern)
                if stem not in IGNORED[category]:
                    result[category].add(stem)
    return result


def load_registry() -> dict[str, set[str]]:
    with open(REGISTRY_PATH) as f:
        data = json.load(f)
    return {
        "pipes":   set(data.get("pipes", [])),
        "stats":   set(data.get("stats", [])),
        "filters": set(data.get("filters", [])),
    }


def main() -> None:
    import argparse
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    args = parser.parse_args()

    print("Fetching VictoriaLogs logstorage file listing...")
    filenames = fetch_file_list(args.token)
    upstream = parse_upstream(filenames)
    registry = load_registry()

    gaps: dict[str, list[str]] = {}
    for category in ("pipes", "stats", "filters"):
        new_upstream = sorted(upstream[category] - registry[category])
        if new_upstream:
            gaps[category] = new_upstream

    if not gaps:
        print("✅  No gaps found — internal/logsql covers all known upstream constructs.")
        return

    print("\n❌  New upstream constructs not yet in internal/logsql:\n")
    total = 0
    for category, items in gaps.items():
        print(f"  {category} ({len(items)} new):")
        for item in items:
            print(f"    - {item}")
        total += len(items)
    print(f"\nTotal: {total} new upstream constructs.")
    print(
        "\nTo resolve: add AST node(s) to internal/logsql/ast.go, implement"
        " String(), update docs/vl-ast-coverage.json, and add tests."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
