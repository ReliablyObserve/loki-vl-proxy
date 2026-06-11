#!/usr/bin/env python3
"""
Check internal/logsql AST coverage against VictoriaLogs upstream.

Fetches the lib/logstorage file listing from GitHub and diffs against
scripts/vl-ast-coverage.json to identify new upstream constructs not yet
implemented in the proxy.

Usage:
    python3 scripts/check-vl-ast-coverage.py [--token GITHUB_TOKEN]

Exit codes:
    0 — no gaps found
    1 — gaps found (new upstream constructs not in our registry)
    2 — API error (rate limit, network failure)
"""
import http.client
import json
import re
import sys
from pathlib import Path

# Hardcoded HTTPS endpoint — no user-controlled URL component.
VL_API_HOST = "api.github.com"
VL_API_PATH = "/repos/VictoriaMetrics/VictoriaLogs/contents/lib/logstorage"
REGISTRY_PATH = Path(__file__).parent / "vl-ast-coverage.json"

CATEGORY_PATTERNS = {
    "pipes":   re.compile(r"^pipe_(.+)\.go$"),
    "stats":   re.compile(r"^stats_(.+)\.go$"),
    "filters": re.compile(r"^filter_(.+)\.go$"),
}

# Upstream file stems we intentionally ignore (internals, not user-facing constructs).
IGNORED = {
    "pipes": {
        "local",             # pipe_local.go (generic internal base)
        "sort_topk",         # pipe_sort_topk.go (internal sort variant)
        "block_stats",       # pipe_block_stats.go (internal block-level stats)
        "blocks_count",      # pipe_blocks_count.go (internal block counting)
        "field_values_local",  # pipe_field_values_local.go (local shard variant)
        "query_stats_local",   # pipe_query_stats_local.go (local shard variant)
        "uniq_local",          # pipe_uniq_local.go (local shard variant)
        "pack",                # pipe_pack.go (internal base for pack_json/pack_logfmt)
        "unpack",              # pipe_unpack.go (internal base for unpack_json/unpack_logfmt/unpack_syslog)
    },
    "stats":   {"stdvar"},   # stats_stdvar.go: stddev²; not in VL upstream as separate func
    "filters": {"generic", "noop", "stream_id"},  # internal/structural nodes
}


def fetch_file_list(token: str | None) -> list[str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "loki-vl-proxy-coverage-check",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        # nosemgrep: python.lang.security.audit.httpsconnection-detected.httpsconnection-detected
        # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        # Semgrep flags HTTPSConnection because TLS verification semantics
        # changed across Python 3.x versions. This script only runs in CI
        # against api.github.com on the pinned 3.x container (Python ≥ 3.4.3
        # has TLS verification on by default — older than any Python we ship).
        # Connecting to a fixed, trusted host with default verification → safe.
        conn = http.client.HTTPSConnection(VL_API_HOST, timeout=15)
        conn.request("GET", VL_API_PATH, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            print(f"ERROR: GitHub API returned {resp.status}: {resp.reason}", file=sys.stderr)
            if resp.status == 403:
                print("Tip: set GITHUB_TOKEN env var to avoid rate limiting", file=sys.stderr)
            sys.exit(2)
        data = json.loads(body)
    except OSError as e:
        print(f"ERROR: Network failure: {e}", file=sys.stderr)
        sys.exit(2)
    finally:
        conn.close()
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
        " String(), update scripts/vl-ast-coverage.json, and add tests."
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
