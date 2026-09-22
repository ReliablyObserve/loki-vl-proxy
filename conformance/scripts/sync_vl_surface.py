#!/usr/bin/env python3
"""Inventory the VictoriaLogs LogsQL surface per release, and what the proxy uses.

VictoriaLogs implements one file per pipe and per stats function
(lib/logstorage/pipe_*.go, stats_*.go), so the tree of a release tag is an exact
inventory. This script walks the supported tags, records the first release each
pipe and stats function appeared in, checks whether the proxy emits it, and
seeds one registry file per construct under conformance/registry/vl/logsql/.

That gives the answer the registry needs for every LogQL construct: which
VictoriaLogs function can serve it, from which version, and whether the proxy
uses it natively today.

Usage: sync_vl_surface.py [--tags v1.44.0,v1.45.0,...]
"""
import argparse, json, os, re, subprocess, sys
from registry_io import read_text, write_text

DEFAULT_TAGS = ["v1.40.0", "v1.44.0", "v1.45.0", "v1.49.0", "v1.50.0", "v1.51.0", "v1.52.0"]
TREE = ("gh api 'repos/VictoriaMetrics/VictoriaLogs/git/trees/{tag}?recursive=1' "
        "-q '.tree[].path'")
FILE_RE = re.compile(r'^lib/logstorage/(pipe|stats)_([a-z_0-9]+)\.go$')
SKIP = {"pipe": {"utils"}, "stats": {"utils"}}


def tree(tag):
    result = subprocess.run(["/bin/sh", "-c", "env -u GITHUB_TOKEN " + TREE.format(tag=tag)],
                            capture_output=True, text=True)
    return [line for line in result.stdout.splitlines()]


def constructs(tag):
    found = {}
    for path in tree(tag):
        if "_test" in path:
            continue
        match = FILE_RE.match(path)
        if not match:
            continue
        kind, name = match.group(1), match.group(2)
        if name in SKIP.get(kind, ()):
            continue
        found[f"{kind}:{name}"] = path
    return found


def proxy_uses(kind, name):
    """Does the proxy emit this construct in the LogsQL it builds?

    Only string literals count: a Go call like len(x) or sum(x) is not LogsQL.
    Pipes are matched as `| name`, stats functions as `name(`, both inside a
    quoted string in the translator, the LogsQL builder or the proxy.
    """
    if kind == "pipe":
        token = re.compile(r'\|\s*' + re.escape(name) + r'\b')
    else:
        token = re.compile(r'\b' + re.escape(name) + r'\s*\(')
    literal = re.compile(r'"((?:[^"\\]|\\.)*)"')
    hits = []
    for directory in ("internal/translator", "internal/proxy", "internal/logsql"):
        for current, _, files in os.walk(directory):
            for filename in files:
                if not filename.endswith(".go") or filename.endswith("_test.go"):
                    continue
                path = os.path.join(current, filename)
                text = read_text(path, errors="ignore")
                if any(token.search(value) for value in literal.findall(text)):
                    hits.append(path)
    return sorted(set(hits))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", default=",".join(DEFAULT_TAGS))
    args = parser.parse_args()
    tags = args.tags.split(",")
    first_seen, per_tag = {}, {}
    for tag in tags:
        found = constructs(tag)
        if not found:
            print(f"warning: no files listed for {tag}, skipping", file=sys.stderr)
            continue
        per_tag[tag] = sorted(found)
        for key in found:
            first_seen.setdefault(key, tag)
    entries = []
    for key, since in sorted(first_seen.items()):
        kind, name = key.split(":", 1)
        used = proxy_uses(kind, name)
        entries.append({"kind": kind, "name": name, "since": since,
                        "present_in_latest": key in set(per_tag[tags[-1]]),
                        "used_by_proxy": bool(used), "used_in": used})
    os.makedirs("conformance/registry/generated/vl", exist_ok=True)
    with open("conformance/registry/generated/vl/logsql-surface.json", "w") as handle:
        json.dump({"tags": tags, "constructs": entries}, handle, indent=2, sort_keys=True)
        handle.write("\n")

    os.makedirs("conformance/registry/vl/logsql", exist_ok=True)
    written = 0
    for entry in entries:
        identifier = f"{entry['kind']}-{entry['name'].replace('_', '-')}"
        target = f"conformance/registry/vl/logsql/{identifier}.yaml"
        if os.path.exists(target):
            continue
        used_in = "\n".join(f"    - {path}" for path in entry["used_in"][:6]) or "    []"
        write_text(target,
            f"id: {identifier}\nkind: vl_{entry['kind']}\nname: {entry['name']}\n"
            f"since: {entry['since']}\n"
            f"generated:\n  used_by_proxy: {str(entry['used_by_proxy']).lower()}\n"
            f"  used_in:\n{used_in}\n\n"
            "what_it_does: |\n  TODO: what the construct computes in VictoriaLogs.\n"
            "serves_logql: []   # LogQL construct ids this can serve natively\n"
            "proxy_usage: |\n  TODO: native use, the transformation used instead, or why it is\n"
            "  not applicable to Loki semantics.\n"
            "fallback: |\n  TODO: what the proxy does on a backend older than `since`.\n"
            "cases: []\n")
        written += 1
    used = sum(1 for entry in entries if entry["used_by_proxy"])
    by_since = {}
    for entry in entries:
        by_since.setdefault(entry["since"], [0, 0])
        by_since[entry["since"]][0] += 1
        by_since[entry["since"]][1] += 1 if entry["used_by_proxy"] else 0
    print(f"VictoriaLogs LogsQL surface: {len(entries)} constructs across {len(per_tag)} releases, "
          f"{used} used by the proxy ({written} files written)")
    for tag in tags:
        if tag in by_since:
            total, hit = by_since[tag]
            print(f"  first seen in {tag}: {total:3} constructs, {hit} used by the proxy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
