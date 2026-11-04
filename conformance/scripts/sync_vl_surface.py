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
       sync_vl_surface.py --offline conformance/registry/generated/vl/logsql-surface.json
         (recompute only what the proxy emits; the conformance gate checks it is current)
"""
import argparse, json, os, re, subprocess, sys
from registry_io import read_text, write_text

# Every minor release from v1.40.0 to the latest, so `since` is the release a
# construct first appeared in, not the next sampled one.
DEFAULT_TAGS = [f"v1.{minor}.0" for minor in range(40, 54)]
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


# Where the proxy builds LogsQL. internal/logsql only defines the AST and its parser (every construct has a
# String method there), so it proves nothing about emission; the AST types the translator and the proxy
# construct (logsql.PipeUnion{...}) count instead.
EMITTERS = ("internal/translator", "internal/proxy")
AST = "internal/logsql/ast.go"
# The translator's name tables map a LogQL function to the LogsQL stats function it emits.
TABLES = "internal/translator"
# A literal written as a regular expression matches LogQL the proxy reads; it is not LogsQL it writes.
REGEX_LITERAL = re.compile(r'\\[sbdw|]|\(\?[:i]')


def go_literals(text):
    """The interpreted and raw string literals of Go source, in order. Comments and rune literals are
    skipped, so a quote inside them (a '"' rune, a comment) never shifts the pairing of later literals."""
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            i = text.find("\n", i)
            i = n if i < 0 else i
        elif text.startswith("/*", i):
            i = text.find("*/", i + 2)
            i = n if i < 0 else i + 2
        elif c == "'":
            j = i + 1
            while j < n and text[j] != "'":
                j += 2 if text[j] == "\\" else 1
            i = j + 1
        elif c == '"':
            j, buf = i + 1, []
            while j < n and text[j] != '"' and text[j] != "\n":
                if text[j] == "\\" and j + 1 < n:
                    buf.append(text[j:j + 2])
                    j += 2
                else:
                    buf.append(text[j])
                    j += 1
            out.append("".join(buf))
            i = j + 1
        elif c == "`":
            j = text.find("`", i + 1)
            j = n if j < 0 else j
            out.append(text[i + 1:j])
            i = j + 1
        else:
            i += 1
    return out


def token_re(kind, name):
    if kind == "pipe":
        return re.compile(r'\|\s*' + re.escape(name) + r'\b')
    # A call on a LogQL selector (`rate({...}[5m])`) is LogQL the proxy reads or quotes.
    return re.compile(r'(?:^|[^\w.])' + re.escape(name) + r'\s*\((?!\{)')


def ast_types(kind, name, ast_text):
    """The logsql AST types whose String method writes this construct."""
    token, types = token_re(kind, name), set()
    # Each top-level func runs to the next one; a String method's literals are the text it writes.
    for chunk in re.split(r'^(?=func )', ast_text, flags=re.M):
        match = re.match(r'func \((?:\w+ )?\*?(\w+)\) String\(\) string \{', chunk)
        if not match:
            continue
        literals = go_literals(chunk)
        if any(token.search(v) for v in literals) or (kind == "stats" and name + "(" in literals):
            types.add(match.group(1))
    return types


def proxy_uses(kind, name, root="."):
    """The files of the proxy that emit this construct in the LogsQL they build.

    A file counts when one of its Go string literals holds the construct (a pipe as `| name`, a stats
    function as `name(`), when it builds a logsql AST type that writes it, or, for a stats function, when
    its bare name is a map value, an assigned or a returned value in the translator, which picks stats functions from name tables
    and builds the call (translator.go metricFuncs, outerAggregationStatsFn). Literals written as regular
    expressions are LogQL the proxy matches, not LogsQL it writes, and are skipped. LogQL and LogsQL share
    some spellings (`rate(`, `sum(`), so this source scan can still over-count; it is not a translation of
    every construct.
    """
    token = token_re(kind, name)
    ast_path = os.path.join(root, AST)
    types = ast_types(kind, name, read_text(ast_path, errors="ignore")) if os.path.exists(ast_path) else set()
    built = re.compile(r'\blogsql\.(' + "|".join(sorted(types)) + r')\s*[{(]') if types else None
    hits = []
    for directory in EMITTERS:
        for current, _, files in os.walk(os.path.join(root, directory)):
            for filename in files:
                if not filename.endswith(".go") or filename.endswith("_test.go"):
                    continue
                path = os.path.join(current, filename)
                text = read_text(path, errors="ignore")
                # A bare `name(` literal is a prefix test on LogQL unless the call is built from it (`"name(" +`).
                literals = [v for v in go_literals(text) if not REGEX_LITERAL.search(v)
                            and (v != name + "(" or re.search(r'"' + re.escape(v) + r'"\s*\+', text))]
                if (any(token.search(v) for v in literals)
                        or (kind == "stats" and directory == TABLES and re.search(r'(?:[^=!<>]=|:|\breturn)\s*"' + re.escape(name) + '"', text))
                        or (built is not None and built.search(text))):
                    hits.append(os.path.relpath(path, root))
    return sorted(set(hits))


def refresh_usage(path):
    """Recompute used_by_proxy and used_in of an existing inventory from the source tree, without the network
    (the release tags and `since` stay as the last full run wrote them)."""
    with open(path, encoding="utf-8") as handle:
        doc = json.load(handle)
    for entry in doc["constructs"]:
        used = proxy_uses(entry["kind"], entry["name"])
        entry["used_by_proxy"], entry["used_in"] = bool(used), used
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(doc, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return doc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", default=",".join(DEFAULT_TAGS))
    parser.add_argument("--offline", metavar="PATH",
                        help="recompute only which constructs the proxy emits in the inventory at PATH (no network)")
    args = parser.parse_args()
    if args.offline:
        doc = refresh_usage(args.offline)
        print(f"{args.offline}: {sum(1 for e in doc['constructs'] if e['used_by_proxy'])} of "
              f"{len(doc['constructs'])} constructs emitted by the proxy")
        return 0
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
