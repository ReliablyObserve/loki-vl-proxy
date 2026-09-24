#!/usr/bin/env python3
"""Pick the A/B shapes a change needs, from the files it touches.

The mapping is not maintained by hand: every shape in shapes.json declares the
conformance registry items it measures (`covers`), and the registry items name
the code that implements them (implementation sites, `where`, `seen_in`, the
handler of an endpoint in registry/generated/proxy/implementation.json). A
changed source file selects every shape whose covered items point at it. So a
new registry item, a moved implementation or a new shape changes the selection
with no edit here, and `--check` (run by the conformance gate) fails when a
shape cannot be reached from any code.

Rules, per changed path:
  internal/, pkg/ (non-test Go)  shapes whose items name the file, plus the
                                 control smoke subset
  cmd/, go.mod, go.sum           the whole control set (flags, wiring and
                                 the module graph reach every query)
  bench/ab/shapes.json           the shapes the change adds or edits, plus smoke
  A/B harness, stack and data    the control smoke subset (bench/ab/*.py,
  (see HARNESS)                  docker-compose files, Loki config, generator)
  everything else                nothing: docs, CI, Helm chart, Dockerfile,
                                 tests, registry text and website do not
                                 change the host-built binary

Endpoint items (loki_api_v1_*) sit under every shape of an endpoint and their
handler file is touched by most changes, so a handler change selects that
endpoint's smoke shapes, not every shape behind the endpoint.

  selection.py --base origin/main              files from git diff base...HEAD
  selection.py --files internal/proxy/x.go ... explicit file list
  selection.py --check                         gate: every shape is reachable

Prints JSON: {"run": bool, "sets": {set: [shape, ...]}, "why": {...}, ...}.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SHAPES = "bench/ab/shapes.json"
REGISTRY = "conformance/registry"
IMPLEMENTATION = f"{REGISTRY}/generated/proxy/implementation.json"
CONTROL = "control"
GENERIC = re.compile(r"^(loki_api_v1_|api_prom_)")
SOURCE = re.compile(r"\b((?:internal|pkg|cmd)/[A-Za-z0-9_./-]+?\.go)\b")
# Registry keys whose values name other items this item stands on.
LINK_KEYS = ("proves", "cases", "covers", "endpoints", "behaviours", "translations", "logql")
LINK_BLOCK = re.compile(r"^(\s*)(" + "|".join(LINK_KEYS) + r"):[ \t]*(.*)$")
TOKEN = re.compile(r"[A-Za-z0-9_./:-]+")

# The A/B builds are host `go build`s, so the Dockerfile is not what they
# measure; the toolchain and module graph are.
RUNTIME_ALL = ("go.mod", "go.sum")
# The measurement itself: a change here must prove the pipeline still runs.
HARNESS = (
    "bench/ab/perf_matrix.py", "bench/ab/report.py", "bench/ab/stack.py", "bench/ab/selection.py",
    "bench/ab/pr_smoke.py", "bench/ab/comment.py", "bench/ab/docker-compose.ab.yml",
    "test/e2e-compat/docker-compose.yml", "test/e2e-compat/loki-local-config.yaml",
    "test/e2e-compat/log-generator.py", ".github/workflows/perf-ab.yaml",
)


def read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        return f.read()


def load_shapes(text=None):
    return json.loads(text if text is not None else read(SHAPES))


def registry_items():
    """id -> (source files named by the item, ids the item links to)."""
    items = {}
    for path in glob.glob(os.path.join(ROOT, REGISTRY, "**", "*.yaml"), recursive=True):
        rel = os.path.relpath(path, ROOT)
        if any(f"/{d}/" in rel for d in ("generated", "state", "schema")):
            continue
        text = read(rel)
        m = re.search(r"^id:\s*(\S+)", text, re.M)
        if not m:
            continue
        # Registry prose also cites upstream sources (Loki's pkg/...); only
        # files of this repository select anything.
        files = {f for f in SOURCE.findall(text) if os.path.exists(os.path.join(ROOT, f))}
        items[m.group(1).strip("'\"")] = (files, link_tokens(text))
    if os.path.exists(os.path.join(ROOT, IMPLEMENTATION)):
        for entry in json.loads(read(IMPLEMENTATION))["endpoints"]:
            files = {w.split(":")[0] for w in entry.get("where") or []}
            if entry["id"] in items:
                items[entry["id"]][0].update(files)
            else:
                items[entry["id"]] = (files, set())
    return items


def link_tokens(text):
    """Identifiers listed under the link keys, inline ([a, b]) or as a block (- a)."""
    out, indent = set(), None
    for line in text.splitlines():
        m = LINK_BLOCK.match(line)
        if m:
            indent = len(m.group(1))
            out.update(TOKEN.findall(m.group(3).split("#")[0]))
            continue
        if indent is None:
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if len(line) - len(line.lstrip()) <= indent and not stripped.startswith("-"):
            indent = None
            continue
        out.update(TOKEN.findall(stripped.lstrip("-").split("#")[0]))
    return out


def shape_files(shape, items):
    """Source files behind a shape: its covered items, and the items those link to (one hop)."""
    files, generic = set(), set()
    for item in shape.get("covers") or []:
        if GENERIC.match(item):
            generic.update(items.get(item, (set(), set()))[0])
            continue
        own, links = items.get(item, (set(), set()))
        files |= own
        for linked in links:
            if linked in items and not GENERIC.match(linked):
                files |= items[linked][0]
    return files, generic


def shapes_at(ref):
    """The shape sets at a git ref; none when the ref predates shapes.json or it does not parse."""
    try:
        text = subprocess.run(["git", "show", f"{ref}:{SHAPES}"], cwd=ROOT, capture_output=True, text=True,
                              check=True).stdout
        return load_shapes(text)["sets"]
    except (subprocess.CalledProcessError, ValueError):
        return {}


def changed_shapes(base, head=None):
    """(set, shape) pairs shapes.json adds or edits between base and head (default: the working tree)."""
    before = {(s, sh["name"]): sh for s, v in shapes_at(base).items() for sh in v["shapes"]}
    after = shapes_at(head) if head else load_shapes()["sets"]
    return [(s, sh["name"]) for s, v in after.items() for sh in v["shapes"] if before.get((s, sh["name"])) != sh]


def classify(path):
    if path in HARNESS or path.startswith("bench/ab/") and path.endswith(".py") and "/tests/" not in path:
        return "harness"
    if path == SHAPES:
        return "shapes"
    if path.endswith("_test.go"):
        return "ignored"
    if path.startswith("cmd/") and path.endswith(".go") or path in RUNTIME_ALL:
        return "runtime"
    if path.startswith(("internal/", "pkg/")) and path.endswith(".go"):
        return "code"
    return "ignored"


def select(files, base=None, spec=None, items=None, head=None):
    spec = spec or load_shapes()
    items = items if items is not None else registry_items()
    chosen = {}  # (set, shape) -> reasons

    def add(set_name, shape_name, why):
        chosen.setdefault((set_name, shape_name), [])
        if why not in chosen[(set_name, shape_name)]:
            chosen[(set_name, shape_name)].append(why)

    kinds = {f: classify(f) for f in files}
    relevant = [f for f, k in kinds.items() if k != "ignored"]
    control = spec["sets"].get(CONTROL, {"shapes": []})["shapes"]
    smoke = [s for s in control if s.get("smoke")]
    code = {f for f, k in kinds.items() if k == "code"}
    for set_name, shape_set in spec["sets"].items():
        for shape in shape_set["shapes"]:
            specific, generic = shape_files(shape, items)
            for f in sorted(code & specific):
                add(set_name, shape["name"], f)
            if set_name == CONTROL and shape.get("smoke"):
                for f in sorted(code & generic):
                    add(set_name, shape["name"], f"{f} (endpoint handler)")
    for f, kind in kinds.items():
        if kind == "runtime":
            for shape in control:
                add(CONTROL, shape["name"], f)
        elif kind == "shapes" and base:
            for set_name, shape_name in changed_shapes(base, head):
                add(set_name, shape_name, "shape added or edited")
    if relevant:
        for shape in smoke:
            add(CONTROL, shape["name"], "control smoke")
    sets = {}
    for set_name, shape_set in spec["sets"].items():
        names = [s["name"] for s in shape_set["shapes"] if (set_name, s["name"]) in chosen]
        if names:
            sets[set_name] = names
    return {
        "run": bool(sets),
        "changed": len(files),
        "relevant": relevant,
        "ignored": [f for f, k in kinds.items() if k == "ignored"],
        "sets": sets,
        "why": {f"{s}/{n}": r for (s, n), r in chosen.items()},
    }


def check(spec=None, items=None):
    spec = spec or load_shapes()
    items = items if items is not None else registry_items()
    problems = []
    control = spec["sets"].get(CONTROL)
    if not control or not any(s.get("smoke") for s in control["shapes"]):
        problems.append(f"{SHAPES}: the '{CONTROL}' set must mark at least one shape \"smoke\": true")
    for set_name, shape_set in spec["sets"].items():
        for shape in shape_set["shapes"]:
            if shape.get("smoke") and set_name != CONTROL:
                problems.append(f"{SHAPES}: {set_name}/{shape['name']} is marked smoke outside '{CONTROL}'")
            specific, _ = shape_files(shape, items)
            if set_name != CONTROL and not specific:
                problems.append(f"{SHAPES}: {set_name}/{shape['name']} covers no registry item that names code, "
                                "so no change can select it; cover the behaviour, translation or case it measures")
    return problems


def changed_files(base, head="HEAD"):
    out = subprocess.run(["git", "diff", "--name-only", f"{base}...{head}"], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout
    return [line for line in out.splitlines() if line]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="", help="git ref; the change is base...HEAD")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--files", nargs="*", help="explicit changed files (instead of git diff)")
    ap.add_argument("--check", action="store_true", help="gate: every shape is reachable from code")
    ap.add_argument("--out", default="", help="also write the JSON here")
    ap.add_argument("--root", default="", help="repository checkout to read shapes and registry from (default: this one)")
    args = ap.parse_args()
    if args.root:
        global ROOT
        ROOT = os.path.abspath(args.root)
    if args.check:
        problems = check()
        for p in problems:
            print(p)
        print(f"bench/ab selection: {'FAILED' if problems else 'ok'} ({len(problems)} problem(s))")
        return 1 if problems else 0
    base = args.base or "origin/main"
    files = args.files if args.files is not None else changed_files(base, args.head)
    result = select(files, base=base, head=None if args.head == "HEAD" else args.head)
    text = json.dumps(result, indent=1)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
