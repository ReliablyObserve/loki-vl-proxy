#!/usr/bin/env python3
"""Census of what the proof pipeline claims: the baseline a change to the registry, its runners or its
reports is compared against, so no case, mode, shape, page or test disappears unnoticed.

It counts, from the sources as committed:
  * registry cases and their modes (the comparison mode, and whether the case has a query, a perf: block,
    a locked perf: block, a gap status), read through the strict loader (test/registry);
  * bench/ab shapes, bench/visual pages and query-semantics-matrix cases;
  * Playwright spec files, their tests and tags, and the CI e2e-ui shards that select them;
  * the tests that claim registry items, test file x item (conformance/registry/generated/wiring.json);
  * the gates that scripts/ci/check_conformance.py enforces.

Writes conformance/reports/census.md (counts) and conformance/registry/generated/census.json (every cell, so
two censuses can be diffed). `--diff OLD.json NEW.json` lists the cells that only one of them holds and exits
1 when any cell is missing from NEW (a cell may only move under another id).

Usage: python3 conformance/scripts/census.py [--diff OLD.json NEW.json]
"""
import argparse
import collections
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from registry_io import dump_json, load_cases, load_json, read_text, write_text  # noqa: E402

ROOT = "conformance/registry"
JSON_OUT = f"{ROOT}/generated/census.json"
MD_OUT = "conformance/reports/census.md"
SHAPES = "bench/ab/shapes.json"
VISUAL = "bench/visual/spec.json"
QSM = "test/e2e-compat/query-semantics-matrix.json"
WIRING = f"{ROOT}/generated/wiring.json"
CI = ".github/workflows/ci.yaml"
GATE_SCRIPT = "scripts/ci/check_conformance.py"
SPEC_DIR = "test/e2e-ui/tests"
TEST_CALL = re.compile(r"^\s*test(?:\.(?:only|skip|fixme))?\(\s*([`'\"])(.*?)\1\s*,", re.M)
TAG = re.compile(r"@([A-Za-z][A-Za-z0-9_-]*)")


def case_cells(cases):
    cells = []
    for case in cases:
        doc = case["doc"]
        modes = [f"compare:{doc['compare']['mode']}"]
        if (doc["request"].get("params") or {}).get("query"):
            modes.append("query")
        perf = doc.get("perf")
        if perf:
            modes.append("perf")
            if perf.get("locked"):
                modes.append("perf:locked")
        if doc.get("gap"):
            modes.append(f"gap:{doc['gap']['status']}")
        if doc.get("applies_to"):
            modes.append("applies_to")
        cells += [f"{case['id']} | {mode}" for mode in modes]
    return sorted(cells)


def shape_cells():
    spec = load_json(SHAPES)
    cells, path_only, covers = [], 0, set()
    for set_name, value in sorted(spec["sets"].items()):
        for shape in value["shapes"]:
            cells.append(f"{set_name}/{shape['name']}")
            path_only += 0 if shape.get("query") else 1
            covers.update(shape.get("covers") or [])
    return sorted(cells), path_only, len(covers)


def page_cells():
    pages = load_json(VISUAL)["pages"]
    kinds = collections.Counter(page.get("kind", "?") for page in pages)
    return sorted(page["id"] for page in pages), kinds


def qsm_cells():
    cases = load_json(QSM)["cases"]
    return sorted(case["id"] for case in cases), collections.Counter(case.get("compare") or "expect:" + str(case.get("expectation")) for case in cases)


def shards():
    """name -> list of `playwright test` option lines for each e2e-ui shard in the CI workflow."""
    text = read_text(CI)
    start = text.index("  e2e-ui:")
    section = text[start:]
    out = {}
    for match in re.finditer(r"- name: (\S+)\n\s+commands: \|\n((?:\s{14,}\S.*\n)+)", section):
        out[match.group(1)] = [line.strip() for line in match.group(2).splitlines() if line.strip()]
    return out


def spec_selected(tags, name, options):
    """Whether a shard's option lines select a spec file, judged by its file name or its tags. A per-spec
    approximation: Playwright matches each test title, so a spec with one selected tag counts as selected."""
    for line in options:
        if line.startswith("--grep"):
            wanted = TAG.findall(line.split(None, 1)[1] if " " in line else "")
            if tags & set(wanted):
                return True
        elif line.endswith(name) or line.endswith("/" + name):
            return True
    return False


def playwright():
    shard_options = shards()
    specs = {}
    for name in sorted(os.listdir(SPEC_DIR)):
        if not name.endswith(".spec.ts"):
            continue
        text = read_text(os.path.join(SPEC_DIR, name))
        tags = set(TAG.findall(text)) - {"cov", "playwright"}
        titles = [match[1] for match in TEST_CALL.findall(text)]
        specs[name] = {"tests": len(titles), "titles": sorted(set(titles)), "tags": sorted(tags),
                       "shards": sorted(s for s, opts in shard_options.items() if spec_selected(tags, name, opts))}
    return specs, sorted(shard_options)


def gates():
    return sorted(set(re.findall(r'failures\.append\(f?"([^"{]+)"\)', read_text(GATE_SCRIPT))))


def build():
    cases = load_cases(ROOT)
    shapes, path_only, shape_covers = shape_cells()
    pages, page_kinds = page_cells()
    qsm, qsm_modes = qsm_cells()
    specs, shard_names = playwright()
    wiring = load_json(WIRING)
    claims = collections.Counter()
    for path in wiring["test_to_items"]:
        claims["go" if path.endswith(".go") else "playwright"] += 1
    playwright_cells = sorted([f"{name} | {title}" for name, spec in specs.items() for title in spec["titles"]]
                              + [f"{name} | shard:{shard}" for name, spec in specs.items() for shard in spec["shards"]])
    return {
        # Cell lists: --diff fails when one of these loses a cell.
        "cases": case_cells(cases),
        "shapes": shapes,
        "pages": pages,
        "qsm": qsm,
        "playwright_cells": playwright_cells,
        "claim_cells": sorted(f"{path} | {item}" for path, items in wiring["test_to_items"].items() for item in items),
        "gates": gates(),
        # Counts: shown by --diff, never a failure on their own.
        "counts": {
            "case_consumers": collections.Counter(c["doc"]["consumer"] for c in cases),
            "shapes_path_only": path_only, "shape_covers_distinct": shape_covers,
            "page_kinds": page_kinds, "qsm_modes": qsm_modes,
            "claiming_tests": dict(claims), "wired_items": wiring["wired_items"], "unwired_items": len(wiring["unwired_items"]),
        },
        "_cases": cases, "_playwright": specs, "_shards": shard_names, "_claiming_tests": len(wiring["test_to_items"]),
    }


def matrix(cases):
    """consumer x comparison mode, the case x mode baseline."""
    modes = sorted({c["doc"]["compare"]["mode"] for c in cases})
    rows = collections.defaultdict(collections.Counter)
    for case in cases:
        rows[case["doc"]["consumer"]][case["doc"]["compare"]["mode"]] += 1
    return modes, rows


def render(data):
    cases = data["_cases"]
    modes, rows = matrix(cases)
    cell_kinds = collections.Counter(cell.split(" | ", 1)[1].split(":")[0] for cell in data["cases"])
    status = collections.Counter(cell.split(" | ", 1)[1] for cell in data["cases"] if " | gap:" in cell)
    lines = ["# Census", "",
             ("Generated by `conformance/scripts/census.py`. The baseline of what the proof pipeline claims: a change to "
              "the registry, its runners or its reports is compared against these counts, and "
              "`conformance/registry/generated/census.json` lists every cell so two censuses can be diffed "
              "(`python3 conformance/scripts/census.py --diff OLD.json NEW.json`)."), "",
             "## Registry cases", "",
             f"- Case files: **{len(cases)}**, all loaded by the strict loader (`go run ./test/registry/cmd/regload check`)",
             f"- Case x mode cells: **{len(data['cases'])}** "
             f"(compare {cell_kinds['compare']}, query {cell_kinds['query']}, perf {cell_kinds['perf']} incl. locked, "
             f"applies_to {cell_kinds['applies_to']}, gap {cell_kinds['gap']})",
             f"- Locked perf rows: **{sum(1 for c in data['cases'] if c.endswith(' | perf:locked'))}**",
             "- Gap status: " + ", ".join(f"{k.split(':')[1]} {v}" for k, v in sorted(status.items())), "",
             "| Consumer | " + " | ".join(modes) + " | Total |", "|---|" + "---:|" * (len(modes) + 1)]
    for consumer in sorted(rows):
        lines.append(f"| {consumer} | " + " | ".join(str(rows[consumer][m]) or "0" for m in modes)
                     + f" | {sum(rows[consumer].values())} |")
    lines.append("| **all** | " + " | ".join(str(sum(rows[c][m] for c in rows)) for m in modes) + f" | {len(cases)} |")
    lines += ["", "## Other runners' inputs", "",
              "| Source | Count | Detail |", "|---|---:|---|",
              f"| `{SHAPES}` shapes | {len(data['shapes'])} | {data['counts']['shapes_path_only']} path-only; "
              f"{data['counts']['shape_covers_distinct']} distinct `covers` ids |",
              f"| `{VISUAL}` pages | {len(data['pages'])} | " + ", ".join(f"{k} {v}" for k, v in sorted(data['counts']['page_kinds'].items())) + " |",
              f"| `{QSM}` cases | {len(data['qsm'])} | " + ", ".join(f"{k} {v}" for k, v in sorted(data['counts']['qsm_modes'].items())) + " |",
              f"| Tests claiming registry items | {data['_claiming_tests']} | "
              + ", ".join(f"{k} {v}" for k, v in sorted(data['counts']['claiming_tests'].items()))
              + f"; {len(data['claim_cells'])} test x item claims; items wired {data['counts']['wired_items']}, "
              f"unwired {data['counts']['unwired_items']} |",
              f"| `{GATE_SCRIPT}` gates | {len(data['gates'])} | |", "",
              "## Playwright specs and the e2e-ui shards", "",
              ("A spec counts as selected by a shard when a `--grep` tag of the shard appears in the spec or the shard names "
               "the file; Playwright matches each test title, so this is a per-spec reading of "
               f"`{CI}`. Shards: " + ", ".join(f"`{s}`" for s in data["_shards"]) + "."), "",
              "| Spec | Tests | Tags | Shards |", "|---|---:|---|---|"]
    unselected = 0
    for name, spec in sorted(data["_playwright"].items()):
        lines.append(f"| `{name}` | {spec['tests']} | {', '.join(spec['tags']) or '—'} | {', '.join(spec['shards']) or '**none**'} |")
        unselected += spec["tests"] if not spec["shards"] else 0
    total = sum(s["tests"] for s in data["_playwright"].values())
    lines += ["", f"Playwright tests: **{total}**; in specs no shard selects: **{unselected}**.", "",
              "## Gates", ""]
    lines += [f"- {gate}" for gate in data["gates"]]
    return "\n".join(lines) + "\n"


def diff(old_path, new_path):
    """Cell lists gate: a cell the new census lacks is a failure (a cell may only move under another id, which the
    caller maps by hand). The "counts" dict is information: a change is printed, never a failure."""
    old, new = load_json(old_path), load_json(new_path)
    missing = 0
    for key in sorted(set(old) | set(new)):
        before, after = old.get(key), new.get(key)
        if isinstance(before, dict) and isinstance(after, dict):
            for name in sorted(set(before) | set(after)):
                if before.get(name) != after.get(name):
                    print(f"{key}.{name}: {before.get(name)} -> {after.get(name)} (information)")
            continue
        if not isinstance(before, list) or not isinstance(after, list):
            continue
        gone, added = sorted(set(before) - set(after)), sorted(set(after) - set(before))
        print(f"{key}: {len(before)} -> {len(after)} (removed {len(gone)}, added {len(added)})")
        for cell in gone:
            print(f"  - {cell}")
        for cell in added:
            print(f"  + {cell}")
        missing += len(gone)
    return 1 if missing else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--diff", nargs=2, metavar=("OLD", "NEW"))
    args = parser.parse_args()
    if args.diff:
        return diff(*args.diff)
    data = build()
    write_text(MD_OUT, render(data))
    dump_json(JSON_OUT, {k: v for k, v in data.items() if not k.startswith("_")})
    print(f"{MD_OUT}: {len(data['_cases'])} cases, {len(data['cases'])} case x mode cells, {len(data['shapes'])} shapes, "
          f"{len(data['pages'])} pages, {len(data['qsm'])} qsm cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
