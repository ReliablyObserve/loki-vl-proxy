#!/usr/bin/env python3
"""Render the sticky visual-smoke PR comment and decide the gate.

  comment.py --out OUT --mode branch --repo OWNER/NAME --pr 123 --md comment.md

Reads OUT/plan.json, compare.json, pixeldiff.json, meta.json (and error.json
when the run died); writes the markdown and OUT/verdict.json, whose "exit" is:

  exit 0  nothing unexpected (pixel differences and Loki differences the base
          already had are shown, never fail)
  exit 1  the PR changed the data Grafana receives (base vs PR), a panel that
          showed data on the base is empty, or the PR shows an error banner,
          panel error or "No data" panel the base does not
  exit 3  the run did not complete, or a planned capture is missing

mode "branch": montages are on the pr-visuals branch; only rows that are not
clean, and one collapsed block with the core montages, embed an image. mode
"artifact" (fork PRs, no write token): text only, with the artifact link.
"""
import argparse
import os
import sys

from vio import dump_json, load_json, write_text

MARKER = "<!-- visual-smoke -->"
PIXEL_WARN = 0.03  # share of changed pixels main vs PR; header and time picker noise stays below it
ORDER = {"15m": 0, "1h": 1, "6h": 2, "24h": 3, "7d": 4, "live": 5}


def cell(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def ui_problems(main, pr):
    """What the PR side shows that the base does not."""
    out = []
    if pr.get("noData", 0) > main.get("noData", 0):
        out.append(f"new empty panel (\"No data\" {main.get('noData', 0)} on base, {pr.get('noData', 0)} on PR)")
    new = [b for b in pr.get("banners", []) if b not in main.get("banners", [])]
    if new:
        out.append(f"error banner only on the PR: {new[0][:100]}")
    if pr.get("panelErrors", 0) > main.get("panelErrors", 0):
        out.append(f"panel error ({main.get('panelErrors', 0)} on base, {pr.get('panelErrors', 0)} on PR)")
    return out


def assess(row, pixel):
    """(failures, warnings) of one capture."""
    fails, warns = [], []
    if row["main_pr_diffs"]:
        fails.append(f"data differs base vs PR ({len(row['main_pr_diffs'])}): {row['main_pr_diffs'][0][:160]}")
    fails += ui_problems(row.get("ui_main") or {}, row.get("ui_pr") or {})
    if row.get("points_main", 0) > 0 and row.get("points_pr", 0) == 0:
        fails.append("empty on the PR, data on the base")
    if row.get("loki_new") and not row["main_pr_diffs"]:
        fails.append(f"{len(row['loki_new'])} new difference(s) from Loki: {row['loki_new'][0][:140]}")
    if row.get("points_main", 0) == 0 and row.get("points_pr", 0) == 0 and row["range"] != "live":
        warns.append("no data on either side (nothing was compared)")
    if not row.get("settled", True):
        warns.append("page did not settle")
    if pixel is not None and pixel > PIXEL_WARN and row["range"] != "live":  # a live stream differs by nature
        warns.append(f"pixel diff {pixel:.1%}")
    return fails, warns


def vs_loki(row):
    if row["range"] == "live":
        return cell(row["pr_vs_loki"])[:120]
    if not row.get("loki_compared"):
        return "n/a (beyond the Loki window)"
    n = len(row["loki_diffs"])
    if not n:
        return "identical"
    new = len(row["loki_new"])
    return f"{n} difference(s), {n - new} on base too" + (f", **{new} new**" if new else "")


def key(row):
    return f"{row['page']}-{row['range']}"


def captures(plan):
    """[(page, range)] the plan asked for."""
    return [(pid, r) for pid, e in plan["entries"].items() for r in e["ranges"]]


def evaluate(rows, pixeldiff, plan):
    """Per-capture assessments and the verdict."""
    by = {(r["page"], r["range"]): r for r in rows}
    items, failed, missing = [], [], []
    for pid, rng in captures(plan):
        row = by.get((pid, rng))
        if row is None:
            missing.append(f"{pid} {rng}")
            continue
        fails, warns = assess(row, pixeldiff.get(key(row)))
        items.append(dict(row=row, fails=fails, warns=warns, pixel=pixeldiff.get(key(row)),
                          core=plan["entries"][pid]["core"] and rng == plan.get("core_range", "1h"), why=plan["entries"][pid]["why"]))
        if fails:
            failed.append(f"{pid} {rng}: {fails[0]}")
    code = 3 if missing or not items else (1 if failed else 0)
    return items, dict(exit=code, failures=failed, missing=missing,
                       warnings=sum(1 for i in items if i["warns"] and not i["fails"]), captures=len(items))


def rawbase(a):
    return f"https://raw.githubusercontent.com/{a.repo}/pr-visuals/pr-{a.pr}"


def image(a, name, meta):
    return f"![{name}]({rawbase(a)}/{name}.png?v={str(meta.get('head', ''))[:8]})"


def header(icon, text, a, meta, plan=None):
    sha = lambda k: str(meta.get(k, ""))[:10]  # noqa: E731
    lines = [MARKER, f"### {icon} Visual smoke: {text}", ""]
    lines.append(f"Base `{sha('base')}` vs PR `{sha('head')}`, Grafana through the proxy builds and Loki on one fresh stack"
                 + (f"; [run]({a.run_url})" if a.run_url else "") + ".")
    if meta.get("recaptured"):
        lines.append(f"Loaded again, once and more patiently, after a first-pass difference or an unsettled page: {', '.join(meta['recaptured'])}.")
    if plan and plan.get("entries"):
        core = [p for p, e in plan["entries"].items() if e["core"]]
        det = [p for p, e in plan["entries"].items() if e["why"] != ["core set"]]
        lines.append(f"Core set: {len(core)} capture(s) at {plan.get('core_range', '1h')}. Detailed set: {len(det)} entr{'y' if len(det) == 1 else 'ies'}"
                     + (f" ({', '.join(f'`{d}`' for d in det[:8])}{', ...' if len(det) > 8 else ''})" if det else " (nothing visual touched)")
                     + f". {plan['captures']} capture(s), 15m/1h/6h, 24h and 7d stay out of PR runs.")
        if plan.get("trimmed"):
            lines.append(f"Trimmed to the 1h range for the capture budget: {', '.join(plan['trimmed'])}.")
        if plan.get("dropped"):
            lines.append(f"**Not run (capture budget): {', '.join(plan['dropped'])}.**")
    return lines


def skipped(a, meta):
    lines = header("⚪", "skipped", a, meta)[:3]
    lines += ["", "No visual-relevant file changed (docs, CI, tests, chart and registry text do not change what Grafana shows)."]
    return "\n".join(lines) + "\n", dict(exit=0, failures=[], missing=[], warnings=0, captures=0)


def errored(a, meta, message):
    lines = header("⚠️", "did not complete", a, meta)[:3]
    lines += ["", f"The run stopped before it could compare: `{message[:300]}`. See the run log and the uploaded artifact."]
    return "\n".join(lines) + "\n", dict(exit=3, failures=[], missing=[], warnings=0, captures=0, error=message[:300])


def render(rows, pixeldiff, plan, meta, a):
    items, verdict = evaluate(rows, pixeldiff, plan)
    if verdict["missing"] or not items:
        text, v = errored(a, meta, "no capture for: " + (", ".join(verdict["missing"][:6]) or "any planned page"))
        return text, dict(v, missing=verdict["missing"])
    icon, text = ("❌", "failed") if verdict["exit"] == 1 else (("✅", "passed") if not verdict["warnings"] else ("✅", "passed with warnings"))
    lines = header(icon, text, a, meta, plan)
    rank = lambda i: (not i["core"], i["row"]["page"], ORDER[i["row"]["range"]])  # noqa: E731
    items.sort(key=rank)
    lines += ["", "| page | range | set | base = PR (data) | vs Loki (PR) | pixel diff | result |", "|---|---|---|---|---|---|---|"]
    for i in items:
        r = i["row"]
        same = "✅ identical" if not r["main_pr_diffs"] else f"❌ {cell(r['main_pr_diffs'][0])[:70]}"
        res = ("❌ " + cell(i["fails"][0])[:80]) if i["fails"] else (("⚠️ " + cell(i["warns"][0])[:60]) if i["warns"] else "✅")
        px = "" if i["pixel"] is None else f"{i['pixel']:.2%}"
        lines.append(f"| {r['page']} | {r['range']} | {'core' if i['core'] else 'detailed'} | {same} | {vs_loki(r)} | {px} | {res} |")
    lines += ["", "Gate: fails on a base-vs-PR data difference, a panel empty on the PR but not on the base, or an error banner, "
              "panel error or new \"No data\" panel only on the PR. Pixel differences and differences from Loki the base already has only inform."]
    if a.mode == "branch":
        unclean = [i for i in items if i["fails"] or i["warns"]]
        for i in unclean:
            name = key(i["row"])
            lines += ["", f"<details open><summary><b>{name}</b>: {cell((i['fails'] or i['warns'])[0])[:100]}</summary>", ""]
            lines += [f"- {x}" for x in i["fails"] + i["warns"]]
            lines += [f"- vs Loki: {x}" for x in i["row"]["loki_diffs"][:4]]
            lines += ["", image(a, name, meta), "", "</details>"]
        core = [i for i in items if i["core"] and not (i["fails"] or i["warns"])]
        if core:
            lines += ["", f"<details><summary>Core set montages ({len(core)}): base | PR | Loki</summary>", ""]
            for i in core:
                lines += [f"**{key(i['row'])}**", "", image(a, key(i["row"]), meta), ""]
            lines += ["</details>"]
        lines += ["", f"All montages of this run: [`pr-visuals/pr-{a.pr}`](https://github.com/{a.repo}/tree/pr-visuals/pr-{a.pr})."]
    else:
        lines += ["", "Montages (base | PR | Loki): " + (f"[artifact]({a.artifact_url})" if a.artifact_url else "in the run's artifacts")
                  + ". Fork pull requests cannot publish images to the repository."]
    return "\n".join(lines) + "\n", verdict


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=["branch", "artifact"], default="branch")
    ap.add_argument("--repo", default="")
    ap.add_argument("--pr", default="0")
    ap.add_argument("--run-url", default="")
    ap.add_argument("--artifact-url", default="")
    ap.add_argument("--md", default="")
    ap.add_argument("--verdict", default="")
    a = ap.parse_args()
    meta = load_json(os.path.join(a.out, "meta.json")) if os.path.exists(os.path.join(a.out, "meta.json")) else {}
    plan = load_json(os.path.join(a.out, "plan.json")) if os.path.exists(os.path.join(a.out, "plan.json")) else {"run": False, "entries": {}}
    err = os.path.join(a.out, "error.json")
    if not plan["run"]:
        text, verdict = skipped(a, meta)
    elif os.path.exists(err):
        text, verdict = errored(a, meta, load_json(err).get("error", "unknown"))
    else:
        rows = load_json(os.path.join(a.out, "compare.json")) if os.path.exists(os.path.join(a.out, "compare.json")) else []
        px = load_json(os.path.join(a.out, "pixeldiff.json")) if os.path.exists(os.path.join(a.out, "pixeldiff.json")) else {}
        text, verdict = render(rows, px, plan, meta, a)
    write_text(a.md or os.path.join(a.out, "comment.md"), text)
    dump_json(a.verdict or os.path.join(a.out, "verdict.json"), verdict)
    print(text)
    return 0  # the verdict ("exit" in verdict.json), not this status, decides


if __name__ == "__main__":
    sys.exit(main())
