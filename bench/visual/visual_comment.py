#!/usr/bin/env python3
"""Render the sticky visual-smoke PR comment and decide the gate.

  visual_comment.py --out OUT --mode branch --repo OWNER/NAME --pr 123 --md comment.md [--expected-change]

Reads OUT/plan.json, compare.json, pixeldiff.json, meta.json (and error.json
when the run died); writes the markdown and OUT/verdict.json, whose "exit" is:

  0  pass
  1  a capture failed the gate (below)
  3  the run did not complete, or a planned capture is missing

A capture whose base-vs-PR data differs is classified against Loki, where Loki
holds data for the range:

  PR matches Loki, base did not        improved (closer to Loki): passes
  PR has fewer differences, none new   improved (closer to Loki): passes
  base matched Loki, PR diverges       regressed vs Loki: fails
  neither matches, or no Loki data     unexpected change: fails, unless the pull
                                       request carries the label
                                       `visual-change-expected` (--expected-change):
                                       then it passes as an expected change

Also failing: a panel with data on the base that is empty on the PR, an error
banner, panel error or new "No data" panel only on the PR, any error answer on the
PR side (outside spec.json `allowed_errors`), a PR side that never settled, and a
capture whose difference flipped between the first pass and the recapture
(non-deterministic). Pixel differences, a missing Loki capture and captures with no
data on either side only warn.

Every string that comes from the pull request (page names, queries, banner text,
differences) is escaped before it reaches the markdown, so the comment cannot be
made to carry markup or links.

mode "branch": montages are on the pr-visuals branch; only rows that are not
clean, and one collapsed block with the core montages, embed an image. mode
"artifact" (fork PRs, or a publish that did not happen): text only, with the
artifact link.
"""
import argparse
import html
import os
import re
import sys

from vio import dump_json, load_json, write_text

MARKER = "<!-- visual-smoke -->"
LABEL = "visual-change-expected"
PIXEL_WARN = 0.03  # share of changed pixels main vs PR; header and time picker noise stays below it
ORDER = {"15m": 0, "1h": 1, "6h": 2, "24h": 3, "7d": 4, "live": 5}
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
MD_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+!|<>~&$:@])")


def esc(text, limit=0):
    """Text for a markdown table cell or list item: no markup, links, mentions or line breaks survive.
    The limit applies before escaping, so an escape sequence is never cut in half."""
    text = str(text).replace("\r", " ").replace("\n", " ")
    return MD_SPECIAL.sub(r"\\\1", text[:limit] if limit else text)


def esc_html(text, limit=0):
    """Text for a <summary>: raw HTML context, where backslash escapes do not apply."""
    text = str(text).replace("\r", " ").replace("\n", " ")
    return html.escape(text[:limit] if limit else text, quote=True)


def ui_problems(main, pr, loki=None):
    """What the PR side shows that the base does not. An empty panel Loki shows too is Loki's answer, not a regression."""
    out = []
    loki_no_data = (loki or {}).get("noData", 0)
    if pr.get("noData", 0) > max(main.get("noData", 0), loki_no_data):
        out.append(f"new empty panel (\"No data\" {main.get('noData', 0)} on base, {pr.get('noData', 0)} on PR)")
    new = [b for b in pr.get("banners", []) if b not in main.get("banners", [])]
    if new:
        # The error behind the banner (capture.spec.ts records the error boundary's "Details").
        why = next((ln.strip() for ln in str(pr.get("details", "")).splitlines() if "Error" in ln), "")
        out.append(f"error banner only on the PR: {new[0][:100]}" + (f" ({why[:160]})" if why else ""))
    if pr.get("panelErrors", 0) > main.get("panelErrors", 0):
        out.append(f"panel error ({main.get('panelErrors', 0)} on base, {pr.get('panelErrors', 0)} on PR)")
    return out


def classify(row):
    """Base vs PR difference against Loki: 'improved', 'regressed', 'unsettled' (neither matches) or 'no-loki'."""
    if not (row.get("loki_compared") and row.get("points_loki", 0) > 0):
        return "no-loki"
    base_matches = row.get("loki_main_n", 1) == 0
    pr_matches = not row.get("loki_diffs")
    if pr_matches and not base_matches:
        return "improved"
    # The PR adds no difference from Loki and removes at least one the base has: what is left was already on the
    # base (e.g. an open gap on another request of the page), so the change moves the page closer to Loki.
    if (row.get("loki_diffs") and not row.get("loki_new")
            and row.get("loki_main_n", 0) > len(row["loki_diffs"])):
        return "improved"
    if base_matches and not pr_matches:
        return "regressed"
    return "unsettled"


def signature(diff):
    """A base-vs-PR difference with its window removed (timestamps, ids, hashes, counts), so the same
    request shape at another range compares equal."""
    s = re.sub(r"[0-9a-f]{12,}", "#", diff)
    s = re.sub(r"\d{4}-\d\d-\d\dT[\d:.%A-Z]+", "#", s)
    return re.sub(r"\d+", "#", s)


def proven_improvements(rows):
    """page -> signatures of base-vs-PR differences that Loki judged an improvement at some range."""
    out = {}
    for r in rows:
        if r.get("main_pr_diffs") and classify(r) == "improved":
            out.setdefault(r["page"], set()).update(signature(d) for d in r["main_pr_diffs"])
    return out


def assess(row, pixel, expected=False, flipped=(), proven=frozenset()):
    """(failures, warnings, status) of one capture; status is '', 'improved' or 'expected'."""
    fails, warns, status = [], [], ""
    diffs = row["main_pr_diffs"]
    if diffs:
        kind = classify(row)
        detail = f"({len(diffs)}): {diffs[0][:160]}"
        if kind == "improved":
            status = "improved"
        elif kind == "no-loki" and all(signature(d) in proven for d in diffs):
            # Loki holds no data for this range, but the same page proved every one of these
            # differences an improvement against Loki at a shorter range.
            status = "improved"
            warns.append(f"no Loki data at this range; the same differences match Loki at a shorter range {detail}")
        elif kind == "regressed":
            fails.append(f"regressed vs Loki: the base matched Loki, the PR diverges {detail}")
        elif expected:
            status = "expected"
            warns.append(f"expected change (label {LABEL}) {detail}")
        else:
            why = "no Loki data for this range" if kind == "no-loki" else "neither build matches Loki"
            fails.append(f"data differs base vs PR and {why}; label the pull request `{LABEL}` if intended {detail}")
    if row.get("main_pr_config"):
        warns.append(f"deployment configuration difference, not gated ({len(row['main_pr_config'])}): {row['main_pr_config'][0][:160]}")
    if row.get("main_pr_nondet"):
        warns.append(f"history-dependent difference, not gated ({len(row['main_pr_nondet'])}): {row['main_pr_nondet'][0][:160]}")
    if f"{row['page']} {row['range']}" in flipped:
        fails.append("non-deterministic: the difference between base and PR was gone on the recapture")
    fails += ui_problems(row.get("ui_main") or {}, row.get("ui_pr") or {}, row.get("ui_loki") if row.get("loki_compared") else None)
    if row.get("points_main", 0) > 0 and row.get("points_pr", 0) == 0:
        fails.append("empty on the PR, data on the base")
    if row.get("errors_pr"):
        fails.append(f"{len(row['errors_pr'])} error answer(s) on the PR side: {row['errors_pr'][0][:120]}")
    if not row.get("settled_pr", True):
        fails.append("the PR side never settled")
    if row.get("loki_missing"):
        warns.append("Loki was not captured for this range (no comparison with Loki)")
    if row.get("points_main", 0) == 0 and row.get("points_pr", 0) == 0 and row["range"] != "live":
        warns.append("no data on either side (nothing was compared)")
    if not row.get("settled", True) and row.get("settled_pr", True):
        warns.append("a side did not settle")
    if pixel is not None and pixel > PIXEL_WARN and row["range"] != "live":  # a live stream differs by nature
        warns.append(f"pixel diff {pixel:.1%}")
    return fails, warns, status


def vs_loki(row):
    if row["range"] == "live":
        return esc(row["pr_vs_loki"], 160)
    if row.get("loki_missing"):
        return "⚠️ not captured"
    if not row.get("loki_compared"):
        return "n/a (beyond the Loki window)"
    n = len(row["loki_diffs"])
    # By-design differences (documented deviations, Loki's own accounting) and history-dependent patterns are
    # listed by compare.py and never counted as differences.
    notes = ", ".join(f"{len(row[k])} {label}" for k, label in (("loki_explained", "explained"), ("loki_nondet", "history-dependent"))
                      if row.get(k))
    if not n:
        return "identical" + (f" ({notes})" if notes else "")
    new = len(row["loki_new"])
    return f"{n} difference(s), {n - new} on base too" + (f", **{new} new**" if new else "") + (f"; {notes}" if notes else "")


def key(row):
    return f"{row['page']}-{row['range']}"


def captures(plan):
    """[(page, range)] the plan asked for."""
    return [(pid, r) for pid, e in plan["entries"].items() for r in e["ranges"]]


def evaluate(rows, pixeldiff, plan, expected=False, flipped=()):
    """Per-capture assessments and the verdict."""
    by = {(r["page"], r["range"]): r for r in rows}
    proven = proven_improvements(rows)
    items, failed, missing = [], [], []
    for pid, rng in captures(plan):
        row = by.get((pid, rng))
        if row is None:
            missing.append(f"{pid} {rng}")
            continue
        fails, warns, status = assess(row, pixeldiff.get(key(row)), expected, flipped, proven.get(pid, frozenset()))
        items.append(dict(row=row, fails=fails, warns=warns, status=status, pixel=pixeldiff.get(key(row)),
                          core=plan["entries"][pid]["core"] and rng == plan.get("core_range", "1h"), why=plan["entries"][pid]["why"]))
        if fails:
            failed.append(f"{pid} {rng}: {fails[0]}")
    code = 3 if missing or not items else (1 if failed else 0)
    return items, dict(exit=code, failures=failed, missing=missing, captures=len(items),
                       warnings=sum(1 for i in items if i["warns"] and not i["fails"]),
                       improved=sum(1 for i in items if i["status"] == "improved"),
                       expected=sum(1 for i in items if i["status"] == "expected"))


def rawbase(a):
    return f"https://raw.githubusercontent.com/{a.repo}/pr-visuals/pr-{a.pr}"


def image(a, name, meta):
    if not SAFE_NAME.match(name):
        return ""
    return f"![{name}]({rawbase(a)}/{name}.png?v={re.sub(r'[^0-9a-f]', '', str(meta.get('head', '')))[:8]})"


def header(icon, text, a, meta, plan=None):
    sha = lambda k: re.sub(r"[^0-9a-f]", "", str(meta.get(k, "")))[:10]  # noqa: E731
    lines = [MARKER, f"### {icon} Visual smoke: {text}", ""]
    lines.append(f"Base `{sha('base')}` vs PR `{sha('head')}`, Grafana through the proxy builds and Loki on one fresh stack"
                 + (f"; [run]({a.run_url})" if a.run_url else "") + ".")
    if meta.get("recaptured"):
        lines.append("Loaded again, once and more patiently, after a first-pass difference or an unsettled page: "
                     + esc(", ".join(meta["recaptured"])) + ".")
    if plan and plan.get("entries"):
        core = [p for p, e in plan["entries"].items() if e["core"]]
        det = [p for p, e in plan["entries"].items() if e["why"] != ["core set"]]
        lines.append(f"Core set: {len(core)} capture(s) at {plan.get('core_range', '1h')}. Detailed set: {len(det)} entr{'y' if len(det) == 1 else 'ies'}"
                     + (f" ({', '.join(f'`{esc(d)}`' for d in det[:8])}{', ...' if len(det) > 8 else ''})" if det else " (nothing visual touched)")
                     + f". {int(plan['captures'])} capture(s), 15m/1h/6h, 24h and 7d stay out of PR runs.")
        if plan.get("trimmed"):
            lines.append(f"Trimmed to the 1h range for the capture budget: {esc(', '.join(plan['trimmed']))}.")
        if plan.get("dropped"):
            lines.append(f"**Not run (capture budget): {esc(', '.join(plan['dropped']))}.**")
    return lines


def skipped(a, meta):
    lines = header("⚪", "skipped", a, meta)[:3]
    lines += ["", "No visual-relevant file changed (docs, CI, tests, chart and registry text do not change what Grafana shows)."]
    return "\n".join(lines) + "\n", dict(exit=0, failures=[], missing=[], warnings=0, captures=0)


def errored(a, meta, message):
    lines = header("⚠️", "did not complete", a, meta)[:3]
    lines += ["", f"The run stopped before it could compare: {esc(message[:300])}. See the run log and the uploaded artifact."]
    return "\n".join(lines) + "\n", dict(exit=3, failures=[], missing=[], warnings=0, captures=0, error=message[:300])


def result_cell(i):
    if i["fails"]:
        return "❌ " + esc(i["fails"][0], 110)
    if i["status"] == "improved":
        return "✅ improved (closer to Loki)"
    if i["warns"]:
        return ("⚠️ expected change" if i["status"] == "expected" else "⚠️ " + esc(i["warns"][0], 80))
    return "✅"


def render(rows, pixeldiff, plan, meta, a):
    expected = bool(getattr(a, "expected_change", False))
    items, verdict = evaluate(rows, pixeldiff, plan, expected, tuple(meta.get("flipped") or ()))
    if verdict["missing"] or not items:
        text, v = errored(a, meta, "no capture for: " + (", ".join(verdict["missing"][:6]) or "any planned page"))
        return text, dict(v, missing=verdict["missing"])
    if verdict["exit"] == 1:
        icon, text = "❌", "failed"
    else:
        icon = "✅"
        text = "passed" + (" with warnings" if verdict["warnings"] or verdict["expected"] else "")
    lines = header(icon, text, a, meta, plan)
    items.sort(key=lambda i: (not i["core"], i["row"]["page"], ORDER[i["row"]["range"]]))
    lines += ["", "| page | range | set | base = PR (data) | vs Loki (PR) | pixel diff | result |", "|---|---|---|---|---|---|---|"]
    for i in items:
        r = i["row"]
        same = "✅ identical" if not r["main_pr_diffs"] else f"differs ({len(r['main_pr_diffs'])}): {esc(r['main_pr_diffs'][0], 60)}"
        px = "" if i["pixel"] is None else f"{i['pixel']:.2%}"
        lines.append(f"| {esc(r['page'])} | {esc(r['range'])} | {'core' if i['core'] else 'detailed'} | {same} | {vs_loki(r)} | {px} | {result_cell(i)} |")
    lines += ["", "Gate: a base-vs-PR data difference passes when the PR is closer to Loki, and fails when the PR diverges from Loki "
              f"or Loki cannot decide it (at a range Loki does not hold, a difference passes when the same page proved it closer to Loki at a shorter range; otherwise label the pull request `{LABEL}` to accept an intended change). Also failing: a panel empty on the PR "
              "but not on the base, an error banner, panel error, error answer or new \"No data\" panel on the PR, a PR side that never "
              "settled, a difference that did not reproduce on the recapture. Pixel differences only warn."]
    if a.mode == "branch":
        unclean = [i for i in items if i["fails"] or i["warns"]]
        for i in unclean:
            name = key(i["row"])
            first = (i["fails"] or i["warns"])[0]
            lines += ["", f"<details open><summary><b>{esc_html(name)}</b>: {esc_html(first, 120)}</summary>", ""]
            lines += [f"- {esc(x)}" for x in i["fails"] + i["warns"]]
            lines += [f"- vs Loki: {esc(x)}" for x in i["row"]["loki_diffs"][:4]]
            lines += [f"- vs Loki, explained: {esc(x)}" for x in (i["row"].get("loki_explained") or [])[:4]]
            lines += ["", image(a, name, meta), "", "</details>"]
        core = [i for i in items if i["core"] and not (i["fails"] or i["warns"])]
        if core:
            lines += ["", f"<details><summary>Core set montages ({len(core)}): base | PR | Loki</summary>", ""]
            for i in core:
                lines += [f"**{esc(key(i['row']))}**", "", image(a, key(i["row"]), meta), ""]
            lines += ["</details>"]
        lines += ["", f"All montages of this run: [`pr-visuals/pr-{int(a.pr)}`](https://github.com/{a.repo}/tree/pr-visuals/pr-{int(a.pr)})."]
    else:
        lines += ["", "Montages (base | PR | Loki): " + (f"[artifact]({a.artifact_url})" if a.artifact_url else "in the run's artifacts")
                  + ". They are not on the repository for this run."]
    return "\n".join(lines) + "\n", verdict


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=["branch", "artifact"], default="branch")
    ap.add_argument("--repo", default="")
    ap.add_argument("--pr", default="0")
    ap.add_argument("--run-url", default="")
    ap.add_argument("--artifact-url", default="")
    ap.add_argument("--expected-change", action="store_true", help=f"the pull request carries the label {LABEL}")
    ap.add_argument("--md", default="")
    ap.add_argument("--verdict", default="")
    a = ap.parse_args()
    a.expected_change = a.expected_change or os.environ.get("VISUAL_CHANGE_EXPECTED") == "true"
    a.pr = re.sub(r"[^0-9]", "", str(a.pr)) or "0"
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
