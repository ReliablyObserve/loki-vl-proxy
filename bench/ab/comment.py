#!/usr/bin/env python3
"""Render the pull request A/B comment from report.py summaries.

  comment.py SUMMARY.json [SUMMARY.json ...] --selection selection.json \\
      [--meta meta.json] [--out comment.md] [--verdict verdict.json]

One table row per shape x range: base and PR warm p50, the change with an
icon, Loki's p50, the PR's time as a multiple of Loki's, and whether the PR's
answer matches Loki's. Rows that did not move are folded into a collapsed
section, as are cold (first-run) timings and why each shape was selected.

Exit status (the `perf-smoke` check): 1 when a shape broke (base answered, PR
does not), is slower beyond noise on the confirmation re-run, or returns a
result that differs from Loki where the base's did not; 2 when the run is
invalid (VictoriaLogs restarted mid-run); 3 when the run did not complete
(pr_smoke.py renders that comment); 0 otherwise.

Loki profiles (bench/ab/README.md, "Like for like"): the summaries of the "aligned" and
"loki-defaults" profiles are shown together; each table row names its profile, a build that
did not run the profile's setting is marked ‡ and never compared, and a cache-served Loki
cold run is marked † and left out of every ratio.

Above 2× Loki (over2x.gate, given the base's bench/ab/history/over-2x-loki.json):
rows the PR adds above 2× Loki or makes worse, in either profile, are listed in a ⚠️ section
at the top of the comment with both profiles' ratios. In meta's over_2x_loki "warn" mode (the default) the state and
exit status stay what they would be without them; in "fail" mode they fail the
check (exit 1). Known above-2× rows are listed in a collapsed section.

Locked rows (the registry's perf: blocks as committed on the base, over2x.bounds_at): a locked row the PR
takes above its bound is a regression of the acceptance lock, listed in its own 🔒 section above the other
above-2× rows; it follows the same warn|fail switch. A row whose base build was below Loki and whose PR build
is above Loki (beyond noise) is listed as lost below-Loki: flagged, never failing.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import over2x  # noqa: E402
import stack  # noqa: E402

MARKER = "<!-- perf-ab-smoke -->"
# Known above-2× rows listed in the collapsed section; a comment is limited to 65,536 characters.
KNOWN_CAP = 40
ICON = {"fixed": "✅", "broken": "❌", "slower": "🔴", "faster": "🟢", "same": "⚪", "n/a": "⚪"}
ORDER = {"broken": 0, "slower": 1, "fixed": 2, "faster": 3, "same": 4, "n/a": 5}


SHORT = {"aligned": "aligned", "loki-defaults": "defaults"}


def profile_col(rows):
    """Whether the table has a profile column: only when some row came from a profile."""
    return any(row.get("profile") for _, row in rows)


def tag(row, target, text):
    """text, marked ‡ when the target is not like-for-like with the row's Loki profile."""
    return text + (" ‡" if target in row.get("unlike", []) else "")


def load(path):
    with open(path) as f:
        return json.load(f)


def secs(v, status=None):
    if status and status != "200":
        return f"**{status}**"
    if v is None:
        return "—"
    return f"{v * 1000:.0f} ms" if v < 1 else f"{v:.2f} s"


def change(row, b, c):
    verdict = row["verdict"]
    icon = ICON.get(verdict, "⚪")
    if verdict in ("fixed", "broken"):
        return f"{icon} {verdict}"
    if {b, c} & set(row.get("unlike", [])):
        return f"{icon} n/a ‡"
    bv, cv = row["p50"].get(b), row["p50"].get(c)
    if not bv or cv is None:
        return f"{icon} n/a"
    pct = (cv - bv) / bv * 100
    return f"{icon} {pct:+.0f}%"


def vs_loki(row, c, r):
    cv, rv = row["p50"].get(c), row["p50"].get(r)
    if c in row.get("unlike", []):
        return "‡"
    if row["status"].get(c) != "200" or row["status"].get(r) != "200" or not cv or not rv:
        return "—"
    ratio = cv / rv
    text = f"{ratio:.2f}×" if ratio < 10 else f"{ratio:.0f}×"
    return f"**{text}**" if ratio > 1.25 and cv - rv > 0.05 else text


def parity(row):
    p = row.get("parity_vs_reference", "n/a")
    if p == "same":
        return "✅ same"
    if p.startswith("same series"):
        return "✅ " + p.replace("same series, sum ", "sum ")
    if p == "differs":
        if row.get("baseline_parity_vs_reference") == "differs":
            return "➖ differs (as base)"
        return "⚠️ **differs**"
    return "—"


def new_difference(row):
    return row.get("parity_vs_reference") == "differs" and row.get("baseline_parity_vs_reference") != "differs"


def table(rows, b, c, r):
    prof = profile_col(rows)
    head = "| | shape | range |" + (" Loki profile |" if prof else "") + " base p50 | PR p50 | change | Loki p50 | PR ÷ Loki | result vs Loki |"
    out = [head, "|---|---|---|" + ("---|" if prof else "") + "--:|--:|--:|--:|--:|---|"]
    for set_name, row in rows:
        mark = " ¹" if row.get("confirmed") else ""
        out.append(
            f"| {ICON.get(row['verdict'], '⚪')} | {cell(row['shape'])} | {row['range']} | "
            + (f"{SHORT.get(row.get('profile'), '—')} | " if prof else "")
            + f"{tag(row, b, secs(row['p50'].get(b), row['status'].get(b)))} | "
            f"{tag(row, c, secs(row['p50'].get(c), row['status'].get(c)))}{mark} | "
            f"{change(row, b, c)} | {secs(row['p50'].get(r), row['status'].get(r))} | {vs_loki(row, c, r)} | {parity(row)} |")
    return out


def cold_ratio(row, c, r):
    """PR cold ÷ Loki cold, or "—": only when both answered 200, the PR build is like-for-like with the
    profile and Loki computed its cold run (a cache-served cold is never part of a ratio)."""
    cc, rc = row["cold"].get(c), over2x.loki_cold(row, r)
    if c in row.get("unlike", []):
        return "‡"
    if not cc or not rc or not row["status"].get(c) == row["status"].get(r) == "200":
        return "—"
    return f"{cc / rc:.2f}×"


def cold_table(rows, b, c, r):
    """Cold timings; with a refresh run, also each target's refresh p50 and the PR's multiple of Loki's."""
    refresh = any("refresh" in row for _, row in rows)
    prof = profile_col(rows)
    head = "| shape | range |" + (" Loki profile |" if prof else "") + " base cold | PR cold | Loki cold | PR ÷ Loki cold |"
    out = [head + (" base refresh | PR refresh | Loki refresh | PR ÷ Loki refresh |" if refresh else ""),
           "|---|---|" + ("---|" if prof else "") + "--:|--:|--:|--:|" + ("--:|--:|--:|--:|" if refresh else "")]
    for _, row in rows:
        cc, rc = row["cold"].get(c), row["cold"].get(r)
        # Loki's cold: † when its cache answered (the time is then not a computed one), ↺ when it is the
        # first set's computed cold, ¹ when it is the first pass's because the confirm pass was cache-served.
        served = (" †" if over2x.cold_served(row, r) else
                  " ↺" if row.get("loki_cold_reused_from") else " ¹" if row.get("cold_from_first_pass") else "")
        line = (f"| {cell(row['shape'])} | {row['range']} | "
                + (f"{SHORT.get(row.get('profile'), '—')} | " if prof else "")
                + f"{tag(row, b, secs(row['cold'].get(b), row['status'].get(b)))} | "
                f"{tag(row, c, secs(cc, row['status'].get(c)))} | {secs(rc, row['status'].get(r))}{served} | "
                f"{cold_ratio(row, c, r)} |")
        if refresh:
            rf, st = row.get("refresh", {}), row.get("refresh_status", {})
            pr, lk = rf.get(c), rf.get(r)
            multiple = ("‡" if c in row.get("unlike", []) else
                        f"{pr / lk:.2f}×" if pr and lk and st.get(c) == st.get(r) == "200" else "—")
            line += "".join(f" {tag(row, t, secs(rf.get(t), st.get(t)))} |" for t in (b, c, r)) + f" {multiple} |"
        out.append(line)
    return out


def cell(text):
    return str(text).replace("|", "\\|")


def skipped(selection, meta):
    lines = [MARKER, "### ⏭️ Performance A/B: skipped", "",
             f"No change to the proxy's runtime code in this pull request ({selection.get('changed', 0)} file(s) changed: "
             "docs, CI, tests, Dockerfile, Helm chart or registry text), so no A/B run was needed."]
    if meta.get("head"):
        lines += ["", f"<sub>Commit `{meta['head'][:10]}`.</sub>"]
    return "\n".join(lines) + "\n", {"state": "skipped", "failed": False, "exit": 0}


def errored(selection, meta, message):
    """The run did not complete: say so in the same comment instead of leaving a stale table."""
    shapes = sum(len(v) for v in selection.get("sets", {}).values())
    lines = [MARKER, "### ⚠️ Performance A/B: did not complete", "",
             f"The A/B run for {shapes} selected shape(s) stopped before a result: `{message[:300]}`. "
             "The job log and the uploaded `perf-ab-smoke` artifact have the details; re-run the job once the "
             "cause is fixed.", "",
             f"<sub>base `{meta.get('base', '?')[:10]}` → PR `{meta.get('head', '?')[:10]}`.</sub>"]
    return "\n".join(lines) + "\n", {"state": "error", "failed": True, "exit": 3}


def over_2x_table(findings, with_note=False):
    """Known rows, one per profile finding."""
    out = ["| | profile | mode | shape | range | PR | Loki | PR ÷ Loki | base ÷ Loki | listed ÷ Loki |" + (" why |" if with_note else ""),
           "|---|---|---|---|---|--:|--:|--:|--:|--:|" + ("---|" if with_note else "")]
    for f in findings:
        mark = " ¹" if f.get("confirmed") else ""
        out.append(f"| {f['kind']} | {SHORT.get(f.get('profile'), '—')} | {f['mode']} | {cell(f['shape'])} | {f['range']} | "
                   f"{secs(f['proxy_s'])}{mark} | {secs(f['loki_s'])} | **{over2x.times(f['ratio'])}** | "
                   f"{over2x.times(f['base_ratio'])} | {over2x.times(f['baseline_ratio'])} |"
                   + (f" {f.get('note', '')} |" if with_note else ""))
    return out


def over_2x_failing_table(merged):
    """New and worsened rows, one per shape × range × mode: above 2× in either profile, both ratios shown;
    the detail columns are the profile with the highest ratio."""
    out = ["| | mode | shape | range | PR ÷ Loki aligned | PR ÷ Loki defaults | worst: PR | Loki | base ÷ Loki | listed ÷ Loki |",
           "|---|---|---|---|--:|--:|--:|--:|--:|--:|"]
    for m in merged:
        f = max(m["profiles"].values(), key=lambda x: x["ratio"] or 0)
        mark = " ¹" if m.get("confirmed") else ""
        shown = {p: ("not run" if p not in m["ratios"] else over2x.times(m["ratios"][p])) for p in stack.PROFILES}
        for p in m["profiles"]:
            shown[p] = f"**{shown[p]}**"
        out.append(f"| {m['kind']} | {m['mode']} | {cell(m['shape'])} | {m['range']} | {shown['aligned']} | "
                   f"{shown['loki-defaults']} | {secs(f['proxy_s'])}{mark} | {secs(f['loki_s'])} | "
                   f"{over2x.times(f['base_ratio'])} | {over2x.times(f['baseline_ratio'])} |")
    return out


def effect(mode):
    return (("🔴", "This fails `perf-smoke`.") if mode == "fail" else
            ("⚠️", "Warning only: `perf-smoke` does not fail on it yet; it becomes blocking once the "
                   f"above-2× list (`{over2x.BASELINE_REL}`) is empty."))


def locked_section(locked, mode):
    """Locked rows the PR takes above their bound: a regression of the acceptance lock."""
    icon, text = effect(mode)
    return [f"#### {icon} 🔒 Locked rows above their bound: {len(locked)}", "",
            "These rows are locked in the registry (a `perf:` block with `locked: true` on an item their shape "
            "covers, as committed on the base): every daily run measured them within their bound, and this PR "
            f"takes them above it beyond noise. {text} How to read and clear one: `bench/ab/README.md`.", "",
            *over_2x_failing_table(locked), "",
            "Locked by: " + "; ".join(sorted({f"`{i}`" for f in locked for i in f.get("locked_by", [])})), ""]


def lost_below_section(lost):
    out = ["<details><summary>🟡 " + f"{len(lost)} shape×range×mode lost below-Loki: the base build was below "
           "Loki, the PR build is above it (flagged, not failing)</summary>", "",
           "| profile | mode | shape | range | PR | Loki | PR ÷ Loki | base ÷ Loki |", "|---|---|---|---|--:|--:|--:|--:|"]
    for f in lost:
        out.append(f"| {SHORT.get(f.get('profile'), '—')} | {f['mode']} | {cell(f['shape'])} | {f['range']} | {secs(f['proxy_s'])} | {secs(f['loki_s'])} | "
                   f"{over2x.times(f['ratio'])} | {over2x.times(f['base_ratio'])} |")
    return out + ["", "</details>"]


def over_2x_section(bad, mode):
    """The warning (or failure) at the top of the comment: rows the PR adds above 2× Loki or makes worse,
    in either profile (bad: over2x.merge_failing)."""
    new = sum(1 for f in bad if f["kind"] == "new")
    icon, text = effect(mode)
    return [f"#### {icon} Above 2× Loki: {new} new, {len(bad) - new} worsened", "",
            "A shape × range slower than 2× Loki (cold, warm or refresh, beyond the "
            f"{over2x.MIN_GAP * 1000:.0f} ms floor) is a defect, in either Loki profile (aligned: the stack's Loki; "
            "defaults: Loki with its own defaults). These rows are above 2× on this PR and either are "
            "not in the list and not as slow on the base build, or are worse than both the list and the base build "
            f"beyond noise. {text} How to clear one: `bench/ab/README.md`.", "",
            *over_2x_failing_table(bad), ""]


def profile_lines(meta, summaries):
    """What each Loki profile compared: whether it ran and why not, and which builds were like-for-like."""
    info = meta.get("profiles") or {}
    out = []
    for p in stack.PROFILES:
        mine = [s for s in summaries if s.get("profile") == p]
        entry = info.get(p, {})
        if mine:
            unlike = sorted({t for s in mine for t in s.get("not_like_for_like", [])})
            align = (mine[0].get("proxy_align") or entry.get("proxy_align") or {})
            note = (f"; ‡ not like-for-like, never compared: {', '.join(unlike)} (no `-align-queries-with-step`, "
                    "so it does not answer on the grid the stack's Loki does)" if unlike else "")
            out.append(f"- **{p}**: {sum(len(s['rows']) for s in mine)} shape×range"
                       + (f"; proxy flag {', '.join(f'{t} {a}' for t, a in sorted(align.items()))}" if align else "")
                       + note)
        elif info:
            out.append(f"- **{p}**: not run" + (f" ({entry['why_not']})" if entry.get("why_not") else ""))
    return out


def render(summaries, selection, meta, baseline=None, bounds=None):
    """baseline: the above-2× Loki list (over2x.read_baseline / baseline_at); None leaves that check out.
    bounds: the registry's perf: blocks at the base (over2x.bounds_at); None locks no row."""
    if not summaries:
        return skipped(selection, meta)
    b, c, r = summaries[0]["baseline"], summaries[0]["candidate"], summaries[0]["reference"]
    rows = [(s["set"], dict(row, profile=s.get("profile"), unlike=s.get("not_like_for_like", [])))
            for s in summaries for row in s["rows"]]
    rows.sort(key=lambda x: (ORDER.get(x[1]["verdict"], 9), x[0], x[1]["shape"]))
    valid = all(s.get("valid", True) for s in summaries)
    counts = {}
    for _, row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    new_diffs = [row for _, row in rows if new_difference(row)]
    pre = sum(1 for _, row in rows if row.get("parity_vs_reference") == "differs" and not new_difference(row))
    same_as_loki = sum(1 for _, row in rows if row.get("parity_vs_reference", "").startswith("same"))
    failed = bool(counts.get("broken") or counts.get("slower") or new_diffs)
    mode = meta.get("over_2x_loki", over2x.DEFAULT_GATE)
    findings = ([f for s in summaries for f in over2x.gate(s, baseline, meta.get("noise", 0.30),
                                                            meta.get("min_delta", 0.10), bounds)]
                if baseline is not None else [])
    bad = over2x.merge_failing(findings, summaries)  # one record per row, above 2× in either profile
    locked = over2x.locked_failing(bad)
    lost = [f for f in findings if f["kind"] == "lost_below"]
    failed_2x = mode == "fail" and bool(bad)

    if not valid:
        title, state = "⚠️ Performance A/B: invalid run (VictoriaLogs restarted), re-run the job", "invalid"
    elif failed:
        title, state = "🔴 Performance A/B: regression", "failed"
    elif failed_2x:
        title, state = ("🔴 Performance A/B: locked row above its bound" if locked else
                        "🔴 Performance A/B: above 2× Loki"), "failed"
    elif counts.get("faster") or counts.get("fixed"):
        title, state = "🟢 Performance A/B: faster, no regression", "passed"
    else:
        title, state = "⚪ Performance A/B: no change beyond noise", "passed"

    n = len(rows)
    verdict_bits = [f"{counts[k]} {ICON[k]} {k}" for k in ("broken", "slower", "fixed", "faster", "same") if counts.get(k)]
    others = [f for f in bad if not f.get("locked")]
    lines = [MARKER, f"### {title}", "", *(locked_section(locked, mode) if locked else []),
             *(over_2x_section(others, mode) if others else []),
             f"**{n} shape×range** — " + " · ".join(verdict_bits) +
             f" · results vs Loki: {same_as_loki} same, {len(new_diffs)} new difference(s), {pre} pre-existing", ""]
    prof_info = profile_lines(meta, summaries) if any(s.get("profile") for s in summaries) else []
    if prof_info:
        lines += ["Loki profiles compared (like for like):", "", *prof_info, ""]
    moved = [x for x in rows if x[1]["verdict"] not in ("same", "n/a") or new_difference(x[1])]
    still = [x for x in rows if x not in moved]
    if moved:
        lines += table(moved, b, c, r)
    else:
        lines.append(f"Every shape is within noise of the base build (±{meta.get('noise', 0.25) * 100:.0f}% "
                     f"and {meta.get('min_delta', 0.05) * 1000:.0f} ms).")
    if any(row.get("confirmed") for _, row in rows):
        lines += ["", f"¹ re-measured with {meta.get('confirm_runs', '?')} runs, on windows the first pass did not use, after it moved beyond noise."]
    if still:
        lines += ["", f"<details><summary>{len(still)} unchanged shape×range (within noise)</summary>", ""]
        lines += table(still, b, c, r)
        lines += ["", "</details>"]
    refresh = any("refresh" in row for _, row in rows)
    lines += ["", "<details><summary>Cold (first-run)" + (" and refresh" if refresh else "") +
              " timings — what a user opening a panel" + (" and then refreshing it" if refresh else "") +
              " sees</summary>", ""]
    lines += cold_table(rows, b, c, r)
    if any(over2x.cold_served(row, r) for _, row in rows):
        lines += ["", "† Loki's results cache answered this cold run (its own counters, or identical answers for "
                  "windows a minute apart): not a computed time, so it is in no ratio."]
    if any(row.get("cold_from_first_pass") for _, row in rows):
        lines += ["", "¹ (Loki cold) the confirm pass's cold run was cache-served, so every cold column of the row is "
                  "the first pass's."]
    if any(row.get("unlike") for _, row in rows):
        lines += ["", "‡ this build did not run the setting of the row's Loki profile (no "
                  "`-align-queries-with-step` in the aligned profile): shown, never compared."]
    if any(row.get("loki_cold_reused_from") for _, row in rows):
        lines += ["", "↺ Loki cold taken from the first set that ran the same query; Loki's cache answered this one."]
    lines += ["", "</details>"]

    known = [f for f in findings if f["kind"] == "known"]
    cleared = sum(1 for f in findings if f["kind"] == "cleared")
    loki_failed = sum(1 for f in findings if f["kind"] == "loki_failed")
    if known or cleared or loki_failed:
        lines += ["", f"<details><summary>{len(known)} shape×range above 2× Loki already (listed, or as slow on the "
                  "base build)</summary>", ""]
        if known:
            lines += over_2x_table(known[:KNOWN_CAP], with_note=True)
            if len(known) > KNOWN_CAP:
                lines.append(f"… and {len(known) - KNOWN_CAP} more (`verdict.json` counts them; the job's "
                             "`summary-<set>.json` files have every row).")
        if cleared:
            lines += ["", f"{cleared} listed shape×range within 2× Loki in this run (the daily run removes a row from "
                      "the list after two runs under 2×)."]
        if loki_failed:
            lines += ["", f"{loki_failed} shape×range×mode where Loki did not answer 200: not compared (not a proxy "
                      "defect)."]
        lines += ["", "</details>"]

    if lost:
        lines += ["", *lost_below_section(lost)]

    why = selection.get("why", {})
    if why:
        lines += ["", "<details><summary>Why these shapes</summary>", ""]
        by_reason = {}
        for key, reasons in why.items():
            for reason in reasons:
                by_reason.setdefault(reason, []).append(key)
        for reason, keys in sorted(by_reason.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            lines.append(f"- `{reason}` → {len(keys)}: " + ", ".join(cell(k) for k in sorted(keys)[:8]) +
                         (" …" if len(keys) > 8 else ""))
        lines += ["", "</details>"]

    stamp = [f"base `{meta.get('base', '?')[:10]}` → PR `{meta.get('head', '?')[:10]}`",
             f"{meta.get('runs', summaries[0].get('runs'))} runs (first is cold), warm p50",
             f"ranges {', '.join(meta.get('ranges', ['1h']))}",
             f"noise ±{meta.get('noise', 0.25) * 100:.0f}% and {meta.get('min_delta', 0.05) * 1000:.0f} ms",
             f"VictoriaLogs restarts {summaries[0].get('restart_before')}→{summaries[-1].get('restart_after')}"]
    if meta.get("elapsed_s"):
        stamp.append(f"{meta['elapsed_s'] / 60:.1f} min")
    if meta.get("phases_s"):
        stamp.append("phases " + ", ".join(f"{k} {v}s" for k, v in meta["phases_s"].items()))
    if meta.get("refresh_s"):
        stamp.append(f"refresh p50: windows moving forward {meta['refresh_s']} s per run, informational")
    lines += ["", "<sub>" + " · ".join(stamp) + ". Same runner, one fresh stack, targets interleaved per request. "
              "How to read and reproduce: `bench/ab/README.md`.</sub>"]
    exit_code = 2 if not valid else (1 if failed or failed_2x else 0)
    verdict = {"state": state, "failed": failed or failed_2x or not valid, "exit": exit_code, "counts": counts,
               "new_differences": len(new_diffs), "preexisting_differences": pre, "rows": n}
    if prof_info:
        verdict["profiles"] = {p: {"ran": any(s.get("profile") == p for s in summaries),
                                   "not_like_for_like": sorted({t for s in summaries if s.get("profile") == p
                                                                for t in s.get("not_like_for_like", [])})}
                               for p in stack.PROFILES}
    if baseline is not None:
        verdict["over_2x"] = {"mode": mode, "new": sum(1 for f in bad if f["kind"] == "new"),
                              "worsened": sum(1 for f in bad if f["kind"] == "worsened"), "known": len(known),
                              "cleared": cleared, "loki_not_200": loki_failed, "rows": bad,
                              "locked": len(locked), "lost_below": len(lost)}
    return "\n".join(lines) + "\n", verdict


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("summaries", nargs="*")
    ap.add_argument("--selection", required=True)
    ap.add_argument("--meta", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--verdict", default="")
    args = ap.parse_args()
    meta = load(args.meta) if args.meta else {}
    text, verdict = render([load(p) for p in args.summaries], load(args.selection), meta)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    if args.verdict:
        with open(args.verdict, "w") as f:
            json.dump(verdict, f, indent=1)
    print(text)
    return verdict["exit"]


if __name__ == "__main__":
    sys.exit(main())
