#!/usr/bin/env python3
"""What a baseline run leaves behind: files a person can accept as the new baseline.

A baseline run (change scope: the `baseline` label, or a big shipped diff on a
pull request into main; selection.py --all) measures every shape of every set.
pr_smoke.py --baseline-candidate DIR then writes, from that run's summaries:

  DIR/over-2x-loki.json   the above-2x Loki list as it would be after this run: the
                          committed list's rows for set x range pairs this run did
                          not measure, plus the rows this run measured above 2x
  DIR/acceptance-day.json one day in the shape of an entry of bench/ab/history/acceptance.json:
                          where the compared rows sit against Loki (below, close,
                          1-2x, above 2x) per mode. A report, not a drop-in file:
                          history/acceptance.json is regenerated from history/*.jsonl
                          by every daily run
  DIR/candidate.md        the "new baseline candidate" section of the PR comment
  DIR/meta.json           what was measured, on which commit

Nothing here touches bench/ab/history/ or any committed file: the CI job uploads
DIR as an artifact and the comment says where it is. Accepting means:

  over-2x-loki.json   a person copies it over bench/ab/history/over-2x-loki.json in a
                      pull request (check_conformance.py checks it against the registry
                      locks; the daily run keeps it from there)
  acceptance-day.json a report to read. It is not copied: acceptance.json is rebuilt from
                      the history lines, so the real new distribution is the next daily
                      run's, which runs whenever a measured path changed on main
  the visual captures a report to read

The run uses the PR smoke's profile (4 runs, 1h, the smoke's line rate), not the daily
run's (7 runs, ranges up to 24h, a third of the line rate): its numbers show the
direction, not row for row the committed ones. The file keeps the committed rows of the
ranges it did not measure (24h, 7d) instead of dropping them.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import over2x  # noqa: E402

ROOT = over2x.ROOT
ACCEPTANCE_REL = "bench/ab/history/acceptance.json"
ARTIFACT = "perf-baseline-candidate"
LISTED = 10  # rows named in the comment; the file has them all
# A comment is limited to 65,536 characters: leave room for the section below the tables.
COMMENT_LIMIT = 60000


def measured_pairs(summaries):
    """{(profile, set, range)} the summaries cover: a profile that did not run leaves its rows as committed."""
    return {(over2x.profile_of(s), s["set"], row["range"]) for s in summaries for row in s["rows"]}


def bands(summaries):
    """{profile: [(mode, band)]} of every comparable row (both answered 200; on cold, Loki's cache did not answer;
    a build that is not like-for-like with the profile has no pair)."""
    out = {}
    for s in summaries:
        for row in s["rows"]:
            for mode, pair in over2x.summary_pairs(s, row, s["candidate"]).items():
                if over2x.judge(*pair) in ("above", "within"):
                    out.setdefault(over2x.profile_of(s), []).append((mode, over2x.band(pair[0], pair[1])))
    return out


def candidate_over_2x(summaries, committed, date="", commit=""):
    """(rows, added, removed): the list after this run, and how it differs from the committed one.

    A committed row whose set x range was measured is kept only if this run
    measured it above 2x again (the daily run's two-runs rule is not applied
    here: a person decides whether one run is enough). Rows of pairs not
    measured stay as committed.
    """
    seen = measured_pairs(summaries)
    fresh = {over2x.key(r): r for r in over2x.seed(summaries, date=date, commit=commit)}
    pair = lambda v: (over2x.profile_of(v), v["set"], v["range"])  # noqa: E731
    rows = {k: v for k, v in committed.items() if pair(v) not in seen}
    rows.update(fresh)
    added = sorted(k for k in fresh if k not in committed)
    removed = sorted(k for k, v in committed.items() if pair(v) in seen and k not in fresh)
    return list(rows.values()), added, removed


def acceptance_day(summaries, date="", commit=""):
    """One entry of acceptance.json's days, for this run: the distribution per Loki profile that ran."""
    profiles = {}
    for profile, pairs in bands(summaries).items():
        dist = over2x.distribution(pairs)
        profiles[profile] = {"sets": sum(1 for s in summaries if over2x.profile_of(s) == profile),
                             **{m: {k: dist[m][k] for k in ("rows", "share_below", "below", "close", "between", "above")}
                                for m in dist}}
    return {"date": date, "commits": [commit[:10]] if commit else [], "profiles": profiles}


def committed_day(ref, root=ROOT):
    """The newest day of acceptance.json as committed at ref; {} when there is none."""
    try:
        text = subprocess.run(["git", "show", f"{ref}:{ACCEPTANCE_REL}"], cwd=root, check=True,
                              capture_output=True, text=True).stdout
        days = json.loads(text).get("days") or []
    except (subprocess.CalledProcessError, ValueError, AttributeError):
        return {}
    return days[-1] if days else {}


def render(day, committed, added, removed, rows, reasons=(), elapsed_s=0):
    """The comment section."""
    lines = ["", "<details open><summary>🧭 New baseline candidate</summary>", "",
             "This pull request got the baseline run" + (": " + "; ".join(reasons) if reasons else "") +
             f". Every shape of every set was measured at the smoke's ranges (1h, 4 runs). Nothing was committed. The " +
             f"`{ARTIFACT}` artifact of this run holds `over-2x-loki.json`, which a person can copy over " +
             "`bench/ab/history/over-2x-loki.json` to accept it, and `acceptance-day.json`, a report: the committed " +
             "`acceptance.json` is rebuilt from the daily history, so the next daily run after the merge writes the real " +
             "new distribution.", "",
             "| profile | mode | rows compared | below Loki | close | 1–2× | above 2× | share below | " +
             "committed share below |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for profile, by_mode in day.get("profiles", {}).items():
        # a committed day from before the profiles existed holds its modes at the top: they are aligned
        old_day = (committed.get("profiles") or {}).get(profile) or (committed if profile == over2x.LEGACY_PROFILE
                                                                      and "profiles" not in committed else {})
        for mode in over2x.MODES:
            d = by_mode.get(mode)
            if not d:
                continue
            old = old_day.get(mode, {})
            lines.append(f"| {profile} | {mode} | {d['rows']} | {d['below']} | {d['close']} | {d['between']} | "
                         f"{d['above']} | {d['share_below']:.1f}% | "
                         + (f"{old['share_below']:.1f}% ({committed.get('date', '?')})" if old else "—") + " |")
    lines += ["", "The committed column is the newest daily run (7 runs, ranges up to 24h): the direction is comparable, " +
              "the rows are not the same ones.",
              "", f"Above-2× list: {len(rows)} rows after this run, {len(added)} added, {len(removed)} not above 2× in " +
              "this run's profile (the daily run's two-run rule decides for the committed list). A profile that did not "
              "run leaves its committed rows as they are."]
    for title, keys in (("Added", added), ("Not above 2× in this run", removed)):
        if keys:
            lines += ["", f"{title}:"]
            lines += [f"- {k[0]} / `{k[1]}` / {k[2]} / {k[3]} / {k[4]}" for k in keys[:LISTED]]
            if len(keys) > LISTED:
                lines.append(f"- … and {len(keys) - LISTED} more (in `over-2x-loki.json`)")
    if elapsed_s:
        lines += ["", f"<sub>{elapsed_s / 60:.0f} min.</sub>"]
    lines += ["", "</details>", ""]
    return "\n".join(lines)


def write(out_dir, summaries, committed_rows, committed_acceptance, date="", commit="", reasons=(), elapsed_s=0):
    """Write the candidate files; returns the comment section. Raises nothing for an invalid run:
    an invalid run (VictoriaLogs restarted) is not evidence and gets a section saying so."""
    os.makedirs(out_dir, exist_ok=True)
    if not summaries or not all(s.get("valid", True) for s in summaries):
        text = ("\n<details open><summary>🧭 New baseline candidate</summary>\n\nNot written: the run was invalid "
                "(VictoriaLogs restarted) or measured nothing. Re-run the job.\n\n</details>\n")
        with open(os.path.join(out_dir, "candidate.md"), "w") as f:
            f.write(text)
        return text
    rows, added, removed = candidate_over_2x(summaries, committed_rows, date, commit)
    day = acceptance_day(summaries, date, commit)
    over2x.write_baseline(rows, os.path.join(out_dir, over2x.BASELINE_NAME))
    with open(os.path.join(out_dir, "acceptance-day.json"), "w") as f:
        json.dump({"about": "A report: one baseline run's distribution of proxy ÷ Loki, in the shape of an entry of "
                            "bench/ab/history/acceptance.json (its days list), measured with the PR smoke's profile "
                            "(4 runs, 1h). Not a replacement for that file: it is regenerated from the history lines "
                            "by every daily run. Not committed.",
                   "days": [day]}, f, indent=1, sort_keys=True, ensure_ascii=False)
        f.write("\n")
    meta = {"commit": commit, "date": date, "sets": [s["set"] for s in summaries],
            "rows": sum(len(s["rows"]) for s in summaries), "reasons": list(reasons),
            "over_2x_rows": len(rows), "added": len(added), "removed": len(removed)}
    with open(os.path.join(out_dir, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    text = render(day, committed_acceptance, added, removed, rows, reasons, elapsed_s)
    with open(os.path.join(out_dir, "candidate.md"), "w") as f:
        f.write(text)
    return text


def fit(text, limit=COMMENT_LIMIT):
    """Make a comment fit GitHub's limit by replacing its largest <details> blocks with a pointer."""
    while len(text) > limit:
        blocks, pos = [], 0
        while True:
            start = text.find("<details", pos)
            if start < 0:
                break
            end = text.find("</details>", start)
            if end < 0:
                break
            end += len("</details>")
            body = text[start:end]
            # Only the A/B tables; never the candidate section or an already shortened block.
            if "New baseline candidate" not in body and "omitted to fit" not in body:
                blocks.append((len(body), start, end))
            pos = end
        if not blocks:
            return text[:limit]
        size, start, end = max(blocks)
        text = (text[:start] + f"<details><summary>{size} characters omitted to fit the comment limit; every row is "
                f"in the `summary-<set>.json` files of the `perf-ab-smoke` artifact</summary></details>" + text[end:])
    return text
