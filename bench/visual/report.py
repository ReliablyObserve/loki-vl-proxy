#!/usr/bin/env python3
"""The "Visual proof" section of a pull request description.

  report.py OUT --pr 641 [--before 1192befd --after 34fb774a] > visual-proof.md

Reads OUT/compare.json, OUT/pixeldiff.json and OUT/montage/*.png; images are
referenced from the pr-visuals branch (pr-<number>/<panel>-<range>.png).
"""
import argparse
import os

from vio import load_json

RAW = "https://raw.githubusercontent.com/ReliablyObserve/loki-vl-proxy/pr-visuals"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--pr", required=True)
    ap.add_argument("--before", default="main")
    ap.add_argument("--after", default="the PR")
    ap.add_argument("--raw", default=RAW)
    a = ap.parse_args()
    rows = load_json(os.path.join(a.out, "compare.json"))
    px = load_json(os.path.join(a.out, "pixeldiff.json"))
    pages = {}
    for r in rows:
        pages.setdefault(r["page"], []).append(r)
    order = {"15m": 0, "1h": 1, "6h": 2, "24h": 3, "7d": 4, "live": 5}
    tot = sum(r["requests"] for r in rows)
    bad = [r for r in rows if r["main_pr_diffs"]]
    print(f"Before = `{a.before}`, after = `{a.after}`, reference = Loki, all on VictoriaLogs data through Grafana. "
          f"{len(rows)} page/range captures, {tot} backend requests compared; "
          f"{len(rows) - len(bad)} identical between before and after, {len(bad)} with differences listed below.\n")
    for page, rs in sorted(pages.items()):
        print(f"<details><summary><b>{page}</b></summary>\n")
        print("| range | before = after (identical requests) | after vs Loki | pixel diff | before \\| after \\| Loki |")
        print("|---|---|---|---|---|")
        for r in sorted(rs, key=lambda r: order[r["range"]]):
            k = f"{page}-{r['range']}"
            img = f"{a.raw}/pr-{a.pr}/{k}.png"
            print(f"| {r['range']} | {r['main_vs_pr']} | {r['pr_vs_loki']} | {px.get(k, '')} | ![{k}]({img}) |")
        print("\n</details>\n")
    if bad:
        print("Differences between before and after:\n")
        for r in bad:
            print(f"- {r['page']} {r['range']}: " + "; ".join(x[:200] for x in r["main_pr_diffs"][:3]))
        print()


if __name__ == "__main__":
    main()
