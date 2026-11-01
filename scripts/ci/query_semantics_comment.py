#!/usr/bin/env python3
"""Publish the query catalog bridge comment until the trusted reporter lands."""
import json
import os
import sys
from pathlib import Path

from pr_evidence import GitHub, latest_run, log_scope
from pr_quality_report import cell, fmt, mapping, table

MARKER = "<!-- pr-query-semantics-catalog -->"


def pinned_result(api, head, merge):
    runs = api.pages(f"actions/workflows/compat-loki.yaml/runs?event=pull_request&head_sha={head}", "workflow_runs", 2)
    run = latest_run(runs, head)
    if not run or run.get("status") != "completed":
        return {}, "—"
    run_id = int(run["id"])
    url = f"https://github.com/{api.repository}/actions/runs/{run_id}"
    jobs = api.pages(f"actions/runs/{run_id}/attempts/{int(run.get('run_attempt', 1))}/jobs", "jobs")
    job = next((item for item in jobs if item.get("name") == "loki-pinned" and item.get("status") == "completed"), None)
    if not job:
        return {}, url
    raw = api.request(f"actions/jobs/{int(job['id'])}/logs", binary=True)
    scope = log_scope(raw.decode("utf-8", errors="replace"), {"head": head, "merge": merge})
    return mapping(scope.get("query_semantics_matrix")) if scope.get("state") == "available" else {}, url


def render(base, head, observed, url):
    before = mapping(mapping(base.get("quality_inventory")).get("query_semantics"))
    after = mapping(mapping(head.get("quality_inventory")).get("query_semantics"))
    if before.get("performance_cases", 0) or after.get("state") != "available":
        return ""
    rows = []
    for key, label in (("cases", "Quality cases"), ("operations", "Tracked operations"),
                       ("depth_paths", "Parser × depth paths"),
                       ("exact_metric_points", "Exact metric-point contracts"),
                       ("performance_cases", "Catalogued latency scenarios"),
                       ("performance_shared", "Latency scenarios reusing a quality query")):
        rows.append([label, fmt(before.get(key)), fmt(after.get(key))])
    lines = [MARKER, "### Query semantics catalog and CI proof", "",
             "Declared base/PR counts from the PR Quality Report snapshots:", "",
             *table(["Signal", "Base", "PR"], rows), "",
             "Observed against pinned Loki in the same PR CI run:", "",
             *table(["Matrix", "Passed", "Failed", "Skipped"],
                    [["Live query cases", *(fmt(observed.get(key)) for key in ("passed", "failed", "skipped"))]]), ""]
    if observed:
        lines.append("Failing case IDs: " + (", ".join(cell(item) for item in observed.get("failed_cases", [])[:8]) or "none") + ".")
    else:
        lines.append("The pinned Loki result is pending or unavailable; no pass is inferred.")
    if url.startswith("https://github.com/"):
        lines.append(f"[Pinned Loki CI run]({url}).")
    lines.append("Performance counts describe retained scenarios, not measured speed. Visual evidence is reported separately.")
    return "\n".join(lines) + "\n"


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: query_semantics_comment.py BASE.json HEAD.json PR_NUMBER")
    base, head = (json.loads(Path(name).read_text()) for name in sys.argv[1:3])
    if mapping(mapping(base.get("quality_inventory")).get("query_semantics")).get("performance_cases", 0):
        return  # The trusted default-branch reporter already owns this section.
    api = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    number = int(sys.argv[3])
    pr = api.request(f"pulls/{number}")
    if pr.get("state") != "open" or pr["head"]["sha"] != os.environ["HEAD_SHA"]:
        return
    try:
        observed, url = pinned_result(api, pr["head"]["sha"], pr.get("merge_commit_sha"))
    except (KeyError, ValueError, TypeError, OSError, UnicodeError):
        observed, url = {}, "—"
    body = render(base, head, observed, url)
    if not body:
        return
    # Recheck the tested revision before writing the comment.
    current = api.request(f"pulls/{number}")
    if current.get("state") != "open" or current["head"]["sha"] != pr["head"]["sha"] or current["base"]["sha"] != pr["base"]["sha"]:
        return
    comments = api.pages(f"issues/{number}/comments")
    existing = next((c for c in comments if c.get("user", {}).get("login") == "github-actions[bot]" and MARKER in c.get("body", "")), None)
    if existing:
        api.request(f"issues/comments/{int(existing['id'])}", {"body": body}, "PATCH")
    else:
        api.request(f"issues/{number}/comments", {"body": body}, "POST")


if __name__ == "__main__":
    main()
