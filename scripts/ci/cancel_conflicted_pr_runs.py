#!/usr/bin/env python3
"""Cancel the pull request runs of open pull requests that conflict with main.

GitHub does not start `pull_request` runs while a pull request has a merge
conflict, but runs already queued or running when `main` moves keep going,
and their results are thrown away once the branch is rebased. The
`cancel-conflicted-pr-runs` workflow runs this on every push to `main`.

Only `pull_request` runs of a conflicting pull request's current head commit
are cancelled; `pull_request_target`, push, schedule and manual runs are left
alone.

Usage: GH_TOKEN=... GITHUB_REPOSITORY=owner/repo \
       scripts/ci/cancel_conflicted_pr_runs.py [--dry-run]
"""

import argparse
import json
import os
import subprocess
import sys
import time

ACTIVE = {"queued", "in_progress", "waiting", "pending", "requested"}

# GitHub computes mergeability in the background after the base moves and
# reports UNKNOWN until it is done. The first query waits a little too, so a
# value computed before the push is not read as current.
FIRST_QUERY_DELAY_SECONDS = 20
MERGEABLE_ATTEMPTS = 6
MERGEABLE_WAIT_SECONDS = 10


def gh_json(args):
    out = subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else None


def conflicting(prs):
    """The open pull requests GitHub reports as conflicting."""
    return [pr for pr in prs if pr.get("mergeable") == "CONFLICTING"]


def runs_to_cancel(runs, head_sha):
    """The active pull_request runs of one head commit."""
    return [
        run for run in runs
        if run.get("event") == "pull_request"
        and run.get("head_sha") == head_sha
        and run.get("status") in ACTIVE
    ]


def list_prs(repo):
    prs = []
    time.sleep(FIRST_QUERY_DELAY_SECONDS)
    for attempt in range(MERGEABLE_ATTEMPTS):
        prs = gh_json([
            "pr", "list", "--repo", repo, "--state", "open", "--base", "main",
            "--limit", "200", "--json", "number,headRefOid,mergeable",
        ]) or []
        if not any(pr.get("mergeable") == "UNKNOWN" for pr in prs):
            break
        if attempt + 1 < MERGEABLE_ATTEMPTS:
            time.sleep(MERGEABLE_WAIT_SECONDS)
    return prs


def list_runs(repo, head_sha):
    data = gh_json([
        "api", f"repos/{repo}/actions/runs?head_sha={head_sha}&per_page=100",
    ]) or {}
    return data.get("workflow_runs", [])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="list the runs without cancelling them")
    args = parser.parse_args(argv)

    repo = os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        print("GITHUB_REPOSITORY is not set", file=sys.stderr)
        return 2

    prs = list_prs(repo)
    unknown = [pr["number"] for pr in prs if pr.get("mergeable") == "UNKNOWN"]
    if unknown:
        print(f"mergeability still unknown, skipped: {', '.join(f'#{n}' for n in unknown)}")

    cancelled = 0
    for pr in conflicting(prs):
        for run in runs_to_cancel(list_runs(repo, pr["headRefOid"]), pr["headRefOid"]):
            print(f"#{pr['number']} conflicts with main: cancelling {run.get('name')} run {run['id']}")
            if not args.dry_run:
                # A run can finish between listing and cancelling; that is not an error.
                result = subprocess.run(
                    ["gh", "api", "-X", "POST", f"repos/{repo}/actions/runs/{run['id']}/cancel"],
                    capture_output=True, text=True,
                )
                if result.returncode != 0:
                    print(f"  not cancelled: {result.stderr.strip()}")
                    continue
            cancelled += 1
    verb = "would cancel" if args.dry_run else "cancelled"
    print(f"{len(conflicting(prs))} conflicting pull requests, {verb} {cancelled} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
