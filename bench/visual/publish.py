#!/usr/bin/env python3
"""Publish a run's montages to the orphan pr-visuals branch under pr-<number>/.

  publish.py --montage DIR --pr 123 --remote https://github.com/OWNER/NAME.git [--branch pr-visuals]

The PR's folder is removed and rewritten in one commit by the github-actions
identity (CI cannot sign); nothing else on the branch is touched. A push that
loses a race with another PR's job is redone on the new tip. The token, when
PUBLISH_TOKEN is set, goes to git as an Authorization header, never into the
command line, the remote URL or the output. The repository's push-triggered
workflows run for main only, and a push made with the workflow token starts
none, so publishing here triggers nothing.

Prints the commit sha (or "unchanged"). Exits 1 on failure.
"""
import argparse
import base64
import os
import re
import shutil
import subprocess
import sys
import tempfile

BOT = ("github-actions[bot]", "41898282+github-actions[bot]@users.noreply.github.com")
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*\.png$")
MAX_BYTES = 300_000
MAX_FILES = 80


def git(repo, *args, check=True, auth=None):
    cmd = ["git", "-C", repo]
    if auth:
        cmd += ["-c", f"http.extraheader=AUTHORIZATION: basic {auth}"]
    cmd += ["-c", f"user.name={BOT[0]}", "-c", f"user.email={BOT[1]}", "-c", "commit.gpgsign=false", *args]
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def montages(src):
    """The files to publish: PNGs with plain names, each within the size limit."""
    names = sorted(os.listdir(src))
    bad = [n for n in names if not NAME.match(n) or os.path.getsize(os.path.join(src, n)) > MAX_BYTES]
    if bad:
        raise SystemExit(f"refusing to publish (name or size): {bad[:5]}")
    if len(names) > MAX_FILES:
        raise SystemExit(f"refusing to publish {len(names)} files (limit {MAX_FILES})")
    return names


def publish(src, pr, remote, branch, token="", attempts=4):
    names = montages(src)
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode() if token else None
    folder = f"pr-{int(pr)}"
    with tempfile.TemporaryDirectory(prefix="pr-visuals-") as repo:
        git(repo, "init", "-q")
        git(repo, "remote", "add", "origin", remote)
        for attempt in range(attempts):
            have = git(repo, "fetch", "-q", "--depth=1", "origin", branch, check=False, auth=auth)
            if have.returncode == 0:
                git(repo, "checkout", "-q", "-B", branch, "FETCH_HEAD")
            else:
                git(repo, "checkout", "-q", "--orphan", branch)
                git(repo, "rm", "-rfq", "--ignore-unmatch", ".", check=False)
            shutil.rmtree(os.path.join(repo, folder), ignore_errors=True)  # prune this PR's folder first
            os.makedirs(os.path.join(repo, folder))
            for n in names:
                shutil.copy(os.path.join(src, n), os.path.join(repo, folder, n))
            git(repo, "add", "-A", "--", folder)
            if not git(repo, "status", "--porcelain", "--", folder).stdout.strip():
                return "unchanged"
            git(repo, "commit", "-q", "-m", f"visual smoke: montages of pull request {int(pr)}")
            push = git(repo, "push", "-q", "origin", f"HEAD:refs/heads/{branch}", check=False, auth=auth)
            if push.returncode == 0:
                return git(repo, "rev-parse", "HEAD").stdout.strip()
            err = push.stderr.strip()[-300:]
            print(f"push attempt {attempt + 1} failed: {err.replace(token, '***') if token else err}", file=sys.stderr)
        raise SystemExit("could not push to " + branch)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--montage", required=True)
    ap.add_argument("--pr", required=True)
    ap.add_argument("--remote", required=True)
    ap.add_argument("--branch", default="pr-visuals")
    a = ap.parse_args()
    print(publish(a.montage, a.pr, a.remote, a.branch, os.environ.get("PUBLISH_TOKEN", "")))


if __name__ == "__main__":
    main()
