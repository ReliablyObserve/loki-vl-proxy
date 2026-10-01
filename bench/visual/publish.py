#!/usr/bin/env python3
"""Maintain the orphan pr-visuals branch: montages under pr-<number>/.

  publish.py publish --montage DIR --pr 123 --remote URL [--branch pr-visuals]
  publish.py remove  --pr 123 --remote URL        a closed pull request's folder
  publish.py prune   --keep 12,34 --remote URL    every pr-* folder not in the list

Every change is one commit with no parent whose tree is the previous tree plus
the change, pushed with --force-with-lease against the tip it was built on. The
branch therefore never grows a history (old images are unreachable at once), and
a push that loses a race with another job is rebuilt on the new tip. The PR's
folder is removed and rewritten, nothing else is touched. The commit is by the
github-actions identity (CI cannot sign).

The montage directory is untrusted (a pull request built it): it is accepted only
when every entry is a regular file (no symlink, no directory) named like
`page-range.png`, within 300 KB, and there are at most 80 of them.

The token, when PUBLISH_TOKEN is set, goes to git as an Authorization header,
never into the command line, the remote URL or the output. The repository's
push-triggered workflows run for main only, and a push made with the workflow
token starts none.

Prints the commit sha (or "unchanged"). Exits 1 on failure. Run it from the base
branch's checkout: it holds a write token.
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
FOLDER = re.compile(r"^pr-[0-9]+$")
MAX_BYTES = 300_000
MAX_FILES = 80
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


def git(repo, *args, check=True, auth=None):
    cmd = ["git", "-C", repo]
    if auth:
        cmd += ["-c", f"http.extraheader=AUTHORIZATION: basic {auth}"]
    cmd += ["-c", f"user.name={BOT[0]}", "-c", f"user.email={BOT[1]}", "-c", "commit.gpgsign=false", *args]
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def montages(src):
    """The files to publish, validated: regular files only, plain PNG names, within the size and count limits."""
    names = sorted(os.listdir(src))
    bad = []
    for n in names:
        p = os.path.join(src, n)
        if not (NAME.match(n) and os.path.isfile(p) and not os.path.islink(p) and os.path.getsize(p) <= MAX_BYTES):
            bad.append(n)
    if bad:
        raise SystemExit(f"refusing to publish (name, type or size): {bad[:5]}")
    if len(names) > MAX_FILES:
        raise SystemExit(f"refusing to publish {len(names)} files (limit {MAX_FILES})")
    return names


def run_change(change, remote, branch, token="", attempts=4):
    """Apply change(repo) to the branch tip as one parentless commit; push with a lease on that tip."""
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode() if token else None
    with tempfile.TemporaryDirectory(prefix="pr-visuals-") as repo:
        git(repo, "init", "-q")
        git(repo, "remote", "add", "origin", remote)
        for attempt in range(attempts):
            have = git(repo, "fetch", "-q", "--depth=1", "origin", branch, check=False, auth=auth)
            if have.returncode == 0:
                tip = git(repo, "rev-parse", "FETCH_HEAD").stdout.strip()
                git(repo, "checkout", "-q", "--detach", tip)
                git(repo, "clean", "-fdxq")
            else:
                tip = ""
                git(repo, "checkout", "-q", "--orphan", f"{branch}-new")
                git(repo, "rm", "-rfq", "--ignore-unmatch", ".", check=False)
            change(repo)
            git(repo, "add", "-A")
            tree = git(repo, "write-tree").stdout.strip()
            if tree == (git(repo, "rev-parse", f"{tip}^{{tree}}").stdout.strip() if tip else EMPTY_TREE):
                return "unchanged"
            sha = git(repo, "commit-tree", tree, "-m", "visual smoke montages").stdout.strip()
            lease = f"--force-with-lease=refs/heads/{branch}:{tip}"  # empty tip: the branch must not exist yet
            push = git(repo, "push", "-q", lease, "origin", f"{sha}:refs/heads/{branch}", check=False, auth=auth)
            if push.returncode == 0:
                return sha
            err = push.stderr.strip()[-300:]
            print(f"push attempt {attempt + 1} failed: {err.replace(token, '***') if token else err}", file=sys.stderr)
        raise SystemExit("could not push to " + branch)


def publish(src, pr, remote, branch, token="", attempts=4):
    names = montages(src)
    folder = f"pr-{int(pr)}"

    def change(repo):
        shutil.rmtree(os.path.join(repo, folder), ignore_errors=True)  # prune this PR's folder first
        os.makedirs(os.path.join(repo, folder))
        for n in names:
            shutil.copyfile(os.path.join(src, n), os.path.join(repo, folder, n))

    return run_change(change, remote, branch, token, attempts)


def remove(prs, remote, branch, token="", keep=None):
    """Drop the folders of `prs`, or every pr-* folder not in `keep`."""
    def change(repo):
        for d in sorted(os.listdir(repo)):
            number = d[3:]
            if FOLDER.match(d) and ((keep is not None and number not in keep) or (prs and number in prs)):
                shutil.rmtree(os.path.join(repo, d), ignore_errors=True)

    return run_change(change, remote, branch, token)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["publish", "remove", "prune"])
    ap.add_argument("--montage")
    ap.add_argument("--pr", default="")
    ap.add_argument("--keep", default="")
    ap.add_argument("--remote", required=True)
    ap.add_argument("--branch", default="pr-visuals")
    a = ap.parse_args()
    token = os.environ.get("PUBLISH_TOKEN", "")
    if a.cmd == "publish":
        print(publish(a.montage, a.pr, a.remote, a.branch, token))
    elif a.cmd == "remove":
        print(remove({str(int(a.pr))}, a.remote, a.branch, token))
    else:
        print(remove(set(), a.remote, a.branch, token, keep={k for k in a.keep.split(",") if k.isdigit()}))


if __name__ == "__main__":
    main()
