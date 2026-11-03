#!/usr/bin/env python3
"""Build and keep up to date the release metadata branch.

The metadata for a release (the CHANGELOG move, the chart version, the README
badges) is generated against the current tip of main, never against the commit
that was tagged. The CHANGELOG move itself reads the tagged commit
(changelog_release.py), so it carries exactly the entries that release shipped
and leaves every entry added since in `[Unreleased]`.

  prepare --version A.B.C   generate against main, commit the files on top of
                            main, publish them as release/metadata-vA.B.C, and
                            generate again when main moved while it ran.
  refresh                   for the open release/metadata-v* pull requests of
                            the release bot or the owner: regenerate the newest
                            one when what main needs differs from what the
                            branch holds, and close the older ones. The newest
                            branch files every tag section main lacks, so an
                            older pull request has nothing left to do.

The new commit is built on a temporary branch off main (an API commit, so it is
signed) and the pull request branch is then moved once, straight to that commit:
it is never set to main's tip first, where it would hold no change. When the
move is refused (a token without the workflow scope may not move a branch across
workflow-file changes) the branch is deleted and created again, and the pull
request is reopened or opened again; a pull request that cannot be brought back
fails the run.

Both commands need GH_TOKEN (the bot token) and GITHUB_REPOSITORY, and run in a
checkout with every tag. They reset the working tree to main's tip and run the
scripts main has there, which are the ones the release run itself started from
or newer.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
FILES = ["CHANGELOG.md", "README.md", "docs/observability.md", "charts/loki-vl-proxy/Chart.yaml"]
BRANCH_RE = re.compile(r"^release/metadata-v(\d+)\.(\d+)\.(\d+)$")
BADGE_PREFIXES = ("[![Tests](", "[![Coverage](", "[![Lines of Code](", "[![Lines](")
# The logins release-pr-validation accepts as the author of these pull requests.
ALLOWED_AUTHORS = {"reliablyobserve-bot", "szibis"}
ATTEMPTS = 3


def run(args, *, cwd=".", env=None, check=True, capture=True):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=capture, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed: {(result.stderr or result.stdout or '').strip()}")
    return result.stdout.strip() if capture else ""


def git_as_bot(repo, token, *args, check=True):
    """git against the remote with the token handed over by a credential helper."""
    helper = '!f() { test "$1" = get && printf "username=x-access-token\\npassword=%s\\n" "${GIT_AUTH_TOKEN}"; }; f'
    env = {**os.environ, "GIT_AUTH_TOKEN": token}
    return run(["git", "-c", "credential.helper=", "-c", f"credential.helper={helper}", *args], cwd=repo, env=env, check=check)


def gh(token, *args, check=True):
    return run(["gh", *args], env={**os.environ, "GH_TOKEN": token}, check=check)


def gh_ok(token, *args):
    return subprocess.run(["gh", *args], env={**os.environ, "GH_TOKEN": token}, capture_output=True).returncode == 0


def branch_name(version):
    return f"release/metadata-v{version}"


def temp_branch_name(version):
    return f"release/metadata-tmp-{version}"


def copy_badges(source, target):
    """Put the Tests/Coverage/Lines badge lines of `source` into `target`."""
    wanted = {}
    for line in source.split("\n"):
        for prefix in BADGE_PREFIXES:
            if line.startswith(prefix):
                wanted.setdefault(prefix, line)
    out = []
    for line in target.split("\n"):
        for prefix, new in wanted.items():
            if line.startswith(prefix):
                line = new
                break
        out.append(line)
    return "\n".join(out)


def pr_body(version, warnings):
    lines = [
        f"Sync release metadata files for v{version} after the automated release:",
        "",
        "- CHANGELOG: the entries the tags shipped move into their sections; entries added since stay in Unreleased",
        "- Helm chart version and appVersion",
        "- README release badges",
    ]
    if warnings:
        lines += ["", "Entries that needed a decision (check the CHANGELOG sections before merging):", ""]
        lines += [f"- {w}" for w in warnings]
    return "\n".join(lines) + "\n"


def generate(repo, version, repo_slug, tests="", coverage="", code_lines=""):
    """Rewrite the metadata files in the working tree; return (changed files, warnings)."""
    with tempfile.NamedTemporaryFile("r", suffix=".warnings") as warn:
        env = {**os.environ, "RELEASE_WARNINGS_FILE": warn.name}
        run(["bash", str(HERE / "sync_release_metadata.sh"), version, "", tests, coverage, code_lines, ".", repo_slug],
            cwd=repo, env=env)
        warnings = [l for l in Path(warn.name).read_text(encoding="utf-8").split("\n") if l]
    changed = run(["git", "diff", "--name-only", "--", *FILES], cwd=repo)
    return [f for f in changed.split("\n") if f], warnings


def fetch_main(repo, token):
    git_as_bot(repo, token, "fetch", "--force", "--tags", "origin", "main")
    return run(["git", "rev-parse", "origin/main"], cwd=repo)


# ---- branches --------------------------------------------------------------


def ref_exists(slug, token, branch):
    return gh_ok(token, "api", f"repos/{slug}/git/ref/heads/{branch}")


def delete_branch(slug, token, branch):
    gh_ok(token, "api", "-X", "DELETE", f"repos/{slug}/git/refs/heads/{branch}")


def create_branch(slug, token, branch, sha):
    gh(token, "api", "-X", "POST", f"repos/{slug}/git/refs", "-f", f"ref=refs/heads/{branch}", "-f", f"sha={sha}")


def commit(repo, slug, branch, base, version, files, token):
    """A signed API commit of `files` on top of base, on a branch that is created for it."""
    script = os.environ.get("COMMIT_VIA_API", str(HERE / "commit_via_api.sh"))
    env = {**os.environ, "GH_TOKEN": token}
    return run(["bash", script, slug, branch, base, f"docs: sync release metadata for v{version}", *files], cwd=repo, env=env)


def publish_branch(repo, slug, token, branch, base, version, files):
    """Put the commit on `branch`; returns (sha, recreated).

    The commit is made on a temporary branch off `base`, then `branch` is created
    at it, or moved to it in one step. `recreated` is true when the branch had to
    be deleted and created again (its pull request is then closed by GitHub).
    """
    tmp = temp_branch_name(version)
    delete_branch(slug, token, tmp)
    sha = commit(repo, slug, tmp, base, version, files, token)
    recreated = False
    try:
        if not ref_exists(slug, token, branch):
            create_branch(slug, token, branch, sha)
        elif not gh_ok(token, "api", "-X", "PATCH", f"repos/{slug}/git/refs/heads/{branch}", "-f", f"sha={sha}",
                       "-F", "force=true"):
            print(f"::warning::{branch} could not be moved; deleting it and creating it again")
            delete_branch(slug, token, branch)
            create_branch(slug, token, branch, sha)
            recreated = True
    finally:
        delete_branch(slug, token, tmp)
    return sha, recreated


# ---- prepare ---------------------------------------------------------------


def prepare(repo, version, slug, token, tests="", coverage="", code_lines="", body_out=None):
    """Returns (has_changes, branch). Regenerates while main keeps moving."""
    branch = branch_name(version)
    pushed = False
    for attempt in range(1, ATTEMPTS + 1):
        try:
            base = fetch_main(repo, token)
            run(["git", "reset", "--hard", "-q", base], cwd=repo)
            files, warnings = generate(repo, version, slug, tests, coverage, code_lines)
            if not files:
                print(f"release metadata for v{version} is already on main {base[:8]}")
                return pushed, branch
            for w in warnings:
                print(f"::warning::{w}")
            sha, _ = publish_branch(repo, slug, token, branch, base, version, files)
            pushed = True
            print(f"release metadata commit {sha} on {branch}, on main {base[:8]} (attempt {attempt})")
            if body_out:
                Path(body_out).write_text(pr_body(version, warnings), encoding="utf-8")
            if fetch_main(repo, token) == base:
                return True, branch
            print("main moved while the branch was built; regenerating from the new main")
        except RuntimeError as exc:
            if not pushed:
                raise
            # The branch exists from an earlier attempt: keep it, so its pull
            # request is opened and the refresh job brings it up to date.
            print(f"::warning::regenerating the release metadata failed ({exc}); keeping the branch as pushed")
            return True, branch
    print("::warning::main kept moving; the refresh job will bring the branch up to date")
    return True, branch


# ---- refresh ---------------------------------------------------------------


def gh_json(token, *args):
    return json.loads(gh(token, *args) or "null")


def open_metadata_prs(slug, token):
    """Open metadata pull requests of the bot or the owner, from this repository, oldest first."""
    prs = gh_json(token, "pr", "list", "--repo", slug, "--base", "main", "--state", "open", "--limit", "100",
                  "--json", "number,headRefName,author,isCrossRepository") or []
    found = []
    for pr in prs:
        match = BRANCH_RE.match(pr["headRefName"])
        login = ((pr.get("author") or {}).get("login") or "").lower()
        if match and not pr.get("isCrossRepository") and login in ALLOWED_AUTHORS:
            found.append((tuple(int(g) for g in match.groups()), pr["number"], pr["headRefName"]))
    return sorted(found)


def close_pr(slug, token, number, why):
    gh(token, "pr", "close", str(number), "--repo", slug, "--delete-branch", "--comment", why, check=False)


def pr_state(slug, token, number):
    return (gh_json(token, "pr", "view", str(number), "--repo", slug, "--json", "state") or {}).get("state", "")


def ensure_open(slug, token, number, branch, version, body_file):
    """The pull request is open after its branch moved; bring it back or fail."""
    if pr_state(slug, token, number) == "OPEN":
        return number
    print(f"::warning::#{number} is {pr_state(slug, token, number) or 'gone'}; reopening it")
    gh(token, "pr", "reopen", str(number), "--repo", slug, check=False)
    if pr_state(slug, token, number) != "OPEN":
        gh(token, "pr", "create", "--repo", slug, "--base", "main", "--head", branch,
           "--title", f"docs: sync release metadata for v{version}", "--body-file", body_file)
        number = next((n for _, n, b in open_metadata_prs(slug, token) if b == branch), None)
        if number is None:
            raise RuntimeError(f"the pull request for {branch} could not be reopened or opened again")
    # Merging stays with the owner's approval and the checks; auto-merge only arms it.
    gh(token, "pr", "merge", str(number), "--repo", slug, "--auto", "--squash", check=False)
    return number


def refresh(repo, slug, token):
    """Bring the newest open metadata pull request up to date with main."""
    prs = open_metadata_prs(slug, token)
    if not prs:
        print("no open release metadata pull request")
        return
    version_t, number, branch = prs[-1]
    version = ".".join(str(p) for p in version_t)
    older = [n for _, n, _ in prs[:-1]]

    base = fetch_main(repo, token)
    git_as_bot(repo, token, "fetch", "--force", "origin", f"{branch}:refs/remotes/origin/{branch}")
    run(["git", "reset", "--hard", "-q", base], cwd=repo)
    files, warnings = generate(repo, version, slug)
    # The badge numbers come from the release run's tests; keep the branch's.
    branch_readme = run(["git", "show", f"origin/{branch}:README.md"], cwd=repo, check=False)
    if branch_readme:
        readme = Path(repo) / "README.md"
        text = readme.read_text(encoding="utf-8")
        updated = copy_badges(branch_readme, text)
        if updated != text:
            readme.write_text(updated, encoding="utf-8")
        files = [f for f in run(["git", "diff", "--name-only", "--", *FILES], cwd=repo).split("\n") if f]

    def close_older():
        # Only after the newest one is known to be right.
        for n in older:
            close_pr(slug, token, n, f"Superseded by #{number}, which files every release section main lacks.")

    if not files:
        close_pr(slug, token, number, f"main already carries the v{version} release metadata.")
        close_older()
        return
    # The branch is touched only when what main needs differs from what it holds.
    # A merge of a branch that holds the right files is correct whatever else main
    # changed, and rewriting it would replace a commit the owner re-signed and
    # start its checks again.
    stale = subprocess.run(["git", "diff", "--quiet", f"origin/{branch}", "--", *FILES], cwd=repo).returncode != 0
    if not stale:
        print(f"#{number} ({branch}) holds what main needs")
        close_older()
        return
    sha, recreated = publish_branch(repo, slug, token, branch, base, version, files)
    body_file = tempfile.mkstemp(suffix=".md")[1]
    Path(body_file).write_text(pr_body(version, warnings), encoding="utf-8")
    number = ensure_open(slug, token, number, branch, version, body_file)
    if warnings and not recreated:
        gh(token, "pr", "edit", str(number), "--repo", slug, "--body-file", body_file, check=False)
    print(f"#{number}: regenerated {branch} on main {base[:8]} as {sha}")
    close_older()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=".")
    sub = parser.add_subparsers(dest="cmd", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--version", required=True)
    prep.add_argument("--tests", default="")
    prep.add_argument("--coverage", default="")
    prep.add_argument("--lines", default="")
    prep.add_argument("--github-output", default="")
    prep.add_argument("--body-file", default="", help="write the pull request body here")
    sub.add_parser("refresh")
    args = parser.parse_args(argv)

    slug = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    if args.cmd == "refresh":
        refresh(args.repo, slug, token)
        return 0
    if not re.match(r"^\d+\.\d+\.\d+$", args.version):
        print(f"::error::release version '{args.version}' is not MAJOR.MINOR.PATCH", file=sys.stderr)
        return 1
    changes, branch = prepare(args.repo, args.version, slug, token, args.tests, args.coverage, args.lines,
                              args.body_file or None)
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            fh.write(f"has_changes={'true' if changes else 'false'}\nbranch={branch}\n")
            if args.body_file and Path(args.body_file).exists():
                fh.write(f"body_file={args.body_file}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
