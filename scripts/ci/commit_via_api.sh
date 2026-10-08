#!/usr/bin/env bash
# commit_via_api.sh REPO BRANCH BASE_SHA MESSAGE FILE...
#
# Commits the working-tree contents of FILE... onto BRANCH, first reset to
# BASE_SHA, through GitHub's createCommitOnBranch GraphQL mutation. GitHub signs
# commits it creates through its API, so the commit is Verified and passes a
# "require signed commits" rule without an admin bypass (a local `git commit` by
# github-actions[bot] is unsigned). A FILE that no longer exists is deleted.
# Needs GH_TOKEN with contents:write. Prints the new commit's oid.
set -euo pipefail

if [ "$#" -lt 5 ]; then
  echo "usage: $0 REPO BRANCH BASE_SHA MESSAGE FILE..." >&2
  exit 2
fi
repo=$1 branch=$2 base=$3 message=$4
shift 4

# Point the branch at BASE_SHA: create it, or force an existing one back.
if gh api "repos/${repo}/git/ref/heads/${branch}" >/dev/null 2>&1; then
  gh api -X PATCH "repos/${repo}/git/refs/heads/${branch}" -f sha="${base}" -F force=true >/dev/null
else
  gh api -X POST "repos/${repo}/git/refs" -f ref="refs/heads/${branch}" -f sha="${base}" >/dev/null
fi

payload=$(mktemp)
trap 'rm -f "${payload}"' EXIT
python3 - "${repo}" "${branch}" "${base}" "${message}" "$@" >"${payload}" <<'PY'
import base64
import json
import os
import sys

repo, branch, base, message, *files = sys.argv[1:]
additions, deletions = [], []
for path in files:
    if os.path.exists(path):
        with open(path, "rb") as f:
            additions.append({"path": path, "contents": base64.b64encode(f.read()).decode()})
    else:
        deletions.append({"path": path})
query = "mutation($input: CreateCommitOnBranchInput!) { createCommitOnBranch(input: $input) { commit { oid } } }"
print(json.dumps({"query": query, "variables": {"input": {
    "branch": {"repositoryNameWithOwner": repo, "branchName": branch},
    "message": {"headline": message},
    "expectedHeadOid": base,
    "fileChanges": {"additions": additions, "deletions": deletions},
}}}))
PY

oid=$(gh api graphql --input "${payload}" --jq '.data.createCommitOnBranch.commit.oid')
if [ -z "${oid}" ] || [ "${oid}" = "null" ]; then
  echo "createCommitOnBranch returned no commit" >&2
  exit 1
fi
echo "${oid}"
