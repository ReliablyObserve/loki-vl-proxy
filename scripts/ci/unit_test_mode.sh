#!/usr/bin/env bash
# unit_test_mode.sh - decide whether the unit-test shards may replay results from Go's test cache.
#
# Prints "fresh=true" or "fresh=false" (the lines of $GITHUB_OUTPUT). A pull request replays: a
# package whose test binary, flags, environment and opened files are unchanged returns its cached
# result. Everything else runs every test again: the stored results are expired first
# (`go clean -testcache`) and the run stores new ones, so the next pull request can replay them
# (-count=1 would run the tests but store nothing). That is a push to main, a scheduled or manual
# run, a pull request carrying the ci-full label, and any case where the answer is in doubt
# (no pull request number, labels that cannot be read).
#
# The labels are read when the job runs, not taken from the event payload, so a re-run after the
# label was added sees it.
#
# Env: EVENT_NAME, REPO, PR_NUMBER, GH_TOKEN (read by gh).
set -u

reason=""
if [ "${EVENT_NAME:-}" != "pull_request" ]; then
  reason="event is ${EVENT_NAME:-unknown}, not pull_request"
elif [ -z "${PR_NUMBER:-}" ]; then
  reason="no pull request number"
elif labels="$(gh api "repos/${REPO:?}/pulls/${PR_NUMBER}" --jq '.labels[].name' 2>/dev/null)"; then
  if printf '%s\n' "$labels" | grep -qx 'ci-full'; then
    reason="the ci-full label is set"
  fi
else
  reason="could not read the pull request labels"
fi

if [ -n "$reason" ]; then
  echo "fresh=true"
  echo "unit tests: full fresh run ($reason)" >&2
else
  echo "fresh=false"
  echo "unit tests: pull request, unchanged packages replay from the Go test cache" >&2
fi
