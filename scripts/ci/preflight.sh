#!/usr/bin/env bash
# preflight.sh - run the CI checks that fail late, locally, before a push.
#
# A pull request spends 20-30 minutes in GitHub Actions before it reports a data race, a stale
# conformance report, a changelog entry under a released section, a lint finding at the pinned
# version, or a test that is flaky only under the `report` job's coverage run. Each of those can
# be caught here in a few minutes. Every command below is the one CI runs: the lists that change
# (the Python unit-test suites, the golangci-lint version, the concurrent-regression pattern,
# the fuzz targets) are read from .github/workflows/ci.yaml and scripts/ci at run time, so this
# script does not drift from them. scripts/ci/preflight.py holds the parts that read the diff
# and the workflows (tested by scripts/ci/tests/test_preflight.py).
#
# Modes (what each mirrors is named at its step):
#   --quick     static checks and gates only (the `lint` job and the gates of `conformance`,
#               `changelog` and `fuzz-smoke`): about a minute and a half. The Python suites run one
#               process per module in parallel here; the other modes run them in one interpreter, as CI does.
#   (default)   --quick, then the tests of the Go packages the diff touches: plain -short, -race
#               (internal/proxy: only the tests of the changed files), and the `report` job's
#               coverage-mode run.
#   --full      every package with -short, plus the bench module, govulncheck, the
#               race-sensitive tests and the internal/proxy -race shards named by --race-shard.
#   --fuzz      adds the fuzz-smoke targets of the touched packages (all of them with --full).
#
# Options:
#   --base REF            what the pull request is compared with (default origin/main; a stacked
#                         pull request uses its base branch). check_conformance.py reads origin/main
#                         for its ratchets whatever this is.
#   --race-shard N|all    run CI's race-stress shard N (1-5 or other) of internal/proxy, or all six
#                         one after another: 10+ minutes each. Not part of --quick.
#   --dependents          also test the packages that import a touched package (can pull in all
#                         of internal/proxy).
#   --keep-going          run every step and report all failures (default: stop at the first).
#   --fetch               git fetch the base branch first.
#   --dry-run             print the plan and the steps without running them.
#
# Not mirrored (they need Docker, a browser, a long stack or the network of a runner): the e2e,
# visual, perf A/B, fleet, helm, security-scan and docker jobs, `docker compose config`
# (validate_compose.sh), and the conformance evidence check, which needs the unit-test results of
# the whole CI run. See CONTRIBUTING.md, "Local preflight".
#
# The working tree is never modified: the conformance gate rewrites reports that are stale, so it
# runs on a copy; every step is followed by a check that `git status` and `git diff HEAD` are
# unchanged, and a step that changed them fails (a fuzz crasher in testdata/fuzz is such a change).
# Do not edit files while it runs: the edit is reported as the step's.
#
# Usage: scripts/ci/preflight.sh [--quick|--full] [--fuzz] [--base REF] [--race-shard N|all] ...
# Exit status: 0 when every step passed, 1 otherwise, 2 for a usage error.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
HELPER="python3 $ROOT/scripts/ci/preflight.py"
export PYTHONDONTWRITEBYTECODE=1

MODE=default
BASE=origin/main
RACE_SHARD=""
FUZZ=0
DEPENDENTS=0
KEEP_GOING=0
FETCH=0
DRY_RUN=0

usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }
die() { echo "preflight: $*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --quick) MODE=quick ;;
    --full) MODE=full ;;
    --fuzz) FUZZ=1 ;;
    --base) [ $# -ge 2 ] || die "--base needs a ref"; BASE="$2"; shift ;;
    --race-shard) [ $# -ge 2 ] || die "--race-shard needs N|all"; RACE_SHARD="$2"; shift ;;
    --dependents) DEPENDENTS=1 ;;
    --keep-going) KEEP_GOING=1 ;;
    --fetch) FETCH=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
  shift
done
case "$RACE_SHARD" in ''|1|2|3|4|5|other|all) ;; *) die "--race-shard wants 1-5, other or all" ;; esac
if [ -n "$RACE_SHARD" ] && [ "$MODE" = quick ]; then die "--race-shard does not belong in --quick"; fi

if [ "$FETCH" -eq 1 ]; then
  case "$BASE" in origin/*) git fetch --quiet origin "${BASE#origin/}" ;; *) die "--fetch needs an origin/<branch> base" ;; esac
fi
git rev-parse --verify --quiet "$BASE^{commit}" >/dev/null \
  || die "base $BASE not found; run: git fetch origin ${BASE#origin/} (or pass --base)"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/preflight.XXXXXX")"
cleanup() {
  if [ "${FAILED:-0}" -ne 0 ]; then echo "preflight: logs kept in $WORK"; else rm -rf "$WORK"; fi
}
trap cleanup EXIT
trap 'exit 1' INT TERM HUP PIPE

# --- the step runner ----------------------------------------------------------------------------

STEP_N=0
FAILED=0
PASSED=0
SKIPPED=0
LINES=""
START=$SECONDS

tree_state() { { git status --porcelain=v1 -uall; git diff HEAD --binary | cksum; } 2>&1; }
tree_state > "$WORK/state"

summary() {
  echo
  echo "== preflight ($MODE): $PASSED passed, $FAILED failed, $SKIPPED skipped, $((SECONDS - START))s =="
  if [ "$FAILED" -ne 0 ]; then printf '%s' "$LINES" | grep '^FAIL' | sed 's/^/  /' || true; fi
}

# step <name> <command...>: run it (in a subshell, output to a log), print PASS/FAIL <name> (<n>s).
# A step that leaves the working tree different from before fails. Stops at the first failure
# unless --keep-going. The command must not rely on `set -e`: it is run in an `if`.
step() {
  local name="$1" log rc=0 began=$SECONDS line
  shift
  STEP_N=$((STEP_N + 1))
  log="$WORK/$(printf '%02d' "$STEP_N").log"
  if [ "$DRY_RUN" -eq 1 ]; then echo "WOULD $name"; return 0; fi
  ( "$@" ) < /dev/null > "$log" 2>&1 || rc=$?
  tree_state > "$WORK/state.new"
  if ! cmp -s "$WORK/state" "$WORK/state.new"; then
    {
      echo "the working tree changed while this step ran: the step wrote a file (it must not) or you edited a file during the run:"
      diff "$WORK/state" "$WORK/state.new" || true
      git diff HEAD --stat
    } >> "$log"
    cp "$WORK/state.new" "$WORK/state"
    rc=1
  fi
  if [ "$rc" -eq 0 ]; then
    line="PASS $name ($((SECONDS - began))s)"
    PASSED=$((PASSED + 1))
  else
    line="FAIL $name ($((SECONDS - began))s)"
    FAILED=$((FAILED + 1))
  fi
  echo "$line"
  LINES="$LINES$line
"
  if [ "$rc" -ne 0 ]; then
    echo "--- $name: the lines that name the failure, then the last 40 (full log $log)"
    if grep -qE '^\s*(--- FAIL|FAIL|panic:|WARNING: DATA RACE|.*_test\.go:[0-9]+:|.* is stale)' "$log"; then
      grep -E '^\s*(--- FAIL|FAIL|panic:|WARNING: DATA RACE|.*_test\.go:[0-9]+:|.* is stale)' "$log" | head -n 30 | cut -c1-300 | sed 's/^/    /'
      echo "    ..."
    fi
    tail -n 40 "$log" | cut -c1-300 | sed 's/^/    /'
    echo "---"
    if [ "$KEEP_GOING" -eq 0 ]; then summary; exit 1; fi
  fi
  return 0
}

skip() { echo "SKIP $1 ($2)"; SKIPPED=$((SKIPPED + 1)); }
note() { echo "NOTE $*"; }

# Test runs are capped the way the repo asks: memory limit, 4 procs, one package at a time.
capped() { env GOMEMLIMIT="${PREFLIGHT_GOMEMLIMIT:-$MEMLIMIT}" GOMAXPROCS=4 "$@"; }
MEMLIMIT=2GiB
if [ "$MODE" = full ]; then MEMLIMIT=3GiB; fi

# --- the plan -----------------------------------------------------------------------------------

want_go="$(awk '$1 == "go" { print $2; exit }' go.mod)"
have_go="$(go env GOVERSION | sed 's/^go//')"
if [ "$want_go" != "$have_go" ]; then
  echo "WARN go $have_go here, CI uses go $want_go (go.mod): results can differ"
fi

PLAN_ALL=0 PLAN_PKGS="" PLAN_PROXY=0 PLAN_RUN="" PLAN_UNMATCHED="" PLAN_BENCH=0 PLAN_COMPOSE=0 PLAN_CACHE=0
plan_args="--base $BASE"
if [ "$DEPENDENTS" -eq 1 ]; then plan_args="$plan_args --dependents"; fi
# shellcheck disable=SC2086
plan_out="$($HELPER plan $plan_args)" || die "could not plan the diff against $BASE"
while IFS= read -r kv; do
  val="${kv#*=}"
  case "${kv%%=*}" in
    all) PLAN_ALL="$val" ;;
    packages) PLAN_PKGS="$val" ;;
    proxy) PLAN_PROXY="$val" ;;
    proxy_run) PLAN_RUN="$val" ;;
    proxy_unmatched) PLAN_UNMATCHED="$val" ;;
    bench) PLAN_BENCH="$val" ;;
    compose) PLAN_COMPOSE="$val" ;;
    cache) PLAN_CACHE="$val" ;;
  esac
done <<EOF
$plan_out
EOF
if [ "$PLAN_ALL" = 1 ]; then
  note "go.mod or go.sum changed: every package is affected"
  PLAN_PKGS="$(go list ./... | sed "s|^$(go list -m)|.|" | tr '\n' ' ')"
  PLAN_PROXY=1
  PLAN_CACHE=1
fi
# internal/proxy runs on its own (a -run subset under -race), every other package whole.
OTHER_PKGS=""
for p in $PLAN_PKGS; do
  if [ "$p" != ./internal/proxy ]; then OTHER_PKGS="$OTHER_PKGS $p"; fi
done
OTHER_PKGS="${OTHER_PKGS# }"

echo "preflight: mode $MODE, base $BASE ($(git log -1 --format='%h, %cr' "$BASE")), head $(git log -1 --format='%h' HEAD)"
echo "preflight: packages touched: ${PLAN_PKGS:-none}"
if [ -n "$(git status --porcelain)" ]; then
  note "uncommitted changes are tested, but the changelog gate reads the committed HEAD only"
fi

# --- static checks (the `lint` job and the `test` job's build and vet) ----------------------------

go_files() {
  git ls-files -z --cached --others --exclude-standard -- '*.go' \
    | while IFS= read -r -d '' f; do if [ -f "$f" ]; then printf '%s\0' "$f"; fi; done
}

check_gofmt() {
  local bad
  bad="$(go_files | xargs -0 gofmt -s -l)" || return 1
  if [ -n "$bad" ]; then
    echo "The following files are not formatted with gofmt -s (run: gofmt -s -w <file>):"
    echo "$bad"
    return 1
  fi
}

LINT_VERSION="$($HELPER golangci-version)" || die "cannot read the golangci-lint version from ci.yaml"
LINT_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/loki-vl-proxy/bin/golangci-lint-$LINT_VERSION"
run_lint() {
  if ! "$LINT_DIR/golangci-lint" --version 2>/dev/null | grep -q "version ${LINT_VERSION#v} "; then
    echo "installing golangci-lint $LINT_VERSION into $LINT_DIR"
    mkdir -p "$LINT_DIR" || return 1
    GOBIN="$LINT_DIR" go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$LINT_VERSION" || return 1
  fi
  "$LINT_DIR/golangci-lint" run --timeout=5m
}

# --- gates (changelog, fuzz-smoke list, test/testcache, conformance, Python suites) ------------

gate_changelog() {
  python3 scripts/ci/check_changelog_pr.py --base "$BASE" --head HEAD || {
    echo "(fix: add the entry under ## [Unreleased]; never edit a released section. Rebased across a release?"
    echo " check that none of your entries landed in the version that was cut. See CONTRIBUTING.md, Changelog Gate)"
    return 1
  }
}

# The conformance gate regenerates the reports it checks and leaves a stale one rewritten, so it
# runs on a copy of the working tree (git still answers from this checkout).
gate_conformance() {
  local tree="$WORK/tree" out rc=0
  mkdir -p "$tree" || return 1
  $HELPER snapshot "$tree" || return 1
  out="$(cd "$tree" && GIT_DIR="$(git -C "$ROOT" rev-parse --absolute-git-dir)" python3 scripts/ci/check_conformance.py 2>&1)" || rc=$?
  printf '%s\n' "$out"
  if [ "$rc" -ne 0 ]; then
    if printf '%s\n' "$out" | grep -q ' is stale; run '; then
      echo "regenerate in your checkout, then commit the result:"
      printf '%s\n' "$out" | sed -n 's/^.* is stale; run /  /p'
    fi
    return 1
  fi
}

# Run one `python3 -m unittest <args>` line of ci.yaml. CI runs the whole module list in one
# interpreter, and several scripts edit sys.path on import, so the default is the same single
# process; --quick trades that for one process per module, four at a time.
unittest_line() {
  if [ "$MODE" = quick ]; then $HELPER run-unittest --parallel 4 "$1"; else $HELPER run-unittest "$1"; fi
}

# --- tests of what changed ----------------------------------------------------------------------

# The concurrent-regression pattern of the race job, ci.yaml "Concurrent regression tests".
REGRESSION_RUN="$($HELPER regression-run)"

race_proxy() {
  local run="$REGRESSION_RUN"
  if [ -n "$PLAN_RUN" ]; then run="$PLAN_RUN|$REGRESSION_RUN"; fi
  capped go test -race -count=1 -timeout=12m -run "$run" ./internal/proxy/
}

# The `report` job's run (collect_quality_metrics.sh): -json -cover, uncached, NOT -short and not
# -race. A test can pass in the race job and still flake only here, because of the timing the
# coverage instrumentation changes. internal/proxy runs its changed tests only (all in --full).
coverage_run() {
  local json="$WORK/cover-$1.json"
  shift
  capped go test -count=1 -p 1 -coverprofile="$WORK/cover.out" -json "$@" > "$json" || {
    $HELPER summarize-json "$json" || true
    return 1
  }
}

# The `test` job's guard on internal/cache coverage (ci.yaml "Cache package coverage guard").
cache_coverage_guard() {
  local cov
  go test ./internal/cache -count=1 -coverprofile="$WORK/coverage-cache.out" > /dev/null || return 1
  cov="$(go tool cover -func="$WORK/coverage-cache.out" | awk 'END {gsub("%","",$3); print $3}')"
  echo "internal/cache coverage: ${cov}%"
  awk -v cov="$cov" 'BEGIN { min=79.0; if (cov+0 < min) { printf "FAIL: internal/cache coverage %.2f%% is below guard %.1f%%\n", cov+0, min; exit 1 } }'
}

# One race-stress shard of internal/proxy exactly as the `race-stress-shard` job runs it
# (five shards of the top-level tests by scripts/ci/test_shards.py, then `other`).
race_shard() {
  local shard="$1" list="$WORK/shard-list.txt" regex
  if [ "$shard" = other ]; then
    capped go test -race -timeout=8m ./internal/cache/... ./internal/middleware/... ./internal/observability/... || return 1
    local sub
    sub="$(go list ./internal/proxy/... | grep -v '/internal/proxy$' || true)"
    # shellcheck disable=SC2086
    if [ -n "$sub" ]; then capped go test -race -timeout=8m $sub || return 1; fi
    capped go test -race -count=1 -v -timeout=2m -run "$REGRESSION_RUN" ./internal/proxy/ ./internal/observability/
    return
  fi
  [ -s "$list" ] || go test -list '.*' ./internal/proxy/ > "$list" || return 1
  python3 scripts/ci/test_shards.py check --shards 5 --list "$list" || return 1
  regex="$(python3 scripts/ci/test_shards.py regex --shards 5 --shard "$shard" --list "$list")" || return 1
  [ -n "$regex" ] || return 1
  capped go test -race -timeout=12m -run "$regex" ./internal/proxy/
}

# One smoke target as scripts/ci/fuzz_smoke.py runs it (Target.command).
fuzz_target() {
  local extra=""
  if [ "$4" != "-" ]; then extra="-timeout=$4"; fi
  capped go test "./$1" -run '^$' "-fuzz=^$2\$" "-fuzztime=$3" ${extra:+"$extra"}
}

# --- run ----------------------------------------------------------------------------------------

step "gofmt -s" check_gofmt
step "sync observability assets" ./scripts/ci/sync_observability_assets.sh --check
step "changelog gate (vs $BASE)" gate_changelog
step "fuzz target list" python3 scripts/ci/fuzz_smoke.py check

$HELPER unittests > "$WORK/unittests.txt"
while IFS= read -r line; do
  case "$line" in
    "discover -s "*) name="unittest ${line#discover -s }"; name="${name%% -p *}" ;;
    *) name="unittest scripts.ci.tests ($(printf '%s' "$line" | wc -w | tr -d ' ') suites)" ;;
  esac
  step "$name" unittest_line "$line"
done < "$WORK/unittests.txt"

step "go build ./..." go build ./...
step "go vet ./..." go vet ./...
step "configuration reference" go run ./cmd/configdoc -check
step "README support policy" go test ./internal/logsql -run '^TestVictoriaLogsSupportPolicy$' -count=1
step "golangci-lint $LINT_VERSION" run_lint
step "test/testcache policy" go test -count=1 ./test/testcache/
step "conformance gate (on a copy)" gate_conformance

if [ "$PLAN_COMPOSE" -eq 1 ]; then
  note "a compose file changed: run scripts/ci/validate_compose.sh (needs the docker CLI, not run here)"
fi
if [ "$PLAN_BENCH" -eq 1 ] || [ "$MODE" = full ]; then
  step "bench module vet + race tests" sh -c 'cd bench && go vet ./... && go test ./... -count=1 -race'
fi

if [ "$MODE" != quick ]; then
  if [ "$MODE" = full ]; then
    step "go test -short ./... (all packages)" capped go test -short -count=1 -p 1 ./...
    step "tests that behave differently under -race" scripts/ci/race_sensitive_tests.sh "$WORK/race-sensitive.json"
    step "govulncheck" go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
  elif [ -n "$PLAN_PKGS" ]; then
    # shellcheck disable=SC2086
    step "go test -short ($PLAN_PKGS)" capped go test -short -count=1 -p 1 $PLAN_PKGS
  else
    skip "go test -short" "no Go package touched"
  fi

  if [ -n "$OTHER_PKGS" ]; then
    # shellcheck disable=SC2086
    step "go test -race (changed packages except internal/proxy)" capped go test -race -count=1 -p 1 -timeout=8m $OTHER_PKGS
  fi
  if [ "$PLAN_PROXY" -eq 1 ]; then
    if [ -z "$PLAN_RUN" ]; then
      note "internal/proxy changed (${PLAN_UNMATCHED:-no file}) but no test file maps to it: only the concurrent regression tests run under -race; use --race-shard N|all for a CI shard"
    fi
    step "go test -race internal/proxy (changed tests + regression set)" race_proxy
    step "tuple contract" go test ./internal/proxy -run '^TestTupleContract_' -count=1
  fi
  if [ "$PLAN_CACHE" -eq 1 ] || [ "$MODE" = full ]; then
    step "internal/cache coverage guard" cache_coverage_guard
  fi

  # shellcheck disable=SC2086
  if [ -n "$OTHER_PKGS" ]; then step "report job: go test -cover -json (changed packages)" coverage_run other $OTHER_PKGS; fi
  if [ "$PLAN_PROXY" -eq 1 ] && [ -n "$PLAN_RUN" ]; then
    step "report job: go test -cover -json (internal/proxy changed tests)" coverage_run proxy -run "$PLAN_RUN" ./internal/proxy/
  fi

  if [ "$RACE_SHARD" = all ]; then
    for shard in 1 2 3 4 5 other; do step "race-stress shard $shard" race_shard "$shard"; done
  elif [ -n "$RACE_SHARD" ]; then
    step "race-stress shard $RACE_SHARD" race_shard "$RACE_SHARD"
  fi
fi

# --- fuzz (the `fuzz-smoke` job, for the packages touched) ----------------------------------------

if [ "$FUZZ" -eq 1 ]; then
  fuzz_pkgs=""
  if [ "$MODE" != full ]; then fuzz_pkgs="$(for p in $PLAN_PKGS; do printf '%s ' "${p#./}"; done)"; fi
  # shellcheck disable=SC2086
  $HELPER fuzz-targets --packages $fuzz_pkgs > "$WORK/fuzz.tsv"
  if [ ! -s "$WORK/fuzz.tsv" ]; then
    skip "fuzz" "no smoke target in the packages touched"
  fi
  while IFS="$(printf '\t')" read -r pkg target fuzztime timeout; do
    step "fuzz $pkg $target" fuzz_target "$pkg" "$target" "${fuzztime:-10s}" "${timeout:--}"
  done < "$WORK/fuzz.tsv"
fi

summary
if [ "$FAILED" -ne 0 ]; then exit 1; fi
