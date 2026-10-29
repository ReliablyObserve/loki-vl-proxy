# Contributing to Loki-VL-proxy

## Getting Started

```bash
git clone https://github.com/ReliablyObserve/Loki-VL-proxy.git
cd Loki-VL-proxy
go build ./...
go test ./...
```

## Development Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make changes following the code style below
4. Run tests: `go test ./... -race`
5. Run the same static checks as the CI `lint` and `test` jobs (see below)
6. Add a `## [Unreleased]` entry to `CHANGELOG.md` when the changelog gate requires one (see below)
7. Commit with conventional commits: `feat:`, `fix:`, `test:`, `docs:`
8. Open a Pull Request

## Pull Request Evidence Rules

Every pull request must have a current-head **Visual smoke** CI comment with two
separate kinds of evidence:

1. **Proof of the actual changes.** Each change visible in Grafana has a targeted
   capture reproducing its trigger, with `main | PR | Loki` screenshots side by
   side, the query and time range, and the compared result. Generic Explore or
   Drilldown screenshots do not prove a particular fix. Add or update the
   corresponding `bench/visual/spec.json` scene and its `fixes` mapping; all of
   the PR's proof scenes must be captured and published, not silently trimmed.
   A deliberately rejected query must show Loki's matching rejection, rather
   than being treated as an unexplained panel error.
2. **General regression controls.** A fixed, bounded core set of log views,
   metric graphs and Drilldown pages runs on every PR, including documentation
   and CI changes. The core set is independent of the PR's selected fix scenes
   and cannot be removed to make the PR pass. Compare the underlying responses,
   series, values and errors as well as the screenshots; new empty panels or
   errors are regressions even when the screenshots look similar.

CI publishes these images directly in its sticky **Visual smoke** comment,
with the measured base and PR commit IDs and a link to the complete artifacts.
An artifact download link, historical screenshot or manually posted image alone
does not satisfy the automatic current-head proof requirement. After a rebase,
refresh the proof for the new head. Inspect the posted images before claiming
the PR is ready.

For a change with no Grafana-visible effect, state why and provide the exact API,
transport, security, performance or test evidence that proves it. Do not substitute
an unrelated screenshot. Such changes still run the general core controls.
Use one shared isolated stack for `main`, PR and Loki, keep the normal core at
the short smoke range, and reuse the existing full-baseline policy for broader
coverage. The capture and publication details are in
[the visual proof guide](bench/visual/README.md).

## Static Checks

CI (`.github/workflows/ci.yaml`) runs these on pull requests (`lint` always, `test` unless the pull request is infra-only, see CI Scope):

```bash
# formatting (lint job): must print nothing
gofmt -s -l $(git ls-files '*.go')

# golangci-lint v2.13.2 (lint job)
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
"$(go env GOPATH)/bin/golangci-lint" run --timeout=5m

# build, vet and vulnerability scan (test job)
go build ./...
go vet ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...
```

## Changelog Gate

A changelog entry and a release are tied to one question: does the pull request change something users receive? The `changelog` check (`scripts/ci/check_changelog_pr.py`) requires a new bullet under `## [Unreleased]` in `CHANGELOG.md` only when a changed file ships. The commit type does not matter: a `fix:` that touches nothing that ships needs no entry. What ships is listed in `SHIPPED` in `scripts/ci/change_scope.py`:

- `cmd/`, `internal/` and `pkg/` (`_test.go` files and `testdata/` excluded),
- `go.mod`, `go.sum`, `Dockerfile` and `.dockerignore`,
- `charts/`, `dashboard/` and `alerting/`.

Everything else (tests, `bench/`, `conformance/`, `scripts/`, `.github/`, compose files, `examples/`, `docs/`, `website/`) never needs an entry and never cuts a release. The release bot (`auto-release.yaml`) applies the same rule to the files changed since the last tag, and releases only when `[Unreleased]` has content.

Dependency-only pull requests (all commits `build(deps):` / `build(deps-dev):`) are exempt. Released version sections are never edited: put entries under `[Unreleased]`, and after a rebase across a release check that none slipped into the version that was cut (a deliberate history fix uses a `docs(changelog):` commit). Check locally against your base branch:

```bash
python3 scripts/ci/check_changelog_pr.py --base origin/main --head HEAD
```

## CI Scope

`change_scope.py` also sorts each changed path into a lane (`lane()`; `classify()` answers whether it is the full product lane). A change runs the jobs of the highest lane any of its paths needs; lint, helm, Security / static, CodeQL and the visual core regression set run in every lane. The visual plan adds targeted proof scenes for the actual change:

| Lane | Paths | Jobs it adds |
| --- | --- | --- |
| infra | `.github/`, `scripts/`, `docs/`, `website/`, `examples/`, Markdown at the root and under `bench/` and `test/` (Markdown under `conformance/`, `charts/`, `cmd/`, `internal/`, `pkg/`, `dashboard/` and `alerting/` stays product: a gate reads it or it ships), the benchmark Loki configs, compose files no job starts | none |
| perf | `bench/ab/` (except the files `bench/visual`, `bench/parity` and the conformance gate read: `stack.py`, `selection.py`, `shapes.json`, the compose overlays) | perf-smoke; the `bench/ab` unit tests run in lint |
| stack | compose files and configs the CI stacks mount (`test/e2e-compat/loki-local-config.yaml`, `grafana-*.yaml`, `vm*.yml`, `nginx-tail.conf`, the e2e-compat, e2e-fleet and e2e-pipeline compose files) | the jobs that start a stack: e2e-compat, e2e-fleet, e2e-ui, tuple-smoke, compat-*, e2e-pipeline, Security / runtime, visual-smoke, perf-smoke |
| product | everything else: Go, charts, `go.mod`, `Dockerfile`, the registry, the gates, the classifier and its workflow | all of them |

A path no rule recognises counts as product, and so does an empty file list. Add the `ci-full` label to force a full run. Pushes to `main` always run everything. The bench module tests (they include the check that keeps the benchmark Loki configs equal to the CI one) run in lint, not in the Go unit test job, so a pull request that skips the unit tests still runs them.

### Baseline run

A very large change to the proxy re-measures everything once, so that later pull requests are compared with a fresh baseline. A baseline run replaces the selection-based smoke of perf-smoke and visual-smoke with every A/B shape (at the smoke's ranges) and every visual page, and runs the whole VictoriaLogs version matrix (compat-vl `vl-matrix`) on the pull request. It is never run on every pull request:

- It runs for a pull request into `main` whose shipped diff is big (`ships()` paths only, so tests and testdata do not count): more than 1,500 changed lines, or more than 40 files, or at least 150 changed lines in the files every query passes through (`BASELINE_ENTRY_POINTS` in `scripts/ci/change_scope.py`: the route table, the translation entry, the request scope, the cache keys, the middleware and `translator.go`).
- The `baseline` label forces it (and the full product CI). Add the label, then push or re-run the workflows: the label is read when they run. `ci-full` forces it too, together with the full product CI.
- A pull request whose base is not `main` (a stacked one) starts none of these workflows and never gets it. Each layer is judged on its own diff once it targets `main` (retargeting fires `edited`, which these workflows do not listen for, so the run starts with the next push or a re-run): a big change split into layers that are each under the thresholds does not trigger, and several big layers trigger once each. Put the `baseline` label on the last layer of a split refactor.
- Results are artifacts and a "new baseline candidate" section of the A/B and visual comments: `perf-baseline-candidate` (`over-2x-loki.json`, the above-2x Loki list as it would become, which a person can copy over `bench/ab/history/over-2x-loki.json`; `acceptance-day.json`, a report of the run's distribution; a summary) and `visual-baseline-candidate` (the captures). Nothing is committed to `main`. `bench/ab/history/acceptance.json` is rebuilt from the daily history, so it is not replaced by hand: the next daily run after the merge (it runs whenever a measured path changed on `main`) writes the real new distribution, and the run on the pull request is its preview, measured with the smoke's profile (4 runs, 1h) rather than the daily's (7 runs, up to 24h).

CI spends no runs on a pull request that conflicts with `main`. GitHub does not start pull request runs while a conflict exists, and after every push to `main` the `cancel-conflicted-pr-runs` workflow (`scripts/ci/cancel_conflicted_pr_runs.py`) cancels the queued and running pull request runs of every open pull request that now conflicts. Rebase, then push once.

A Go toolchain bump changes the shipped binary, so bump it by hand and keep `go.mod`, the `Dockerfile` builder image and every `setup-go` `go-version` equal; `scripts/ci/tests/test_go_toolchain_pins.py` fails when they differ.

## Compatibility Rules

Loki defines what clients observe; VictoriaLogs is where the data lives. Every change
keeps Loki's behaviour (responses, series, values, errors) and chooses where to compute
it in this order:

1. **VictoriaLogs first.** If a VictoriaLogs query (native stats, pipes, filters) gives
   Loki's exact result, use it. This is always the first choice: it keeps the proxy
   light and lets VictoriaLogs do the work it is fast at.
2. **The proxy when VictoriaLogs cannot.** If VictoriaLogs cannot produce Loki's exact
   result, the proxy completes it: post-processing, merging buckets, interpolation,
   re-aggregation or a bounded extra query. A difference from Loki or an approximation
   is not an acceptable outcome. Say in the PR why VictoriaLogs could not do it.
3. **Bounded either way.** Proxy-side work reads aggregated input where possible, stays
   within configurable limits and answers a Loki-style error naming the flag when a
   limit is reached. Evaluating raw log rows is the last resort and must stream with
   bounded memory.
4. **Errors are Loki's.** A failure reaches the client only through the proxy's single
   error layer, with Loki's status, `errorType` and text, never VictoriaLogs' status,
   message, LogsQL or host. A new route or backend call is added to the
   `TestErrorLayer_NoVictoriaLogsLeak` gate; see "Errors: one Loki-shaped layer" in
   `conformance/registry/README.md`.

## Performance Rules

This is the bar a contribution is held to, not a claim that the proxy already meets it
for every shape: for every query shape and time range, the proxy answers at least as
fast as Loki, with Loki's result, on low and bounded resources. Where it does not yet,
the reports below list the gap.

1. **At least as fast as Loki.** The daily A/B (`bench/ab/daily.py`, run by
   `perf-daily.yaml`) compares the last release, `main` and Loki on every shape set and
   range up to 24h, and records for each shape and range the first-run (cold) and the
   repeated (warm) p50 (`bench/ab/history/trend.md`). The goal is no slower than Loki on
   both. Refresh latency, a dashboard that refreshes with the window moving forward, is
   measured only when an A/B is run with `--refresh-s` (opt-in, `bench/ab/pr_smoke.py` or
   `perf_matrix.py`); the goal is no slower than Loki there too, measured with that
   option. A change that is faster but returns anything Loki would not (series, values,
   errors, what a Grafana panel shows) is not an improvement.
2. **Low, bounded CPU and memory.** Proxy CPU and memory stay low and bounded under
   concurrent long-range load, and speed is never bought by pushing VictoriaLogs' CPU or
   memory up. What the checks report today:
   - VictoriaLogs CPU is sampled during the A/B (`docker stats`, peak per shape and
     range, in the `summary-<set>.json` files of the `perf-ab-smoke` artifact).
   - Proxy CPU and memory come from `pr-quality-report.yaml`: the Go benchmarks
     (`ns/op`, bytes and allocations per operation for query_range and labels, with and
     without the cache) and the high-concurrency load test (requests per second and
     memory growth), each measured for the base and the branch on the same runner and
     compared by `scripts/ci/check_quality_gate.py`.
   - VictoriaLogs memory is not reported automatically. Where a change could affect it,
     report it in the pull request.
3. **No regressions.** The pull-request A/B (`perf-ab.yaml`, `bench/ab/pr_smoke.py`)
   compares the base (`main`) with the branch and Loki on the shape sets the change
   touches, chosen from the changed files by `bench/ab/selection.py`, plus the control
   smoke set (the whole control set when `cmd/`, `go.mod` or `go.sum` change). It fails
   on a broken shape, a slowdown confirmed by a re-run, or a new difference from Loki. A
   shape that reached Loki's speed or Loki's result stays there.
4. **Largest gaps first.** `conformance/reports/performance.md` lists the shapes slower
   than Loki on first load and on repeat views, and by registry item. Errors where Loki
   answers show as a status in the daily trend's timing columns and as a difference in
   its `result vs Loki` column; `conformance/reports/gaps.md` (Error surface) lists the
   error responses whose messages need a registry entry stating Loki's text. Work in this
   order: errors, results that differ, a slower first answer, a slower repeated answer;
   largest gap first within each.

## Code Style

- Follow standard Go conventions (`gofmt -s`, `go vet`)
- Use `slog` for structured logging
- Add tests for all new functionality (TDD preferred)
- Keep functions small and focused
- Use `sync.Pool` for hot-path allocations
- Prefer `io.Reader` streaming over `io.ReadAll` for large responses

## Testing

```bash
# Unit tests
go test ./... -race -count=1

# Benchmarks
go test ./internal/proxy/ -bench . -benchmem -run "^$"

# Load tests
go test ./internal/proxy/ -run "TestLoad" -v

# E2E (requires Docker): start the stack, wait for readiness, run from the repository root
(cd test/e2e-compat && docker compose up -d --build && ../../scripts/ci/wait_e2e_stack.sh 180)
go test -v -tags=e2e -timeout=300s -count=1 -run '^TestSetup_IngestLogs$|^TestCompat_' ./test/e2e-compat/
(cd test/e2e-compat && docker compose down -v)
```

CI splits `test/e2e-compat` into five groups, each on a fresh stack, and runs the Playwright UI shards against a stack started with `--profile ui`. See [docs/testing.md](docs/testing.md) for the group patterns, UI shards, and the full CI job list.

## Areas for Contribution

- LogQL translation coverage (see `docs/translation-reference.md` for unsupported features)
- Performance optimization (see `docs/benchmarks.md` for hot paths)
- E2E test coverage
- Documentation improvements

## Documentation Policy

`docs/` documents the project as it is: operational and user-facing guides, final
architecture explained in full depth, compatibility/parity references, and the evidence
that the project works and is reliable — benchmark results with the harnesses to rerun
them, cost calculations, and testing guides. PRs should document final behavior, not
the process that led to it.

## Reporting Issues

Please include:
- Loki-VL-proxy version
- VictoriaLogs version
- Grafana version
- The LogQL query that fails
- Expected vs actual behavior
