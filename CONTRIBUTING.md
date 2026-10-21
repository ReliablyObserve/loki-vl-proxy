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

## Static Checks

CI (`.github/workflows/ci.yaml`) runs these on every pull request:

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

The `changelog` check (`scripts/ci/check_changelog_pr.py`) requires a new bullet under `## [Unreleased]` in `CHANGELOG.md` when a pull request:

- has a `feat`, `fix`, `perf` or `revert` commit, or a commit subject containing "breaking change", or
- changes files under `cmd/`, `internal/`, `pkg/` or `charts/`, or `go.mod`, `go.sum` or `Dockerfile` (`_test.go` files under `cmd/`, `internal/` and `pkg/` are exempt), or
- changes any other file that is not on the non-release list (`docs/`, `website/`, `scripts/ci/tests/`, `README.md`, `CHANGELOG.md`, `LICENSE`, `scripts/ci/check_changelog_pr.py`).

Dependency-only pull requests (all commits `build(deps):` / `build(deps-dev):`) are skipped. Check locally against your base branch:

```bash
python3 scripts/ci/check_changelog_pr.py --base origin/main --head HEAD
```

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
