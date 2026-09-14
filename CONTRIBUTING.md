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
