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
5. Run linter: `golangci-lint run`
6. Commit with conventional commits: `feat:`, `fix:`, `test:`, `docs:`
7. Open a Pull Request

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
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

# E2E (requires Docker)
cd test/e2e-compat && docker compose up -d --build
go test -v -tags=e2e -timeout=180s ./test/e2e-compat/
```

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
