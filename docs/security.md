# Security

## Security Model

`loki-vl-proxy` is intentionally read-focused. The default posture is:

- read APIs enabled for Loki-compatible querying
- write ingestion API (`/loki/api/v1/push`) blocked (`405`)
- admin/debug APIs disabled unless explicitly enabled

The only write-path exception is `/loki/api/v1/delete`, gated by strict safeguards.

## High-Impact Controls

### 1) Tenant Isolation

- `X-Scope-OrgID` is mapped to VictoriaLogs tenant IDs via `-tenant-map`
- optional multi-tenant fanout is explicit (`tenant-a|tenant-b`)
- wildcard tenant mode (`*`) is proxy-specific and requires explicit allow config

### 2) `/tail` Browser-Origin Controls

- `/loki/api/v1/tail` can enforce allowed browser origins
- use `-tail.allowed-origins` for Grafana/browser clients
- keep restrictive defaults for internet-exposed deployments

### 3) Delete Safeguards

`/loki/api/v1/delete` requires:

- `X-Delete-Confirmation: true`
- explicit query selector (no broad wildcard delete)
- explicit `start` and `end` time bounds
- tenant-scoped execution and audit logging

### 4) Request Hardening

- max request body/header limits
- request timeout boundaries
- built-in rate limiting and global concurrency guards
- request coalescing + circuit breaker to reduce backend cascade risk

### 5) Transport Security

- frontend TLS and optional mTLS support
- backend TLS controls for VictoriaLogs/OTLP exporters
- controlled forwarding of auth headers/cookies to backend
- optional peer-cache shared-token protection via `-peer-auth-token`

## CI Security Lanes

The repository now treats security validation as its own layered test surface instead of burying it inside generic CI.

### Fast PR Blockers

Defined in `.github/workflows/security-pr.yaml`.

- `gitleaks` for secret detection in the repository
- `gosec` for Go-focused SAST on the proxy and related packages
- `Trivy` filesystem scanning for vulnerabilities, misconfigurations, and secrets
- `actionlint` for GitHub Actions workflow validation
- `hadolint` for Dockerfile hygiene and hardening
- `OpenSSF Scorecard` for repository and supply-chain posture

This lane is supposed to fail quickly on issues that should never merge.

### Runtime PR Security

Also defined in `.github/workflows/security-pr.yaml`.

- custom Go security regressions from `scripts/ci/run_security_regressions.sh`
- OWASP ZAP baseline scan from `scripts/ci/run_zap_scan.sh baseline`

This lane validates the running stack rather than just the source tree. It is intentionally pointed at a short allowlist in `security/zap/targets.txt` so the baseline scan exercises the real user and admin/debug surface without wandering into unrelated compose internals.

### Heavy Scheduled Security

Defined in `.github/workflows/security-heavy.yaml`.

- Trivy image scan against the built runtime image
- SBOM generation for downstream review and artifact retention
- longer fuzz runs
- broader `Semgrep` coverage
- OWASP ZAP active scan
- curated `Nuclei` templates from `security/nuclei/`

This lane is intentionally heavier and is meant for scheduled or manual deep validation rather than fast PR feedback.

## Repository-Specific Threat Model

Generic scanners are useful here, but the highest-risk bugs for this project are still proxy-specific:

- tenant isolation around `X-Scope-OrgID` and any tenant-derived cache keys
- cache isolation across memory, disk, and peer cache layers
- metadata, label, and field enumeration leaks between tenants
- auth-boundary confusion across downstream requests, upstream requests, and forwarded headers/cookies
- `/tail` browser-origin enforcement and websocket handling
- oversized bodies, oversized headers, huge query windows, and malformed LogQL payloads
- debug/admin exposure on non-loopback listeners

The custom regression suite is biased toward these risks rather than only generic scanner output.

## Response-Header Baseline

The proxy now applies the same baseline security response headers across normal routes, `404`s, and disabled admin/debug endpoints:

- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Cross-Origin-Resource-Policy: same-origin`
- `Cache-Control: no-store, no-cache, must-revalidate, max-age=0`
- `Pragma: no-cache`
- `Expires: 0`

That removes the weaker edge-path behavior where scanners could still reach missing or disabled routes without the same browser and cache protections as the main API surface.

## Container And Chart Posture

- the runtime image now runs as a non-root user
- the runtime image keeps a read-only root filesystem
- Helm drops all capabilities and blocks privilege escalation
- the chart can optionally mount host `/proc` read-only for richer process/system metrics

The host `/proc` mount is intentional. Trivy would normally flag this, so CI uses a narrow `.trivyignore.yaml` exception for the specific chart template path rather than disabling the broader class of checks.

## Admin and Debug Endpoints

The following are disabled by default and should stay restricted:

- `/debug/queries`
- `/debug/pprof/*`

Enable only for controlled troubleshooting windows. On non-loopback listen addresses the proxy now refuses to start with these enabled unless `-server.admin-auth-token` is set.

`/metrics` stays available on the main listener when instrumentation is enabled, but the default export now suppresses per-tenant and per-client identity labels. Opt back in with `-metrics.export-sensitive-labels=true` only on trusted scrape paths.

## Recommended Production Baseline

- explicit `-tenant-map` (avoid implicit defaults for multi-tenant production)
- keep `-tenant.allow-global=false` unless you intentionally need wildcard backend-default access
- strict `/tail` origin allowlist
- conservative request-size and timeout limits
- explicit `-http-read-header-timeout` and bounded `/metrics` concurrency
- `ServiceMonitor` + alerting on `5xx`, circuit breaker open state, and backend latency
- `-server.admin-auth-token` for debug/admin surfaces
- `-peer-auth-token` when peer cache crosses network trust boundaries
- avoid exposing debug/admin endpoints publicly

## Local Security Validation

Useful local commands while working on hardening or CI changes:

```bash
# repo secret scan
docker run --rm -v "$PWD:/repo" -w /repo \
  ghcr.io/gitleaks/gitleaks:v8.28.0 \
  detect --source . --report-format sarif --report-path gitleaks.sarif --exit-code 1

# Go SAST
go install github.com/securego/gosec/v2/cmd/gosec@v2.22.7
"$(go env GOPATH)/bin/gosec" \
  -exclude=G104,G108,G115,G301,G302,G304,G306,G402,G404 \
  -exclude-generated \
  ./...

# filesystem scan with the same allowlist CI uses
docker run --rm -v "$PWD:/repo" -w /repo \
  aquasec/trivy:0.69.3 \
  fs . \
  --ignorefile .trivyignore.yaml \
  --scanners vuln,misconfig,secret \
  --severity HIGH,CRITICAL \
  --ignore-unfixed \
  --exit-code 1 \
  --skip-version-check

# workflow and Dockerfile linting
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.7 -color
docker run --rm -i -v "$PWD/.hadolint.yaml:/root/.config/hadolint.yaml:ro" \
  hadolint/hadolint:v2.12.0 < Dockerfile

# supply-chain posture gate
docker run --rm \
  -e GITHUB_AUTH_TOKEN="${GITHUB_TOKEN}" \
  gcr.io/openssf/scorecard:stable \
  --repo="github.com/ReliablyObserve/Loki-VL-proxy" \
  --format json \
  --show-details > scorecard.json
python3 scripts/ci/check_scorecard.py scorecard.json \
  --min-overall 5.0 \
  --require-check Dangerous-Workflow=10 \
  --require-check Binary-Artifacts=10 \
  --require-check CI-Tests=8 \
  --require-check SAST=7

# repo-specific runtime checks
./scripts/ci/run_security_regressions.sh
./scripts/ci/run_zap_scan.sh baseline
./scripts/ci/run_nuclei_scan.sh
```

When reproducing ZAP locally, expect occasional `10049 Non-Storable Content` warnings on deliberate `404` discovery paths such as `/` or disabled `/debug/*` endpoints. Those reports are useful for visibility but are not currently treated as exploitable proxy issues.

## Related Docs

- [Configuration](configuration.md)
- [Testing](testing.md)
- [API Reference](api-reference.md)
- [Observability](observability.md)
- [Known Issues](KNOWN_ISSUES.md)
