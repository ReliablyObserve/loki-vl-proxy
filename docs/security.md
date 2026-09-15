---
sidebar_label: Security
description: Security model, network exposure, tenant isolation, header forwarding, and TLS configuration.
---

# Security

## Security Model

`loki-vl-proxy` is intentionally read-focused. The default posture is:

- read APIs enabled for Loki-compatible querying
- write ingestion API (`/loki/api/v1/push`) blocked (`405`)
- admin/debug APIs disabled unless explicitly enabled
- `/metrics` not served unless `-server.register-instrumentation=true`

`/loki/api/v1/delete` is registered with request safeguards, but deletion is not supported against VictoriaLogs (see [Delete API](#3-delete-api-unsupported)).

Behavior changes from the 1.67.0 and 1.68.0 hardening releases (wildcard denial, tenant-scoped cache keys, execution limits) are described in the [security hardening migration guide](security-hardening-migration.md).

## High-Impact Controls

### 1) Tenant Isolation

- `X-Scope-OrgID` is mapped to VictoriaLogs tenant IDs via `-tenant-map`
- optional multi-tenant fanout is explicit (`tenant-a|tenant-b`)
- wildcard tenant mode (`*`) is proxy-specific and requires explicit allow config: an unmapped `X-Scope-OrgID: *` returns `403` in native and label-routing (`-tenant-label`) modes unless `-tenant.allow-global=true`; an explicit tenant-map entry for `*` takes precedence
- label-routing mode enforces the tenant constraint through VictoriaLogs `extra_stream_filters`, so request parameters and query pipelines cannot override it; the configured tenant field must be a stream field
- cache and coalescer keys (memory, disk, peer, window, metadata and label-value indexes) include the resolved tenant routing, backend identity and forwarded-identity fingerprint, so entries are never shared across tenants or authorization scopes

**Lightweight tenant enforcement**: `-require-tenant-header=true` rejects any request missing an `X-Scope-OrgID` header with HTTP 401. This is a lighter alternative to full auth — it catches misconfigured clients without requiring a token/credential system.

**Backend tenant header forwarding**: Set `FORWARD_TENANT_HEADER=false` to prevent the proxy from forwarding `X-Scope-OrgID` to the backend (useful if the VL backend does not support multi-tenancy).

### 2) `/tail` Browser-Origin Controls

- `/loki/api/v1/tail` can enforce allowed browser origins
- use `-tail.allowed-origins` for Grafana/browser clients
- keep restrictive defaults for internet-exposed deployments

### 3) Delete API (unsupported)

`/loki/api/v1/delete` is always registered and validates each request before contacting the backend:

- `POST` only and `X-Delete-Confirmation: true`
- explicit query selector (`{}` and `*` are rejected)
- explicit `start` and `end` time bounds, at most 30 days apart
- tenant-scoped execution and a warning-level audit log entry

The handler then forwards to `/select/logsql/delete`, which VictoriaLogs rejects as an unsupported path (VictoriaLogs deletes through its asynchronous `/delete/run_task` API). Deletion is therefore not supported; do not rely on this endpoint. See [Remaining delete API gap](security-hardening-migration.md#remaining-delete-api-gap).

### 4) Request Hardening

- max request body/header limits
- request timeout boundaries
- built-in rate limiting (per-client token bucket keyed on the connection address; `-rate-limit-per-second`, `-rate-limit-burst`) and global concurrency guards (`-max-concurrent`, which also bounds backend operations until their bodies are consumed)
- 64 KB maximum query string length and optional `-default-max-query-length` time-range ceiling
- request coalescing + circuit breaker to reduce backend cascade risk

### 5) Execution Limits

Excessive query work is rejected with an explicit error instead of returning partial or oversized successful responses:

- raw metric evaluation: `-manual-range-metric-row-limit` rows (default 1,000,000) and `-max-stats-query-series` series (default 500), plus 64 MiB input/output and one million output samples
- binary expressions: nesting depth 64, 1,024 child evaluations, 256 MiB captured child responses, two million decoded arrays, one million constructed samples, 64 MiB label work and 64 MiB per encoded result
- `line_format`: 64 KiB per formatted line and 16 MiB per response, with bounded template depth and execution work (HTTP `400` on overflow)
- coalesced backend bodies above 256 MiB and tail client messages above 4 KiB are rejected

Limits and rollout guidance are in the [migration guide](security-hardening-migration.md#execution-and-storage-limits).

### 6) Transport Security

- frontend TLS and optional mTLS support
- backend TLS controls for VictoriaLogs/OTLP exporters
- controlled forwarding of auth headers/cookies to backend
- peer-cache shared-token protection via `-peer-auth-token`, required whenever peer discovery or static peers are configured; startup fails without it unless `-peer-insecure-ip-allowlist=true` restores the legacy source-IP check. The token is compared in constant time. The Helm chart generates a `<release>-peer-auth` Secret when `peerCache.authToken` and `peerCache.existingSecret` are unset

**mTLS / client certificate flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `-tls-require-client-cert` | `false` | Require client TLS certificate (mTLS) |
| `-tls-client-ca-file` | — | CA certificate for validating client certs |

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
- with `systemMetrics.hostProc.enabled=true` (the default), the chart mounts five individual host files read-only as `hostPath` `type: File` volumes — `/proc/stat`, `/proc/meminfo`, `/proc/pressure/cpu`, `/proc/pressure/memory` and `/proc/pressure/io` — under `/host/proc` and passes `-host-proc-root`; the host's `/proc` directory and other workloads' per-process data are not mounted

These `hostPath` mounts are intentional. Trivy would normally flag them (`KSV-0121`), so CI uses a narrow `.trivyignore.yaml` exception for the chart deployment template rather than disabling the broader class of checks. Disable `systemMetrics.hostProc` on clusters that forbid `hostPath`.

## Admin and Debug Endpoints

The following are disabled by default and should stay restricted:

- `/debug/queries` (`-server.enable-query-analytics`)
- `/debug/pprof/*` (`-server.enable-pprof`)
- `/admin/cache/flush` (registered with `-server.register-instrumentation=true`)

Enable only for controlled troubleshooting windows. Without `-server.admin-auth-token`, admin/debug routes are served only on `-admin-listen` (default `127.0.0.1:3101`), and the proxy refuses to start when these routes are enabled on a non-loopback address without a token. With a token, they move onto the main listener and require `Authorization: Bearer <token>` or `X-Admin-Token`. The chart keeps `admin-listen` on loopback and never publishes it through the Service.

`/metrics` is not served by default. Set `-server.register-instrumentation=true` to enable it on the main listener, or add `-metrics-listen` for a dedicated port (the chart uses `:9091`). The default export suppresses per-tenant and per-client identity labels. Opt back in with `-metrics.export-sensitive-labels=true` only on trusted scrape paths.

## Log and Error Redaction

- all log output passes through a redacting handler that masks API keys, bearer tokens, passwords, AWS credentials and URL-embedded credentials
- debug logs record LogQL/LogsQL and backend parameters as `sha256:<8hex> len=<n>` fingerprints; `-debug-log-raw-queries=true` restores raw values for local debugging only
- the request log records the authentication mechanism (`auth.source`) but not the Basic-Auth principal
- VictoriaLogs error bodies are reduced to their message and stripped of stream selectors, long quoted literals and long hex identifiers before they are logged or returned; transport errors no longer echo backend URLs with query parameters. Redaction is disabled only by `-debug-log-raw-queries=true`

## Recommended Production Baseline

- explicit `-tenant-map` (avoid implicit defaults for multi-tenant production)
- keep `-tenant.allow-global=false` unless you intentionally need wildcard backend-default access
- strict `/tail` origin allowlist
- conservative request-size and timeout limits
- explicit `-http-read-header-timeout` and bounded `/metrics` concurrency
- `ServiceMonitor` + alerting on `5xx`, circuit breaker open state, and backend latency
- `-server.admin-auth-token` for debug/admin surfaces
- `-peer-auth-token` for peer cache, with `-peer-insecure-ip-allowlist=false`
- keep `-debug-log-raw-queries=false`
- validate representative wide queries against the execution limits before rollout
- avoid exposing debug/admin endpoints publicly

## Local Security Validation

Useful local commands while working on hardening or CI changes:

```bash
# repo secret scan
docker run --rm -v "$PWD:/repo" -w /repo \
  ghcr.io/gitleaks/gitleaks:v8.28.0 \
  detect --source . --report-format sarif --report-path gitleaks.sarif --exit-code 1

# Go SAST
go install github.com/securego/gosec/v2/cmd/gosec@v2.29.0
"$(go env GOPATH)/bin/gosec" \
  -exclude=G104,G108,G115,G118,G301,G302,G304,G306,G402,G404,G704,G705 \
  -exclude-generated \
  -exclude-dir=bench \
  ./...

# filesystem scan with the same allowlist CI uses
docker run --rm -v "$PWD:/repo" -w /repo \
  aquasec/trivy:0.71.0 \
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

- [Security hardening migration](security-hardening-migration.md)
- [Configuration](configuration.md)
- [Testing](testing.md)
- [API Reference](api-reference.md)
- [Observability](observability.md)
- [Known Issues](KNOWN_ISSUES.md)