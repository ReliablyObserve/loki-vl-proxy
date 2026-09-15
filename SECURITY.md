# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x     | Yes       |
| < 1.0   | No        |

## Reporting a Vulnerability

Please report security vulnerabilities via GitHub Security Advisories:
https://github.com/ReliablyObserve/Loki-VL-proxy/security/advisories/new

Do NOT open a public issue for security vulnerabilities.

## Default Security Posture

`loki-vl-proxy` is designed as a read-focused compatibility layer. The project defaults are intentionally restrictive:

- read APIs enabled for Loki-compatible querying
- write ingestion API (`/loki/api/v1/push`) blocked with `405`
- delete API unsupported: the `/loki/api/v1/delete` handler validates requests (confirmation header, time bounds, selector, tenant scope, audit log) but forwards to a VictoriaLogs path that VictoriaLogs rejects, so no deletion is performed
- admin/debug endpoints disabled unless explicitly enabled; without `-server.admin-auth-token` they bind to a loopback-only `-admin-listen` (default `127.0.0.1:3101`)
- `/metrics` not served unless `-server.register-instrumentation=true` (optionally on a dedicated `-metrics-listen`)
- peer cache refuses to start without `-peer-auth-token` unless `-peer-insecure-ip-allowlist=true` is set explicitly
- runtime image runs as a non-root user
- runtime image keeps a read-only root filesystem
- Helm chart drops Linux capabilities and blocks privilege escalation

## Built-In Controls

- **Read-only by default**: `/loki/api/v1/push` blocked with 405
- **Delete not supported**: request safeguards (confirmation header, tenant scoping, 30-day time range limit, audit logging) run before the backend call, but the backend rejects the target path; see [Security hardening migration](docs/security-hardening-migration.md#remaining-delete-api-gap)
- **Rate limiting**: Per-client token bucket using `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **Query length limit**: 64KB max to prevent abuse
- **Security headers**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`
- **Consistent response hardening**: the proxy now applies the same baseline security headers on success, `404`, and disabled admin/debug responses so scanners and browsers do not see a weaker edge path
- **HTTP hardening**: Configurable read/write/idle timeouts, max header/body bytes
- **TLS support**: Server-side HTTPS via `-tls-cert-file`/`-tls-key-file`
- **Tail browser-origin controls**: `/tail` can enforce explicit allowed origins
- **Admin token enforcement**: non-loopback admin/debug exposure requires `-server.admin-auth-token`; without a token, admin/debug routes are served only on `-admin-listen` (loopback by default)
- **Peer-cache protection**: `-peer-auth-token` is required when peer discovery is configured (constant-time `X-Peer-Token` check); the Helm chart generates a token Secret when `peerCache.authToken` is unset
- **Global tenant denial**: an unmapped `X-Scope-OrgID: *` returns `403` in both native and label-routing modes unless `-tenant.allow-global=true`
- **Tenant-scoped caches**: response, window, metadata, persisted and peer cache keys and coalescer keys include the resolved tenant routing and forwarded-identity fingerprint
- **Query redaction**: debug logs record queries as `sha256:<8hex> len=<n>` unless `-debug-log-raw-queries=true`; VictoriaLogs error bodies and transport errors are redacted before they are logged or returned to clients
- **Execution limits**: raw metric row/series limits, binary-expression budgets, `line_format` output limits and bounded coalesced bodies reject excessive work with an explicit error instead of partial results; see [Security hardening migration](docs/security-hardening-migration.md#execution-and-storage-limits)
- **Sensitive metrics off by default**: per-tenant and per-client identity labels are not exported unless explicitly enabled
- **No secrets in logs**: all log output passes through a redacting slog handler that detects and masks API keys, bearer tokens, passwords, AWS credentials, and URL-embedded credentials

## Known Security Considerations

- **`text/template` in `| line_format`**: Go templates are executed on query results. The template FuncMap is restricted to safe string functions, but this is still a surface to treat carefully in shared environments.
- **Disk cache**: No application-level encryption. Use cloud-provider or node-level disk encryption for data at rest.
- **Backend TLS skip**: `-backend-tls-skip-verify` disables certificate validation for VictoriaLogs connections.
- **Host `/proc` files for system metrics**: with `systemMetrics.hostProc.enabled=true` (the default), the Helm chart mounts five individual host files read-only (`/proc/stat`, `/proc/meminfo`, `/proc/pressure/{cpu,memory,io}`) and sets `-host-proc-root`. The whole host `/proc` directory is not mounted. The `hostPath` exception is narrowly allowlisted in CI; disable it on clusters that forbid `hostPath`.

## Security Testing And CI

The repository now keeps a dedicated layered security pipeline in addition to `CodeQL` and the normal compatibility suite.

- **Fast PR blockers** in `.github/workflows/security-pr.yaml`: `gitleaks`, `gosec`, `Trivy` filesystem scan, `actionlint`, `hadolint`, and `OpenSSF Scorecard`
- **Runtime PR lane** in `.github/workflows/security-pr.yaml`: custom Go regressions plus an OWASP ZAP baseline scan against the local compose stack
- **Heavy scheduled lane** in `.github/workflows/security-heavy.yaml`: image scanning, SBOM generation, longer fuzzing, `Semgrep`, OWASP ZAP active scan, and curated `Nuclei`

These lanes are intended to catch different classes of failures:

- source-level security mistakes
- workflow and supply-chain misconfigurations
- container hardening regressions
- tenant-isolation and auth-boundary regressions
- live HTTP surface issues in the running stack

The PR runtime lane targets a short allowlist of user-facing and admin/debug URLs. Local ZAP runs may still report `10049 Non-Storable Content` on intentional `404` discovery paths such as `/` or disabled `/debug/*` endpoints; CI treats that as report noise rather than an exploitable proxy-path failure.

## Recommended Production Baseline

- define an explicit `-tenant-map` for multi-tenant production
- keep `-tenant.allow-global=false` unless wildcard backend-default access is intentional
- set a strict `-tail.allowed-origins` list for browser clients
- keep request-size and timeout limits bounded
- require `-server.admin-auth-token` before enabling debug/admin surfaces
- set `-peer-auth-token` for peer cache and keep `-peer-insecure-ip-allowlist=false`
- validate representative wide queries against the execution limits before rollout
- keep `-metrics.export-sensitive-labels=false` on shared scrape paths
- enable TLS and validate upstream certificates in production

## Related Docs

- [Extended security guide](docs/security.md)
- [Security hardening migration](docs/security-hardening-migration.md)
- [Testing and local validation](docs/testing.md)
- [Configuration](docs/configuration.md)
- [API Reference](docs/api-reference.md)
- [Observability](docs/observability.md)
- [Known Issues](docs/KNOWN_ISSUES.md)
