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
- delete API gated behind confirmation, time-bounds, selector validation, tenant scoping, and audit logging
- admin/debug endpoints disabled unless explicitly enabled
- runtime image runs as a non-root user
- runtime image keeps a read-only root filesystem
- Helm chart drops Linux capabilities and blocks privilege escalation

## Built-In Controls

- **Read-only by default**: `/loki/api/v1/push` blocked with 405
- **Delete safeguards**: Confirmation header, tenant scoping, time range limits, audit logging
- **Rate limiting**: Per-client token bucket using `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **Query length limit**: 64KB max to prevent abuse
- **Security headers**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`
- **Consistent response hardening**: the proxy now applies the same baseline security headers on success, `404`, and disabled admin/debug responses so scanners and browsers do not see a weaker edge path
- **HTTP hardening**: Configurable read/write/idle timeouts, max header/body bytes
- **TLS support**: Server-side HTTPS via `-tls-cert-file`/`-tls-key-file`
- **Tail browser-origin controls**: `/tail` can enforce explicit allowed origins
- **Admin token enforcement**: non-loopback admin/debug exposure requires `-server.admin-auth-token`
- **Peer-cache protection**: `-peer-auth-token` can protect fleet peer cache traffic
- **Sensitive metrics off by default**: per-tenant and per-client identity labels are not exported unless explicitly enabled
- **No secrets in logs**: all log output passes through a redacting slog handler that detects and masks API keys, bearer tokens, passwords, AWS credentials, and URL-embedded credentials

## Known Security Considerations

- **`text/template` in `| line_format`**: Go templates are executed on query results. The template FuncMap is restricted to safe string functions, but this is still a surface to treat carefully in shared environments.
- **Disk cache**: No application-level encryption. Use cloud-provider or node-level disk encryption for data at rest.
- **Backend TLS skip**: `-backend-tls-skip-verify` disables certificate validation for VictoriaLogs connections.
- **Host `/proc` mount for system metrics**: the Helm chart can mount host `/proc` read-only to expose richer process and PSI metrics. This is intentional and narrowly allowlisted in CI because some clusters forbid `hostPath`.

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
- set `-peer-auth-token` when peer-cache traffic crosses trust boundaries
- keep `-metrics.export-sensitive-labels=false` on shared scrape paths
- enable TLS and validate upstream certificates in production

## Related Docs

- [Extended security guide](docs/security.md)
- [Testing and local validation](docs/testing.md)
- [Configuration](docs/configuration.md)
- [API Reference](docs/api-reference.md)
- [Observability](docs/observability.md)
- [Known Issues](docs/KNOWN_ISSUES.md)
