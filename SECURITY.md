# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.18.x  | Yes       |
| < 0.17  | No        |

## Reporting a Vulnerability

Please report security vulnerabilities via GitHub Security Advisories:
https://github.com/szibis/Loki-VL-proxy/security/advisories/new

Do NOT open a public issue for security vulnerabilities.

## Security Features

- **Read-only by default**: `/loki/api/v1/push` blocked with 405
- **Delete safeguards**: Confirmation header, tenant scoping, time range limits, audit logging
- **Rate limiting**: Per-client token bucket using `RemoteAddr` (not spoofable `X-Forwarded-For`)
- **Query length limit**: 64KB max to prevent abuse
- **Security headers**: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Cache-Control: no-store`
- **HTTP hardening**: Configurable read/write/idle timeouts, max header/body bytes
- **TLS support**: Server-side HTTPS via `-tls-cert-file`/`-tls-key-file`
- **No secrets in logs**: All log output passes through a redacting slog handler that detects and masks API keys, bearer tokens, passwords, AWS credentials, and URL-embedded credentials. Query strings are also truncated at 200 chars.
- **Pod security**: Helm chart sets `readOnlyRootFilesystem`, `runAsNonRoot`, drops all capabilities

## Known Security Considerations

- **`text/template` in `| line_format`**: Go templates are executed on query results. The template FuncMap is restricted to safe string functions, but this is a surface for template injection if the proxy is shared across untrusted users.
- **Disk cache**: No application-level encryption — use cloud provider disk encryption (EBS, PD, etc.) for data at rest.
- **Backend TLS skip**: `-backend-tls-skip-verify` disables certificate validation for VL connections.
