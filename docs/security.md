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

## Related Docs

- [Configuration](configuration.md)
- [API Reference](api-reference.md)
- [Observability](observability.md)
- [Known Issues](KNOWN_ISSUES.md)
