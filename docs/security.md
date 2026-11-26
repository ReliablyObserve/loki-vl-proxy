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
- query callers are not authenticated (see [Caller authentication](#caller-authentication))

`/loki/api/v1/delete` is not registered by default and answers `404`, as Loki does with deletion disabled; `-delete-api-enabled=true` registers it behind its own token (`-delete-api-auth-token`, required: the proxy refuses to start without it) and runs Loki's delete requests as VictoriaLogs delete tasks (see [Delete API](#3-delete-api)).

Behavior changes from the 1.67.0 and 1.68.0 hardening releases (wildcard denial, tenant-scoped cache keys, execution limits) are described in the [security hardening migration guide](security-hardening-migration.md).

## Caller Authentication

The proxy does not authenticate the clients that query it, and neither does Loki: Loki ships no authentication layer and expects an authenticating reverse proxy in front of it. `X-Scope-OrgID` is taken as sent and selects the VictoriaLogs tenant (see [Tenant Isolation](#1-tenant-isolation)); `-auth.enabled` and `-require-tenant-header` only require the header to be present. Anyone who can reach the listener can therefore query any tenant the tenant map routes to.

In production, run the proxy behind an authenticating gateway (an ingress or reverse proxy with OIDC, OAuth2 or another identity check) that:

- is the only network path to the proxy listener (a Kubernetes `NetworkPolicy`, a private Service, or a loopback or sidecar bind);
- sets `X-Scope-OrgID` from the authenticated identity and drops any value the client sent;
- covers every route Grafana uses, including the `/loki/api/v1/tail` WebSocket upgrade.

Where a gateway is not available, require client certificates at the proxy: `-tls-cert-file`, `-tls-key-file`, `-tls-client-ca-file` and `-tls-require-client-cert=true`. A client certificate proves which client connected, not which tenant it may read, so combine it with a tenant map that only routes the tenants those clients are allowed to see.

## High-Impact Controls

### 1) Tenant Isolation

Setups, defaults and every option: [Tenancy and authentication](tenancy-and-auth.md).

- tenancy is off by default: `X-Scope-OrgID` is ignored and every client reads VictoriaLogs account `0:0`, as Loki with `auth_enabled: false`. It is on with any of `-auth.enabled`, `-require-tenant-header`, `-tenant-map`, `-tenant-map-file`, `-tenant-label` or `-tenant.numeric-passthrough=true`
- with tenancy on, `X-Scope-OrgID` is checked as Loki checks it (Loki's `401 no org id` and `400` texts) and mapped to VictoriaLogs tenants via the tenant map, numeric passthrough or the tenant label
- optional multi-tenant fanout is explicit (`tenant-a|tenant-b`)
- wildcard tenant mode (`*`) is proxy-specific and requires explicit allow config: an unmapped `X-Scope-OrgID: *` returns `403` in native and label-routing (`-tenant-label`) modes unless `-tenant.allow-global=true`; an explicit tenant-map entry for `*` takes precedence; in label-routing mode, and with strict routing (an explicit `-tenant.numeric-passthrough=false` next to a tenant map), the default aliases (`0`, `fake`, `default`) and an absent or empty `X-Scope-OrgID` follow the same rule, except that no map entry admits an empty org
- numeric tenants are canonical: with numeric passthrough, `X-Scope-OrgID: 7` routes to VictoriaLogs account `7`, while `007`, `00`, `-1` and anything above `4294967295` is refused with `403 unknown tenant` unless the tenant map lists that exact string, because Loki keeps `007` and `7` as different tenants and VictoriaLogs would otherwise merge them (or receive an invalid `AccountID`). Admission and the `AccountID`/`ProjectID` headers the proxy sets come from one resolver, so an admitted org is never served from `0:0` by accident (the cold tier also receives `X-Scope-OrgID` and resolves it itself; mirror tenant-map entries in its own alias table); see [Numeric tenants](tenancy-and-auth.md#numeric-tenants-and-strict-routing)
- label-routing mode enforces the tenant constraint through VictoriaLogs `extra_stream_filters`, so request parameters and query pipelines cannot override it; the configured tenant field must be a stream field
- cache and coalescer keys (memory, disk, peer, window, metadata and label-value indexes) include the resolved tenant routing, backend identity and forwarded-identity fingerprint, so entries are never shared across tenants or authorization scopes
- credentials enter cache keys only as keyed digests: forwarded `Authorization` headers, other `-forward-headers` and `-forward-cookies` values, and `-backend-basic-auth` are digested with HMAC-SHA256 under a key derived (HKDF-SHA256) from `-cache-key-secret`, else from `-peer-auth-token`, else from random bytes per process. A cache key read from the disk cache, a snapshot, a peer or a debug log line cannot be used to test credential guesses without that secret, which is never written to the cache volume. Replicas or restarts with the same secret share credential-scoped entries; with different or no secret they miss (never another caller's answer), and without any secret credential-scoped entries are not reused after a restart or by other replicas (the proxy logs a warning at start). Keys of requests that carry no credential are unchanged. See [Cache key secret](#cache-key-secret)
- entries computed with a caller's forwarded credentials stay on the replica that computed them: every key built from such a request carries the credential fingerprint, or a mark when the key hashes it, and the peer cache neither pushes those keys, lists them in its hot index, nor prefetches them; `/patterns` requests with forwarded credentials are answered by the replica that receives them instead of being forwarded to the pattern state's owner. A peer still answers a read of the exact key, which only a request with the same credentials and cache key secret can name
- user information in the `-backend` or cold backend URL (`http://user:password@host`) is still sent to VictoriaLogs, but cache keys and the restart hand-over scope hold the URL without it (cache keys add its keyed digest), and log lines mask it

**Lightweight tenant enforcement**: `-require-tenant-header=true` rejects any request missing an `X-Scope-OrgID` header with HTTP 401. This is a lighter alternative to full auth — it catches misconfigured clients without requiring a token/credential system.

**Backend tenant header forwarding**: Set `FORWARD_TENANT_HEADER=false` to prevent the proxy from forwarding `X-Scope-OrgID` to the backend (useful if the VL backend does not support multi-tenancy).

### 2) `/tail` Browser-Origin Controls

- `/loki/api/v1/tail` can enforce allowed browser origins
- use `-tail.allowed-origins` for Grafana/browser clients
- keep restrictive defaults for internet-exposed deployments

### 3) Delete API

By default `/loki/api/v1/delete` is not registered: every method answers `404` with the body `404 page not found`, which is what Loki returns when deletion is disabled (Loki registers the route only with `-compactor.retention-enabled=true`, off by default). No tenant header or other check runs first, as in Loki. With `-delete-api-enabled=true` the route is registered:

- the delete token, in `X-Delete-Token` or `Authorization: Bearer` (constant-time compare): `-delete-api-enabled=true` requires `-delete-api-auth-token`, `-delete-api-auth-token-file` or `DELETE_API_AUTH_TOKEN` (Helm: `deleteAuth.existingSecret`), and the proxy and the chart refuse to start or render without it. A request without the token answers Loki's `401 no org id` before any other check, so an unauthenticated caller learns nothing about the route; the log line names the missing token. Bare Loki has no authentication of its own; this check is the proxy's. The token is removed from the request before it is handled, so it is never forwarded to VictoriaLogs (even with `-forward-authorization`) or logged. It is separate from `-server.admin-auth-token`: neither token opens the other's routes, and enabling deletes leaves `/admin/*` and `/debug/*` on the loopback `-admin-listen` exactly as before
- the tenant, as Loki checks it (`401 no org id`, Loki's tenant ID texts, `400 multiple org IDs present` for a multi-tenant header)
- Loki's checks of the request, with Loki's texts (`query not set`, `invalid query expression`, `start time not set`, the time formats, `start time can't be greater than or equal to end time`, `max_interval`). Loki has no other guard, and neither has the proxy after the token: a token holder can delete every stream of a tenant (`{}`) over any range. Treat the token like a credential that can erase data
- a create request runs as one VictoriaLogs delete task (`/delete/run_task`) scoped by the tenant's `AccountID`/`ProjectID` and a filter of the time range (both ends included) and the rows the same log query reads; a warning-level audit log line names the tenant, the query, the range, the client and the task id
- a cancel stops a task only when VictoriaLogs lists it with the request's tenant alone (`/delete/stop_task` checks no tenant itself); any other id answers Loki's `404`

Not served, each answered with Loki's `500`: the list of requests, a query whose rows a VictoriaLogs filter cannot select (a parser, a filter on a parsed label), a tenant tenant-label routing confines with its label (a tenant mapped to its own account is served) and the unmapped global tenant `*`. VictoriaLogs answers the delete task API only with `-delete.enable` (and, from v1.53, a `-deleteAuthKey` the proxy does not send yet); a refusal is Loki's `500`, VictoriaLogs' text in the log line only. See [Remaining delete API gap](security-hardening-migration.md#remaining-delete-api-gap).

### 4) Request Hardening

- max request body/header limits
- request timeout boundaries
- built-in rate limiting (per-client token bucket keyed on the connection address; `-rate-limit-per-second`, `-rate-limit-burst`) and global concurrency guards (`-max-concurrent`, which also bounds backend operations until their bodies are consumed)
- 64 KB maximum query string length and the `-default-max-query-length` time-range ceiling (Loki's `721h` by default, `0` turns it off)
- request coalescing + circuit breaker to reduce backend cascade risk

### 5) Execution Limits

Excessive query work is rejected with an explicit error instead of returning partial or oversized successful responses:

- raw metric evaluation: `-manual-range-metric-row-limit` rows (default 1,000,000) and `-max-stats-query-series` series (default 1000), plus 64 MiB input/output and one million output samples
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

### OpenSSF Scorecard

`.github/workflows/scorecard.yaml` publishes the Scorecard result for `main` to the
public Scorecard API on every push to `main` and weekly; the README badge and
[the Scorecard viewer](https://scorecard.dev/viewer/?uri=github.com/ReliablyObserve/loki-vl-proxy)
show it. Pull requests run the same scanner in the fast lane above, with
`scripts/ci/check_scorecard.py` failing a pull request below an overall 5.0 or when
`Dangerous-Workflow` or `Binary-Artifacts` drop below 10, `CI-Tests` below 8 or `SAST`
below 7 (a pull request's commit is scored on fewer checks than `main`).

The badge shows the published score once `scorecard.yaml` has run on `main`. A local
run of Scorecard v5.3.0 against `main` on 2026-10-21 scored 6.9; the target is 8.0
or higher.

- `Token-Permissions`: this change moves the write permissions of the badge
  workflows to the one job that needs them, which takes the check from 0 to 10
  (Scorecard `--local` on the workflow tree with this change applied).
- `Signed-Releases`: releases since v2.22.1 carry `checksums.txt.sigstore.json`;
  releases cut after the change that attaches `loki-vl-proxy-<version>.intoto.jsonl`
  (the build provenance and SBOM attestation bundles) also carry provenance. The check
  scores the five most recent releases (8 for a signed release, 10 for one with an
  `.intoto.jsonl` asset; the probe matches the asset name suffix only), so it
  reaches 10 once five releases carry the file.
- `Code-Review` and `Branch-Protection` count required approvals by a second
  maintainer and `CII-Best-Practices` the OpenSSF Best Practices badge; those are
  project-process changes rather than code changes.

The pull-request scanner image (`ghcr.io/ossf/scorecard`) is pinned to the Scorecard
version bundled by the `ossf/scorecard-action` pin (v2.5.0 bundles v5.6.0), so a pull
request is scored by the same scanner version as the published result.

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
- with default values the rendered pod spec has no `hostPath` volume and passes the Pod Security `baseline` and `restricted` profiles
- `systemMetrics.hostProc.enabled` is `false` by default. Setting it to `true` makes the chart mount five individual host files read-only as `hostPath` `type: File` volumes — `/proc/stat`, `/proc/meminfo`, `/proc/pressure/cpu`, `/proc/pressure/memory` and `/proc/pressure/io` — under `/host/proc` and pass `-host-proc-root`; the host's `/proc` directory and other workloads' per-process data are not mounted

The opt-in `hostPath` mounts are rejected by Pod Security `baseline` and `restricted`, so the namespace must be labeled `privileged` or the workload exempted in your admission policy. Trivy flags them as `KSV-0121`; the repository's `.trivyignore.yaml` carries no exception for them because the default render does not contain them. Leave `systemMetrics.hostProc` off on clusters that forbid `hostPath`.

## Release Verification

Release images, Helm charts and binaries are signed with Sigstore, ship with SBOMs and GitHub build provenance, and the binaries are reproducible from the tag. The verification commands are in [SECURITY.md](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/SECURITY.md#verifying-releases).

## Admin and Debug Endpoints

The following are disabled by default and should stay restricted:

- `/debug/queries` (`-server.enable-query-analytics`)
- `/debug/pprof/*` (`-server.enable-pprof`)
- `/admin/cache/flush` and `GET /admin/capabilities` (the VictoriaLogs releases seen and every capability gate; registered with `-server.register-instrumentation=true`, admin-token protected like the flush, and showing no secret, backend address or tenant)

Enable only for controlled troubleshooting windows. Without `-server.admin-auth-token`, admin/debug routes are served only on `-admin-listen` (default `127.0.0.1:3101`), and the proxy refuses to start when these routes are enabled on a non-loopback address without a token. With a token, they move onto the main listener and require `Authorization: Bearer <token>` or `X-Admin-Token`. The chart keeps `admin-listen` on loopback and never publishes it through the Service.

`/metrics` is not served by default. Set `-server.register-instrumentation=true` to enable it on the main listener, or add `-metrics-listen` for a dedicated port (the chart uses `:9091`). The default export suppresses per-tenant and per-client identity labels. Opt back in with `-metrics.export-sensitive-labels=true` only on trusted scrape paths.

## Secrets in Process Arguments

A flag value is part of the process command line. `/proc/<pid>/cmdline` is readable by every user that can see the process (unless `/proc` is mounted with `hidepid`), so it shows up in `ps` on the node, in containers that share the pod's process namespace, and in process listings collected by crash reports or node diagnostics. A `$(VAR)` reference in Kubernetes `args` does not help: the kubelet expands it before the container starts, so the plain value is in the arguments. An environment variable is narrower: `/proc/<pid>/environ` is readable only by a process allowed to trace it (the same user or `CAP_SYS_PTRACE`). A file is readable by whoever can read the file in the container's filesystem.

The six secrets can be given without a flag value:

| Secret | File flag | Environment variable | Chart value |
|---|---|---|---|
| Peer cache token | `-peer-auth-token-file` | `PEER_AUTH_TOKEN` | `peerCache.authToken` / `peerCache.existingSecret` |
| Admin token | `-server.admin-auth-token-file` | `ADMIN_AUTH_TOKEN` | `adminAuth.existingSecret` |
| Delete API token | `-delete-api-auth-token-file` | `DELETE_API_AUTH_TOKEN` | `deleteAuth.existingSecret` |
| VictoriaLogs basic auth | `-backend-basic-auth-file` | `BACKEND_BASIC_AUTH` | `backendAuth.existingSecret` |
| Version URL basic auth | `-backend-version-basic-auth-file` | `BACKEND_VERSION_BASIC_AUTH` | `backendVersionAuth.existingSecret` |
| Cache key secret | `-cache-key-secret-file` | `CACHE_KEY_SECRET` | `cacheKeySecret.existingSecret` |

- The value flag wins, then the file flag, then the environment variable. A value flag and a file flag for the same secret stop startup.
- A file flag whose file is missing, unreadable, empty or larger than 64 KiB stops startup. Line endings at the end of the file are dropped.
- Files and variables are read once at startup. Restart the proxy after rotating a secret.
- The environment variables are read whenever they are set. `ADMIN_AUTH_TOKEN` in the environment therefore has the effect of `-server.admin-auth-token`: the admin and debug routes move to the main listener behind that token.
- The Helm chart mounts each Secret key as a read-only file and passes the `-...-file` flag. A proxy image older than the chart does not know these flags and exits, so it never serves without the credential.
- Values passed through `extraArgs` (including `$(NAME)` references) keep working and stay visible in the command line: move them to the chart values above or to `-...-file` flags.

## Cache Key Secret

Cache keys are stored with the answers: in the disk cache and its restart hand-over, in pattern and label-index snapshots, on peer replicas, in the peers' hot-key index and in debug log lines. When the proxy forwards credentials (`-forward-authorization`, `-forward-headers`, `-forward-cookies`) or authenticates to VictoriaLogs (`-backend-basic-auth`), those keys must keep different credentials apart without letting anyone who holds the keys check a guessed password. The proxy therefore digests credentials with HMAC-SHA256 under a key it derives at start:

| Configured | Key | Credential-scoped entries |
|---|---|---|
| `-cache-key-secret` (or `-cache-key-secret-file`, `CACHE_KEY_SECRET`) | derived from that secret | reused by every replica and restart with the same secret |
| else `-peer-auth-token` | derived from the peer token (never the token itself) | reused across a peer fleet, which shares the token already |
| neither | random per process | not reused after a restart or by another replica; a fleet on `-peer-insecure-ip-allowlist` stops sharing them between replicas, and with `-backend-basic-auth` that is every entry |

- Set `-cache-key-secret` to the same long random value on every replica that shares a disk cache or peers. A single replica with a persistent disk cache and forwarded or backend credentials needs it to keep those entries across restarts, and so does a fleet without a peer token (`-peer-insecure-ip-allowlist`) to share them.
- The Helm chart provides one by default: with `cacheKeySecret.existingSecret` unset (and `cacheKeySecret.generate=true`), it keeps a random 64-character value in the Secret `<release>-cache-key`, created on the first install and kept on every upgrade through `lookup`. Every replica of the release mounts it, so a single replica with a persistent cache keeps credential-scoped entries across restarts. `cacheKeySecret.existingSecret` wins; so does a `-cache-key-secret`/`-cache-key-secret-file` set through `extraArgs`.
- Renderers without cluster access (`helm template`, Argo CD and any tool where `lookup` returns nothing) cannot read the stored value. The chart then renders and mounts no cache key at all, instead of a new random value on every render that would replace the key at each sync. The proxy falls back to the peer token, else a key per process, and logs the per-process case at start. Use `cacheKeySecret.existingSecret` with such renderers.
- Rotation: deleting `<release>-cache-key` (or pointing `existingSecret` at another value) makes the next install or upgrade use a new key once pods restart. Pods on the new key miss the credential-scoped entries of the old key, which age out; during the rolling restart old and new pods do not share those entries. No answer is served across keys.
- Changing the secret, or the peer token it is derived from, makes credential-scoped entries miss once; nothing is served across the change.
- Entries written by a build before keyed digests are never read by a later build: their keys have another form.
- Requests that carry no credential (no forwarded header or cookie, no backend basic auth) have the same cache keys as before; only the tenant and routing scope is hashed for them, as before.
- The restart hand-over scope never includes credential flags (`-backend-basic-auth`, `-backend-version-basic-auth`, tokens, secrets) or the user information of a URL value.

## Log and Error Redaction

- all log output passes through a redacting handler that masks API keys, bearer tokens, passwords, AWS credentials and URL-embedded credentials
- debug logs record LogQL/LogsQL and backend parameters as `sha256:<8hex> len=<n>` fingerprints; `-debug-log-raw-queries=true` restores raw values for local debugging only
- the request log records the authentication mechanism (`auth.source`) but not the Basic-Auth principal
- VictoriaLogs error bodies are reduced to their message and stripped of stream selectors, long quoted literals and long hex identifiers before they are logged or returned; transport errors no longer echo backend URLs with query parameters. Redaction is disabled only by `-debug-log-raw-queries=true`

## Recommended Production Baseline

- an authenticating gateway in front of the proxy that sets `X-Scope-OrgID` (see [Caller authentication](#caller-authentication)), or client certificates required at the proxy
- explicit tenancy (`-tenant-map` or `-tenant-map-file`, with `-auth.enabled`): the default is one shared tenant
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
  ghcr.io/ossf/scorecard:v5.6.0@sha256:c0a21a1c33a2d8819937d0a8a62e6f4189c800639abd9f0bfbe61c90c71fae90 \
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