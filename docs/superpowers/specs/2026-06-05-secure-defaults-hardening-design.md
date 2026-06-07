# Secure-by-default startup + hardening

**Status:** Draft — awaiting user approval
**Date:** 2026-06-05
**Branch:** `szibis/secure-defaults-hardening` (cut from `origin/main` @ cbf26bf, v1.55.0)
**Scope:** Single PR addressing 7 of 8 findings from the recent security/reliability review. Finding #8 (proxy.go / main.go refactor) is deferred to the existing proxy-architecture-refactor plan.

## Goal

Ship a Loki-VL-proxy build whose binary and Helm chart defaults are safe to run as-is — no surprise panics on startup, no implicit IP-only peer trust, no unbounded metadata scans, no raw query literals in logs, no broad `/host/proc` exposure — without regressing the metrics, observability, or peer-cache behavior that current operators depend on.

## Findings addressed

| # | Finding | Severity | Breaking? | Approach |
|---|---|---|---|---|
| 1 | Binary defaults won't start (admin token required) | P0 | **Yes** | Loopback admin by default; flip `register-instrumentation` default to false |
| 2 | Peer cache runs without shared token (IP-only fallback) | High | **Yes** | Require token when peer discovery configured; explicit `--peer-insecure-ip-allowlist` opt-out |
| 3 | Metadata `/labels`, `/label/{name}/values` unbounded scans | Medium | No | `--metadata-default-lookback=12h` (0 disables) |
| 4 | Debug logs include raw LogQL/LogsQL + backend params | Medium | No | Redact to `sha256:<8hex>+len=<n>` by default; raw via `--debug-log-raw-queries` flag |
| 5 | Helm mounts entire host `/proc` hostPath | Medium | No | Split `--host-proc-root` from `--proc-root`; chart mounts 5 specific files |
| 6 | Chart disables proxy rate limits by default | Medium | **Yes** | Conservative defaults: `maxConcurrentRequests=64`, `rateLimit.perSecond=50`, `burst=100` |
| 7 | Backend version gate is intentionally soft | Low/Med | No | Add `--backend-version-strict=false` opt-in for hard failure |
| 8 | Core `proxy.go` / `main.go` are too large | Low | — | **Deferred** — owned by existing refactor plan |

Three of the seven (#1, #2, #6) are deliberately breaking. Each gets an explicit `### BREAKING CHANGES` entry in `CHANGELOG.md` with old → new behavior, migration command, and the one-line escape hatch.

## Architecture

### Branch & PR shape
- Cut from latest `origin/main` (cbf26bf, v1.55.0).
- Branch: `szibis/secure-defaults-hardening`.
- One PR. ~7 commits, one per finding, plus a final `docs(changelog)` commit.
- No upstream VL/VT modifications (per [[feedback_vl_vt_upstream]]).

### Code-level design (per finding)

#### 1. Startup defaults (binary) — BREAKING

**Current state** (`cmd/proxy/main.go:519, 639, 1129`): default `-listen=:3100`, `-server.register-instrumentation=true`, `-server.admin-auth-token=""`. Startup validator at `:1129` rejects empty admin token when admin endpoints are reachable from non-loopback. Net effect: `go run ./cmd/proxy -backend=...` exits immediately.

**Change:**
- New flag `--admin-listen` (default `127.0.0.1:3101`). When `-server.admin-auth-token` is empty, `/admin/*` and `/debug/*` are wired onto a dedicated listener on `--admin-listen`. Main `--listen` only serves proxy traffic.
- Default of `-server.register-instrumentation` flips `true → false`. New flag `--metrics-listen` (default `""`) gates a dedicated metrics listener. Semantics: when `register-instrumentation=true` AND `metrics-listen != ""`, `/metrics` is served on the dedicated listener; when `register-instrumentation=true` AND `metrics-listen == ""`, `/metrics` is served on the main `--listen` (back-compat); when `register-instrumentation=false` (new default), `/metrics` is not served at all. The chart sets `register-instrumentation=true` and `metrics-listen=:9091` so Prometheus / ServiceMonitor scraping continues without operator action; binary users who scrape `:3100/metrics` today must add `-server.register-instrumentation=true`.
- Startup validator (`main.go:1129`) only rejects when admin endpoints are bound to a non-loopback address with no token. Loopback-bound admin without a token is allowed.
- When `-server.admin-auth-token` is set, admin endpoints are exposed on the main listener as today (unchanged for current users who already have a token).

**Outcome:** `go run ./cmd/proxy -backend=http://127.0.0.1:9428` boots and serves proxy traffic on `:3100`; admin/debug reachable only from `127.0.0.1:3101`; `/metrics` off unless `--metrics-listen` is set.

#### 2. Peer cache token — BREAKING

**Current state** (`internal/proxy/middleware.go:255`, `internal/cache/peer.go:540`): if `--peer-auth-token` is empty, peer endpoints (`/_cache/get`, `/_cache/set`, `/_cache/hot`) accept any request from a source IP that matches the discovered peer list. In flat Kubernetes east-west networking this is weaker than a shared secret.

**Change:**
- At startup in `main.go`: if `--peer-discovery` is set (`dns` or `static`) AND `--peer-auth-token` is empty AND `--peer-insecure-ip-allowlist` is not `true`, refuse to start with a clear error pointing to both fixes.
- The existing IP-allowlist fallback in `middleware.go:255` is kept but gated behind `--peer-insecure-ip-allowlist=true`. The constant-time token comparison path (already in code) becomes the only default path.
- Chart change: `peerCache.authToken` in `values.yaml` becomes required when `peerCache.enabled=true`. Resolution order: (a) explicit non-empty `peerCache.authToken` → used verbatim; (b) explicit `peerCache.existingSecret` → referenced as Secret env source; (c) both unset AND `helm lookup` finds a prior chart-managed Secret → reuse existing value (idempotent upgrade); (d) all unset on first install → generate 32-byte token via `randAlphaNum 32`, store in chart-managed Secret. Explicit empty `peerCache.authToken: ""` is treated as a chart values error and fails templating with a clear message, NOT as "generate one for me" — operators must either set a value or remove the key. Token Secret name is templated from release name so multiple releases coexist.

**Outcome:** Default-installed chart has a real shared secret. Binary users get a clear startup error rather than silent IP-only trust.

#### 3. Metadata default lookback — non-breaking

**Current state** (`internal/proxy/label_metadata.go:80, 123`, `internal/proxy/label_handlers.go:49, 184`): `start`/`end` are forwarded to the backend only when the client supplies them. Grafana frequently omits them on `/labels`, triggering full-retention scans.

**Change:**
- New flag `--metadata-default-lookback=12h` (`0` disables). Default `12h`.
- In `metadataQueryParams`: when both `start` and `end` are blank and `metadataDefaultLookback > 0`, inject `now-lookback` → `now` (UnixNano strings, matching existing format).
- Applied to `/labels`, `/label/{name}/values`, and `/series`. The `/series` call site is verified to share the same `metadataQueryParams` helper; if not, the same guard is added there.
- The pending diff stashed on `szibis/fix-ghcr-badges` is the starting point; it covers `/labels` and `/label/{name}/values`. `/series` parity is added here.

**Outcome:** Defaults bound the window; clients that already pass `start`/`end` are unaffected. Setting `--metadata-default-lookback=0` restores prior behavior.

#### 4. Debug log redaction — non-breaking

**Current state:** `internal/proxy/proxy.go:1774, 1884` and `internal/proxy/backend.go:566, 620` (and any other `level=debug` sites) log raw `query`/`logsql_query`/encoded VL params. Debug-only, but query literals often contain sensitive values (IDs, tokens, customer-identifying strings).

**Change:**
- New helper file `internal/proxy/debuglog.go` with `redactQuery(s string) string` returning `"sha256:" + hex(sha256(s))[:8] + " len=" + strconv.Itoa(len(s))` for empty-or-not values.
- All call sites in `proxy.go` and `backend.go` route through the helper. Grep verifies no others.
- New flag `--debug-log-raw-queries=false` (default `false`). When `true`, the helper short-circuits and returns the raw string (for local dev / repro).
- Tenant ID and orgID are NOT redacted — operationally critical, not sensitive in our threat model. Documented in the helper's package doc.

**Outcome:** Default debug logs identify queries uniquely (hash + length) without leaking literals. Operators who need raw output flip the flag.

#### 5. Helm hostPath `/proc` — non-breaking (refined)

**Current state** (`charts/loki-vl-proxy/values.yaml:750`, `charts/loki-vl-proxy/templates/deployment.yaml:307`): the chart mounts `/proc` from the host as a directory hostPath at `/host/proc`. `internal/metrics/system.go` reads files via `selfProcPath(...)` resolved against `--proc-root`.

**Real file dependencies** (traced from `internal/metrics/system.go`):

| Scope | Path | Required? |
|---|---|---|
| Host CPU | `/proc/stat` | Host |
| Host memory | `/proc/meminfo` | Host |
| Host PSI | `/proc/pressure/{cpu,memory,io}` | Host |
| Proxy resource use | `/proc/self/{status,io,stat,fd}` | Self (container `/proc`) |
| Proxy network | `/proc/net/dev` | Self (container netns) |

Only 5 system-wide files genuinely require host-scope. `self/*` and `net/dev` work fine via the container's own `/proc` (kernel resolves `self` to the calling process; `net/dev` reads from the calling netns).

**Change:**
- `internal/metrics/system.go`: introduce a second root, `hostProcRoot`, used only by the three host-wide reads (`stat`, `meminfo`, `pressure/*`). `selfProcPath` stays bound to local `/proc` (or `--proc-root`, unchanged).
- `cmd/proxy/main.go`: new flag `--host-proc-root` (default `/proc`). Set to `/host/proc` in the chart.
- The 5 host-wide paths are declared as a package-level slice `hostProcFiles` in `system.go` so the chart and the code share a single source of truth (and a unit test asserts the slice covers every `procPath(...)` call site).
- Chart change: replace the broad `hostPath: /proc` directory mount with 5 individual `hostPath` mounts of `type: File` for the 5 paths, each mounted read-only at `/host/proc/<same path>`. `securityContext.readOnlyRootFilesystem` compatible.
- `systemMetrics.hostProc.enabled` default stays `true` — host metrics keep working out of the box. Setting it to `false` skips all five mounts (operators on restricted PSA who don't want any hostPath).

**Outcome:** Default chart still exposes host CPU/memory/PSI. Surface shrinks from "all of `/proc` including every process's command line" to "5 system-wide counter files." Per-process info for other workloads is no longer reachable.

#### 6. Helm chart rate limits — BREAKING

**Current state** (`charts/loki-vl-proxy/values.yaml:61`): `proxy.maxConcurrentRequests=0` (unlimited) and `proxy.rateLimit.perSecond=0`, `rateLimit.burst=0` (disabled). A single noisy Grafana instance can drive expensive fanout.

**Change:**
- `proxy.maxConcurrentRequests: 64` (per-replica concurrent in-flight ceiling).
- `proxy.rateLimit.perSecond: 50`, `rateLimit.burst: 100` (per-client token bucket).
- `values.yaml` comments link to a new `docs/tuning.md` section and explicitly call out that `0` restores unlimited behavior.
- `templates/NOTES.txt` prints the rendered values on install.

**Outcome:** Default installs are protected against a noisy client. Operators with explicit ingress-level limits opt back to unlimited with three `--set` flags.

#### 7. Backend version gate — non-breaking

**Current state** (`internal/proxy/backend.go:455`): `/health` failure, non-2xx response, or missing/sub-min semver only logs and continues.

**Change:**
- New flag `--backend-version-strict=false`.
- When `true`, the same condition that currently logs is upgraded to returning an error from `New()` (or whatever constructor wires the version check). Proxy refuses to start.
- Default stays soft (logs only) for backward compatibility with current operators.

**Outcome:** Operators who want hard guarantees on backend version flip one flag. Default behavior is unchanged.

## Data flow & error handling

- Request path is unchanged except that debug-level log lines for query/params go through `redactQuery` and metadata endpoints inject default `start`/`end` when both are blank.
- Startup path: order is (a) flag parse, (b) config build, (c) validators in order — admin-bind sanity → peer-token sanity → version-strict check. Each emits a single clear error and exits with non-zero status. No partial start.
- Peer cache request path: when `--peer-insecure-ip-allowlist=false` (default), every `/_cache/*` request requires constant-time-equal token match; failure returns 401 with no body. When `true`, the existing IP-membership fallback is used (one warn log per minute that the proxy is in legacy mode).

## Testing strategy

Per [[feedback_layered_test_strategy]] — unit + integration + parity + e2e + regression guards, not just verifications.

**Unit:**
- `internal/proxy/debuglog_test.go` — empty, short, long, multi-line, non-ASCII inputs; stability of hash; `--debug-log-raw-queries=true` short-circuit.
- `internal/proxy/label_metadata_test.go` — both `start`/`end` blank → injection; one provided → no injection; `--metadata-default-lookback=0` → no injection.
- `internal/proxy/backend_test.go` — strict mode error paths (health 5xx, missing semver, sub-min semver) and soft-mode log-only paths.
- `internal/metrics/system_test.go` — `hostProcFiles` slice is exhaustive vs. the `procPath()` call sites (string-match test on the source file or table-driven).
- `cmd/proxy/main_test.go` — admin-bind selector logic (token set → main listener; token empty → admin listener); peer-token validator (discovery set + token empty + no insecure flag → error).

**Integration** (existing harness in `test/integration/`):
- Boot proxy with `-backend=<test-vl>` and no other flags → assert: main `:3100` returns proxy 200, `127.0.0.1:3101/admin/healthy` returns 200, non-loopback `:3100/admin/healthy` returns 404.
- Boot with `--peer-discovery=static --peer-static=10.0.0.1:3100` and no token → assert non-zero exit + stderr matches expected error string.
- Boot with the same + `--peer-auth-token=test` → assert clean startup, `/_cache/get` without `X-Peer-Token` returns 401, with correct token returns 200.
- Boot with `--metadata-default-lookback=1h` + `/labels` request with no `start`/`end` → assert backend received `start`/`end` in the 1h window.

**Parity:**
- Run the existing query-shape parity suite against a backend pre/post change → no diffs (no semantic regression).

**Helm:**
- `helm template` snapshot tests for the three values changes (hostProc mounts, rate limits, peer secret generation).
- `helm install --dry-run` with `peerCache.enabled=true` and no `authToken` → assert generated Secret in the rendered output.
- One chart-test pod that runs the rendered Deployment + connects to a test VL and confirms `/admin/healthy` is reachable from inside the cluster via the new admin Service.

**E2E:**
- `docker compose -f deployment/docker/docker-compose-e2e.yml up --build` runs to green with no flag overrides.
- Smoke probe: `go run ./cmd/proxy -backend=http://127.0.0.1:9428` exits non-zero only if it can't reach the backend (not because of missing flags) — asserted in CI as a 5-second timeout.

**Regression guards** (per [[feedback_harden_and_lock]]):
- Each test above asserts the pre-fix failure mode first (compiled out via build tag or asserted as `t.Skip()` on the pre-fix branch). Verified to FAIL on `main` and PASS on the new branch before commit.

## CHANGELOG

Top of `CHANGELOG.md` gets a new `## [Unreleased]` section:

```
## [Unreleased]

### BREAKING CHANGES

- **Binary admin endpoints now bind to loopback by default.** Previously `-server.register-instrumentation=true` and `-server.admin-auth-token=""` caused immediate startup failure. Now admin/debug routes are served on a dedicated `--admin-listen` (default `127.0.0.1:3101`) when no token is set; main `--listen` serves proxy traffic only. To restore the previous behavior, set `-server.admin-auth-token` to a value of your choice. Affected: operators running the binary without a token who relied on `:3100/admin/*` being reachable from outside the host.

- **`/metrics` is no longer served by default.** `-server.register-instrumentation` default flips from `true` to `false`. New `--metrics-listen` flag (default `""`) supports a dedicated listener. The chart now sets `register-instrumentation=true` and `metrics-listen=:9091` so ServiceMonitor / Prometheus scrapes keep working without operator action. Binary users who scrape `:3100/metrics` today must add `-server.register-instrumentation=true` (and optionally `--metrics-listen=:9091` for a dedicated port).

- **Peer cache requires a shared token by default.** When `--peer-discovery` is set and `--peer-auth-token` is empty, the proxy now refuses to start. To restore the previous IP-allowlist-only behavior, pass `--peer-insecure-ip-allowlist=true`. The Helm chart auto-generates a token Secret on first install if `peerCache.authToken` is unset, so chart users see no operational change.

- **Chart now enforces conservative request limits by default.** `proxy.maxConcurrentRequests` defaults from `0` (unlimited) to `64`. `proxy.rateLimit.perSecond` from `0` to `50`, `rateLimit.burst` from `0` to `100`. To restore unlimited behavior, set all three back to `0` in your values file.

### Added

- `--metadata-default-lookback=12h` flag bounds `/labels`, `/label/{name}/values`, and `/series` requests when the client omits `start`/`end`. `0` disables (prior behavior).
- `--backend-version-strict=false` flag promotes the existing soft version check to a hard startup failure when set.
- `--host-proc-root` flag for the proxy binary, allowing host-scope and self-scope `/proc` reads to use different roots.

### Security

- Debug logs no longer include raw LogQL/LogsQL or backend query params by default. Each query is logged as `sha256:<8hex>+len=<n>`. To restore raw debug output, pass `--debug-log-raw-queries=true`.
- Chart no longer mounts the host's entire `/proc` directory. Five specific system-wide counter files are mounted individually (`/proc/{stat,meminfo,pressure/cpu,pressure/memory,pressure/io}`). Per-process info for other workloads on the host is no longer reachable from the proxy container.

### Notes

- Finding #8 from the v1.55.0 review (size of `cmd/proxy/main.go` and `internal/proxy/proxy.go`) is tracked under the existing proxy architecture refactor plan and is intentionally out of scope here.
```

## Risks & mitigations

- **Binary CI scripts that hit `:3100/admin/*` without a token break silently.** Mitigation: the proxy logs a single info line at startup naming the admin listener address. Integration test covers the move.
- **Operators with restricted PSA who currently can't run the chart get a smaller surface, but five hostPath mounts may still trip strict policies.** Mitigation: setting `systemMetrics.hostProc.enabled=false` skips all five mounts; documented in `values.yaml` and NOTES.txt.
- **Existing peer cache deployments without a token break on upgrade.** Mitigation: chart auto-generates a Secret on `helm upgrade` if one doesn't exist; binary users get a clear error pointing to the two opt-outs. Release notes will be flagged in the BREAKING block.
- **Adding a new system-wide `/proc` read in `system.go` without updating `hostProcFiles` silently breaks the metric under the chart.** Mitigation: unit test in `internal/metrics/system_test.go` parses `system.go` for `procPath(...)` calls and diffs against `hostProcFiles`.

## Out of scope

- Refactor of `proxy.go` / `main.go` (#8) — existing plan owns this.
- AST-based debug-log redaction that preserves query structure but redacts literals — future PR after the typed LogQL parser stabilizes further.
- Tenant-aware lookback override — future PR.
- Replacing the `hostPath` mounts with a sidecar / node-exporter pull pattern — future PR.

## Migration commands

For operators upgrading:

```bash
# Binary: restore prior admin exposure on main listener
./loki-vl-proxy --server.admin-auth-token="$(openssl rand -hex 32)" ...

# Binary: restore prior peer-cache IP-only mode (NOT recommended)
./loki-vl-proxy --peer-insecure-ip-allowlist=true ...

# Helm: restore unlimited request limits (NOT recommended)
helm upgrade loki-vl-proxy . \
  --set proxy.maxConcurrentRequests=0 \
  --set proxy.rateLimit.perSecond=0 \
  --set proxy.rateLimit.burst=0

# Helm: turn off host metrics entirely on restricted PSA clusters
helm upgrade loki-vl-proxy . --set systemMetrics.hostProc.enabled=false
```
