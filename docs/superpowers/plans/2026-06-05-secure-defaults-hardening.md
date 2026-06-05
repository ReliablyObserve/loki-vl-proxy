# Secure-by-default startup + hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Loki-VL-proxy binary and Helm chart safe to run with default flags/values, without regressing existing operators who have already configured them.

**Architecture:** One PR, one branch `szibis/secure-defaults-hardening` cut from latest `origin/main`. Seven sequential commits, one per finding. Each finding ships code + tests + chart change (where applicable) + CHANGELOG entry. A final commit consolidates the CHANGELOG `[Unreleased]` section. All breaking changes get explicit `### BREAKING CHANGES` entries with old → new behavior and an opt-out command.

**Tech Stack:** Go 1.22+, slog, Helm v3, Ginkgo for integration tests, the existing `test/integration/` harness, `go test ./...` for unit, `helm template`/`helm unittest` for chart snapshots.

**Spec:** `docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md` (always re-read before each task — single source of truth).

---

## File structure

**New files:**
- `internal/proxy/debuglog.go` — `redactQuery` helper for finding #4.
- `internal/proxy/debuglog_test.go` — unit tests for the redactor.
- `charts/loki-vl-proxy/templates/peer-secret.yaml` — chart-managed Secret for the peer auth token (finding #2).
- `charts/loki-vl-proxy/tests/peer_secret_test.yaml` (or equivalent `helm unittest` file) — chart snapshot tests.

**Modified files:**
- `cmd/proxy/main.go` — new flags (`--admin-listen`, `--metrics-listen`, `--peer-insecure-ip-allowlist`, `--metadata-default-lookback`, `--backend-version-strict`, `--host-proc-root`, `--debug-log-raw-queries`); modified `validateAdminExposure`; new peer-token validator; new version-strict validator; dedicated admin/metrics listener wiring.
- `internal/proxy/proxy.go` — new `Config` fields, routing of debug logs through `redactQuery`, propagation of `MetadataDefaultLookback`, `DebugLogRawQueries`, `BackendVersionStrict`.
- `internal/proxy/backend.go` — debug log site redaction; promotion of version-check failure to error when strict.
- `internal/proxy/label_metadata.go` — default-lookback injection in `metadataQueryParams`.
- `internal/proxy/label_handlers.go` — verify `/series` shares the same helper, else add the same guard.
- `internal/proxy/middleware.go:255` — gate IP-allowlist fallback behind `PeerInsecureIPAllowlist`.
- `internal/metrics/system.go` — split `--proc-root` into `hostProcRoot` for host-scope reads; new package-level `hostProcFiles` slice.
- `charts/loki-vl-proxy/values.yaml` — defaults: `hostProc` mounts trimmed to 5 files; `maxConcurrentRequests=64`; `rateLimit.perSecond=50`, `burst=100`; chart sets `register-instrumentation=true`, `metrics-listen=:9091`, `admin-listen=127.0.0.1:3101`.
- `charts/loki-vl-proxy/templates/deployment.yaml` — replace broad `hostPath: /proc` directory mount with five `type: File` mounts; wire env from peer Secret; add admin Service if needed.
- `charts/loki-vl-proxy/templates/NOTES.txt` — call out new defaults and breaking changes.
- `CHANGELOG.md` — `[Unreleased]` section with the three blocks defined in the spec.
- `docs/tuning.md` — new section on rate-limit defaults (or append to existing tuning notes).

---

## Conventions (apply to every task)

- **Always work on branch `szibis/secure-defaults-hardening`.** Verify with `git branch --show-current` before any commit.
- **Re-read the spec** (`docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md`) at the start of every task. Single source of truth.
- **TDD:** write the failing test first, run it to confirm it fails, then implement, then confirm it passes. Per [[feedback_tdd_coverage]].
- **Verification before completion:** never mark a task complete until tests pass AND a manual smoke (where the task touches startup, listeners, or chart rendering) confirms behavior. Per [[feedback_close_feedback_loops]].
- **Commit message style:** match existing repo style — `feat(scope):`, `fix(scope):`, `chore(scope):` lowercase, present tense, no Claude attribution per [[feedback_no_claude_attribution]]. Sign with `szibis` identity per [[feedback_git_identity]]. Push over SSH.
- **No upstream VL/VT modifications** per [[feedback_vl_vt_upstream]] — all changes are in this repo.

---

## Task 1 — Branch + spec checkpoint

**Files:**
- Verify: `docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md` exists.
- Verify: branch `szibis/secure-defaults-hardening` is current.

- [ ] **Step 1: Confirm branch state**

```bash
cd ~/github/Loki-VL-proxy
git fetch origin main --quiet
git status -sb
git branch --show-current
```

Expected: branch is `szibis/secure-defaults-hardening`, working tree clean, branch tracks (or will track) latest main.

- [ ] **Step 2: Confirm spec exists and is committed**

```bash
git log --oneline -- docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md | head -3
```

If empty: `git add docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md docs/superpowers/plans/2026-06-05-secure-defaults-hardening.md && git commit -m "docs: add secure-defaults-hardening spec and plan"`

- [ ] **Step 3: Push the branch upstream**

```bash
git push -u origin szibis/secure-defaults-hardening
```

Expected: branch created on remote, no PR yet.

---

## Task 2 — Finding #4: Debug log redaction (lowest risk; warm up first)

**Why first:** No flag interactions, no chart changes, contained to two source files and one new helper. Establishes the test/commit/CI cadence before touching startup paths.

**Files:**
- Create: `internal/proxy/debuglog.go`
- Create: `internal/proxy/debuglog_test.go`
- Modify: `internal/proxy/proxy.go:1774, 1884, 2049`
- Modify: `internal/proxy/backend.go:620` (and any other `level=debug` query/param sites; grep first)
- Modify: `cmd/proxy/main.go` — add `--debug-log-raw-queries=false` flag, propagate via `proxyRuntimeConfig` → `proxy.Config`.
- Modify: `internal/proxy/proxy.go` — add `DebugLogRawQueries bool` to `Config`, store on `Proxy`.

- [ ] **Step 1: Locate every debug-log site that takes a query or backend params**

```bash
cd ~/github/Loki-VL-proxy
grep -rn 'log\.Debug\|\.Debug(' internal/proxy/ | grep -iE 'logql|logsql|query|params=' | sort -u
```

Expected: at least the four sites named in the spec; record any extras.

- [ ] **Step 2: Write the failing test**

Create `internal/proxy/debuglog_test.go`:

```go
package proxy

import (
	"strings"
	"testing"
)

func TestRedactQuery_DefaultRedacts(t *testing.T) {
	got := redactQuery("{job=\"nginx\"} |= \"customer-1234\"", false)
	if !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected sha256 prefix, got %q", got)
	}
	if !strings.Contains(got, "len=") {
		t.Fatalf("expected len= field, got %q", got)
	}
	if strings.Contains(got, "customer-1234") {
		t.Fatalf("literal leaked into redacted output: %q", got)
	}
}

func TestRedactQuery_RawPassthrough(t *testing.T) {
	in := "{job=\"nginx\"} |= \"customer-1234\""
	if got := redactQuery(in, true); got != in {
		t.Fatalf("raw flag should return input verbatim, got %q", got)
	}
}

func TestRedactQuery_StableForSameInput(t *testing.T) {
	a := redactQuery("foo", false)
	b := redactQuery("foo", false)
	if a != b {
		t.Fatalf("expected stable hash, got %q vs %q", a, b)
	}
}

func TestRedactQuery_DifferentInputsDifferentHash(t *testing.T) {
	a := redactQuery("foo", false)
	b := redactQuery("foo ", false)
	if a == b {
		t.Fatalf("expected different hashes for different inputs, both %q", a)
	}
}

func TestRedactQuery_EmptyInput(t *testing.T) {
	got := redactQuery("", false)
	if !strings.Contains(got, "len=0") {
		t.Fatalf("expected len=0 for empty input, got %q", got)
	}
}
```

- [ ] **Step 3: Run the test, confirm it fails to compile (no `redactQuery` yet)**

```bash
go test ./internal/proxy/ -run TestRedactQuery -count=1
```

Expected: build fails with `undefined: redactQuery`.

- [ ] **Step 4: Implement `redactQuery`**

Create `internal/proxy/debuglog.go`:

```go
package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// redactQuery returns a deterministic fingerprint of s suitable for debug
// logging without leaking literal contents.
//
// Format: "sha256:<8 hex chars> len=<n>"
//
// When raw is true, s is returned verbatim. Intended for local dev only via
// the --debug-log-raw-queries flag.
//
// Tenant IDs / orgIDs are NOT routed through this helper; they remain in plain
// text in debug logs because they are operationally critical and not sensitive
// in this proxy's threat model.
func redactQuery(s string, raw bool) string {
	if raw {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])[:8] + " len=" + strconv.Itoa(len(s))
}
```

- [ ] **Step 5: Run the test, confirm it passes**

```bash
go test ./internal/proxy/ -run TestRedactQuery -count=1
```

Expected: PASS for all five subtests.

- [ ] **Step 6: Add `DebugLogRawQueries` to `Config` and `Proxy`**

In `internal/proxy/proxy.go`, in the `Config` struct (next to existing string flags), add:

```go
// DebugLogRawQueries, when true, disables redaction of LogQL/LogsQL and
// backend query params in debug-level logs. Intended for local development
// only. Default is false (redacted).
DebugLogRawQueries bool
```

In the `Proxy` struct, add `debugLogRawQueries bool` next to other simple toggles. In `New(cfg Config) (*Proxy, error)`, assign `debugLogRawQueries: cfg.DebugLogRawQueries`.

- [ ] **Step 7: Route every debug-log query/param site through `redactQuery`**

At `internal/proxy/proxy.go:1774`:

```go
p.log.Debug("query_range request", "logql", redactQuery(logqlQuery, p.debugLogRawQueries))
```

At `internal/proxy/proxy.go:1884`:

```go
p.log.Debug("translated query", "logsql", redactQuery(logsqlQuery, p.debugLogRawQueries), "without", withoutLabels)
```

At `internal/proxy/proxy.go:2049`:

```go
p.log.Debug("query request", "logql", redactQuery(logqlQuery, p.debugLogRawQueries))
```

At `internal/proxy/backend.go:620`:

```go
p.log.Debug("VL request", "method", "POST", "url", u.String(), "params", redactQuery(params.Encode(), p.debugLogRawQueries))
```

Plus any extras discovered in Step 1 — apply the same wrapping. The `url` value at backend.go:620 also typically contains query params; if the helper test above showed it does, wrap `u.String()` too with `redactURL` (a sibling helper that strips query string and applies the same `sha256+len` on the raw query part). If `url` is path-only, leave as-is — verify by reading the call site.

- [ ] **Step 8: Wire the flag through `cmd/proxy/main.go`**

In `cmd/proxy/main.go` near other `fs.Bool` calls in `run(...)`:

```go
debugLogRawQueries := fs.Bool("debug-log-raw-queries", false, "When true, debug logs include raw LogQL/LogsQL and backend params verbatim. Default false (redacted to sha256+len).")
```

Add `debugLogRawQueries bool` to `proxyRuntimeConfig`. Assign in the runtime-config builder: `debugLogRawQueries: *debugLogRawQueries`. In `buildProxyConfig`: `DebugLogRawQueries: cfg.debugLogRawQueries`.

- [ ] **Step 9: Build and run full unit tests**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/proxy/ -count=1
```

Per [[project_gowork_off]] — GOWORK=off is mandatory.

Expected: green build, all tests pass.

- [ ] **Step 10: Manual smoke (with `--debug-log-raw-queries=true` and `false`)**

```bash
GOWORK=off go build -o /tmp/lvlp ./cmd/proxy
# Boot fails fast because admin-bind isn't done yet; just verify the flag is parsed:
/tmp/lvlp --help 2>&1 | grep debug-log-raw-queries
```

Expected: help line for the new flag.

- [ ] **Step 11: Commit**

```bash
git add internal/proxy/debuglog.go internal/proxy/debuglog_test.go \
        internal/proxy/proxy.go internal/proxy/backend.go cmd/proxy/main.go
git commit -m "feat(proxy): redact LogQL/LogsQL and backend params in debug logs

Default debug-level logs now emit sha256:<8hex>+len=<n> for query strings
and backend params. Raw output gated behind --debug-log-raw-queries=false."
```

Push at the end of every commit:

```bash
git push
```

---

## Task 3 — Finding #3: Metadata default lookback (port stashed work + extend to /series)

**Files:**
- Modify: `internal/proxy/label_metadata.go` (the `metadataQueryParams` helper)
- Modify: `internal/proxy/label_handlers.go` (`/labels`, `/label/{name}/values` already share the helper; verify `/series` does too — if not, add the same default-lookback injection)
- Modify: `internal/proxy/proxy.go` — add `MetadataDefaultLookback time.Duration` to `Config`, store on `Proxy`.
- Modify: `cmd/proxy/main.go` — add `--metadata-default-lookback=12h` flag.
- Create or modify: `internal/proxy/label_metadata_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/proxy/label_metadata_test.go`:

```go
package proxy

import (
	"net/url"
	"testing"
	"time"
)

func TestMetadataDefaultLookback_InjectsWhenBlank(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: time.Hour}
	params := url.Values{}
	p.metadataQueryParams(nil, "candidate", "", "", "", params)
	if params.Get("start") == "" {
		t.Fatalf("expected start to be injected, got empty")
	}
	if params.Get("end") == "" {
		t.Fatalf("expected end to be injected, got empty")
	}
}

func TestMetadataDefaultLookback_DoesNotOverrideClient(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: time.Hour}
	params := url.Values{}
	p.metadataQueryParams(nil, "candidate", "12345", "", "", params)
	if got := params.Get("start"); got != "12345" {
		t.Fatalf("client start should win, got %q", got)
	}
}

func TestMetadataDefaultLookback_ZeroDisables(t *testing.T) {
	p := &Proxy{metadataDefaultLookback: 0}
	params := url.Values{}
	p.metadataQueryParams(nil, "candidate", "", "", "", params)
	if params.Get("start") != "" || params.Get("end") != "" {
		t.Fatalf("expected no injection when lookback=0, got start=%q end=%q",
			params.Get("start"), params.Get("end"))
	}
}
```

Note: the actual `metadataQueryParams` signature in the repo may differ — adjust the test signature to match the real one before running (read `label_metadata.go:80` first). The behavioral contract above stays the same.

- [ ] **Step 2: Run the test, confirm it fails**

```bash
GOWORK=off go test ./internal/proxy/ -run TestMetadataDefaultLookback -count=1
```

Expected: FAIL — either compile error (no `metadataDefaultLookback` field) or assertion failure.

- [ ] **Step 3: Add `MetadataDefaultLookback` to `Config` and `Proxy`**

In `internal/proxy/proxy.go` `Config`:

```go
// MetadataDefaultLookback is the default time window applied to /labels,
// /label/{name}/values, and /series when the client omits both start and
// end. 0 disables (unbounded scan, prior behavior).
MetadataDefaultLookback time.Duration
```

In `Proxy` struct: `metadataDefaultLookback time.Duration`. In `New`: `metadataDefaultLookback: cfg.MetadataDefaultLookback`.

- [ ] **Step 4: Inject default lookback in `metadataQueryParams`**

In `internal/proxy/label_metadata.go`, just before the existing `if strings.TrimSpace(start) != ""` block (around line 80):

```go
if strings.TrimSpace(start) == "" && strings.TrimSpace(end) == "" && p.metadataDefaultLookback > 0 {
    now := time.Now()
    start = fmt.Sprintf("%d", now.Add(-p.metadataDefaultLookback).UnixNano())
    end = fmt.Sprintf("%d", now.UnixNano())
}
```

- [ ] **Step 5: Verify `/series` shares the helper**

```bash
grep -n 'metadataQueryParams\|/series' internal/proxy/label_handlers.go
```

If `/series` does NOT route through `metadataQueryParams`, add the same default-lookback guard at its call site. If it does, no change needed.

- [ ] **Step 6: Run unit test, confirm it passes**

```bash
GOWORK=off go test ./internal/proxy/ -run TestMetadataDefaultLookback -count=1
```

Expected: all three subtests PASS.

- [ ] **Step 7: Wire the flag through `cmd/proxy/main.go`**

Add to `proxyRuntimeConfig`:

```go
metadataDefaultLookback time.Duration
```

Near other duration flags in `run(...)`:

```go
metadataDefaultLookback := fs.Duration("metadata-default-lookback", 12*time.Hour, "Default time window for /labels, /label/{name}/values, and /series when the client omits start/end. 0 disables (unbounded scan).")
```

Assign and propagate through `buildProxyConfig`:

```go
MetadataDefaultLookback: cfg.metadataDefaultLookback,
```

- [ ] **Step 8: Full build + test**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/proxy/ -count=1
```

Expected: green.

- [ ] **Step 9: Commit**

```bash
git add internal/proxy/proxy.go internal/proxy/label_metadata.go \
        internal/proxy/label_handlers.go internal/proxy/label_metadata_test.go \
        cmd/proxy/main.go
git commit -m "feat(proxy): bound metadata queries with --metadata-default-lookback (default 12h)

/labels, /label/{name}/values, and /series now inject start/end of
now-12h..now when the client omits both. 0 disables (prior behavior)."
git push
```

---

## Task 4 — Finding #7: Backend version strict mode

**Files:**
- Modify: `internal/proxy/backend.go:455` (soft version-check site)
- Modify: `internal/proxy/proxy.go` — add `BackendVersionStrict bool` to `Config`.
- Modify: `cmd/proxy/main.go` — `--backend-version-strict=false` flag.
- Create or extend: `internal/proxy/backend_test.go`

- [ ] **Step 1: Read the current version-check code**

```bash
sed -n '430,490p' internal/proxy/backend.go
```

Identify the function name, the three failure modes (health failure, non-2xx, missing/sub-min semver), and the slog call that currently warns.

- [ ] **Step 2: Write the failing test**

In `internal/proxy/backend_test.go`, add (or create) tests that exercise each failure mode under strict=true and strict=false. Pseudocode (adapt to actual constructor signature):

```go
func TestBackendVersionStrict_HardFailsOnSubMin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "VictoriaLogs-v0.1.0")
	}))
	defer srv.Close()
	_, err := newBackend(backendOptions{
		URL: srv.URL, MinVersion: "v1.50.0", Strict: true,
	})
	if err == nil {
		t.Fatalf("expected error in strict mode for sub-min backend version, got nil")
	}
}

func TestBackendVersionStrict_SoftLogsOnSubMin(t *testing.T) {
	// Same setup, Strict: false → no error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "VictoriaLogs-v0.1.0")
	}))
	defer srv.Close()
	if _, err := newBackend(backendOptions{
		URL: srv.URL, MinVersion: "v1.50.0", Strict: false,
	}); err != nil {
		t.Fatalf("expected nil error in soft mode, got %v", err)
	}
}

func TestBackendVersionStrict_HealthFailureHardFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", 500)
	}))
	defer srv.Close()
	if _, err := newBackend(backendOptions{URL: srv.URL, Strict: true}); err == nil {
		t.Fatalf("expected error in strict mode for health 500, got nil")
	}
}
```

The exact function/struct names will be different — adapt to the real types in `backend.go`.

- [ ] **Step 3: Run test, confirm failure**

```bash
GOWORK=off go test ./internal/proxy/ -run TestBackendVersionStrict -count=1
```

Expected: FAIL.

- [ ] **Step 4: Add the strict-mode branch in `backend.go:455`**

Replace the existing warn-only block:

```go
if err != nil {
    p.log.Warn("backend version check failed", "err", err)
    if p.backendVersionStrict {
        return fmt.Errorf("backend version check failed (strict mode): %w", err)
    }
    return nil
}
// existing non-2xx / sub-min branches similarly promoted under strict.
```

Add `backendVersionStrict bool` to the relevant struct, wire from `Config.BackendVersionStrict`.

- [ ] **Step 5: Wire flag through `main.go`**

```go
backendVersionStrict := fs.Bool("backend-version-strict", false, "When true, /health failure, non-2xx response, or missing/sub-min backend semver causes startup to fail. Default false (warn only).")
```

Add field to `proxyRuntimeConfig`, propagate through `buildProxyConfig`.

- [ ] **Step 6: Test passes**

```bash
GOWORK=off go test ./internal/proxy/ -run TestBackendVersionStrict -count=1
```

Expected: PASS.

- [ ] **Step 7: Full build + test suite**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/proxy/ -count=1
```

- [ ] **Step 8: Commit**

```bash
git add internal/proxy/backend.go internal/proxy/proxy.go \
        internal/proxy/backend_test.go cmd/proxy/main.go
git commit -m "feat(proxy): --backend-version-strict promotes version-check failures to startup error

Default soft behavior unchanged. When --backend-version-strict=true, the
proxy refuses to start if /health is unreachable, returns non-2xx, or the
reported semver is below --backend-min-version."
git push
```

---

## Task 5 — Finding #5: Split host/proc from self/proc + surgical chart mounts

**Files:**
- Modify: `internal/metrics/system.go` — declare package-level `hostProcFiles` slice; introduce `hostProcRoot`; route only host-wide reads through it.
- Create or extend: `internal/metrics/system_test.go`
- Modify: `cmd/proxy/main.go` — new `--host-proc-root` flag (default `/proc`).
- Modify: `charts/loki-vl-proxy/templates/deployment.yaml` — replace broad `hostPath: /proc` mount with 5 individual `type: File` mounts.
- Modify: `charts/loki-vl-proxy/values.yaml` — update `systemMetrics.hostProc` comment block; default `enabled: true` stays.

- [ ] **Step 1: Re-confirm the host-scope file set**

```bash
grep -n 'procReadFile\|procPath\|selfProcPath' internal/metrics/system.go
```

Expected file inventory matching the spec:
- Host: `stat`, `meminfo`, `pressure/cpu`, `pressure/memory`, `pressure/io`
- Self: `self/{status,io,stat,fd}`, `net/dev`

If any new reads have appeared since the spec was written, update both the slice and the spec.

- [ ] **Step 2: Write the failing test for `hostProcFiles` exhaustiveness**

In `internal/metrics/system_test.go`:

```go
package metrics

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestHostProcFiles_ExhaustiveVsSource asserts that every procPath(...) call
// site in system.go is covered by the hostProcFiles slice. Prevents future
// metric additions from silently breaking under the surgical chart mount.
func TestHostProcFiles_ExhaustiveVsSource(t *testing.T) {
	src, err := os.ReadFile("system.go")
	if err != nil {
		t.Fatalf("read system.go: %v", err)
	}
	re := regexp.MustCompile(`procPath\(([^)]+)\)`)
	matches := re.FindAllStringSubmatch(string(src), -1)
	seen := map[string]bool{}
	for _, m := range matches {
		args := strings.ReplaceAll(m[1], `"`, "")
		args = strings.ReplaceAll(args, " ", "")
		seen[args] = true
	}
	declared := map[string]bool{}
	for _, f := range hostProcFiles {
		declared[strings.ReplaceAll(f, "/", ",")] = true
	}
	var missing []string
	for k := range seen {
		if !declared[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("procPath() call sites not covered by hostProcFiles: %v\n"+
			"Add them to hostProcFiles in system.go AND to the chart's hostProc volumeMounts.",
			missing)
	}
}
```

- [ ] **Step 3: Run the test, confirm failure**

```bash
GOWORK=off go test ./internal/metrics/ -run TestHostProcFiles -count=1
```

Expected: FAIL — `hostProcFiles` undefined OR slice content drifts from `procPath` calls.

- [ ] **Step 4: Introduce `hostProcRoot` and `hostProcFiles` in `system.go`**

Near the top of `internal/metrics/system.go`:

```go
// hostProcFiles lists every host-scope /proc path the system metrics collector
// reads. Two roles:
//   1. The Helm chart mounts exactly these files (and only these) when
//      systemMetrics.hostProc.enabled is true, shrinking the hostPath surface.
//   2. system_test.go scans this file for procPath(...) calls and fails if
//      any new host-scope path is added without updating this list.
//
// Self/container-scope reads use selfProcPath(...) and do NOT need entries
// here — they read from the calling process's /proc (kernel resolves /self
// per-caller and the container has its own netns for /net).
var hostProcFiles = []string{
	"stat",
	"meminfo",
	"pressure/cpu",
	"pressure/memory",
	"pressure/io",
}
```

Refactor the existing single-`procRoot` model so that:
- `procPath(parts ...string)` uses `hostProcRoot` (new package var, set from CLI).
- `selfProcPath(parts ...string)` continues using the local proc root (`/proc`), unchanged from current behavior.
- The `case "/host/proc"` branch at `system.go:98` becomes `hostProcRoot = "/host/proc"`.

- [ ] **Step 5: Add `--host-proc-root` to `main.go`**

In `cmd/proxy/main.go`, replace or augment the existing `--proc-root` flag (which today switches both roots) with two flags:

```go
hostProcRoot := fs.String("host-proc-root", "/proc", "Filesystem root for host-scope /proc reads (CPU, memory, PSI). Set to /host/proc when running with hostPath:/proc mounts.")
```

Keep `--proc-root` for the self/container root (back-compat). Wire `hostProcRoot` into the metrics collector.

- [ ] **Step 6: Test passes**

```bash
GOWORK=off go test ./internal/metrics/ -count=1
```

Expected: PASS.

- [ ] **Step 7: Update the chart deployment to mount the 5 host files individually**

In `charts/loki-vl-proxy/templates/deployment.yaml`, replace the existing host-proc volume + mount block:

```yaml
{{- if .Values.systemMetrics.hostProc.enabled }}
{{- range $i, $path := list "stat" "meminfo" "pressure/cpu" "pressure/memory" "pressure/io" }}
        - name: host-proc-{{ $i }}
          mountPath: /host/proc/{{ $path }}
          readOnly: true
{{- end }}
{{- end }}
```

And volumes:

```yaml
{{- if .Values.systemMetrics.hostProc.enabled }}
{{- range $i, $path := list "stat" "meminfo" "pressure/cpu" "pressure/memory" "pressure/io" }}
    - name: host-proc-{{ $i }}
      hostPath:
        path: /proc/{{ $path }}
        type: File
{{- end }}
{{- end }}
```

The pod template args must include `--host-proc-root=/host/proc` whenever `systemMetrics.hostProc.enabled=true`.

- [ ] **Step 8: Update `values.yaml` comments**

Replace the existing `systemMetrics.hostProc` comment block with one that explains the 5-file mount, lists the metrics that drop if `enabled=false`, and notes restricted-PSA implications.

- [ ] **Step 9: Chart snapshot smoke**

```bash
helm template loki-vl-proxy charts/loki-vl-proxy \
  --set systemMetrics.hostProc.enabled=true | grep -A 1 host-proc-
```

Expected: 5 mounts and 5 volumes rendered, each `type: File`. No broad `/proc` directory mount.

```bash
helm template loki-vl-proxy charts/loki-vl-proxy \
  --set systemMetrics.hostProc.enabled=false | grep host-proc- || echo OK
```

Expected: `OK` (no mounts when disabled).

- [ ] **Step 10: Full build + test**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/metrics/ ./internal/proxy/ ./cmd/... -count=1
```

- [ ] **Step 11: Commit**

```bash
git add internal/metrics/system.go internal/metrics/system_test.go \
        cmd/proxy/main.go charts/loki-vl-proxy/templates/deployment.yaml \
        charts/loki-vl-proxy/values.yaml
git commit -m "feat(metrics,chart): split host/proc from self/proc; mount only 5 host-scope files

internal/metrics/system.go now uses hostProcRoot for stat/meminfo/PSI and
the unchanged local /proc for self/* and net/dev. The chart mounts exactly
those 5 files individually (hostPath type: File) instead of the full /proc
directory. Per-process info for other workloads is no longer reachable from
the proxy container."
git push
```

---

## Task 6 — Finding #2: Peer cache token required

**Files:**
- Modify: `internal/proxy/middleware.go:255` — gate IP-allowlist fallback.
- Modify: `cmd/proxy/main.go` — new flag `--peer-insecure-ip-allowlist=false`; new startup validator.
- Modify: `internal/proxy/proxy.go` — add `PeerInsecureIPAllowlist bool` to `Config`.
- Modify: `charts/loki-vl-proxy/values.yaml` — `peerCache.authToken`, `peerCache.existingSecret` keys; documented resolution order.
- Create: `charts/loki-vl-proxy/templates/peer-secret.yaml` — chart-managed Secret with `lookup`-based idempotency.
- Modify: `charts/loki-vl-proxy/templates/deployment.yaml` — wire env from the Secret.
- Create or extend: `cmd/proxy/main_test.go` for validator test.

- [ ] **Step 1: Write the failing validator test**

In `cmd/proxy/main_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func TestValidatePeerAuth_DiscoveryWithoutTokenFails(t *testing.T) {
	err := validatePeerAuth("static", "10.0.0.1:3100", "", false)
	if err == nil {
		t.Fatalf("expected error when discovery set and token empty without insecure flag")
	}
	if !strings.Contains(err.Error(), "peer-auth-token") {
		t.Fatalf("error should name the missing flag, got %v", err)
	}
}

func TestValidatePeerAuth_DiscoveryWithoutTokenInsecureFlagPasses(t *testing.T) {
	if err := validatePeerAuth("static", "10.0.0.1:3100", "", true); err != nil {
		t.Fatalf("unexpected error with insecure flag set: %v", err)
	}
}

func TestValidatePeerAuth_TokenPresentPasses(t *testing.T) {
	if err := validatePeerAuth("static", "10.0.0.1:3100", "secret", false); err != nil {
		t.Fatalf("unexpected error with token set: %v", err)
	}
}

func TestValidatePeerAuth_DiscoveryDisabledPasses(t *testing.T) {
	if err := validatePeerAuth("", "", "", false); err != nil {
		t.Fatalf("unexpected error with discovery disabled: %v", err)
	}
}
```

- [ ] **Step 2: Run test, confirm failure**

```bash
GOWORK=off go test ./cmd/proxy/ -run TestValidatePeerAuth -count=1
```

Expected: FAIL (`validatePeerAuth` undefined).

- [ ] **Step 3: Implement `validatePeerAuth` in `cmd/proxy/main.go`**

Add near `validateAdminExposure`:

```go
func validatePeerAuth(discovery, peers, token string, insecureAllowlist bool) error {
	enabled := strings.TrimSpace(discovery) != "" || strings.TrimSpace(peers) != ""
	if !enabled {
		return nil
	}
	if strings.TrimSpace(token) != "" {
		return nil
	}
	if insecureAllowlist {
		return nil
	}
	return fmt.Errorf("peer cache is configured (--peer-discovery=%q) but --peer-auth-token is empty. " +
		"Set --peer-auth-token to a shared secret, or pass --peer-insecure-ip-allowlist=true to keep the legacy IP-only behavior.",
		discovery)
}
```

Add the flag:

```go
peerInsecureIPAllowlist := fs.Bool("peer-insecure-ip-allowlist", false, "When true, allow peer cache requests based on source IP membership alone (legacy behavior). Default false: a shared --peer-auth-token is required when peer discovery is configured.")
```

Add to `proxyRuntimeConfig`, propagate through `buildProxyConfig`.

Call the validator before `proxy.New`:

```go
if err := validatePeerAuth(*peerDiscovery, *peerStatic, *peerAuthToken, *peerInsecureIPAllowlist); err != nil {
    return err
}
```

- [ ] **Step 4: Test passes**

```bash
GOWORK=off go test ./cmd/proxy/ -run TestValidatePeerAuth -count=1
```

Expected: PASS.

- [ ] **Step 5: Gate the IP-allowlist fallback in middleware**

In `internal/proxy/middleware.go` around line 255:

```go
if p.peerInsecureIPAllowlist {
    // existing IP-membership fallback
    if !p.peerCache.IsPeer(remoteIP) {
        http.Error(w, "forbidden", http.StatusForbidden)
        return
    }
} else {
    http.Error(w, "unauthorized: peer-auth-token required", http.StatusUnauthorized)
    return
}
```

The token-match branch above remains primary. Add `peerInsecureIPAllowlist` to the `Proxy` struct, wire from `Config.PeerInsecureIPAllowlist`.

- [ ] **Step 6: Add the chart Secret template**

Create `charts/loki-vl-proxy/templates/peer-secret.yaml`:

```yaml
{{- if .Values.peerCache.enabled }}
{{- if .Values.peerCache.authToken }}
# Operator-provided token — no chart-managed Secret.
{{- else if .Values.peerCache.existingSecret }}
# Operator points to an existing Secret — chart does not manage one.
{{- else }}
{{- $existing := (lookup "v1" "Secret" .Release.Namespace (printf "%s-peer-auth" .Release.Name)) }}
{{- $token := "" }}
{{- if $existing }}
{{-   $token = index $existing.data "token" | b64dec }}
{{- else }}
{{-   $token = randAlphaNum 32 }}
{{- end }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ .Release.Name }}-peer-auth
  labels:
    app.kubernetes.io/name: {{ .Chart.Name }}
    app.kubernetes.io/instance: {{ .Release.Name }}
type: Opaque
data:
  token: {{ $token | b64enc }}
{{- end }}
{{- end }}
```

Add a values-validation block at the top of the template (or in a `_helpers.tpl`) that fails if `peerCache.authToken` is explicitly the empty string:

```yaml
{{- if and .Values.peerCache.enabled (hasKey .Values.peerCache "authToken") (eq (toString .Values.peerCache.authToken) "") }}
{{- fail "peerCache.authToken is set to empty string. Remove the key to let the chart generate a Secret, or set it to a non-empty value." }}
{{- end }}
```

- [ ] **Step 7: Wire env into the deployment**

In `charts/loki-vl-proxy/templates/deployment.yaml`, add to the proxy container env (only when `peerCache.enabled`):

```yaml
{{- if .Values.peerCache.enabled }}
        - name: PEER_AUTH_TOKEN
          valueFrom:
            secretKeyRef:
              {{- if .Values.peerCache.existingSecret }}
              name: {{ .Values.peerCache.existingSecret }}
              key: {{ .Values.peerCache.existingSecretKey | default "token" }}
              {{- else if .Values.peerCache.authToken }}
              name: {{ .Release.Name }}-peer-auth-literal
              key: token
              {{- else }}
              name: {{ .Release.Name }}-peer-auth
              key: token
              {{- end }}
{{- end }}
```

And add `--peer-auth-token=$(PEER_AUTH_TOKEN)` to the args list.

If the user sets `peerCache.authToken` to a literal, also render a Secret for it (cleaner than passing on the command line). Add a second small template `peer-secret-literal.yaml` for that case.

- [ ] **Step 8: Snapshot the rendered chart for the three cases**

```bash
helm template loki-vl-proxy charts/loki-vl-proxy \
  --set peerCache.enabled=true | grep -A 5 peer-auth | head -30
# Expect: Secret with random 32-byte token + env wired.

helm template loki-vl-proxy charts/loki-vl-proxy \
  --set peerCache.enabled=true --set peerCache.authToken="my-token" | grep -A 5 peer-auth | head -30
# Expect: Secret with my-token + env wired.

helm template loki-vl-proxy charts/loki-vl-proxy \
  --set peerCache.enabled=true --set peerCache.existingSecret="my-secret" | grep -A 5 peer-auth | head -30
# Expect: env wired to my-secret, no chart-managed Secret.

helm template loki-vl-proxy charts/loki-vl-proxy \
  --set peerCache.enabled=true --set peerCache.authToken="" 2>&1 | grep -i "empty string" || echo MISSING
# Expect: error message about empty string, NOT "MISSING".
```

- [ ] **Step 9: Full build + test**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/proxy/ ./cmd/proxy/ -count=1
```

- [ ] **Step 10: Commit**

```bash
git add internal/proxy/middleware.go internal/proxy/proxy.go \
        cmd/proxy/main.go cmd/proxy/main_test.go \
        charts/loki-vl-proxy/templates/peer-secret.yaml \
        charts/loki-vl-proxy/templates/deployment.yaml \
        charts/loki-vl-proxy/values.yaml
git commit -m "feat(peer-cache): require --peer-auth-token; IP-allowlist now opt-in via --peer-insecure-ip-allowlist

BREAKING: the proxy refuses to start when --peer-discovery is set and
--peer-auth-token is empty. Pass --peer-insecure-ip-allowlist=true to
restore the previous IP-only behavior.

Chart auto-generates a 32-byte token Secret on first install if
peerCache.authToken and peerCache.existingSecret are both unset. helm
lookup keeps the value stable across upgrades. Explicit empty string for
peerCache.authToken fails templating with a clear error."
git push
```

---

## Task 7 — Finding #6: Conservative chart rate limits

**Files:**
- Modify: `charts/loki-vl-proxy/values.yaml:61` — defaults for `maxConcurrentRequests`, `rateLimit.perSecond`, `rateLimit.burst`.
- Modify: `charts/loki-vl-proxy/templates/NOTES.txt` — call out new defaults.
- Modify: `docs/tuning.md` (create if absent) — short section on tuning rate limits.

- [ ] **Step 1: Update values defaults**

In `charts/loki-vl-proxy/values.yaml`:

```yaml
proxy:
  # Per-replica ceiling on concurrent in-flight backend requests.
  # 0 = unlimited (NOT recommended — a single noisy client can drive
  # expensive fanout). See docs/tuning.md.
  maxConcurrentRequests: 64

  rateLimit:
    # Per-client token bucket (refill rate, tokens/sec).
    # 0 = disabled. See docs/tuning.md.
    perSecond: 50
    # Per-client token bucket burst size.
    # 0 = disabled.
    burst: 100
```

- [ ] **Step 2: Snapshot rendered values**

```bash
helm template loki-vl-proxy charts/loki-vl-proxy | grep -E 'max-concurrent|rate-limit' | head -10
```

Expected: rendered args include `--max-concurrent-requests=64`, `--rate-limit-per-second=50`, `--rate-limit-burst=100`.

- [ ] **Step 3: Update NOTES.txt**

Add a section at the top of `charts/loki-vl-proxy/templates/NOTES.txt`:

```
This release enforces conservative request limits by default:
  - maxConcurrentRequests = 64 (per replica)
  - rateLimit.perSecond  = 50  (per client)
  - rateLimit.burst      = 100 (per client)

To restore unlimited behavior, set all three back to 0 in your values file.
See docs/tuning.md for guidance on appropriate values for your workload.
```

- [ ] **Step 4: Create or extend `docs/tuning.md`**

```markdown
## Rate limits and concurrency

Defaults changed in <version> from unlimited to conservative values to
protect against single-client request storms.

| Setting                          | Default | Disabled when | Notes                                  |
|----------------------------------|---------|---------------|----------------------------------------|
| proxy.maxConcurrentRequests      | 64      | 0             | Per-replica in-flight cap.             |
| proxy.rateLimit.perSecond        | 50      | 0             | Per-client token bucket refill (tps).  |
| proxy.rateLimit.burst            | 100     | 0             | Per-client burst size.                 |

Tune based on:
- Number of Grafana / dashboard replicas calling the proxy.
- Average and p99 backend query latency.
- Backend CPU/memory headroom for fanout.

To restore previous unlimited behavior:

    helm upgrade loki-vl-proxy charts/loki-vl-proxy \
      --set proxy.maxConcurrentRequests=0 \
      --set proxy.rateLimit.perSecond=0 \
      --set proxy.rateLimit.burst=0
```

- [ ] **Step 5: Commit**

```bash
git add charts/loki-vl-proxy/values.yaml \
        charts/loki-vl-proxy/templates/NOTES.txt \
        docs/tuning.md
git commit -m "feat(chart): conservative rate-limit defaults (maxConcurrent=64, rate=50/s burst=100)

BREAKING: chart-default request limits change from unlimited to bounded.
Set all three knobs back to 0 to restore unlimited behavior. See
docs/tuning.md for tuning guidance."
git push
```

---

## Task 8 — Finding #1: Loopback admin listener + dedicated metrics listener

**Why last among code findings:** Largest blast radius (startup wiring), wants the rest of the test suite already in place, and benefits from the integration harness pattern established in earlier tasks.

**Files:**
- Modify: `cmd/proxy/main.go` — new `--admin-listen` and `--metrics-listen` flags; flip `-server.register-instrumentation` default to `false`; revise `validateAdminExposure` semantics; wire dedicated listeners.
- Modify: `charts/loki-vl-proxy/values.yaml` — set `register-instrumentation: true`, `metricsListen: ":9091"`, `adminListen: "127.0.0.1:3101"`.
- Modify: `charts/loki-vl-proxy/templates/deployment.yaml` — pass new args; add ports for admin and metrics; optionally add Service for the metrics port (for ServiceMonitor).
- Modify or create: `cmd/proxy/main_test.go` — admin-bind selector tests.

- [ ] **Step 1: Write failing tests for the admin/metrics listener selector**

In `cmd/proxy/main_test.go`, add:

```go
func TestResolveAdminTarget_TokenSetUsesMainListener(t *testing.T) {
	got := resolveAdminTarget(":3100", "127.0.0.1:3101", "secret-token")
	if got != ":3100" {
		t.Fatalf("with token set, admin should ride main listener (:3100), got %q", got)
	}
}

func TestResolveAdminTarget_NoTokenUsesAdminListener(t *testing.T) {
	got := resolveAdminTarget(":3100", "127.0.0.1:3101", "")
	if got != "127.0.0.1:3101" {
		t.Fatalf("with no token, admin should ride loopback listener, got %q", got)
	}
}

func TestValidateAdminExposure_LoopbackAdminListenerWithoutTokenOK(t *testing.T) {
	err := validateAdminExposure("127.0.0.1:3101", true, false, false, "")
	if err != nil {
		t.Fatalf("loopback admin listener without token should be allowed, got %v", err)
	}
}

func TestValidateAdminExposure_NonLoopbackAdminListenerWithoutTokenFails(t *testing.T) {
	err := validateAdminExposure("0.0.0.0:3101", true, false, false, "")
	if err == nil {
		t.Fatalf("non-loopback admin listener without token must fail")
	}
}
```

- [ ] **Step 2: Run, confirm failure**

```bash
GOWORK=off go test ./cmd/proxy/ -run 'TestResolveAdminTarget|TestValidateAdminExposure_Loopback|TestValidateAdminExposure_NonLoopback' -count=1
```

Expected: FAIL — `resolveAdminTarget` undefined and `validateAdminExposure` semantics differ.

- [ ] **Step 3: Add `--admin-listen` and `--metrics-listen` flags**

In `cmd/proxy/main.go`:

```go
adminListen := fs.String("admin-listen", "127.0.0.1:3101", "Address for admin/debug endpoints when --server.admin-auth-token is empty. Loopback by default for safe local boot.")
metricsListen := fs.String("metrics-listen", "", "Optional dedicated address for the /metrics endpoint. Empty means /metrics is served on --listen when --server.register-instrumentation=true.")
```

Flip the `register-instrumentation` default:

```go
registerInstrumentation := fs.Bool("server.register-instrumentation", false, "Register Prometheus instrumentation handlers. Default false; set true and provide --metrics-listen to expose /metrics.")
```

Add fields to `proxyRuntimeConfig`. Propagate.

- [ ] **Step 4: Implement `resolveAdminTarget` and revise `validateAdminExposure`**

```go
// resolveAdminTarget picks the listener address for admin/debug endpoints.
// When a token is set, admin endpoints ride the main listener (back-compat).
// When no token is set, admin endpoints move to the dedicated adminListen,
// which defaults to a loopback address so the binary boots safely with no
// flags.
func resolveAdminTarget(mainListen, adminListen, token string) string {
	if strings.TrimSpace(token) != "" {
		return mainListen
	}
	return adminListen
}
```

Revise `validateAdminExposure`:

```go
func validateAdminExposure(adminTarget string, registerInstrumentation, enablePprof, enableQueryAnalytics bool, adminAuthToken string) error {
	if strings.TrimSpace(adminAuthToken) != "" {
		return nil
	}
	if !registerInstrumentation && !enablePprof && !enableQueryAnalytics {
		return nil
	}
	if isLoopbackListenAddr(adminTarget) {
		return nil
	}
	return fmt.Errorf("server.admin-auth-token is required when admin/debug endpoints are exposed on non-loopback address %q", adminTarget)
}
```

- [ ] **Step 5: Wire dedicated listeners in `run(...)`**

After flag parsing:

```go
adminTarget := resolveAdminTarget(envCfg.listenAddr, *adminListen, *serverAdminAuthToken)
if err := validateAdminExposure(adminTarget, *registerInstrumentation, *enablePprof, *enableQueryAnalytics, *serverAdminAuthToken); err != nil {
    return err
}
```

In server bootstrap:
- Main mux (on `--listen`) gets proxy routes + (if `registerInstrumentation=true` AND `metricsListen == ""`) `/metrics`.
- Admin mux (on `adminTarget`, which is either main or the loopback admin listener): `/admin/*`, `/debug/*`, query analytics if enabled.
- Metrics mux (on `--metrics-listen`, if set): `/metrics` only.

If `adminTarget == envCfg.listenAddr`, no second listener is started — both share the main mux (existing back-compat path).

If `adminTarget != envCfg.listenAddr`, start a second `http.Server{Addr: adminTarget, ...}` in a goroutine alongside the main listener; graceful-shutdown both on the same context cancel.

If `*metricsListen != ""`, start a third `http.Server` with only the `/metrics` handler registered.

- [ ] **Step 6: Tests pass**

```bash
GOWORK=off go test ./cmd/proxy/ -count=1
```

Expected: PASS for all `TestResolveAdminTarget*` and `TestValidateAdminExposure*` subtests.

- [ ] **Step 7: Manual smoke**

```bash
GOWORK=off go build -o /tmp/lvlp ./cmd/proxy
/tmp/lvlp --backend=http://127.0.0.1:9428 &
PID=$!
sleep 1
curl -sS http://127.0.0.1:3100/ready && echo MAIN-OK
curl -sS http://127.0.0.1:3101/admin/healthy && echo ADMIN-OK
curl -sS http://0.0.0.0:3101/admin/healthy 2>&1 | grep -i 'refused\|404' && echo ADMIN-NOT-REACHABLE-FROM-NONLOOPBACK
kill $PID
```

Expected: MAIN-OK, ADMIN-OK on loopback, no admin reachability from non-loopback.

- [ ] **Step 8: Update chart values**

In `charts/loki-vl-proxy/values.yaml`:

```yaml
proxy:
  # ... existing keys ...
  registerInstrumentation: true  # chart-default keeps /metrics scrapeable
  metricsListen: ":9091"          # dedicated metrics listener for ServiceMonitor
  adminListen: "127.0.0.1:3101"   # loopback by default; safe even inside the pod

service:
  # Existing main service stays unchanged.
  # Optionally add a separate metrics service:
  metrics:
    enabled: true
    port: 9091

# Optional ServiceMonitor:
serviceMonitor:
  port: metrics
```

- [ ] **Step 9: Update deployment template**

In `charts/loki-vl-proxy/templates/deployment.yaml`:

```yaml
args:
  # ... existing ...
  - --admin-listen={{ .Values.proxy.adminListen }}
  - --metrics-listen={{ .Values.proxy.metricsListen }}
  - --server.register-instrumentation={{ .Values.proxy.registerInstrumentation }}
ports:
  - name: http
    containerPort: {{ .Values.proxy.listenPort }}
  - name: metrics
    containerPort: 9091
```

Add a separate `metrics-service.yaml` template gated on `service.metrics.enabled` for the new port. ServiceMonitor template gets updated to point at it.

- [ ] **Step 10: Chart snapshot**

```bash
helm template loki-vl-proxy charts/loki-vl-proxy | grep -E 'metrics-listen|admin-listen|register-instrumentation'
```

Expected: rendered args include the three new flags with chart defaults.

- [ ] **Step 11: Full build + tests**

```bash
GOWORK=off go build ./...
GOWORK=off go test ./internal/proxy/ ./internal/metrics/ ./cmd/proxy/ -count=1
```

- [ ] **Step 12: Commit**

```bash
git add cmd/proxy/main.go cmd/proxy/main_test.go \
        charts/loki-vl-proxy/values.yaml \
        charts/loki-vl-proxy/templates/deployment.yaml \
        charts/loki-vl-proxy/templates/metrics-service.yaml
git commit -m "feat(proxy,chart): loopback admin listener + dedicated metrics listener (safe defaults)

BREAKING: binary default for -server.register-instrumentation flips from
true to false. /metrics is no longer served on --listen unless the
operator opts in. New --metrics-listen flag (default '') supports a
dedicated metrics port. New --admin-listen flag (default
127.0.0.1:3101) hosts admin/debug routes when no admin token is set, so
'go run ./cmd/proxy -backend=...' boots cleanly.

The chart sets register-instrumentation=true, metricsListen=:9091,
adminListen=127.0.0.1:3101, so ServiceMonitor / Prometheus scrapes
continue working without operator action."
git push
```

---

## Task 9 — Integration tests (validation across all 7 findings)

**Files:**
- Modify or extend: `test/integration/*` — boot proxy with the new defaults and assert end-to-end behavior.

- [ ] **Step 1: Identify the integration harness shape**

```bash
ls test/integration/
grep -l 'proxy.New\|cmd/proxy' test/integration/*.go | head -5
```

- [ ] **Step 2: Add an "out-of-the-box boot" test**

In `test/integration/boot_test.go` (create if absent):

```go
//go:build integration

package integration

import (
    "context"
    "io"
    "net/http"
    "os/exec"
    "testing"
    "time"
)

func TestBinaryBootsWithDefaultsOnly(t *testing.T) {
    // Spin up a stub VL backend on :9428.
    backend := startStubBackend(t)
    defer backend.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    cmd := exec.CommandContext(ctx, "go", "run", "./cmd/proxy", "-backend="+backend.URL)
    if err := cmd.Start(); err != nil {
        t.Fatalf("start: %v", err)
    }
    defer cmd.Process.Kill()
    waitForReady(t, "http://127.0.0.1:3100/ready", 5*time.Second)

    resp, err := http.Get("http://127.0.0.1:3101/admin/healthy")
    if err != nil {
        t.Fatalf("loopback admin: %v", err)
    }
    if resp.StatusCode != 200 {
        body, _ := io.ReadAll(resp.Body)
        t.Fatalf("expected 200 on loopback admin, got %d %s", resp.StatusCode, body)
    }
}
```

Add helpers `startStubBackend`, `waitForReady` if not already present in the harness.

- [ ] **Step 3: Add a "peer cache without token errors" test**

```go
func TestBinaryFailsWhenPeerCacheLacksToken(t *testing.T) {
    backend := startStubBackend(t)
    defer backend.Close()
    cmd := exec.Command("go", "run", "./cmd/proxy",
        "-backend="+backend.URL,
        "-peer-discovery=static",
        "-peer-static=10.0.0.1:3100",
    )
    out, _ := cmd.CombinedOutput()
    if cmd.ProcessState.ExitCode() == 0 {
        t.Fatalf("expected non-zero exit for peer cache without token, got 0\nstderr: %s", out)
    }
    if !bytes.Contains(out, []byte("peer-auth-token")) {
        t.Fatalf("error should mention peer-auth-token, got: %s", out)
    }
}
```

- [ ] **Step 4: Add a "metadata default lookback injects start/end" test**

Boot the proxy with `--metadata-default-lookback=1h`, hit `/labels` on the stub backend, assert the backend received `start` and `end` query params within the 1h window.

- [ ] **Step 5: Add a "debug logs redacted by default" test**

Boot the proxy with `--log-level=debug`, send a query, capture stdout, assert no raw query literal appears and `sha256:` is present.

- [ ] **Step 6: Run the integration suite**

```bash
GOWORK=off go test -tags=integration ./test/integration/ -count=1 -v
```

Expected: green.

- [ ] **Step 7: Commit**

```bash
git add test/integration/
git commit -m "test(integration): assert default-boot, peer-token, metadata-lookback, debug-redaction behavior"
git push
```

---

## Task 10 — CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Insert `[Unreleased]` block at the top**

Open `CHANGELOG.md`. Prepend the block defined in the spec (`### BREAKING CHANGES`, `### Added`, `### Security`, `### Notes` sections, with each finding called out by name and migration command).

- [ ] **Step 2: Verify all three breaking changes are explicitly named**

```bash
grep -A 2 'BREAKING CHANGES' CHANGELOG.md | head -20
```

Expected: three bullets (admin loopback default, peer-auth-token required, chart rate-limit defaults). Plus the `/metrics-no-longer-on-main` sub-bullet.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): document breaking changes for secure-defaults hardening"
git push
```

---

## Task 11 — Manual end-to-end on the e2e compose stack

**Files:**
- Verify: `deployment/docker/docker-compose-e2e.yml`

Per [[project_vl_e2e_compose]] — the e2e stack is the operator's last mile.

- [ ] **Step 1: Rebuild and start the stack**

```bash
docker compose -f deployment/docker/docker-compose-e2e.yml up --build -d
```

- [ ] **Step 2: Tail proxy logs and confirm clean boot**

```bash
docker compose -f deployment/docker/docker-compose-e2e.yml logs -f loki-vl-proxy | head -50
```

Expected: no startup errors; admin listener line present.

- [ ] **Step 3: Hit `/labels` from Grafana with no time range**

In Grafana, open the Loki datasource, request `/labels` with the time picker set to a wide range that Grafana usually omits. Confirm the backend receives bounded `start`/`end`.

- [ ] **Step 4: Confirm `/metrics` reachable from inside the stack**

```bash
docker exec -it lokivlproxy-e2e curl -s http://127.0.0.1:9091/metrics | head -5
```

Expected: Prometheus metrics output.

- [ ] **Step 5: Confirm peer cache rejects requests without the token**

```bash
docker exec -it lokivlproxy-e2e curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:3100/_cache/get
```

Expected: 401.

- [ ] **Step 6: Resource caps (per [[feedback_post_work_resource_caps]])**

If this PR touched LH containers (it doesn't — Loki-VL-proxy is separate), re-apply the standard caps. Otherwise: skip.

- [ ] **Step 7: Mark the PR ready for review**

```bash
gh pr create --title "feat: secure-by-default startup + hardening (7 findings)" \
  --body-file docs/superpowers/specs/2026-06-05-secure-defaults-hardening-design.md
```

The PR description points at the spec + CHANGELOG for full migration detail.

---

## Task 12 — Post-merge: full security & quality review + test improvement

This is the final pass the user explicitly asked for. Runs AFTER the PR is merged.

**Files:**
- Run: `security-review` skill (covers full diff)
- Run: `code-review` skill at `--effort high`
- Extend: any test file where coverage is below the 80% project baseline

- [ ] **Step 1: Run `/security-review` against the merged diff**

In a new session targeting this repo:

```
/security-review
```

Address every HIGH finding by opening a follow-up commit. MEDIUM/LOW are queued and triaged.

- [ ] **Step 2: Run `/code-review --effort high`**

Same session:

```
/code-review --effort high
```

Open a follow-up commit for every correctness finding.

- [ ] **Step 3: Coverage audit**

```bash
GOWORK=off go test -coverprofile=/tmp/cover.out ./internal/proxy/ ./internal/metrics/ ./cmd/proxy/
go tool cover -func=/tmp/cover.out | grep -E 'redactQuery|metadataQueryParams|validatePeerAuth|validateAdminExposure|resolveAdminTarget|hostProcFiles'
```

For each function below 90% coverage, add the missing branch test.

- [ ] **Step 4: Negative tests**

For each new flag, add a test that the proxy still works with the flag at its **opposite** value:
- `--debug-log-raw-queries=true` (raw works)
- `--metadata-default-lookback=0` (no injection)
- `--peer-insecure-ip-allowlist=true` (IP fallback still works)
- `--backend-version-strict=true` against a healthy backend (no spurious error)
- `--server.register-instrumentation=true` (back-compat path: /metrics on main listener when no metrics-listen)

- [ ] **Step 5: Commit and push the follow-up**

```bash
git add .
git commit -m "test(secure-defaults): improve coverage on hardening surfaces"
git push
```

---

## Self-review

**Spec coverage:**
- Finding #1 → Task 8 ✓ (incl. /metrics flag fix discovered during planning)
- Finding #2 → Task 6 ✓
- Finding #3 → Task 3 ✓
- Finding #4 → Task 2 ✓
- Finding #5 → Task 5 ✓
- Finding #6 → Task 7 ✓
- Finding #7 → Task 4 ✓
- CHANGELOG → Task 10 ✓
- Integration tests → Task 9 ✓
- E2E compose → Task 11 ✓
- Post-merge review → Task 12 ✓

**Placeholder scan:** no TBD/TODO/"implement later". Every step has either exact code, exact commands, or an exact decision rule.

**Type consistency:** `redactQuery(s, raw)`, `metadataDefaultLookback`, `validatePeerAuth(discovery, peers, token, insecureAllowlist)`, `resolveAdminTarget(mainListen, adminListen, token)`, `validateAdminExposure(adminTarget, ...)`, `hostProcFiles []string`, `hostProcRoot` — all names used consistently across tasks.

**Order rationale:** smallest blast radius first (#4 redaction), then well-scoped behavior changes (#3 lookback, #7 strict mode), then deeper code change (#5 metrics split), then peer + chart (#2), then chart defaults (#6), finally the largest surgery (#1 listeners). CHANGELOG and integration tests after all behavior is locked.
