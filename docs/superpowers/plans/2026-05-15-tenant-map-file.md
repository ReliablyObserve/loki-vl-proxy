# Tenant Map File (`-tenant-map-file`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `-tenant-map-file` flag that loads the OrgID→AccountID/ProjectID tenant mapping from a YAML or JSON file on disk, hot-reloads it via SIGHUP, and automatically detects changes via mtime polling — enabling Kubernetes ConfigMap-based tenant configuration with zero-restart reloads.

**Architecture:** A `loadTenantMapFile` function reads and parses YAML or JSON from disk. On startup it is loaded once alongside the existing inline `-tenant-map` JSON. A background goroutine polls the file's `mtime` every `-tenant-map-reload-interval` (default 30s) and calls `ReloadTenantMap` when the file changes. SIGHUP also re-reads the file. K8s ConfigMap volumes work automatically: Kubernetes atomically replaces the `..data` symlink on ConfigMap update, and `os.Stat` + `os.ReadFile` follow symlinks transparently.

**Tech Stack:** Go stdlib (`os`, `path/filepath`, `encoding/json`), `gopkg.in/yaml.v3` (already in `go.mod`). Changes in `cmd/proxy/main.go` only — no changes to `internal/proxy/` needed (uses existing `ReloadTenantMap` API).

---

## Context for the implementer

**Existing tenant mapping (understand before editing):**

The current flow:
1. `-tenant-map='{"org":{"account_id":"1","project_id":"0"}}'` flag sets an inline JSON string.
2. `parseTenantMapJSON(raw string)` in `cmd/proxy/main.go:1173` parses it into `map[string]proxy.TenantMapping`.
3. `proxy.TenantMapping` struct (`internal/proxy/proxy.go:91`) has `AccountID string` and `ProjectID string` with both `json:` and `yaml:` tags.
4. `p.ReloadTenantMap(m)` hot-swaps the map in the running proxy (SIGHUP-safe, uses `configMu` RWMutex).
5. `reloadDynamicConfig` (`cmd/proxy/main.go:1638`) is called on SIGHUP — currently reads from `TENANT_MAP` env var only.

**New YAML config format (what this plan adds):**
```yaml
# /etc/loki-vl-proxy/tenant-map.yaml
prod-team-eu_staging:
  account_id: "42"
  project_id: "3"
prod-team-eu_prod:
  account_id: "42"
  project_id: "7"
dev_default:
  account_id: "1"
  project_id: "1"
```

**JSON format also supported (auto-detected by `.json` extension):**
```json
{
  "prod-team-eu_staging": {"account_id": "42", "project_id": "3"},
  "prod-team-eu_prod":    {"account_id": "42", "project_id": "7"},
  "dev_default":          {"account_id": "1",  "project_id": "1"}
}
```

**K8s ConfigMap reload mechanism:**
Kubernetes mounts ConfigMaps as a directory of symlinks:
```
/etc/loki-vl-proxy/tenant-map.yaml  →  ../data/tenant-map.yaml  →  ..2026_05_15.../tenant-map.yaml
```
On ConfigMap update, Kubernetes atomically replaces `..data` symlink. `os.ReadFile` follows the chain correctly. `os.Stat` returns the mtime of the new target file (the update time), which triggers the poller.

**Priority when both `-tenant-map` and `-tenant-map-file` are set:**
File takes priority — merge: file entries override inline JSON entries for the same key, with file loaded last.

---

## Task 1: Add `loadTenantMapFile` parser (YAML/JSON auto-detection)

**Files:**
- Modify: `cmd/proxy/main.go`

- [ ] **Step 1: Add required imports if not present**

In `cmd/proxy/main.go`, ensure the import block includes:

```go
import (
    // existing imports...
    "path/filepath"
    "gopkg.in/yaml.v3"
    // json and os are already imported
)
```

- [ ] **Step 2: Add `loadTenantMapFile` after `parseTenantMapJSON`**

Locate `func parseTenantMapJSON` (line ~1173). Add `loadTenantMapFile` immediately after it:

```go
// loadTenantMapFile reads a YAML or JSON tenant-map file from disk.
// Format is auto-detected: files ending in .json are parsed as JSON; all others as YAML.
// YAML format:
//
//	prod-team-eu_staging:
//	  account_id: "42"
//	  project_id: "3"
//
// JSON format: {"org": {"account_id": "42", "project_id": "3"}}
func loadTenantMapFile(path string) (map[string]proxy.TenantMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read tenant-map-file %q: %w", path, err)
	}
	var m map[string]proxy.TenantMapping
	if strings.ToLower(filepath.Ext(path)) == ".json" {
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("parse tenant-map-file %q (JSON): %w", path, err)
		}
	} else {
		if err := yaml.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("parse tenant-map-file %q (YAML): %w", path, err)
		}
	}
	return m, nil
}
```

- [ ] **Step 3: Compile check**

```bash
go build ./cmd/proxy/
```

Expected: no errors. Remove the built binary: `rm -f ./proxy`

- [ ] **Step 4: Commit**

```bash
git add cmd/proxy/main.go
git commit -m "feat(cmd): add loadTenantMapFile for YAML/JSON tenant-map file parsing"
```

---

## Task 2: Add `-tenant-map-file` and `-tenant-map-reload-interval` flags

**Files:**
- Modify: `cmd/proxy/main.go`

- [ ] **Step 1: Add the two flags**

In the flags section, locate the line:
```go
tenantMapJSON := fs.String("tenant-map", "", `JSON tenant mapping: ...`)
```

Add immediately after it:

```go
tenantMapFile     := fs.String("tenant-map-file", "", "Path to YAML or JSON file containing the tenant map (alternative to -tenant-map). Hot-reloaded on SIGHUP and automatically on file change (see -tenant-map-reload-interval). Use with Kubernetes ConfigMap volumes for zero-restart tenant config updates.")
tenantMapReloadInterval := fs.Duration("tenant-map-reload-interval", 30*time.Second, "How often to poll -tenant-map-file for changes (mtime-based). Set to 0 to disable polling (SIGHUP-only reload).")
```

- [ ] **Step 2: Add env var support in `applyEnvOverrides`**

Find the block that reads `TENANT_MAP` (line ~1125). Add after it:

```go
if v := getenv("TENANT_MAP_FILE"); v != "" && *tenantMapFile == "" {
    *tenantMapFile = v
}
```

- [ ] **Step 3: Load file on startup and merge with inline JSON**

Find the startup section where `parseTenantMapJSON(cfg.tenantMapJSON)` is called (line ~1384). Replace/extend it:

```go
tenantMap, err := parseTenantMapJSON(cfg.tenantMapJSON)
if err != nil {
    return fmt.Errorf("parse -tenant-map: %w", err)
}
if cfg.tenantMapFile != "" {
    fileMap, err := loadTenantMapFile(cfg.tenantMapFile)
    if err != nil {
        return fmt.Errorf("load -tenant-map-file: %w", err)
    }
    // Merge: file entries override inline JSON entries for the same key
    if tenantMap == nil {
        tenantMap = fileMap
    } else {
        for k, v := range fileMap {
            tenantMap[k] = v
        }
    }
}
```

- [ ] **Step 4: Add `tenantMapFile` and `tenantMapReloadInterval` to the internal config struct**

Find the internal `cfg` struct (the one that holds parsed flag values, not `proxy.Config`). Add:

```go
tenantMapFile           string
tenantMapReloadInterval time.Duration
```

And wire the flag values into it in the flag-parsing section:

```go
cfg.tenantMapFile           = *tenantMapFile
cfg.tenantMapReloadInterval = *tenantMapReloadInterval
```

- [ ] **Step 5: Compile check**

```bash
go build ./cmd/proxy/
```

Expected: no errors. Remove: `rm -f ./proxy`

- [ ] **Step 6: Commit**

```bash
git add cmd/proxy/main.go
git commit -m "feat(cmd): add -tenant-map-file and -tenant-map-reload-interval flags"
```

---

## Task 3: SIGHUP hot-reload from file

**Files:**
- Modify: `cmd/proxy/main.go`

- [ ] **Step 1: Extend `reloadDynamicConfig` to re-read the file**

Find `func reloadDynamicConfig` (line ~1638). The current function reads from `TENANT_MAP` env var. Extend it to also check the file:

```go
func reloadDynamicConfig(p reloadableProxy, getenv func(string) string, tenantMapFile string, logger *slog.Logger) {
    if tenantMapFile != "" {
        m, err := loadTenantMapFile(tenantMapFile)
        if err != nil {
            logger.Error("SIGHUP: failed to reload tenant-map-file", "path", tenantMapFile, "error", err)
        } else {
            p.ReloadTenantMap(m)
            logger.Info("SIGHUP: reloaded tenant mappings from file", "path", tenantMapFile, "count", len(m))
        }
    } else if v := getenv("TENANT_MAP"); v != "" {
        var newTenantMap map[string]proxy.TenantMapping
        if err := json.Unmarshal([]byte(v), &newTenantMap); err != nil {
            logger.Error("SIGHUP: failed to reload tenant map from env", "error", err)
        } else {
            p.ReloadTenantMap(newTenantMap)
            logger.Info("SIGHUP: reloaded tenant mappings from env", "count", len(newTenantMap))
        }
    }
    // ... rest of existing reloadDynamicConfig body (FIELD_MAPPING etc.)
}
```

- [ ] **Step 2: Update the SIGHUP handler call to pass `tenantMapFile`**

Find where `reloadDynamicConfig` is called (search for `reloadDynamicConfig(`). Update the call signature to pass the file path:

```go
reloadDynamicConfig(p, os.Getenv, cfg.tenantMapFile, logger)
```

- [ ] **Step 3: Compile check**

```bash
go build ./cmd/proxy/
```

Expected: no errors. Remove: `rm -f ./proxy`

- [ ] **Step 4: Commit**

```bash
git add cmd/proxy/main.go
git commit -m "feat(cmd): extend SIGHUP reload to re-read -tenant-map-file from disk"
```

---

## Task 4: Mtime-polling goroutine for K8s ConfigMap hot-reload

**Files:**
- Modify: `cmd/proxy/main.go`

- [ ] **Step 1: Add `watchTenantMapFile` function**

Add after `loadTenantMapFile`:

```go
// watchTenantMapFile polls path every interval for mtime changes and calls
// p.ReloadTenantMap when the file is updated. Exits when ctx is cancelled.
// Works with Kubernetes ConfigMap volumes: K8s atomically replaces the ..data
// symlink on ConfigMap update; os.Stat follows the symlink and returns the new mtime.
func watchTenantMapFile(ctx context.Context, path string, interval time.Duration, p reloadableProxy, logger *slog.Logger) {
    var lastMod time.Time
    if fi, err := os.Stat(path); err == nil {
        lastMod = fi.ModTime()
    }

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            fi, err := os.Stat(path)
            if err != nil {
                logger.Warn("tenant-map-file: stat failed", "path", path, "error", err)
                continue
            }
            if fi.ModTime().Equal(lastMod) {
                continue
            }
            lastMod = fi.ModTime()
            m, err := loadTenantMapFile(path)
            if err != nil {
                logger.Error("tenant-map-file: reload failed after mtime change", "path", path, "error", err)
                continue
            }
            p.ReloadTenantMap(m)
            logger.Info("tenant-map-file: reloaded after change detected", "path", path, "count", len(m))
        }
    }
}
```

- [ ] **Step 2: Start the watcher goroutine after the proxy is created**

Find the section where the proxy is started and background goroutines are launched (search for `go func()` near the SIGHUP handler). Add after the proxy is initialized and before `srv.ListenAndServe()`:

```go
if cfg.tenantMapFile != "" && cfg.tenantMapReloadInterval > 0 {
    go watchTenantMapFile(ctx, cfg.tenantMapFile, cfg.tenantMapReloadInterval, p, logger)
}
```

Where `ctx` is the main context (the one cancelled on shutdown signal). If the proxy uses a different cancellation mechanism, use that instead — the watcher must exit when the proxy shuts down.

- [ ] **Step 3: Compile check**

```bash
go build ./cmd/proxy/
```

Expected: no errors. Remove: `rm -f ./proxy`

- [ ] **Step 4: Commit**

```bash
git add cmd/proxy/main.go
git commit -m "feat(cmd): add mtime-polling watcher for -tenant-map-file Kubernetes ConfigMap hot-reload"
```

---

## Task 5: Unit tests for `loadTenantMapFile` and `watchTenantMapFile`

**Files:**
- Create: `cmd/proxy/tenant_map_file_test.go`

- [ ] **Step 1: Create the test file**

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/loki-vl-proxy/internal/proxy"
)

func TestLoadTenantMapFile_YAML(t *testing.T) {
	f := writeTempFile(t, "tenant-map.yaml", `
prod-team-eu_staging:
  account_id: "42"
  project_id: "3"
prod-team-eu_prod:
  account_id: "42"
  project_id: "7"
dev_default:
  account_id: "1"
  project_id: "1"
`)
	m, err := loadTenantMapFile(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertMapping(t, m, "prod-team-eu_staging", "42", "3")
	assertMapping(t, m, "prod-team-eu_prod", "42", "7")
	assertMapping(t, m, "dev_default", "1", "1")
}

func TestLoadTenantMapFile_JSON(t *testing.T) {
	f := writeTempFile(t, "tenant-map.json", `{
  "prod-team-eu_staging": {"account_id": "42", "project_id": "3"},
  "dev_default":          {"account_id": "1",  "project_id": "1"}
}`)
	m, err := loadTenantMapFile(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertMapping(t, m, "prod-team-eu_staging", "42", "3")
	assertMapping(t, m, "dev_default", "1", "1")
}

func TestLoadTenantMapFile_MissingFile(t *testing.T) {
	_, err := loadTenantMapFile("/nonexistent/path/tenant-map.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadTenantMapFile_InvalidYAML(t *testing.T) {
	f := writeTempFile(t, "bad.yaml", "not: valid: yaml: [")
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected parse error for invalid YAML, got nil")
	}
}

func TestWatchTenantMapFile_DetectsMtimeChange(t *testing.T) {
	f := writeTempFile(t, "tenant-map.yaml", `
org-a:
  account_id: "1"
  project_id: "0"
`)
	reloaded := make(chan map[string]proxy.TenantMapping, 1)
	mock := &mockReloadableProxy{onReload: func(m map[string]proxy.TenantMapping) { reloaded <- m }}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go watchTenantMapFile(ctx, f, 50*time.Millisecond, mock, noopLogger())

	// Give the watcher one tick to record the initial mtime
	time.Sleep(100 * time.Millisecond)

	// Write an updated file (mtime changes)
	if err := os.WriteFile(f, []byte(`
org-a:
  account_id: "99"
  project_id: "5"
`), 0600); err != nil {
		t.Fatalf("failed to update file: %v", err)
	}

	select {
	case m := <-reloaded:
		assertMapping(t, m, "org-a", "99", "5")
	case <-ctx.Done():
		t.Fatal("watcher did not detect file change within 5s")
	}
}

// --- helpers ---

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(f, []byte(content), 0600); err != nil {
		t.Fatalf("writeTempFile: %v", err)
	}
	return f
}

func assertMapping(t *testing.T, m map[string]proxy.TenantMapping, orgID, wantAcct, wantProj string) {
	t.Helper()
	got, ok := m[orgID]
	if !ok {
		t.Fatalf("mapping for %q not found in %v", orgID, m)
	}
	if got.AccountID != wantAcct || got.ProjectID != wantProj {
		t.Fatalf("mapping %q: got AccountID=%q ProjectID=%q, want %q %q",
			orgID, got.AccountID, got.ProjectID, wantAcct, wantProj)
	}
}

type mockReloadableProxy struct {
	onReload func(map[string]proxy.TenantMapping)
}

func (m *mockReloadableProxy) ReloadTenantMap(tm map[string]proxy.TenantMapping) {
	m.onReload(tm)
}
func (m *mockReloadableProxy) ReloadFieldMappings(_ []proxy.FieldMapping) {}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
```

Add missing imports at the top:
```go
import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/loki-vl-proxy/internal/proxy"
)
```

- [ ] **Step 2: Run the tests**

```bash
go test -v ./cmd/proxy/ -run 'TestLoadTenantMapFile|TestWatchTenantMapFile'
```

Expected: All 5 tests PASS. The watcher test uses 50ms poll interval and completes in < 5s.

- [ ] **Step 3: Commit**

```bash
git add cmd/proxy/tenant_map_file_test.go
git commit -m "test(cmd): unit tests for loadTenantMapFile and watchTenantMapFile"
```

---

## Task 6: Kubernetes ConfigMap example and documentation

**Files:**
- Create: `docs/k8s-tenant-map-configmap.yaml`
- Modify: `CHANGELOG.md`
- Modify: `README.md`

- [ ] **Step 1: Create the Kubernetes example file**

```yaml
# docs/k8s-tenant-map-configmap.yaml
# Example: Kubernetes ConfigMap for loki-vl-proxy tenant mapping.
# Update this ConfigMap to add/change/remove tenants — the proxy reloads
# automatically within -tenant-map-reload-interval (default 30s).
#
# Apply with: kubectl apply -f docs/k8s-tenant-map-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: loki-vl-proxy-tenant-map
  namespace: monitoring
data:
  tenant-map.yaml: |
    # Maps Loki X-Scope-OrgID strings → VictoriaLogs AccountID + ProjectID.
    # AccountID and ProjectID must be numeric strings (VL integer tenant IDs).
    prod-team-eu_staging:
      account_id: "42"
      project_id: "3"
    prod-team-eu_prod:
      account_id: "42"
      project_id: "7"
    dev_default:
      account_id: "1"
      project_id: "1"
---
# Deployment snippet — relevant sections only
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loki-vl-proxy
  namespace: monitoring
spec:
  template:
    spec:
      containers:
        - name: loki-vl-proxy
          args:
            - -backend-url=http://victorialogs:9428
            - -tenant-map-file=/etc/loki-vl-proxy/tenant-map.yaml
            # Poll every 30s for ConfigMap changes (default). Set to 0 for SIGHUP-only.
            - -tenant-map-reload-interval=30s
          volumeMounts:
            - name: tenant-map
              mountPath: /etc/loki-vl-proxy
              readOnly: true
      volumes:
        - name: tenant-map
          configMap:
            name: loki-vl-proxy-tenant-map
```

- [ ] **Step 2: Add README section**

Find the flags table in `README.md`. Add rows for the two new flags:

```markdown
| `-tenant-map-file`            | `TENANT_MAP_FILE`            | `""`  | Path to a YAML or JSON file mapping Loki `X-Scope-OrgID` strings to VictoriaLogs `AccountID`/`ProjectID`. Reloaded on SIGHUP and automatically when the file changes (see `-tenant-map-reload-interval`). Suitable for Kubernetes ConfigMap volumes. File entries override `-tenant-map` inline entries for the same key. |
| `-tenant-map-reload-interval` | *(flag only)*                | `30s` | How often to poll `-tenant-map-file` for mtime changes. Set to `0` to disable polling (SIGHUP-only reload). |
```

Also add a `### Kubernetes ConfigMap hot-reload` subsection in the multi-tenancy section of the README pointing to `docs/k8s-tenant-map-configmap.yaml`.

- [ ] **Step 3: Add CHANGELOG entry under `[Unreleased]`**

```markdown
### Added

- **`-tenant-map-file` flag**: Load the OrgID→AccountID/ProjectID tenant mapping from a YAML or JSON file. Supports hot-reload via SIGHUP and automatic mtime-polling (default 30s) for Kubernetes ConfigMap volume updates without proxy restart. File format:
  ```yaml
  prod-team-eu_staging:
    account_id: "42"
    project_id: "3"
  prod-team-eu_prod:
    account_id: "42"
    project_id: "7"
  ```
  `TENANT_MAP_FILE` environment variable also accepted. File entries take priority over `-tenant-map` inline JSON for the same key.

- **`-tenant-map-reload-interval` flag** (default `30s`): Poll interval for detecting `-tenant-map-file` changes. Set to `0` to disable polling and rely on SIGHUP-only reload.
```

- [ ] **Step 4: Commit**

```bash
git add docs/k8s-tenant-map-configmap.yaml CHANGELOG.md README.md
git commit -m "docs: add Kubernetes ConfigMap example and README docs for -tenant-map-file"
```

---

## Self-Review

**Spec coverage:**
- ✅ YAML file format: Task 1 (`loadTenantMapFile` YAML branch)
- ✅ JSON file format: Task 1 (auto-detected by `.json` extension)
- ✅ Simple mapping (`dev_default` → 1/1): covered by Task 1 + test data
- ✅ Advanced multi-project mapping (`prod-team-eu_*` → 42/3, 42/7): covered by test data
- ✅ File loaded on startup: Task 2 (merged with inline JSON)
- ✅ SIGHUP hot-reload from file: Task 3
- ✅ Mtime-polling for K8s ConfigMap: Task 4
- ✅ Unit tests: Task 5
- ✅ K8s ConfigMap example: Task 6
- ✅ Env var `TENANT_MAP_FILE`: Task 2

**No placeholders:** `loadTenantMapFile`, `watchTenantMapFile`, all tests, and K8s YAML are complete.

**Type consistency:** `mockReloadableProxy` implements the same `reloadableProxy` interface already used in `main.go:241`. `proxy.TenantMapping` and `proxy.FieldMapping` are imported from the correct package.

---

## Task 7: Forward per-tenant `X-Scope-OrgID` to upstream (`-forward-tenant-header`)

**Background:** VictoriaLogs ignores unknown HTTP headers, so forwarding `X-Scope-OrgID` is safe and non-breaking for existing VL setups. Victoria Lakehouse (VictoriaMetrics' hosted product) DOES use `X-Scope-OrgID` for native tenant routing — forwarding the per-tenant value enables zero-config Victoria Lakehouse integration. The value forwarded must be the **per-tenant** orgID (from `getOrgID(req.Context())` — the single org for this fanout sub-request), NOT the original pipe-separated multi-tenant value (which lives in `origRequestKey` and is forwarded by `applyBackendHeaders` for other purposes).

**Files:**
- Modify: `internal/proxy/proxy.go` — add `ForwardTenantHeader bool` to `Config`, `forwardTenantHeader bool` to `Proxy`, wire in `New()`
- Modify: `internal/proxy/multitenant.go` — in `forwardTenantHeaders`, if `p.forwardTenantHeader` is true, append `req.Header.Set("X-Scope-OrgID", orgID)` using the per-tenant orgID
- Modify: `cmd/proxy/main.go` — add `-forward-tenant-header` flag (bool, default `true`), env var `FORWARD_TENANT_HEADER`
- Create: `internal/proxy/multitenant_forward_header_test.go`

- [ ] **Step 1: Add `ForwardTenantHeader` to `Config` and `Proxy`**

In `internal/proxy/proxy.go`, locate the `Config` struct (around line 80). Add the new field:

```go
type Config struct {
    // ... existing fields ...
    ForwardTenantHeader bool // forward per-tenant X-Scope-OrgID to upstream (safe for VL, needed for Victoria Lakehouse)
}
```

Locate the `Proxy` struct (the main struct with all fields). Add:

```go
type Proxy struct {
    // ... existing fields ...
    forwardTenantHeader bool
}
```

In `func New(cfg Config, ...) (*Proxy, error)` (or wherever `Proxy` is initialized from `Config`), wire the field:

```go
p := &Proxy{
    // ... existing assignments ...
    forwardTenantHeader: cfg.ForwardTenantHeader,
}
```

- [ ] **Step 2: Extend `forwardTenantHeaders` in `internal/proxy/multitenant.go`**

Locate `func (p *Proxy) forwardTenantHeaders(req *http.Request)`. The function currently ends with the numeric orgID passthrough block. Append at the very end of the function (after all existing logic):

```go
// Forward the per-tenant X-Scope-OrgID to upstream.
// VictoriaLogs ignores it; Victoria Lakehouse uses it for native tenant routing.
// Must use orgID from context (per-tenant value), not the original multi-tenant header.
if p.forwardTenantHeader && orgID != "" {
    req.Header.Set("X-Scope-OrgID", orgID)
}
```

This placement is intentional: it runs after AccountID/ProjectID logic, is always additive, and applies to every tenant path (explicit map, default alias, numeric passthrough).

**Note on default aliases:** When `orgID` is a default alias (e.g., `"fake"`, `""`, `"0"`), `forwardTenantHeaders` returns early before reaching this code. That is correct — default tenants don't need `X-Scope-OrgID` forwarded. If you want default aliases to also forward, move this block before the `isDefaultTenantAlias` early return. The current design (skip default aliases) is preferred.

- [ ] **Step 3: Add `-forward-tenant-header` flag in `cmd/proxy/main.go`**

In the flags section, after the `-tenant-map-reload-interval` flag added in Task 2, add:

```go
forwardTenantHeader := fs.Bool("forward-tenant-header", true, "Forward the per-tenant X-Scope-OrgID header to the upstream backend. Safe for VictoriaLogs (ignores it). Required for Victoria Lakehouse native tenant routing. The forwarded value is the per-tenant OrgID, not the original multi-tenant pipe-separated value.")
```

Add env var support in `applyEnvOverrides` (after the `TENANT_MAP_FILE` block added in Task 2):

```go
if v := getenv("FORWARD_TENANT_HEADER"); v != "" {
    b, err := strconv.ParseBool(v)
    if err == nil {
        *forwardTenantHeader = b
    }
}
```

Wire into the proxy config before `proxy.New(...)` is called:

```go
cfg.ForwardTenantHeader = *forwardTenantHeader
```

Or if the proxy config is built inline:

```go
proxyCfg := proxy.Config{
    // ... existing fields ...
    ForwardTenantHeader: *forwardTenantHeader,
}
```

- [ ] **Step 4: Compile check**

```bash
go build ./cmd/proxy/ ./internal/proxy/
```

Expected: no errors. Remove: `rm -f ./proxy`

- [ ] **Step 5: Write unit tests**

Create `internal/proxy/multitenant_forward_header_test.go`:

```go
package proxy

import (
	"context"
	"net/http"
	"testing"
)

// makeProxyWithForwardTenantHeader creates a minimal Proxy for testing forwardTenantHeaders.
func makeProxyWithForwardTenantHeader(enabled bool) *Proxy {
	return &Proxy{
		forwardTenantHeader: enabled,
	}
}

func TestForwardTenantHeader_Enabled(t *testing.T) {
	p := makeProxyWithForwardTenantHeader(true)
	req, _ := http.NewRequest("GET", "/", nil)
	ctx := withOrgID(req.Context(), "prod-team-eu_staging")
	req = req.WithContext(ctx)

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "prod-team-eu_staging" {
		t.Errorf("X-Scope-OrgID = %q, want %q", got, "prod-team-eu_staging")
	}
}

func TestForwardTenantHeader_Disabled(t *testing.T) {
	p := makeProxyWithForwardTenantHeader(false)
	req, _ := http.NewRequest("GET", "/", nil)
	ctx := withOrgID(req.Context(), "prod-team-eu_staging")
	req = req.WithContext(ctx)

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (flag disabled)", got)
	}
}

func TestForwardTenantHeader_WithTenantMap(t *testing.T) {
	p := makeProxyWithForwardTenantHeader(true)
	p.tenantMap = map[string]TenantMapping{
		"prod-team-eu_staging": {AccountID: "42", ProjectID: "3"},
	}
	req, _ := http.NewRequest("GET", "/", nil)
	ctx := withOrgID(req.Context(), "prod-team-eu_staging")
	req = req.WithContext(ctx)

	p.forwardTenantHeaders(req)

	// AccountID/ProjectID should be set from the map
	if got := req.Header.Get("AccountID"); got != "42" {
		t.Errorf("AccountID = %q, want %q", got, "42")
	}
	if got := req.Header.Get("ProjectID"); got != "3" {
		t.Errorf("ProjectID = %q, want %q", got, "3")
	}
	// X-Scope-OrgID should also be forwarded
	if got := req.Header.Get("X-Scope-OrgID"); got != "prod-team-eu_staging" {
		t.Errorf("X-Scope-OrgID = %q, want %q", got, "prod-team-eu_staging")
	}
}

func TestForwardTenantHeader_DefaultAliasSkipped(t *testing.T) {
	p := makeProxyWithForwardTenantHeader(true)
	req, _ := http.NewRequest("GET", "/", nil)
	// "fake" is a default tenant alias — forwardTenantHeaders returns early
	ctx := withOrgID(req.Context(), "fake")
	req = req.WithContext(ctx)

	p.forwardTenantHeaders(req)

	// Default aliases should NOT get X-Scope-OrgID forwarded (early return)
	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (default alias skips forwarding)", got)
	}
}

func TestForwardTenantHeader_EmptyOrgIDSkipped(t *testing.T) {
	p := makeProxyWithForwardTenantHeader(true)
	req, _ := http.NewRequest("GET", "/", nil)
	// No orgID in context — function returns early
	req = req.WithContext(context.Background())

	p.forwardTenantHeaders(req)

	got := req.Header.Get("X-Scope-OrgID")
	if got != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty (no orgID in context)", got)
	}
}
```

**Note:** If `withOrgID` is not exported from the `proxy` package (it may be unexported as `withOrgID` or similar), look for the correct unexported function name in `internal/proxy/telemetry.go`. The test is in `package proxy` (same package) so it can access unexported identifiers.

- [ ] **Step 6: Run the tests**

```bash
go test -v ./internal/proxy/ -run 'TestForwardTenantHeader'
```

Expected: all 5 tests PASS.

- [ ] **Step 7: Add CHANGELOG entry**

In `CHANGELOG.md` under `[Unreleased] > Added`, append:

```markdown
- **`-forward-tenant-header` flag** (default `true`): Forward the per-tenant `X-Scope-OrgID` header to the upstream backend on each sub-request. Safe for VictoriaLogs (silently ignored). Required for Victoria Lakehouse native tenant routing. The forwarded value is the resolved per-tenant OrgID, not the original pipe-separated multi-tenant value. Set `-forward-tenant-header=false` or `FORWARD_TENANT_HEADER=false` to disable.
```

- [ ] **Step 8: Commit**

```bash
git add internal/proxy/proxy.go internal/proxy/multitenant.go internal/proxy/multitenant_forward_header_test.go cmd/proxy/main.go CHANGELOG.md
git commit -m "feat(proxy): add -forward-tenant-header flag to pass X-Scope-OrgID to upstream (Victoria Lakehouse compat)"
```

---

## Updated Self-Review

**Spec coverage (all 7 tasks):**
- ✅ YAML file format: Task 1
- ✅ JSON file format: Task 1
- ✅ Simple and advanced tenant mappings: Tasks 1+2
- ✅ File loaded on startup + merge with inline JSON: Task 2
- ✅ SIGHUP hot-reload from file: Task 3
- ✅ Mtime-polling for K8s ConfigMap: Task 4
- ✅ Unit tests for file loading + watcher: Task 5
- ✅ K8s ConfigMap example + docs: Task 6
- ✅ `TENANT_MAP_FILE` env var: Task 2
- ✅ Forward per-tenant `X-Scope-OrgID` to upstream: Task 7
- ✅ Configurable (default `true`): Task 7
- ✅ `FORWARD_TENANT_HEADER` env var: Task 7
- ✅ Default alias skip (no spurious header for `fake`/`0`): Task 7 tests
- ✅ Per-tenant value (not pipe-separated original): Task 7 implementation note

**No placeholders:** All tasks contain complete code.

**Type consistency:** `forwardTenantHeader bool` field added to both `Config` (exported) and `Proxy` (unexported field). Tests in `package proxy` access unexported identifiers directly.
