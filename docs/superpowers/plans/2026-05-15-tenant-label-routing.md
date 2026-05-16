# Tenant Label Routing (`-tenant-label`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `-tenant-label` flag that lets deployments where all VL data lives under the default tenant (AccountID=0, ProjectID=0) use a label field (e.g., `org_id`, `account_id`) as the tenant discriminator, injecting `{<label>="<orgID>"}` into every VL query instead of setting `AccountID`/`ProjectID` headers.

**Architecture:** When `-tenant-label=<field>` is set, the proxy's `vlGetInner`/`vlPostInner` inspect the orgID from request context (set by `withOrgID`) and prepend a LogsQL label filter to every VL `query`/`q` param before sending to VL. Explicit `tenantMap` entries still take priority (they use VL native tenancy via headers). Default-tenant aliases (`0`, `fake`, `default`, `*`) bypass the filter (they mean "all tenants"). Multi-tenant fanout (`0|fake`) works transparently: each per-tenant sub-request carries a single OrgID, which the filter injection already handles correctly.

**Tech Stack:** Go, `net/url`, `net/http`. Changes confined to `internal/proxy/` and `cmd/proxy/main.go`. New unit tests in `internal/proxy/multitenant_tenant_label_test.go`. No new dependencies.

---

## Context for the implementer

**Key files:**
- `internal/proxy/proxy.go` — `Config` struct and `Proxy` struct, `New()` constructor
- `internal/proxy/backend.go` — `vlGetInner` and `vlPostInner` (the two functions that dispatch all VL HTTP requests)
- `internal/proxy/multitenant.go` — `forwardTenantHeaders` (maps orgID → VL headers); `isDefaultTenantAlias`
- `internal/proxy/telemetry.go` — `getOrgID(ctx)`, `ctxKey` type, `orgIDKey` constant
- `cmd/proxy/main.go` — all CLI flags via `flag.FlagSet`

**How tenant routing currently works (read before editing):**

1. `withOrgID(r)` stores `X-Scope-OrgID` header value into request context under `orgIDKey`.
2. Every `vlGetInner`/`vlPostInner` call triggers `p.forwardTenantHeaders(req)`.
3. `forwardTenantHeaders` checks context orgID → tenantMap → numeric passthrough → sets `AccountID`/`ProjectID` headers on the VL request.
4. VL routes the request to the appropriate tenant based on those headers. If no headers set, VL uses default tenant (0:0).

**What `-tenant-label` changes:**

Instead of (or in addition to) header-based routing, inject a LogsQL label filter into the VL query params. VL then filters results by that label within the default (0:0) tenant. This is the correct approach when VL multi-tenancy is not enabled and all data is under 0:0.

**Important invariants:**
- `tenantMap` explicit entries always take priority (they use VL native tenancy).
- Default alias orgIDs (`""`, `"0"`, `"fake"`, `"default"`, `"*"`) must NOT get a label filter — they mean "all data".
- The filter must be injected into the `query` param for most endpoints, and `q` param for `/select/logsql/hits`.
- Multi-tenant fanout (e.g., `"0|fake"`) dispatches sub-requests with single orgIDs — filter injection naturally applies per sub-request.

**`isDefaultTenantAlias` is in `multitenant.go`** — call it to check if orgID is a bypass alias.

---

## Task 1: Add `TenantLabel` to `Config` and `Proxy` structs

**Files:**
- Modify: `internal/proxy/proxy.go`

- [ ] **Step 1: Add `TenantLabel` field to `Config` struct**

In `internal/proxy/proxy.go`, find the `Config` struct (around line 104). Add `TenantLabel` after `TenantMap`:

```go
type Config struct {
    // ... existing fields ...
    TenantMap           map[string]TenantMapping // string org ID → VL account/project
    TenantLabel         string                   // VL field name for label-based tenant routing (alternative to AccountID/ProjectID headers)
    // ... rest of existing fields ...
}
```

- [ ] **Step 2: Add `tenantLabel` field to `Proxy` struct**

In the same file, find the `Proxy` struct (around line 340). Add after `tenantMap`:

```go
type proxy struct {
    // ... existing fields ...
    tenantMap   map[string]TenantMapping
    tenantLabel string
    // ... rest of fields ...
}
```

- [ ] **Step 3: Wire `tenantLabel` in `New()` constructor**

In `New()` (around line 927 where `tenantMap: cfg.TenantMap` is set), add:

```go
    tenantMap:   cfg.TenantMap,
    tenantLabel: cfg.TenantLabel,
```

- [ ] **Step 4: Verify it compiles**

```bash
go build ./internal/proxy/
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add internal/proxy/proxy.go
git commit -m "feat(proxy): add TenantLabel field to Config and Proxy for label-based tenant routing"
```

---

## Task 2: Implement `injectTenantLabelFilter` helper and update `forwardTenantHeaders`

**Files:**
- Modify: `internal/proxy/multitenant.go`

- [ ] **Step 1: Add `injectTenantLabelFilter` function at the end of `multitenant.go`**

```go
// injectTenantLabelFilter appends a LogsQL stream-selector filter to the "query"
// or "q" param in params, scoping VL queries to logs with label=orgID.
// Returns a shallow clone of params with the injection applied, leaving the
// original unchanged. If neither "query" nor "q" is present, params is returned as-is.
func injectTenantLabelFilter(params url.Values, label, orgID string) url.Values {
	result := make(url.Values, len(params))
	for k, vs := range params {
		result[k] = append([]string(nil), vs...)
	}
	escaped := strings.ReplaceAll(orgID, `"`, `\"`)
	filter := ` {` + label + `="` + escaped + `"}`
	for _, key := range []string{"query", "q"} {
		if q := result.Get(key); q != "" {
			result.Set(key, q+filter)
			return result
		}
	}
	return result
}
```

- [ ] **Step 2: Update `forwardTenantHeaders` to skip numeric passthrough when `tenantLabel` is set**

In `forwardTenantHeaders` (around line 1166), after the tenantMap check and before the default-alias check, add:

```go
func (p *Proxy) forwardTenantHeaders(req *http.Request) {
	orgID := getOrgID(req.Context())
	if orgID == "" {
		return
	}

	p.configMu.RLock()
	tm := p.tenantMap
	p.configMu.RUnlock()

	if tm != nil {
		if mapping, ok := tm[orgID]; ok {
			req.Header.Set("AccountID", mapping.AccountID)
			req.Header.Set("ProjectID", mapping.ProjectID)
			return
		}
	}

	// If tenantLabel routing is active, tenant isolation is done via query-level
	// label filter injection in vlGetInner/vlPostInner — not via AccountID/ProjectID
	// headers. Skip all header-based routing when tenantLabel is set.
	if p.tenantLabel != "" {
		return
	}

	if isDefaultTenantAlias(orgID) {
		return
	}

	if orgID == "*" {
		if p.globalTenantAllowed() {
			return
		}
		return
	}

	if _, err := strconv.Atoi(orgID); err == nil {
		req.Header.Set("AccountID", orgID)
		req.Header.Set("ProjectID", "0")
	}
}
```

- [ ] **Step 3: Compile check**

```bash
go build ./internal/proxy/
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/proxy/multitenant.go
git commit -m "feat(proxy): add injectTenantLabelFilter; skip AccountID headers when tenantLabel set"
```

---

## Task 3: Inject label filter in `vlGetInner` and `vlPostInner`

**Files:**
- Modify: `internal/proxy/backend.go`

- [ ] **Step 1: Add injection to `vlGetInner`**

In `vlGetInner` (around line 530), after the function signature and before `u := *p.backend`, add the label filter injection:

```go
func (p *Proxy) vlGetInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	// Inject tenant label filter when configured and orgID is a non-default single tenant.
	if p.tenantLabel != "" {
		if orgID := getOrgID(ctx); orgID != "" && !isDefaultTenantAlias(orgID) && orgID != "*" {
			p.configMu.RLock()
			_, hasMapped := p.tenantMap[orgID]
			p.configMu.RUnlock()
			if !hasMapped {
				params = injectTenantLabelFilter(params, p.tenantLabel, orgID)
			}
		}
	}

	u := *p.backend
	// ... rest of existing function unchanged ...
```

- [ ] **Step 2: Add injection to `vlPostInner`**

In `vlPostInner` (around line 575), immediately after the function signature, add the same injection block:

```go
func (p *Proxy) vlPostInner(ctx context.Context, path string, params url.Values) (*http.Response, error) {
	// Inject tenant label filter when configured and orgID is a non-default single tenant.
	if p.tenantLabel != "" {
		if orgID := getOrgID(ctx); orgID != "" && !isDefaultTenantAlias(orgID) && orgID != "*" {
			p.configMu.RLock()
			_, hasMapped := p.tenantMap[orgID]
			p.configMu.RUnlock()
			if !hasMapped {
				params = injectTenantLabelFilter(params, p.tenantLabel, orgID)
			}
		}
	}

	u := *p.backend
	// ... rest of existing function unchanged ...
```

- [ ] **Step 3: Compile check**

```bash
go build ./internal/proxy/
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add internal/proxy/backend.go
git commit -m "feat(proxy): inject tenant label filter into VL query params in vlGetInner/vlPostInner"
```

---

## Task 4: Add `-tenant-label` CLI flag

**Files:**
- Modify: `cmd/proxy/main.go`

- [ ] **Step 1: Locate the tenant-map flag**

In `cmd/proxy/main.go`, search for `tenant-map` (around line 356). The new flag goes immediately after it.

- [ ] **Step 2: Add the flag**

```go
tenantMapJSON := fs.String("tenant-map", "", `JSON tenant mapping: {"org-name":{"account_id":"42","project_id":"0"}}. Maps Loki X-Scope-OrgID strings to VictoriaLogs numeric AccountID/ProjectID. Hot-reloadable via SIGHUP.`)
tenantLabel   := fs.String("tenant-label", "", `VL field name to use as tenant discriminator via label filter injection. When set, X-Scope-OrgID values are injected as {<tenant-label>="<orgID>"} into all VL queries instead of using AccountID/ProjectID headers. Use this when all data lives under VL default tenant (0:0) and is segregated by a label field (e.g. "org_id", "account_id"). Explicit tenant-map entries still take priority.`)
```

- [ ] **Step 3: Wire the flag into the proxy config**

Find where `TenantMap: tenantMap,` is set in `buildProxyConfig` or equivalent (around line 1457). Add:

```go
TenantMap:   tenantMap,
TenantLabel: *tenantLabel,
```

- [ ] **Step 4: Check env variable support**

In the `applyEnvOverrides` function or wherever env vars are read (around line 1125, look for `TENANT_MAP`), add:

```go
if v := getenv("TENANT_LABEL"); v != "" && *tenantLabel == "" {
    *tenantLabel = v
}
```

- [ ] **Step 5: Compile and verify flag appears in help**

```bash
go build ./cmd/proxy/ && ./proxy -help 2>&1 | grep tenant-label
```

Expected: `  -tenant-label string` line appears.

Remove the built binary:
```bash
rm -f ./proxy
```

- [ ] **Step 6: Commit**

```bash
git add cmd/proxy/main.go
git commit -m "feat(proxy): add -tenant-label flag for label-based VL tenant routing"
```

---

## Task 5: Unit tests for `injectTenantLabelFilter` and label routing behavior

**Files:**
- Create: `internal/proxy/multitenant_tenant_label_test.go`

- [ ] **Step 1: Create the test file**

```go
package proxy

import (
	"net/url"
	"testing"
)

func TestInjectTenantLabelFilter_InjectsIntoQueryParam(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="api-gateway"}`)
	params.Set("start", "2026-01-01T00:00:00Z")

	result := injectTenantLabelFilter(params, "org_id", "production")

	got := result.Get("query")
	want := `{app="api-gateway"} {org_id="production"}`
	if got != want {
		t.Fatalf("query param after injection:\n  got:  %q\n  want: %q", got, want)
	}
	// Original must be unmodified
	if params.Get("query") != `{app="api-gateway"}` {
		t.Fatal("injectTenantLabelFilter must not mutate the original params")
	}
}

func TestInjectTenantLabelFilter_InjectsIntoQParam(t *testing.T) {
	// /select/logsql/hits uses "q" instead of "query"
	params := url.Values{}
	params.Set("q", `{service_name="svc"}`)
	params.Set("step", "60")

	result := injectTenantLabelFilter(params, "account_id", "42")

	got := result.Get("q")
	want := `{service_name="svc"} {account_id="42"}`
	if got != want {
		t.Fatalf("q param after injection:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestInjectTenantLabelFilter_NoopWhenNoQueryParam(t *testing.T) {
	// Some endpoint calls have no query/q param (e.g., health check)
	params := url.Values{}
	params.Set("start", "2026-01-01T00:00:00Z")

	result := injectTenantLabelFilter(params, "org_id", "production")

	if result.Get("start") != "2026-01-01T00:00:00Z" {
		t.Fatal("non-query params must be preserved unchanged")
	}
	if result.Get("query") != "" || result.Get("q") != "" {
		t.Fatal("no query/q param must be added when none existed")
	}
}

func TestInjectTenantLabelFilter_EscapesDoubleQuotesInOrgID(t *testing.T) {
	params := url.Values{}
	params.Set("query", `{app="x"}`)

	result := injectTenantLabelFilter(params, "org", `tenant"with"quotes`)

	got := result.Get("query")
	want := `{app="x"} {org="tenant\"with\"quotes"}`
	if got != want {
		t.Fatalf("double-quote escaping:\n  got:  %q\n  want: %q", got, want)
	}
}

func TestInjectTenantLabelFilter_QueryParamTakesPriorityOverQParam(t *testing.T) {
	// When both "query" and "q" are set, inject into "query" only
	params := url.Values{}
	params.Set("query", `{app="a"}`)
	params.Set("q", `{app="b"}`)

	result := injectTenantLabelFilter(params, "org_id", "x")

	if result.Get("query") != `{app="a"} {org_id="x"}` {
		t.Fatalf("expected injection into query, got %q", result.Get("query"))
	}
	if result.Get("q") != `{app="b"}` {
		t.Fatalf("expected q param unchanged, got %q", result.Get("q"))
	}
}
```

- [ ] **Step 2: Run the unit tests**

```bash
go test -v ./internal/proxy/ -run 'TestInjectTenantLabelFilter'
```

Expected: All 5 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/proxy/multitenant_tenant_label_test.go
git commit -m "test(proxy): unit tests for injectTenantLabelFilter label routing"
```

---

## Task 6: Integration test using `httptest` — verify label filter is injected end-to-end

Test the full proxy dispatch: configure a proxy with `TenantLabel: "org_id"`, make a `query_range` request with `X-Scope-OrgID: production`, and verify the VL backend receives the label filter in the query.

**Files:**
- Modify: `internal/proxy/multitenant_tenant_label_test.go`

- [ ] **Step 1: Add the integration test**

```go
import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestTenantLabelRouting_InjectsLabelFilterIntoVLQuery(t *testing.T) {
	// Track the query VL received
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedQuery = r.FormValue("query")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("query")
		}
		// Return a minimal Loki-compatible empty streams response
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`)
	}))
	defer vl.Close()

	p := newTestProxy(t, withBackendURL(vl.URL), withTenantLabel("org_id"))

	req := httptest.NewRequest("GET",
		`/loki/api/v1/query_range?query={app="api-gateway"}&start=0&end=1000000000000&limit=10`,
		nil)
	req.Header.Set("X-Scope-OrgID", "production")

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(receivedQuery, `{org_id="production"}`) {
		t.Fatalf("expected VL query to contain {org_id=\"production\"}, got: %q", receivedQuery)
	}
}

func TestTenantLabelRouting_DefaultAliasSkipsLabelFilter(t *testing.T) {
	var receivedQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedQuery = r.FormValue("query")
		if receivedQuery == "" {
			receivedQuery = r.URL.Query().Get("query")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"streams","result":[],"stats":{}}}`)
	}))
	defer vl.Close()

	p := newTestProxy(t, withBackendURL(vl.URL), withTenantLabel("org_id"))

	for _, alias := range []string{"0", "fake", "default"} {
		receivedQuery = ""
		req := httptest.NewRequest("GET",
			`/loki/api/v1/query_range?query={app="x"}&start=0&end=1000000000000&limit=10`,
			nil)
		req.Header.Set("X-Scope-OrgID", alias)

		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)

		if strings.Contains(receivedQuery, "org_id") {
			t.Fatalf("alias %q must not inject label filter, got query: %q", alias, receivedQuery)
		}
	}
}
```

- [ ] **Step 2: Add `withTenantLabel` option to the test proxy builder**

Search in `internal/proxy/` for `newTestProxy` and `withBackendURL` to find the test helper pattern (likely in `cold_dispatch_test.go` or `compat_coverage_helpers_test.go`). Add:

```go
func withTenantLabel(label string) testProxyOption {
	return func(cfg *Config) {
		cfg.TenantLabel = label
	}
}
```

- [ ] **Step 3: Run the integration tests**

```bash
go test -v ./internal/proxy/ -run 'TestTenantLabelRouting'
```

Expected: Both tests PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/proxy/multitenant_tenant_label_test.go
git commit -m "test(proxy): integration tests for tenant label routing end-to-end dispatch"
```

---

## Task 7: Documentation and CHANGELOG

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md` (find the existing flags table/section)

- [ ] **Step 1: Add CHANGELOG entry under `[Unreleased]`**

```markdown
### Added

- **`-tenant-label` flag**: Label-based VL tenant routing as an alternative to `AccountID`/`ProjectID` header routing. When set to a VL field name (e.g., `-tenant-label=org_id`), the proxy injects `{org_id="<X-Scope-OrgID>"}` into every VL query instead of setting `AccountID`/`ProjectID` headers. Use this when all log data lives under VL's default tenant (AccountID=0, ProjectID=0) and is segregated by a label field. Explicit `--tenant-map` entries continue to use VL native tenancy and take priority. Default-tenant aliases (`0`, `fake`, `default`, `*`) bypass the filter. `TENANT_LABEL` environment variable also accepted.
```

- [ ] **Step 2: Add to README flags section**

Find the flags table in `README.md` (search for `tenant-map`). Add a row:

```markdown
| `-tenant-label` | `TENANT_LABEL` | `""` | VL field name for label-based tenant routing. When set, `X-Scope-OrgID` values are injected as `{<tenant-label>="<orgID>"}` into VL queries instead of `AccountID`/`ProjectID` headers. Use when all data is under VL default tenant (0:0) and segregated by a label. Explicit `tenant-map` entries take priority. |
```

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md README.md
git commit -m "docs: document -tenant-label flag for label-based VL tenant routing"
```

---

## Self-Review

**Spec coverage:**
- ✅ Map string OrgID → VL label filter: Tasks 2, 3
- ✅ Explicit tenantMap entries take priority: enforced in `forwardTenantHeaders` (checked first) and `vlGetInner` (`hasMapped` guard)
- ✅ Default aliases bypass filter: `isDefaultTenantAlias(orgID)` check in injection logic
- ✅ CLI flag: Task 4
- ✅ Env var: Task 4
- ✅ Unit tests for `injectTenantLabelFilter`: Task 5
- ✅ Integration tests for end-to-end routing: Task 6
- ✅ Docs + CHANGELOG: Task 7

**Constraints respected per user clarification:**
- Default VL tenant is 0/0 for all single-tenant setups — the label filter approach avoids requiring VL multi-tenancy to be enabled
- Multi-tenant fanout (`0|fake`) works transparently because each per-tenant sub-request carries a single OrgID

**No placeholders:** `injectTenantLabelFilter`, `withTenantLabel`, and all test cases are fully written.

**Type consistency:** `injectTenantLabelFilter(params url.Values, label, orgID string) url.Values` is used consistently in `backend.go` and the test file. `withTenantLabel` follows the existing `testProxyOption` functional options pattern.
