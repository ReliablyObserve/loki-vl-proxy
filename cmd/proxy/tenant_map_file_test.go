package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

func TestLoadTenantMapFile_YAML(t *testing.T) {
	f := writeTenantMapTempFile(t, "tenant-map.yaml", `
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
	assertTenantMapping(t, m, "prod-team-eu_staging", "42", "3")
	assertTenantMapping(t, m, "prod-team-eu_prod", "42", "7")
	assertTenantMapping(t, m, "dev_default", "1", "1")
}

func TestLoadTenantMapFile_JSON(t *testing.T) {
	f := writeTenantMapTempFile(t, "tenant-map.json", `{
  "prod-team-eu_staging": {"account_id": "42", "project_id": "3"},
  "dev_default":          {"account_id": "1",  "project_id": "1"}
}`)
	m, err := loadTenantMapFile(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTenantMapping(t, m, "prod-team-eu_staging", "42", "3")
	assertTenantMapping(t, m, "dev_default", "1", "1")
}

func TestLoadTenantMapFile_MissingFile(t *testing.T) {
	_, err := loadTenantMapFile("/nonexistent/path/tenant-map.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadTenantMapFile_InvalidYAML(t *testing.T) {
	f := writeTenantMapTempFile(t, "bad.yaml", "not: valid: yaml: [")
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected parse error for invalid YAML, got nil")
	}
}

func TestValidateTenantMap_RejectsNonNumericAccountID(t *testing.T) {
	f := writeTenantMapTempFile(t, "bad-account.yaml", `
org-a:
  account_id: "not-a-number"
  project_id: "0"
`)
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected validation error for non-numeric account_id, got nil")
	}
}

func TestValidateTenantMap_RejectsNonNumericProjectID(t *testing.T) {
	f := writeTenantMapTempFile(t, "bad-project.yaml", `
org-a:
  account_id: "1"
  project_id: "admin"
`)
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected validation error for non-numeric project_id, got nil")
	}
}

func TestValidateTenantMap_RejectsEmptyAccountID(t *testing.T) {
	f := writeTenantMapTempFile(t, "empty-account.yaml", `
org-a:
  account_id: ""
  project_id: "0"
`)
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected validation error for empty account_id, got nil")
	}
}

func TestValidateTenantMap_RejectsNegativeID(t *testing.T) {
	f := writeTenantMapTempFile(t, "negative.yaml", `
org-a:
  account_id: "-1"
  project_id: "0"
`)
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected validation error for negative account_id, got nil")
	}
}

func TestValidateTenantMap_RejectsOverflowID(t *testing.T) {
	f := writeTenantMapTempFile(t, "overflow.yaml", `
org-a:
  account_id: "99999999999"
  project_id: "0"
`)
	_, err := loadTenantMapFile(f)
	if err == nil {
		t.Fatal("expected validation error for out-of-range account_id, got nil")
	}
}

func TestParseTenantMapJSON_RejectsNonNumericID(t *testing.T) {
	raw := `{"prod": {"account_id": "evil-value", "project_id": "0"}}`
	_, err := parseTenantMapJSON(raw)
	if err == nil {
		t.Fatal("expected validation error from parseTenantMapJSON, got nil")
	}
}

func TestValidateTenantMap_AcceptsZero(t *testing.T) {
	f := writeTenantMapTempFile(t, "zero.yaml", `
default:
  account_id: "0"
  project_id: "0"
`)
	m, err := loadTenantMapFile(f)
	if err != nil {
		t.Fatalf("zero IDs should be valid: %v", err)
	}
	assertTenantMapping(t, m, "default", "0", "0")
}

func TestWatchTenantMapFile_DetectsMtimeChange(t *testing.T) {
	f := writeTenantMapTempFile(t, "tenant-map.yaml", `
org-a:
  account_id: "1"
  project_id: "0"
`)
	reloaded := make(chan map[string]proxy.TenantMapping, 1)
	mock := &tenantFileTestProxy{onReload: func(m map[string]proxy.TenantMapping) { reloaded <- m }}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go watchTenantMapFile(ctx, f, 50*time.Millisecond, mock, tenantFileNoopLogger())

	// Give watcher one tick to record initial mtime
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
		assertTenantMapping(t, m, "org-a", "99", "5")
	case <-ctx.Done():
		t.Fatal("watcher did not detect file change within 5s")
	}
}

// --- helpers ---

func writeTenantMapTempFile(t *testing.T, name, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(f, []byte(content), 0600); err != nil {
		t.Fatalf("writeTenantMapTempFile: %v", err)
	}
	return f
}

func assertTenantMapping(t *testing.T, m map[string]proxy.TenantMapping, orgID, wantAcct, wantProj string) {
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

type tenantFileTestProxy struct {
	onReload func(map[string]proxy.TenantMapping)
}

func (m *tenantFileTestProxy) ReloadTenantMap(tm map[string]proxy.TenantMapping) {
	m.onReload(tm)
}
func (m *tenantFileTestProxy) ReloadFieldMappings(_ []proxy.FieldMapping) {}

func tenantFileNoopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
