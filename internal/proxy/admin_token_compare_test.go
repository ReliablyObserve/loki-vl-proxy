package proxy

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestAdminMiddleware_TokenMatrix pins the observable behaviour of the admin
// bearer-token check: only the exact token is accepted, everything else gets
// the same 401 body, and no token configured means no check.
func TestAdminMiddleware_TokenMatrix(t *testing.T) {
	const token = "s3cr3t-admin-token"
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	cases := []struct {
		name       string
		configured string
		header     map[string]string
		wantStatus int
	}{
		{"right bearer token", token, map[string]string{"Authorization": "Bearer " + token}, http.StatusNoContent},
		{"right X-Admin-Token", token, map[string]string{"X-Admin-Token": token}, http.StatusNoContent},
		{"wrong token same length", token, map[string]string{"Authorization": "Bearer " + strings.Repeat("x", len(token))}, http.StatusUnauthorized},
		{"wrong token same length differing last byte", token, map[string]string{"X-Admin-Token": token[:len(token)-1] + "X"}, http.StatusUnauthorized},
		{"wrong token shorter", token, map[string]string{"Authorization": "Bearer short"}, http.StatusUnauthorized},
		{"wrong token longer", token, map[string]string{"Authorization": "Bearer " + token + "-and-more"}, http.StatusUnauthorized},
		{"token is a prefix match only", token, map[string]string{"X-Admin-Token": token[:5]}, http.StatusUnauthorized},
		{"empty Authorization header", token, map[string]string{"Authorization": ""}, http.StatusUnauthorized},
		{"bearer with empty value", token, map[string]string{"Authorization": "Bearer "}, http.StatusUnauthorized},
		{"no credentials", token, nil, http.StatusUnauthorized},
		{"non-bearer scheme", token, map[string]string{"Authorization": "Basic " + token}, http.StatusUnauthorized},
		{"no token configured, no credentials", "", nil, http.StatusNoContent},
		{"no token configured, any credentials", "", map[string]string{"X-Admin-Token": "whatever"}, http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newAdminTestProxy(t, "http://unused", tc.configured)
			req := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			p.adminMiddleware(next).ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusUnauthorized && !strings.Contains(rec.Body.String(), "admin authentication required") {
				t.Fatalf("401 body = %q, want it to contain %q", rec.Body.String(), "admin authentication required")
			}
		})
	}
}

// TestSecretTokenComparisonsAreConstantTime fails when a secret token is
// compared with ==, != or strings.Compare/Equal in non-test code. Secret
// tokens must go through crypto/subtle.ConstantTimeCompare.
func TestSecretTokenComparisonsAreConstantTime(t *testing.T) {
	// Comparing against the empty string only asks "is a token configured" and is allowed.
	forbidden := regexp.MustCompile(`(?:[!=]=\s*p\.(?:adminAuthToken|peerAuthToken)\b)|(?:\bp\.(?:adminAuthToken|peerAuthToken)\s*[!=]=\s*[^"\s])|(?:strings\.(?:EqualFold|Compare)\([^)]*(?:adminAuthToken|peerAuthToken))|(?:bytes\.Equal\([^)]*(?:adminAuthToken|peerAuthToken))`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if m := forbidden.FindString(line); m != "" {
				t.Errorf("%s:%d compares a secret token without subtle.ConstantTimeCompare: %q", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}
