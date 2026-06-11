package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Malformed drop/keep matchers (items that look like matchers but use operators
// Loki's drop/keep stages do not support, e.g. `!=~`) must be rejected with a
// Loki-style HTTP 400 parse error instead of being silently skipped. The typed
// LogQL AST validator (validateLogQLSyntax inside validateQuery) enforces this
// for both query_range and instant query; this test locks that contract.
func TestHardening_MalformedDropKeepMatcherRejected(t *testing.T) {
	p := newTestProxy(t, "http://unused")

	cases := []struct {
		name     string
		path     string
		handler  func(http.ResponseWriter, *http.Request)
		query    string
		wantCode int
	}{
		{
			name:     "query_range malformed keep matcher",
			path:     "/loki/api/v1/query_range",
			handler:  p.handleQueryRange,
			query:    `{app="api"} | keep method="GET", status!=~"5.."`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "query_range malformed drop matcher",
			path:     "/loki/api/v1/query_range",
			handler:  p.handleQueryRange,
			query:    `{app="api"} | drop level!=~"debug"`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "instant query malformed drop matcher",
			path:     "/loki/api/v1/query",
			handler:  p.handleQuery,
			query:    `count_over_time({app="api"} | drop level!=~"debug" [5m])`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", tc.path+"?query="+url.QueryEscape(tc.query), nil)
			tc.handler(w, r)

			if w.Code != tc.wantCode {
				t.Fatalf("expected %d for %q, got %d (body: %s)", tc.wantCode, tc.query, w.Code, w.Body.String())
			}
			body := w.Body.String()
			if !strings.Contains(body, "parse error") || !strings.Contains(body, "bad_data") {
				t.Errorf("expected Loki-style parse error envelope in body, got: %s", body)
			}
		})
	}

	// Valid drop/keep forms must NOT be rejected by validation.
	valid := []string{
		`{app="api"} | drop level="debug"`,
		`{app="api"} | drop level!="debug"`,
		`{app="api"} | keep status=~"5.."`,
		`{app="api"} | drop level, env`,
	}
	for _, q := range valid {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(q), nil)
		p.handleQueryRange(w, r)
		if w.Code == http.StatusBadRequest {
			t.Errorf("valid query %q rejected with 400 (body: %s)", q, w.Body.String())
		}
	}
}
