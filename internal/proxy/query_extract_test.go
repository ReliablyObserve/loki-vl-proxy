package proxy

import (
	"net/http"
	"net/url"
	"testing"
)

func TestExtractQuery_Valid(t *testing.T) {
	queries := []string{
		`{app="nginx"}`,
		`{app="nginx"} |= "error"`,
		`{app="nginx"} | json | level="error"`,
		`rate({app="api"}[5m])`,
		`sum by (app) (rate({app="api"}[5m]))`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			r := requestWithQuery(q)
			got, err := extractQuery(r)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != q {
				t.Errorf("got %q, want %q", got, q)
			}
		})
	}
}

func TestExtractQuery_Missing(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	_, err := extractQuery(r)
	if err == nil {
		t.Error("expected error for missing query, got nil")
	}
}

func TestExtractQuery_Invalid(t *testing.T) {
	bad := []string{
		`{app=`,
		`| json`,
		`not logql`,
		`{app="nginx"} | unknown_xyz`,
	}
	for _, q := range bad {
		t.Run(q, func(t *testing.T) {
			r := requestWithQuery(q)
			_, err := extractQuery(r)
			if err == nil {
				t.Errorf("expected error for %q, got nil", q)
			}
		})
	}
}

func requestWithQuery(q string) *http.Request {
	r, _ := http.NewRequest("GET", "/?query="+url.QueryEscape(q), nil)
	return r
}
