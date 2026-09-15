package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHardening_BackendBudgetBoundsFanout(t *testing.T) {
	var active, peak atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		fmt.Fprint(w, `{"values":[{"value":"app","hits":1}]}`)
	}))
	defer backend.Close()
	p := newTestProxyWithOptions(t, withBackendURL(backend.URL), func(c *Config) { c.MaxConcurrent = 1 })
	w := doCompatProxyRequest(p, "/loki/api/v1/labels", map[string]string{"X-Scope-OrgID": "1|2|3|4"})
	if w.Code != 200 || peak.Load() != 1 {
		t.Fatalf("status=%d peak=%d body=%s", w.Code, peak.Load(), w.Body)
	}
}

func TestHardening_BackendBudgetCoversBodyAndCanceledWait(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") }))
	defer backend.Close()
	p := newTestProxyWithOptions(t, withBackendURL(backend.URL), func(c *Config) { c.MaxConcurrent = 1 })
	r := httptest.NewRequest("GET", backend.URL, nil)
	r.RequestURI = ""
	first, err := p.doBackendRequest(r, p.client)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := p.doBackendRequest(r.WithContext(ctx), p.client); err == nil {
		t.Fatal("permit released before body consumption")
	}
	_, _ = io.Copy(io.Discard, first.Body)
	_ = first.Body.Close()
	last, err := p.doBackendRequest(r, p.client)
	if err != nil {
		t.Fatal(err)
	}
	_ = last.Body.Close()
}

func TestHardening_TemplateBudgetsAndNormalFormatting(t *testing.T) {
	for _, tmpl := range []string{`{{printf "%1048576s" "x"}}`, `{{printf "%*s" 1048576 "x"}}`, `{{Replace ._line "x" "large expansion" 100000}}`, `{{define "r"}}{{template "r" .}}{{end}}{{template "r" .}}`} {
		streams := []map[string]any{{"stream": map[string]string{"app": "api"}, "values": [][]string{{"1", strings.Repeat("x", 65536)}}}}
		if err := applyLineFormatTemplate(streams, tmpl); err == nil {
			t.Fatalf("unbounded template accepted: %s", tmpl)
		}
	}
	streams := []map[string]any{{"stream": map[string]string{"app": "api"}, "values": [][]string{{"1", "hello"}}}}
	if err := applyLineFormatTemplate(streams, `{{printf "20260101 %8s" (.app | ToUpper)}}`); err != nil {
		t.Fatal(err)
	}
	if got := streams[0]["values"].([][]string)[0][1]; got != "20260101      API" {
		t.Fatalf("normal printf changed: %q", got)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := applyLineFormatTemplateWithContext(ctx, streams, `{{.app}}`); err == nil {
		t.Fatal("formatting ignored cancellation")
	}
}
