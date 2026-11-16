package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// TestPatternsQueryWithoutSelectorAnswersLikeLoki: /patterns given a valid query with no stream selector answers
// the recorded Loki's empty success (rows r0291 and r1600, recorded from a Loki that serves data), with no
// VictoriaLogs call, on GET and POST; the other selector routes keep Loki's 400 for the same query, and a query
// Loki rejects keeps its rejection.
//
// conformance: semantics/patterns-query-without-selector, semantics/error-request-validation-parity
func TestPatternsQueryWithoutSelectorAnswersLikeLoki(t *testing.T) {
	recorded := map[string]lokiErrorAnswer{}
	for _, row := range readLokiErrorAnswers(t) {
		recorded[row.ID] = row
	}
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected backend call", http.StatusTeapot)
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	for _, id := range []string{"r0291", "r1600"} {
		row, ok := recorded[id]
		if !ok || row.Loki.Status != http.StatusOK {
			t.Fatalf("%s: recorded answer %+v, want Loki's 200", id, row.Loki)
		}
		u, err := url.Parse(row.Target)
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			var req *http.Request
			if method == http.MethodGet {
				req = httptest.NewRequest(method, row.Target, nil)
			} else {
				req = httptest.NewRequest(method, u.Path, strings.NewReader(u.RawQuery))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
			req.Header.Set("X-Scope-OrgID", "0")
			w := httptest.NewRecorder()
			mux := http.NewServeMux()
			p.RegisterRoutes(mux)
			mux.ServeHTTP(w, req)
			if w.Code != row.Loki.Status || w.Body.String() != row.Loki.Body || w.Header().Get("Content-Type") != row.Loki.ContentType {
				t.Errorf("%s %s: %d %q %q, want Loki's %d %q %q", id, method, w.Code, w.Header().Get("Content-Type"), w.Body, row.Loki.Status, row.Loki.ContentType, row.Loki.Body)
			}
		}
	}
	if calls.Load() != 0 {
		t.Errorf("%d VictoriaLogs calls for a query that names no stream", calls.Load())
	}
	for path, want := range map[string]int{
		"/loki/api/v1/labels?query=" + url.QueryEscape("1 + 1"):                             http.StatusBadRequest,
		"/loki/api/v1/index/stats?query=" + url.QueryEscape("vector(1)"):                    http.StatusBadRequest,
		"/loki/api/v1/patterns?query=" + url.QueryEscape("1 +"):                             http.StatusBadRequest,
		"/loki/api/v1/patterns?query=" + url.QueryEscape("topk(0, rate({app=\"a\"}[5m]))"):  http.StatusBadRequest,
		"/loki/api/v1/patterns?query=" + url.QueryEscape("sum(vector(1)) + 1") + "&step=60": http.StatusOK,
	} {
		w := doCompatProxyRequest(p, path, map[string]string{"X-Scope-OrgID": "0"})
		if w.Code != want {
			t.Errorf("%s: %d %s, want %d", path, w.Code, w.Body, want)
		}
	}
}

// hasStreamSelector reads a selector through every node that can hold one.
//
// conformance: semantics/patterns-query-without-selector
func TestHasStreamSelector(t *testing.T) {
	for q, want := range map[string]bool{
		`1 + 1`: false, `vector(1)`: false, `vector(1) * 2 + 3`: false, `sum(vector(1))`: false,
		`{a="b"}`: true, `{a="b"} | json`: true, `rate({a="b"}[1m])`: true, `vector(1) + rate({a="b"}[1m])`: true,
		`label_replace(vector(1), "x", "y", "z", ".*")`: false, `label_replace(rate({a="b"}[1m]), "x", "y", "z", ".*")`: true,
		`sum by (a) (count_over_time({a="b"}[1m])) > 1`: true,
	} {
		expr, err := logqlpkg.Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := hasStreamSelector(expr); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}
