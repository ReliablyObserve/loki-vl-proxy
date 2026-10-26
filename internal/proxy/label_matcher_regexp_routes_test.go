package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// Every route that takes a LogQL selector sends its regexp matchers to
// VictoriaLogs anchored to the whole value, as Loki compiles them (^(?s:re)$,
// vendor/github.com/prometheus/prometheus/model/labels/regexp.go:76): a
// Grafana multi-value variable `{app=~"api|web"}` must not reach VictoriaLogs'
// substring regexp as `app:~"api|web"`, which also selects api-gateway and
// webhook. The answers the anchored filters select are pinned by
// TestLabelRegexpMatchesWhatLokiMatches (internal/translator).
//
// conformance: profiles/stream-selector-regexp-anchoring
func TestLabelMatcherRegexpAnchoredOnEveryRoute(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if q := r.Form.Get("query"); q != "" {
			mu.Lock()
			queries = append(queries, r.URL.Path+" "+q)
			mu.Unlock()
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/query"):
			w.Header().Set("Content-Type", "application/x-ndjson")
		case strings.Contains(r.URL.Path, "stats_query"):
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		case strings.HasSuffix(r.URL.Path, "/hits"):
			_, _ = w.Write([]byte(`{"hits":[]}`))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)

	const selector = `{app=~"api|web", env=~"prod"}`
	const anchored = `app:~"^(?s:api|web)$"`
	const exact = `env:="prod"`
	window := "&start=1700000000000000000&end=1700000300000000000"
	q := url.QueryEscape(selector)
	for _, route := range []string{
		"/loki/api/v1/query_range?query=" + q + window + "&limit=10",
		"/loki/api/v1/query_range?query=" + url.QueryEscape(`sum(count_over_time(`+selector+`[1m]))`) + window + "&step=60",
		"/loki/api/v1/query?query=" + url.QueryEscape(`sum(count_over_time(`+selector+`[5m]))`) + "&time=1700000300",
		"/loki/api/v1/labels?query=" + q + window,
		"/loki/api/v1/label/app/values?query=" + q + window,
		"/loki/api/v1/series?match[]=" + q + window,
		"/loki/api/v1/index/volume?query=" + q + window,
		"/loki/api/v1/index/volume_range?query=" + q + window + "&step=60",
		"/loki/api/v1/index/stats?query=" + q + window,
		"/loki/api/v1/detected_labels?query=" + q + window,
		"/loki/api/v1/detected_fields?query=" + q + window,
		"/loki/api/v1/detected_field/level/values?query=" + q + window,
		"/loki/api/v1/patterns?query=" + q + window,
	} {
		t.Run(route[:strings.Index(route, "?")], func(t *testing.T) {
			mu.Lock()
			queries = nil
			mu.Unlock()
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route, nil))
			mu.Lock()
			defer mu.Unlock()
			if len(queries) == 0 {
				t.Fatalf("no VictoriaLogs query sent (status %d: %s)", w.Code, w.Body.String())
			}
			for _, sent := range queries {
				if strings.Contains(sent, `app:~"api|web"`) || strings.Contains(sent, `env:~"prod"`) {
					t.Errorf("unanchored matcher reached VictoriaLogs: %s", sent)
				}
				if !strings.Contains(sent, anchored) || !strings.Contains(sent, exact) {
					t.Errorf("VictoriaLogs query without the anchored matchers %s %s: %s", anchored, exact, sent)
				}
			}
		})
	}

	// /tail sends the query its handler translates with translateLogResponseQuery.
	tail, err := p.translateLogResponseQuery(context.Background(), selector)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tail, anchored) || !strings.Contains(tail, exact) {
		t.Errorf("tail query %s lacks %s %s", tail, anchored, exact)
	}
}
