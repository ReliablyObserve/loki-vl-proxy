package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func newOpt6Proxy(t testing.TB, backendURL string, streamFields []string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backendURL,
		Cache:                  cache.New(0, 0),
		LogLevel:               "error",
		EmitStructuredMetadata: true,
		MetadataFieldMode:      MetadataFieldModeHybrid,
		LabelStyle:             LabelStylePassthrough,
		StreamFields:           streamFields,
	})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

func TestOPT6_DeclaredStreamFieldsDoNotHideJSONParserErrors(t *testing.T) {
	var hitsCalled, queryCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			hitsCalled = true
			fmt.Fprint(w, `{"hits":[]}`)
		case "/select/logsql/query":
			queryCalled = true
			fmt.Fprintln(w, `{"_time":"2025-05-01T12:14:20Z","_msg":"not JSON","_stream":"{app=\"worker\",namespace=\"prod\"}"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})
	params := url.Values{"query": {`rate({namespace="prod"}|json[5m])`}, "start": {"2025-05-01T12:15:00Z"}, "end": {"2025-05-01T12:16:00Z"}, "step": {"60"}}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil))
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "JSONParserErr") {
		t.Fatalf("parser error lost: %d %s", rec.Code, rec.Body)
	}
	if !queryCalled || hitsCalled {
		t.Fatalf("parser state bypassed: raw=%v hits=%v", queryCalled, hitsCalled)
	}
}

func TestOPT6_SlidingWindowGate_FallsBackWithoutDeclaredFields(t *testing.T) {
	queryCalled := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/hits":
			t.Error("hits must not be called when declaredLabelFields is nil")
			w.WriteHeader(500)
		case "/select/logsql/query":
			queryCalled = true
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, "") // empty — no entries
		default:
			http.Error(w, "unexpected: "+r.URL.Path, 404)
		}
	}))
	defer srv.Close()

	// No StreamFields → declaredLabelFields is nil → fallback to full-fetch.
	p := newOpt6Proxy(t, srv.URL, nil)

	req := httptest.NewRequest("GET", "/loki/api/v1/query_range?"+url.Values{
		"query": {`rate({namespace="prod"} | json [5m])`},
		"start": {"1746100000000000000"},
		"end":   {"1746103600000000000"},
		"step":  {"60000000000"},
	}.Encode(), nil)
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)

	if !queryCalled {
		t.Error("expected fallback to full-fetch path when declaredLabelFields is nil")
	}
}

func TestOPT6_FetchBareParserMetricSeriesViaHits_Integration(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t0 := now.Add(-3 * time.Minute)

	makeTS := func(offset time.Duration) string {
		return fmt.Sprintf("%d", t0.Add(offset).UnixNano())
	}

	hitsResp := map[string]interface{}{
		"hits": []map[string]interface{}{
			{
				"fields":     map[string]string{"app": "api-gw"},
				"timestamps": []string{makeTS(0), makeTS(time.Minute), makeTS(2 * time.Minute)},
				"values":     []int{10, 20, 30},
			},
			{
				"fields":     map[string]string{"app": "auth"},
				"timestamps": []string{makeTS(0), makeTS(time.Minute)},
				"values":     []int{5, 5},
			},
		},
	}
	hitsJSON, _ := json.Marshal(hitsResp)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/hits" {
			http.Error(w, "unexpected: "+r.URL.Path, 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(hitsJSON)
	}))
	defer srv.Close()

	p := newOpt6Proxy(t, srv.URL, []string{"app", "namespace"})

	spec := bareParserMetricCompatSpec{
		funcName:        "count_over_time",
		baseQuery:       `{namespace="prod"} | json`,
		rangeWindow:     3 * time.Minute,
		rangeWindowExpr: "3m",
	}

	evalStart := t0.UnixNano()
	evalEnd := now.UnixNano()
	stepNs := int64(60) * int64(time.Second)

	series, ok, err := p.fetchBareParserMetricSeriesViaHits(t.Context(), spec, evalStart, evalEnd, stepNs)
	if !ok || err != nil {
		t.Fatalf("fetchBareParserMetricSeriesViaHits: ok=%v err=%v", ok, err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series (api-gw, auth), got %d", len(series))
	}
	for _, s := range series {
		if len(s.Samples) == 0 {
			t.Errorf("series %v has no samples", s.Metric)
		}
	}
}
