package querycorpus_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/proxy"
)

// The proxy side of the Loki metric differential: the real query and query_range handlers of a proxy built with
// proxy.New, in front of VictoriaLogs. Every request the handler sends reaches a front server that either forwards it
// to VictoriaLogs' own query handlers (the serve command of test/oracle/vl, holding the query's data under a tenant of
// its own) and records the answer (record mode), or answers it from the recorded answers (the fast path); a request
// that was not recorded is stale. The handler's whole metric path runs either way: the request it builds from the
// query and the plan's route, and every step after VictoriaLogs' answer (bucket relabelling, zero fill, series
// identity, binary operations, label_replace, offsets, the error layer and the response encoding).

// metricVLAnswer is VictoriaLogs' answer to one request.
type metricVLAnswer struct {
	Status int    `json:"s"`
	Body   string `json:"b"`
}

// metricFront is the VictoriaLogs the handler talks to.
type metricFront struct {
	version string
	// upstream and tenant are the query server and the data set's tenant (record mode); answers the recorded
	// answers by request key.
	upstream string
	tenant   uint32
	answers  map[string]metricVLAnswer

	mu       sync.Mutex
	recorded map[string]metricVLAnswer
	missing  []string
	sent     []string
}

// metricRequestKey is a request as the front server keys it: the path and every form parameter, sorted (the query
// string and a form body merged).
func metricRequestKey(r *http.Request, body []byte) string {
	form := url.Values{}
	for k, v := range r.URL.Query() {
		form[k] = append(form[k], v...)
	}
	if pf, err := url.ParseQuery(string(body)); err == nil && len(body) > 0 {
		for k, v := range pf {
			form[k] = append(form[k], v...)
		}
	}
	for k := range form {
		sort.Strings(form[k])
	}
	return r.URL.Path + "?" + form.Encode()
}

func (s *metricFront) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		_, _ = io.WriteString(w, "OK")
		return
	case "/metrics":
		_, _ = io.WriteString(w, `vm_app_version{version="victoria-logs-20260101-000000-tags-`+s.version+`-0-g0000000", short_version="`+s.version+`"} 1`+"\n")
		return
	}
	body, _ := io.ReadAll(r.Body)
	key := metricRequestKey(r, body)
	s.mu.Lock()
	s.sent = append(s.sent, key)
	s.mu.Unlock()
	var a metricVLAnswer
	if s.upstream != "" {
		a = s.forward(r, body)
		s.mu.Lock()
		s.recorded[key] = a
		s.mu.Unlock()
	} else {
		var ok bool
		if a, ok = s.answers[key]; !ok {
			s.mu.Lock()
			s.missing = append(s.missing, key)
			s.mu.Unlock()
			a = metricVLAnswer{Status: http.StatusServiceUnavailable, Body: "not recorded"}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(a.Status)
	_, _ = io.WriteString(w, a.Body)
}

// forward sends the request to VictoriaLogs under the data set's tenant, on a context of its own: a request the
// handler cancels (a probe whose sibling answered first) is still answered and recorded, so a later run that sends
// it finds its answer.
func (s *metricFront) forward(r *http.Request, body []byte) metricVLAnswer {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, s.upstream+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return metricVLAnswer{Status: http.StatusBadGateway, Body: err.Error()}
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	req.Header.Set("AccountID", strconv.FormatUint(uint64(s.tenant), 10))
	req.Header.Set("ProjectID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return metricVLAnswer{Status: http.StatusBadGateway, Body: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return metricVLAnswer{Status: resp.StatusCode, Body: string(b)}
}

// metricHandlerRun is one request through a fresh proxy.
type metricHandlerRun struct {
	status int
	body   string
	// sent are the request keys the handler sent; missing those the recorded answers lack.
	sent, missing []string
}

// runMetricHandler serves a request from a fresh proxy of the profile (every cache cold) in front of front.
func runMetricHandler(t *testing.T, prof diffProfile, mr metricRequest, front *metricFront) metricHandlerRun {
	t.Helper()
	srv := httptest.NewServer(front)
	defer srv.Close()
	p, err := proxy.New(proxy.Config{
		BackendURL:             srv.URL,
		Cache:                  cache.New(time.Minute, 1000),
		LogLevel:               "error",
		LabelStyle:             prof.style,
		MetadataFieldMode:      prof.mode,
		EmitStructuredMetadata: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_ = p.ValidateBackendVersionCompatibility(context.Background())
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	path := "/loki/api/v1/query_range"
	if mr.instant {
		path = "/loki/api/v1/query"
	}
	req := httptest.NewRequest(http.MethodGet, path+"?"+mr.form.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	rec := httptest.NewRecorder()
	front.mu.Lock()
	front.sent, front.missing = nil, nil
	front.mu.Unlock()
	mux.ServeHTTP(rec, req)
	front.mu.Lock()
	defer front.mu.Unlock()
	return metricHandlerRun{status: rec.Code, body: rec.Body.String(), sent: uniqueSorted(front.sent), missing: uniqueSorted(front.missing)}
}

// uniqueSorted is the set of lines, sorted.
func uniqueSorted(lines []string) []string {
	out := append([]string(nil), lines...)
	sort.Strings(out)
	n := 0
	for i, l := range out {
		if i == 0 || l != out[n-1] {
			out[n] = l
			n++
		}
	}
	return out[:n]
}

// sentQueries are the LogsQL texts of the stats requests among the sent request keys.
func sentStatsQueries(sent []string) []string {
	var out []string
	for _, k := range sent {
		path, raw, _ := strings.Cut(k, "?")
		if !strings.HasPrefix(path, "/select/logsql/stats_query") && path != "/select/logsql/query" {
			continue
		}
		form, err := url.ParseQuery(raw)
		if err == nil {
			out = append(out, form.Get("query"))
		}
	}
	return out
}
