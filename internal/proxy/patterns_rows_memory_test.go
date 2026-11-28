package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// vlSortMemoryRefusal is VictoriaLogs' answer to a sort it refuses for memory, as a vlselect writes it.
func vlSortMemoryRefusal(limit int) string {
	return fmt.Sprintf("cannot execute query [service_name:=\"ingress\" | sort by (_time desc) limit %d]: cannot calculate [sort by (_time desc) limit %d], since it requires more than 1146MB of memory", limit, limit)
}

// patternRowsBackend answers the sampled pattern reads of /patterns: a read asking for more than allow rows is
// refused for memory with status, others are answered with wide rows; every read's limit and query are recorded.
type patternRowsBackend struct {
	mu      sync.Mutex
	allow   int
	status  int
	refusal func(limit int) string
	limits  []int
	queries []string
}

func (b *patternRowsBackend) handler(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	switch r.URL.Path {
	case "/health":
		_, _ = w.Write([]byte("ok"))
		return
	case "/metrics":
		_, _ = fmt.Fprintf(w, "vm_app_version{short_version=%q,version=%q} 1\n", "v1.54.0", "victoria-logs-20260101-000000-tags-v1.54.0-0-gabcdef0123")
		return
	}
	query := r.FormValue("query")
	if r.URL.Path != "/select/logsql/query" || strings.HasPrefix(query, "_time:[2000-01-01T00:00:00Z") {
		w.Header().Set("Content-Type", "application/x-ndjson")
		return
	}
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	b.mu.Lock()
	b.limits = append(b.limits, limit)
	b.queries = append(b.queries, query)
	b.mu.Unlock()
	if limit > b.allow {
		w.WriteHeader(b.status)
		_, _ = w.Write([]byte(b.refusal(limit)))
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	end := time.Unix(0, patternRowsEnd)
	for i := 0; i < min(limit, 600); i++ {
		row := map[string]string{
			"_time":      end.Add(-time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano),
			"_stream":    `{service_name="ingress"}`,
			"_stream_id": "s1",
			"_msg":       fmt.Sprintf("GET /orders/%d status=%d", i, []int{200, 404}[i%2]),
			"level":      "info",
		}
		line, _ := json.Marshal(row)
		_, _ = w.Write(append(line, '\n'))
	}
}

const patternRowsEnd = int64(1795780800) * 1e9

func newPatternRowsProxy(t *testing.T, b *patternRowsBackend) *Proxy {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(b.handler))
	t.Cleanup(backend.Close)
	// The sampled read only: no exact tier, no counts, no pattern state.
	p, err := New(Config{BackendURL: backend.URL, Cache: cache.New(time.Minute, 1000), LogLevel: "error", BackendMinVersion: "v1.40.0", BackendVersionCheckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p.patternsExactMaxLines, p.patternsCounts, p.patternState = 0, false, nil
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if err := p.ValidateBackendVersionCompatibility(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

func patternRowsRequest(p *Proxy) *httptest.ResponseRecorder {
	params := url.Values{
		"query": {`{service_name="ingress"}`},
		"start": {strconv.FormatInt(patternRowsEnd/1e9-900, 10)},
		"end":   {strconv.FormatInt(patternRowsEnd/1e9, 10)},
		"step":  {"10s"},
	}
	return doCompatProxyRequest(p, "/loki/api/v1/patterns?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
}

// The sampled pattern read keeps only the fields mining reads, so a refusal for memory is rarer; when VictoriaLogs
// still refuses one (#1821: "cannot calculate [sort by (_time) desc limit 20000], since it requires more than
// 1146MB of memory", answered 502 by a cluster's vlselect), the read is sent again with half the rows, at most three
// times and never below 1000 rows, each refusal counted. A refusal past that, and any other failure, is answered as
// before.
//
// conformance: drilldown/patterns-rows-fields, loki_api_v1_patterns
func TestPatternRowsMemoryRefusalReadsFewerRows(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusBadGateway} {
		t.Run(fmt.Sprintf("refused %d", status), func(t *testing.T) {
			b := &patternRowsBackend{allow: 5000, status: status, refusal: vlSortMemoryRefusal}
			p := newPatternRowsProxy(t, b)
			res := patternRowsRequest(p)
			if res.Code != http.StatusOK {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			var body struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil || len(body.Data) == 0 {
				t.Fatalf("no patterns: %s", res.Body)
			}
			if len(b.limits) != 3 || b.limits[1] != b.limits[0]/2 || b.limits[2] != b.limits[0]/4 || b.limits[2] > b.allow {
				t.Fatalf("reads asked for %v rows", b.limits)
			}
			for _, q := range b.queries {
				if !strings.HasSuffix(q, " | fields "+strings.Join(patternRowFields, ", ")) {
					t.Fatalf("read %q keeps every field", q)
				}
			}
			if got := internalOperationCount(p, "patterns_rows", "backend_memory_limit"); got != 2 {
				t.Fatalf("backend_memory_limit counted %v, want 2", got)
			}
			if got := internalOperationCount(p, "patterns_rows", "reduced"); got != 1 {
				t.Fatalf("reduced counted %v, want 1", got)
			}
		})
	}

	t.Run("refused at every size", func(t *testing.T) {
		b := &patternRowsBackend{allow: 0, status: http.StatusBadRequest, refusal: vlSortMemoryRefusal}
		p := newPatternRowsProxy(t, b)
		res := patternRowsRequest(p)
		if res.Code < 400 {
			t.Fatalf("%d %s: a read refused at every size answers its failure", res.Code, res.Body)
		}
		if strings.Contains(res.Body.String(), "1146MB") || strings.Contains(res.Body.String(), "sort by") {
			t.Fatalf("VictoriaLogs text reached the client: %s", res.Body)
		}
		if len(b.limits) != 1+patternRowsMemoryRetries || b.limits[len(b.limits)-1] < patternRowsMinRetryLimit {
			t.Fatalf("reads asked for %v rows", b.limits)
		}
	})

	t.Run("another failure is not retried", func(t *testing.T) {
		b := &patternRowsBackend{allow: 0, status: http.StatusBadRequest, refusal: func(int) string {
			return "cannot execute query [x]: -search.maxQueryDuration=60s exceeded"
		}}
		p := newPatternRowsProxy(t, b)
		_ = patternRowsRequest(p)
		if len(b.limits) != 1 {
			t.Fatalf("reads asked for %v rows", b.limits)
		}
		if got := internalOperationCount(p, "patterns_rows", "backend_memory_limit"); got != 0 {
			t.Fatalf("backend_memory_limit counted %v", got)
		}
	})
}

func TestIsVLMemoryLimitMessage(t *testing.T) {
	for raw, want := range map[string]bool{
		vlSortMemoryRefusal(20000): true,
		"cannot execute query [x]: cannot calculate [stats by (a) count()], since it requires more than 512MB of memory": true,
		"cannot execute query [x]: -search.maxQueryDuration=60s exceeded":                                                false,
		"since it requires more than 3 seconds":                                                                          false,
		"":                                                                                                               false,
	} {
		if got := isVLMemoryLimitMessage(raw); got != want {
			t.Fatalf("%q: %v, want %v", raw, got, want)
		}
	}
}
