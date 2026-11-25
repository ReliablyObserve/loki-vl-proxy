package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// valuesVL stands in for VictoriaLogs: field_names lists every field, stream_field_values answers only a stream field
// (an empty list, not an error, for any other) and field_values answers every field, as the querying docs state and as
// v1.54.0 answers (verified against a real backend).
type valuesVL struct {
	mu       sync.Mutex
	calls    map[string]int
	stream   map[string]bool
	values   map[string][]string
	noStream bool // the backend has no stream metadata endpoints (404)
}

func (v *valuesVL) count(path string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls[path]
}

func (v *valuesVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		v.mu.Lock()
		v.calls[r.URL.Path]++
		v.mu.Unlock()
		field := r.URL.Query().Get("field")
		switch r.URL.Path {
		case "/select/logsql/field_names":
			var names []fieldHit
			for name := range v.values {
				names = append(names, fieldHit{Value: name, Hits: 10})
			}
			sort.Slice(names, func(i, j int) bool { return names[i].Value < names[j].Value })
			writeVLFieldNames(w, names)
		case "/select/logsql/field_values":
			// Like VictoriaLogs, the rows without the field count as the empty value.
			writeVLFieldValues(w, append(hits(v.values[field]), fieldHit{Value: "", Hits: 7}))
		case "/select/logsql/stream_field_values":
			if v.noStream {
				http.NotFound(w, r)
				return
			}
			if v.stream[field] {
				writeVLFieldValues(w, hits(v.values[field]))
			} else {
				writeVLFieldValues(w, nil)
			}
		default:
			writeVLFieldNames(w, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hits(values []string) []fieldHit {
	out := make([]fieldHit, len(values))
	for i, value := range values {
		out[i] = fieldHit{Value: value, Hits: 3}
	}
	return out
}

func newValuesVL() *valuesVL {
	return &valuesVL{
		calls:  map[string]int{},
		stream: map[string]bool{"job": true, "k8s.namespace.name": true},
		values: map[string][]string{
			"job": {"api", "web"}, "k8s.namespace.name": {"prod", "stage"},
			"level": {"error", "info"}, "k8s.pod.name": {"pod-a", "pod-b"}, "log.type": {"audit", "normal"},
		},
	}
}

func labelValuesOf(t *testing.T, p *Proxy, label string) []string {
	t.Helper()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/"+label+"/values?start=1703980800000000000&end=1704067200000000000", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", label, rr.Code, rr.Body.String())
	}
	var body struct {
		Data []string `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Data
}

// conformance: semantics/label-values-declared-non-stream-fields
func TestLabelValues_DeclaredNonStreamFieldListsItsValues(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		label         string
		want          []string
		streamCalls   int // stream_field_values reads of the first request
		fieldCalls    int // field_values reads of the first request
	}{
		// v1.50.0 and later: the column index answers every field in one field_values call.
		{"declared non-stream, v1.51", "v1.51.0", "level", []string{"error", "info"}, 0, 1},
		{"declared non-stream through its alias, v1.51", "v1.51.0", "k8s_pod_name", []string{"pod-a", "pod-b"}, 0, 1},
		{"declared stream field, v1.51", "v1.51.0", "job", []string{"api", "web"}, 0, 1},
		// Before v1.50.0: the stream endpoint first, and field_values only when it had no values.
		{"declared non-stream, v1.49", "v1.49.0", "level", []string{"error", "info"}, 1, 1},
		{"declared non-stream through its alias, v1.49", "v1.49.0", "k8s_pod_name", []string{"pod-a", "pod-b"}, 1, 1},
		{"declared stream field, v1.49", "v1.49.0", "job", []string{"api", "web"}, 1, 0},
		// An undeclared non-stream field keeps the empty, Loki-like answer, on both.
		{"undeclared non-stream, v1.51", "v1.51.0", "log_type", nil, 1, 0},
		{"undeclared non-stream, v1.49", "v1.49.0", "log_type", nil, 1, 0},
		// An undeclared stream field is unchanged.
		{"undeclared stream field, v1.51", "v1.51.0", "k8s_namespace_name", []string{"prod", "stage"}, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vl := newValuesVL()
			c := cache.New(60*time.Second, 1000)
			t.Cleanup(c.Close)
			p, err := New(Config{BackendURL: vl.server(t).URL, Cache: c, LogLevel: "error", LabelStyle: LabelStyleUnderscores,
				StreamFields: []string{"job"}, ExtraLabelFields: []string{"level", "k8s.pod.name"}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			p.storeBackendVersion(tc.version, tc.version)
			got := labelValuesOf(t, p, tc.label)
			if len(got) != len(tc.want) {
				t.Fatalf("values = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("values = %v, want %v", got, tc.want)
				}
			}
			// A read is two backend calls: the requested range and the millisecond at its end (Loki's end is inclusive).
			const callsPerRead = 2
			if s, f := vl.count("/select/logsql/stream_field_values"), vl.count("/select/logsql/field_values"); s != tc.streamCalls*callsPerRead || f != tc.fieldCalls*callsPerRead {
				t.Fatalf("stream_field_values calls %d, field_values calls %d; want %d and %d", s, f, tc.streamCalls*callsPerRead, tc.fieldCalls*callsPerRead)
			}
			// A repeat is answered from the response cache: no further backend call.
			before := vl.count("/select/logsql/stream_field_values") + vl.count("/select/logsql/field_values")
			again := labelValuesOf(t, p, tc.label)
			if len(again) != len(got) {
				t.Fatalf("repeat values = %v, want %v", again, got)
			}
			if len(got) > 0 {
				if after := vl.count("/select/logsql/stream_field_values") + vl.count("/select/logsql/field_values"); after != before {
					t.Fatalf("a repeat made %d more backend calls", after-before)
				}
			}
		})
	}
}

// conformance: semantics/label-values-declared-non-stream-fields
func TestLabelValues_BackendWithoutStreamEndpointsReadsFieldValues(t *testing.T) {
	vl := newValuesVL()
	vl.noStream = true
	p, err := New(Config{BackendURL: vl.server(t).URL, LogLevel: "error", LabelStyle: LabelStyleUnderscores, ExtraLabelFields: []string{"level"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.49.0", "v1.49.0")
	if got := labelValuesOf(t, p, "level"); len(got) != 2 {
		t.Fatalf("values = %v", got)
	}
}

// conformance: limits/label-values-response-cap
func TestLabelValues_DeclaredNonStreamFieldIsBoundedByTheResponseCap(t *testing.T) {
	vl := newValuesVL()
	many := make([]string, 400)
	for i := range many {
		many[i] = "pod-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + "-0123456789abcdef"
	}
	vl.values["k8s.pod.name"] = many
	p, err := New(Config{BackendURL: vl.server(t).URL, LogLevel: "error", LabelStyle: LabelStyleUnderscores,
		ExtraLabelFields: []string{"k8s.pod.name"}, ExecutionLimits: ExecutionLimitsConfig{LabelValuesMaxResponseBytes: 2048}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	p.storeBackendVersion("v1.51.0", "v1.51.0")
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/k8s_pod_name/values?start=1703980800000000000&end=1704067200000000000", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	// The cap answers with Loki's ResourceExhausted status (500), not the values read so far.
	if rr.Code != http.StatusInternalServerError || !strings.Contains(rr.Body.String(), "ResourceExhausted") {
		t.Fatalf("status %d body %s, want Loki's ResourceExhausted 500", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "pod-aa") {
		t.Fatal("a rejected response must not leak the values read before the cap")
	}
}
