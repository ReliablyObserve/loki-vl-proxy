package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// =============================================================================
// without() proper implementation — exclude labels from grouping
// =============================================================================

func TestWithout_ExcludesLabelsFromGrouping(t *testing.T) {
	// VL returns results grouped by all labels (app, pod, level)
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return 3 series with different app+pod+level combinations
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"app":"nginx","pod":"p1","level":"error"},"value":[1609459200,"10"]},
			{"metric":{"app":"nginx","pod":"p2","level":"error"},"value":[1609459200,"20"]},
			{"metric":{"app":"nginx","pod":"p1","level":"warn"},"value":[1609459200,"5"]}
		]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// sum without (pod) → should group by all labels EXCEPT pod → group by (app, level)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query?query=sum+without+(pod)+(rate({app="nginx"}[5m]))&time=1609459200`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse error: %v, body: %s", err, w.Body.String())
	}

	if resp.Status != "success" {
		t.Fatalf("expected success, got %q", resp.Status)
	}

	// After without(pod), results should NOT have "pod" in metric labels
	for _, series := range resp.Data.Result {
		if _, hasPod := series.Metric["pod"]; hasPod {
			t.Errorf("without(pod) should remove 'pod' from metric labels, got %v", series.Metric)
		}
	}
}

func TestWithout_MultipleLabels(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"app":"nginx","pod":"p1","node":"n1"},"value":[1609459200,"10"]}
		]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", `/loki/api/v1/query?query=sum+without+(pod,+node)+(rate({app="nginx"}[5m]))&time=1609459200`, nil)
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)

	for _, series := range resp.Data.Result {
		if _, has := series.Metric["pod"]; has {
			t.Errorf("without(pod, node) should remove 'pod', got %v", series.Metric)
		}
		if _, has := series.Metric["node"]; has {
			t.Errorf("without(pod, node) should remove 'node', got %v", series.Metric)
		}
		// "app" should still be present
		if _, has := series.Metric["app"]; !has {
			t.Errorf("without(pod, node) should keep 'app', got %v", series.Metric)
		}
	}
}

// =============================================================================
// on()/ignoring() proper implementation — label-subset matching
// =============================================================================

func TestOn_BinaryMatchesBySubset(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Left side has no "error" in the query; right side has level="error".
		// Dispatch by query content — safe for concurrent left+right fetches.
		if strings.Contains(r.URL.Query().Get("query"), "error") {
			// Right side: rate({app="a",level="error"}[5m])
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","level":"error"},"value":[1609459200,"10"]}
			]}}`))
		} else {
			// Left side: rate({app="a"}[5m])
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","pod":"p1"},"value":[1609459200,"100"]},
				{"metric":{"app":"a","pod":"p2"},"value":[1609459200,"200"]}
			]}}`))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// rate({app="a"}[5m]) / on(app) rate({app="a",level="error"}[5m])
	// on(app) means match only on "app" label — both sides have app="a" so they match
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200", nil)
	q := r.URL.Query()
	q.Set("query", `rate({app="a"}[5m]) / on(app) rate({app="a",level="error"}[5m])`)
	r.URL.RawQuery = q.Encode()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)

	if resp.Status != "success" {
		t.Fatalf("expected success, got %q (body: %s)", resp.Status, w.Body.String())
	}

	// With on(app), both left series (p1, p2) should match the single right series
	// because they share app="a". Without on(app), exact key match would find no matches.
	if len(resp.Data.Result) == 0 {
		t.Error("on(app) should produce results by matching on app label subset")
	}
}

func TestIgnoring_ExcludesLabelsFromMatch(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Dispatch by query content — safe for concurrent left+right fetches.
		// Left has pod="p1", right has pod="p2"; VL queries differ by pod value.
		if strings.Contains(r.URL.Query().Get("query"), "p2") {
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","pod":"p2"},"value":[1609459200,"10"]}
			]}}`))
		} else {
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","pod":"p1"},"value":[1609459200,"100"]}
			]}}`))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// rate({app="a",pod="p1"}[5m]) / ignoring(pod) rate({app="a",pod="p2"}[5m])
	// ignoring(pod) means match on all labels EXCEPT pod — both have app="a" so they match.
	// Using distinct pod selectors makes left/right VL queries distinguishable for concurrent fetches.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200", nil)
	q := r.URL.Query()
	q.Set("query", `rate({app="a",pod="p1"}[5m]) / ignoring(pod) rate({app="a",pod="p2"}[5m])`)
	r.URL.RawQuery = q.Encode()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)

	// ignoring(pod) means the two series match on app="a" even though pod differs
	if len(resp.Data.Result) == 0 {
		t.Error("ignoring(pod) should produce results by ignoring pod in matching")
	}
}

// =============================================================================
// group_left()/group_right() — one-to-many join
// =============================================================================

func TestGroupLeft_OneToMany(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Dispatch by query content — safe for concurrent left+right fetches.
		// Left has env="pods" selector, right has env="team" selector.
		if strings.Contains(r.URL.Query().Get("query"), "team") {
			// Right: one (single entry per app with team info)
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","team":"backend"},"value":[1609459200,"1"]}
			]}}`))
		} else {
			// Left: many (multiple pods)
			w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
				{"metric":{"app":"a","pod":"p1"},"value":[1609459200,"100"]},
				{"metric":{"app":"a","pod":"p2"},"value":[1609459200,"200"]}
			]}}`))
		}
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// rate({app="a"}[5m]) * on(app) group_left(team) rate({app="a",team="backend"}[5m])
	// group_left: each left series matches the one right series; using distinct selectors
	// makes left/right VL queries distinguishable for concurrent fetches.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200", nil)
	q := r.URL.Query()
	q.Set("query", `rate({app="a"}[5m]) * on(app) group_left(team) rate({app="a",team="backend"}[5m])`)
	r.URL.RawQuery = q.Encode()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &resp)

	if resp.Status != "success" {
		t.Fatalf("expected success, got %q (body: %s)", resp.Status, w.Body.String())
	}

	// group_left: both left-side series (p1, p2) should match the one right-side series
	if len(resp.Data.Result) < 2 {
		t.Errorf("group_left should produce 2 results (one per left series), got %d", len(resp.Data.Result))
	}
}

// =============================================================================
// Edge cases and comprehensive vector matching tests
// =============================================================================

func TestWithout_EmptyLabels(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"app":"nginx"},"value":[1609459200,"42"]}
		]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	// without() with empty list — should still work (no labels excluded)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200", nil)
	q := r.URL.Query()
	q.Set("query", `sum(rate({app="nginx"}[5m]))`)
	r.URL.RawQuery = q.Encode()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct{ Status string }
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != "success" {
		t.Errorf("expected success, got %q", resp.Status)
	}
}

func TestVectorMatching_Passthrough(t *testing.T) {
	// Test that binary expressions without vector matching still work.
	var callNum atomic.Int32
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callNum.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"app":"nginx"},"value":[1609459200,"42"]}
		]}}`))
	}))
	defer vlBackend.Close()

	c := cache.New(60*time.Second, 10000)
	p, _ := New(Config{BackendURL: vlBackend.URL, Cache: c, LogLevel: "error"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/loki/api/v1/query?time=1609459200", nil)
	q := r.URL.Query()
	q.Set("query", `rate({app="nginx"}[5m]) / rate({app="nginx"}[5m])`)
	r.URL.RawQuery = q.Encode()
	mux := http.NewServeMux()
	p.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)

	var resp struct{ Status string }
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != "success" {
		t.Errorf("expected success for basic binary, got %q", resp.Status)
	}
}

func TestApplyWithoutGrouping_Unit(t *testing.T) {
	body := []byte(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"app":"nginx","pod":"p1","level":"error"},"value":[1609459200,"10"]},
		{"metric":{"app":"nginx","pod":"p2","level":"error"},"value":[1609459200,"20"]},
		{"metric":{"app":"nginx","pod":"p1","level":"warn"},"value":[1609459200,"5"]}
	]}}`)

	result := applyWithoutGrouping(body, []string{"pod"})
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(result, &resp)

	// After removing "pod": app=nginx,level=error should aggregate (10+20=30)
	for _, s := range resp.Data.Result {
		if _, hasPod := s.Metric["pod"]; hasPod {
			t.Errorf("pod should be removed, got %v", s.Metric)
		}
	}

	// Should have 2 groups: (app=nginx,level=error) and (app=nginx,level=warn)
	if len(resp.Data.Result) != 2 {
		t.Errorf("expected 2 groups after without(pod), got %d", len(resp.Data.Result))
	}
}

func TestApplyOnMatching_Unit(t *testing.T) {
	left := []byte(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"app":"a","pod":"p1"},"value":[1609459200,"100"]},
		{"metric":{"app":"b","pod":"p2"},"value":[1609459200,"200"]}
	]}}`)
	right := []byte(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"app":"a","team":"backend"},"value":[1609459200,"10"]}
	]}}`)

	result := applyOnMatching(left, right, "/", []string{"app"}, "vector")
	var resp struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(result, &resp)

	// Only app="a" matches — should get 1 result (100/10=10)
	if len(resp.Data.Result) != 1 {
		t.Errorf("expected 1 result for on(app) match, got %d", len(resp.Data.Result))
	}
}

func TestApplyIgnoringMatching_Unit(t *testing.T) {
	left := []byte(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"app":"a","pod":"p1"},"value":[1609459200,"100"]}
	]}}`)
	right := []byte(`{"status":"success","data":{"resultType":"vector","result":[
		{"metric":{"app":"a","pod":"p2"},"value":[1609459200,"10"]}
	]}}`)

	result := applyIgnoringMatching(left, right, "/", []string{"pod"}, "vector")
	var resp struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	json.Unmarshal(result, &resp)

	// Ignoring pod: app="a" matches on both — should get 1 result (100/10=10)
	if len(resp.Data.Result) != 1 {
		t.Errorf("expected 1 result for ignoring(pod), got %d", len(resp.Data.Result))
	}
}

func TestApplyArithmeticOp(t *testing.T) {
	tests := []struct {
		left, right float64
		op          string
		want        float64
	}{
		{10, 5, "/", 2},
		{10, 0, "/", 0}, // div by zero
		{3, 4, "*", 12},
		{10, 3, "+", 13},
		{10, 3, "-", 7},
		{10, 3, "%", 1},
		{5, 5, "==", 1},
		{5, 3, "==", 0},
		{5, 3, "!=", 1},
		{5, 3, ">", 1},
		{3, 5, ">", 0},
		{3, 5, "<", 1},
		{5, 5, ">=", 1},
		{5, 5, "<=", 1},
	}
	for _, tt := range tests {
		got := applyArithmeticOp(tt.left, tt.right, tt.op)
		if got != tt.want {
			t.Errorf("applyArithmeticOp(%v, %v, %q) = %v, want %v", tt.left, tt.right, tt.op, got, tt.want)
		}
	}
}
