package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are the Fields requests reported by Grafana's query splitter. The fake
// backend exercises the real HTTP route and translated stats response handling;
// it does not establish actual VictoriaLogs parser parity or container RSS.
var longrangeFieldShapes = []struct{ field, query string }{
	{"trace_id", `sum by (trace_id) (count_over_time({env="production"} | trace_id!="" [1m]))`},
	{"span_id", `sum by (span_id) (count_over_time({env="production"} | span_id!="" [1m]))`},
	{"version", `sum by (version) (count_over_time({env="production"} | version!="" [1m]))`},
	{"request_id", `sum by (request_id) (count_over_time({env="production"} | json request_id="[\"request_id\"]" | drop __error__, __error_details__ | request_id!="" [1m]))`},
	{"user_id", `sum by (user_id) (count_over_time({env="production"} | json user_id="[\"user_id\"]" | drop __error__, __error_details__ | user_id!="" [1m]))`},
	{"total", `sum by (total) (count_over_time({env="production"} | json total="[\"total\"]" | drop __error__, __error_details__ | total!="" [1m]))`},
}

// Duration conversion and JSON parsing have their own differential suites. This
// fixture supplies already evaluated VL stats, checking the reported ttl query's
// long-range HTTP/axis handling without adding another fake parser.
func TestDrilldownLongRangeFields_DurationAverage(t *testing.T) {
	const query = `avg_over_time({env="production"} | logfmt | ttl!="" | unwrap duration(ttl) | __error__="" [1m]) by ()`
	for _, span := range []time.Duration{12 * time.Hour, 48 * time.Hour} {
		t.Run(span.String(), func(t *testing.T) {
			start := time.Unix(1793413140, 123*int64(time.Millisecond)).UTC()
			end := start.Add(span + 17431*time.Millisecond)
			type sample struct {
				ts    int64
				value float64
			}
			var samples []sample
			for _, offset := range []time.Duration{-time.Minute, 0, time.Minute, span / 2, span} {
				samples = append(samples, sample{start.Add(offset).UnixNano(), 0.25}, sample{start.Add(offset - 10*time.Second).UnixNano(), 1.75})
			}
			var seen atomic.Int64
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					_, _ = w.Write([]byte("ok"))
					return
				}
				if r.URL.Path != "/select/logsql/stats_query_range" {
					http.Error(w, "unexpected backend endpoint", http.StatusNotFound)
					return
				}
				if err := r.ParseForm(); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				seen.Add(1)
				if q := r.Form.Get("query"); !strings.Contains(q, "__lvp_d/1000000000") || !strings.Contains(q, "avg(") {
					t.Errorf("duration-average translation missing: %s", q)
				}
				lo, hi, step, offset, ok := slidingBucketParams(t, r)
				if !ok {
					return
				}
				type aggregate struct {
					sum   float64
					count int
				}
				buckets := map[int64]aggregate{}
				for _, sample := range samples {
					if sample.ts < lo || sample.ts >= hi {
						continue
					}
					ts := vlTruncate(sample.ts, step, offset)
					a := buckets[ts]
					a.sum += sample.value
					a.count++
					buckets[ts] = a
				}
				var times []int64
				for ts := range buckets {
					times = append(times, ts)
				}
				sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
				var values [][2]any
				for _, ts := range times {
					a := buckets[ts]
					values = append(values, [2]any{float64(ts) / 1e9, strconv.FormatFloat(a.sum/float64(a.count), 'f', -1, 64)})
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{"__name__": "c"}, "values": values}}}})
			}))
			t.Cleanup(backend.Close)
			_, mux := newTenantLimitsProxy(t, backend.URL, Config{})
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, longrangeFieldRequest(t.Context(), query, start, end))
			if rec.Code != http.StatusOK {
				t.Fatalf("ttl Fields: %d %s", rec.Code, rec.Body.String())
			}
			want := map[string]map[int64]string{"{}": {}}
			for at := start; !at.After(end); at = at.Add(time.Minute) {
				var sum float64
				var count int
				for _, sample := range samples {
					if sample.ts > at.Add(-time.Minute).UnixNano() && sample.ts <= at.UnixNano() {
						sum += sample.value
						count++
					}
				}
				if count > 0 {
					want["{}"][at.Unix()] = strconv.FormatFloat(sum/float64(count), 'f', -1, 64)
				}
			}
			got := decodeTumblingSeries(t, query, rec.Body.Bytes())
			assertLongrangeFieldAxis(t, rec.Body.Bytes(), start, end)
			if len(got) != 1 {
				t.Fatalf("got %d ttl series, want 1", len(got))
			}
			assertTumblingEqual(t, query, want, got)
			if seen.Load() == 0 {
				t.Fatal("ttl request never reached VL stats")
			}
		})
	}
}

func longrangeFieldFixture(start time.Time, span time.Duration) []tumblingLine {
	var lines []tumblingLine
	// Sparse, but enough distinct series and points to exceed the 64 KiB pooled
	// response size. Version stays low-cardinality in the concurrent mix.
	for i := range 288 {
		id := fmt.Sprintf("%032x", i+1)
		labels := map[string]string{"env": "production", "trace_id": id, "span_id": id,
			"request_id": id, "user_id": id, "total": id, "version": fmt.Sprint(i % 3)}
		for _, offset := range []time.Duration{-time.Minute, 0, time.Minute, 2 * time.Minute,
			span / 4, span / 2, span - time.Minute, span} {
			lines = append(lines, tumblingLine{ts: start.Add(offset).UnixNano(), labels: labels,
				msg: fmt.Sprintf(`{"request_id":%q,"user_id":%q,"total":%q}`, id, id, id)})
		}
	}
	return lines
}

func longrangeFieldRequest(ctx context.Context, query string, start, end time.Time) *http.Request {
	params := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)},
		"end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"60000ms"}, "direction": {"backward"}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil).WithContext(ctx)
	req.Header.Set("X-Scope-OrgID", "tenant-a")
	req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	return req
}

func assertLongrangeFieldAxis(t *testing.T, body []byte, start, end time.Time) {
	t.Helper()
	var response struct {
		Data struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	for _, series := range response.Data.Result {
		var previous int64 = math.MinInt64
		for _, point := range series.Values {
			ts, ok := point[0].(float64)
			if !ok {
				t.Fatalf("non-numeric evaluation timestamp: %v", point[0])
			}
			ms := int64(math.Round(ts * 1000))
			if ms < start.UnixMilli() || ms > end.UnixMilli() || (ms-start.UnixMilli())%time.Minute.Milliseconds() != 0 {
				t.Fatalf("point %.3f is outside the requested axis or has lost the start's millisecond phase", ts)
			}
			if ms <= previous {
				t.Fatalf("non-increasing or duplicate evaluation timestamp: %.3f", ts)
			}
			previous = ms
		}
	}
}

// conformance: loki_api_v1_query_range, limits/drilldown-breakdown-exact
func TestDrilldownLongRangeFields_ConcurrentSparseAnswers(t *testing.T) {
	for _, span := range []time.Duration{12 * time.Hour, 48 * time.Hour} {
		t.Run(span.String(), func(t *testing.T) {
			start := time.Unix(1793413140, 123*int64(time.Millisecond)).UTC()
			end := start.Add(span + 17431*time.Millisecond)
			lines := longrangeFieldFixture(start, span)
			backend, _ := newTumblingFakeVL(t, lines)
			_, mux := newTenantLimitsProxy(t, backend.URL, Config{})
			type heldAnswer struct {
				recorder *httptest.ResponseRecorder
				copy     []byte
				query    string
			}
			var mu sync.Mutex
			var held []heldAnswer
			t.Run("concurrent fields", func(t *testing.T) {
				for _, shape := range longrangeFieldShapes {
					t.Run(shape.field, func(t *testing.T) {
						t.Parallel()
						want := lokiTumblingReference(lines, "count_over_time", []string{shape.field}, start, end, time.Minute, time.Minute)
						rec := httptest.NewRecorder()
						mux.ServeHTTP(rec, longrangeFieldRequest(t.Context(), shape.query, start, end))
						if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"warnings"`) {
							t.Fatalf("Fields failed: %d %.1000s", rec.Code, rec.Body.String())
						}
						got := decodeTumblingSeries(t, shape.query, rec.Body.Bytes())
						assertLongrangeFieldAxis(t, rec.Body.Bytes(), start, end)
						if len(got) != len(want) {
							t.Fatalf("got %d series, want %d", len(got), len(want))
						}
						assertTumblingEqual(t, shape.query, want, got)
						if shape.field != "version" && rec.Body.Len() <= 64<<10 {
							t.Fatalf("fixture must exceed 64 KiB, got %d bytes", rec.Body.Len())
						}
						mu.Lock()
						held = append(held, heldAnswer{rec, bytes.Clone(rec.Body.Bytes()), shape.query})
						mu.Unlock()
					})
				}
			})
			// Additional responses exercise buffer/cache reuse after all the large
			// answers have been retained by their callers.
			for range 3 {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, longrangeFieldRequest(t.Context(), longrangeFieldShapes[2].query, start, end))
				if rec.Code != http.StatusOK {
					t.Fatalf("query after concurrent Fields: %d %s", rec.Code, rec.Body.String())
				}
			}
			for _, answer := range held {
				if !bytes.Equal(answer.copy, answer.recorder.Body.Bytes()) {
					t.Fatal("a subsequent response mutated a retained answer")
				}
				repeated := httptest.NewRecorder()
				mux.ServeHTTP(repeated, longrangeFieldRequest(t.Context(), answer.query, start, end))
				if repeated.Code != http.StatusOK {
					t.Fatalf("repeat after buffer reuse: %d %s", repeated.Code, repeated.Body.String())
				}
				want := decodeTumblingSeries(t, answer.query, answer.copy)
				got := decodeTumblingSeries(t, answer.query, repeated.Body.Bytes())
				if len(got) != len(want) {
					t.Fatalf("repeat got %d series, want %d", len(got), len(want))
				}
				assertTumblingEqual(t, answer.query, want, got)
			}
			ready := httptest.NewRecorder()
			mux.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if ready.Code != http.StatusOK {
				t.Fatalf("readiness after concurrent Fields: %d %s", ready.Code, ready.Body.String())
			}
		})
	}
}

// Cancellation and backend errors must not poison the next Fields request or
// turn a failed query into the successful empty response shown as "No data".
func TestDrilldownLongRangeFields_CancelAndErrorRecovery(t *testing.T) {
	for _, mode := range []string{"cancel", "backend error"} {
		t.Run(mode, func(t *testing.T) {
			start := time.Unix(1793413140, 123*int64(time.Millisecond)).UTC()
			end := start.Add(48 * time.Hour)
			lines := longrangeFieldFixture(start, 48*time.Hour)
			fake, _ := newTumblingFakeVL(t, lines)
			var failing atomic.Bool
			failing.Store(true)
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failing.Load() && r.URL.Path == "/select/logsql/stats_query_range" {
					// Drain the request body so net/http can observe the client
					// closing its connection while this handler is blocked.
					if err := r.ParseForm(); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					if mode == "backend error" {
						http.Error(w, "fixture backend unavailable", http.StatusServiceUnavailable)
						return
					}
					select {
					case entered <- struct{}{}:
					default:
					}
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				fake.Config.Handler.ServeHTTP(w, r)
			}))
			t.Cleanup(backend.Close)
			t.Cleanup(func() { close(release) })
			_, mux := newTenantLimitsProxy(t, backend.URL, Config{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				mux.ServeHTTP(rec, longrangeFieldRequest(ctx, longrangeFieldShapes[0].query, start, end))
			}()
			if mode == "cancel" {
				select {
				case <-entered:
					cancel()
				case <-time.After(5 * time.Second):
					t.Fatal("query did not reach the backend")
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("failed/cancelled query did not finish")
			}
			wantStatus := http.StatusInternalServerError
			if mode == "cancel" {
				wantStatus = 499
			}
			if rec.Code != wantStatus {
				t.Fatalf("failed/cancelled query: got %d, want %d: %s", rec.Code, wantStatus, rec.Body.String())
			}
			var failure struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &failure); err != nil || failure.Status != "error" {
				t.Fatalf("failed/cancelled query lacks error envelope: %s", rec.Body.String())
			}
			failing.Store(false)
			recovered := httptest.NewRecorder()
			mux.ServeHTTP(recovered, longrangeFieldRequest(t.Context(), longrangeFieldShapes[0].query, start, end))
			if recovered.Code != http.StatusOK {
				t.Fatalf("query after failure: %d %s", recovered.Code, recovered.Body.String())
			}
			want := lokiTumblingReference(lines, "count_over_time", []string{"trace_id"}, start, end, time.Minute, time.Minute)
			got := decodeTumblingSeries(t, longrangeFieldShapes[0].query, recovered.Body.Bytes())
			if len(got) != len(want) {
				t.Fatalf("recovery got %d series, want %d", len(got), len(want))
			}
			assertTumblingEqual(t, longrangeFieldShapes[0].query, want, got)
			ready := httptest.NewRecorder()
			mux.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if ready.Code != http.StatusOK {
				t.Fatalf("readiness after recovery: %d %s", ready.Code, ready.Body.String())
			}
		})
	}
}
