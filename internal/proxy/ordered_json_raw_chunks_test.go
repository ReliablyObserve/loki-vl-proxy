package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
	"github.com/klauspost/compress/zstd"
)

type orderedRawChunkAttempt struct {
	from, to int64
	bytes    int
}

func mixedEnvelopeChunkBody(t *testing.T, rows []pushdownRow, from, to int64) []byte {
	t.Helper()
	var out bytes.Buffer
	for _, row := range rows {
		if row.ts.UnixNano() < from || row.ts.UnixNano() >= to {
			continue
		}
		original, err := json.Marshal(map[string]string{"_time": row.ts.UTC().Format(time.RFC3339Nano), "_stream": `{app="api"}`, "_msg": row.msg, mixedClassOriginalField: "original collision"})
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(&out).Encode(map[string]string{mixedClassOriginalField: string(original)}); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes()
}

// This fixed unsafe fixture serves envelope transport only; it interprets no
// SQL/parser/class predicates. Actual class membership has separate VL proofs.
func newMixedEnvelopeChunkProxy(t *testing.T, rows []pushdownRow, encoding string, cap int64, afterRead ...func()) (*Proxy, func() []orderedRawChunkAttempt) {
	t.Helper()
	var mu sync.Mutex
	var attempts []orderedRawChunkAttempt
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/query" {
			t.Errorf("unexpected envelope transport path %s", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		from, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
		to, _ := parseFlexibleUnixNanos(r.Form.Get("end"))
		body := mixedEnvelopeChunkBody(t, rows, from, to)
		mu.Lock()
		attempts = append(attempts, orderedRawChunkAttempt{from, to, len(body)})
		mu.Unlock()
		for _, callback := range afterRead {
			callback()
		}
		var out bytes.Buffer
		var writer io.WriteCloser
		if encoding == "gzip" {
			writer = gzip.NewWriter(&out)
		} else {
			var err error
			writer, err = zstd.NewWriter(&out, zstd.WithEncoderConcurrency(1))
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		_, _ = writer.Write(body)
		_ = writer.Close()
		w.Header().Set("Content-Encoding", encoding)
		_, _ = w.Write(out.Bytes())
	}))
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), BackendCompression: encoding, LogLevel: "error", MetadataInventoryParallelism: -1, ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: int(cap)}})
	if err != nil {
		t.Fatal(err)
	}
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, func() []orderedRawChunkAttempt {
		mu.Lock()
		defer mu.Unlock()
		return append([]orderedRawChunkAttempt(nil), attempts...)
	}
}

func TestOrderedMixedClassCutEnvelopeRetriesBothOrders(t *testing.T) {
	const cap = 512
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at := time.Unix(1700002800, 0)
	rows := []pushdownRow{{ts: at, msg: "k=first padding=" + strings.Repeat("x", 180)}, {ts: at.Add(time.Nanosecond), msg: "k=second padding=" + strings.Repeat("y", 180)}}
	for _, pipeline := range []string{`| json k="[\"k\"]" | logfmt`, `| logfmt | json`} {
		for _, encoding := range []string{"gzip", "zstd"} {
			t.Run(pipeline+encoding, func(t *testing.T) {
				plan, _ := compileOrderedJSONMetric(`sum by(k)(count_over_time({app="api"} ` + pipeline + ` | drop __error__, __error_details__ | k!="" [1m]))`)
				class, ok := plan.mixedStatsClass()
				if !ok {
					t.Fatal("fixture lost typed static class admission")
				}
				spans := [][2]time.Time{{at, at.Add(time.Nanosecond)}}
				control, _ := newMixedEnvelopeChunkProxy(t, rows, encoding, 1<<20)
				want, err := control.collectMixedClassRowSpans(context.Background(), plan, `app:="api"`, class, spans, at.Add(time.Nanosecond), at.Add(time.Nanosecond), time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				p, attempts := newMixedEnvelopeChunkProxy(t, rows, encoding, cap)
				got, err := p.collectMixedClassRowSpans(context.Background(), plan, `app:="api"`, class, spans, at.Add(time.Nanosecond), at.Add(time.Nanosecond), time.Minute)
				if err != nil || !reflect.DeepEqual(got, want) || len(got) != 2 || len(attempts()) != 3 || attempts()[0].bytes <= cap {
					t.Fatalf("lost/cut/duplicated envelope boundary: groups=%d error=%v attempts=%v", len(got), err, attempts())
				}
				if plan.projectedLabel == "" {
					uncertified, calls := newMixedEnvelopeChunkProxy(t, rows, encoding, cap)
					reader, _ := uncertified.newOrderedJSONRowReader()
					reader.originalField = class.originalField // decoding alone is not certification
					err := uncertified.readOrderedJSONMetricRows(context.Background(), plan, reader, "*", at, at.Add(time.Nanosecond), at.Add(time.Nanosecond), at.Add(time.Nanosecond), time.Minute)
					var cut *backendBodyTooLargeError
					if !errors.As(err, &cut) || len(calls()) != 1 {
						t.Fatalf("ordinary reverse unexpectedly retried: %v %v", err, calls())
					}
				}
				for _, budget := range []string{"bytes", "rows", "cancel", "indivisible"} {
					t.Run(budget, func(t *testing.T) {
						fixture := append([]pushdownRow(nil), rows...)
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						var callbacks []func()
						if budget == "cancel" {
							callbacks = append(callbacks, cancel)
						}
						if budget == "indivisible" {
							fixture[1].ts = at
						}
						limited, calls := newMixedEnvelopeChunkProxy(t, fixture, encoding, cap, callbacks...)
						if budget == "rows" {
							limited.rangeMetricRowLimit = 2
						}
						if budget == "bytes" {
							limited.orderedJSONMaxBytes = int64(len(mixedEnvelopeChunkBody(t, fixture, at.UnixNano(), at.Add(2*time.Nanosecond).UnixNano())) + 1)
						}
						end := at.Add(time.Nanosecond)
						if budget == "indivisible" {
							end = at
						}
						got, err := limited.collectMixedClassRowSpans(ctx, plan, "*", class, [][2]time.Time{{at, end}}, end, end, time.Minute)
						if err == nil || got != nil || len(calls()) > 3 {
							t.Fatalf("budget/cancel returned a partial result or kept retrying: %v %v %v", got, err, calls())
						}
						if budget == "cancel" && !errors.Is(err, context.Canceled) {
							t.Fatalf("lost cancellation: %v", err)
						}
						if budget == "indivisible" && len(calls()) != 1 {
							t.Fatal("indivisible timestamp retried")
						}
					})
				}
			})
		}
	}
}

func newOrderedRawChunkProxy(t *testing.T, rows []pushdownRow, encoding string, cap int64, afterRead ...func()) (*Proxy, func() []orderedRawChunkAttempt) {
	t.Helper()
	backend, _ := newPushdownFakeVL(t, rows, func(s string) string { return s })
	var mu sync.Mutex
	var attempts []orderedRawChunkAttempt
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		backend.Config.Handler.ServeHTTP(rec, r)
		body := rec.Body.Bytes()
		if r.URL.Path == "/select/logsql/query" {
			from, _ := parseFlexibleUnixNanos(r.Form.Get("start"))
			to, _ := parseFlexibleUnixNanos(r.Form.Get("end"))
			mu.Lock()
			attempts = append(attempts, orderedRawChunkAttempt{from, to, len(body)})
			mu.Unlock()
			for _, callback := range afterRead {
				callback()
			}
		}
		if encoding != "" {
			var compressed bytes.Buffer
			var writer io.WriteCloser
			if encoding == "gzip" {
				writer = gzip.NewWriter(&compressed)
			} else {
				writer, _ = zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
			}
			_, _ = writer.Write(body)
			_ = writer.Close()
			body = compressed.Bytes()
			w.Header().Set("Content-Encoding", encoding)
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), BackendCompression: encoding,
		LogLevel: "error", MetadataInventoryParallelism: -1,
		ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: int(cap)}})
	if err != nil {
		t.Fatal(err)
	}
	// This fake implements raw transport, not typed mixed-class stats. Its
	// known older version selects the documented raw fallback without starting
	// an unrelated background version probe against the transport fixture.
	p.storeBackendVersion("v1.48.0", "v1.48.0")
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, func() []orderedRawChunkAttempt {
		mu.Lock()
		defer mu.Unlock()
		return append([]orderedRawChunkAttempt(nil), attempts...)
	}
}

func TestOrderedMixedStatsCompressedBodyCapFallsBackToCompleteRaw(t *testing.T) {
	const bodyCap = 512
	decodedBackendBodyFloorOverride.Store(bodyCap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at := time.Unix(1700002800, 0).UTC()
	query := `sum by(k)(count_over_time({app="api"} | json k="[\"k\"]" | logfmt | drop __error__, __error_details__ | k!="" [1m]))`
	plan, ok := compileOrderedJSONMetric(query)
	if !ok {
		t.Fatal("mixed plan missing")
	}
	metric := map[string]string{"k": "body"}
	rows := map[string]manualSeriesSamples{canonicalLabelsKey(metric): {Metric: metric, Samples: []rangeMetricSample{{ts: at.UnixNano(), value: 1}}}}
	want, err := buildOrderedJSONMetric(context.Background(), plan, rows, at, at.Add(30*time.Second), 30*time.Second, true, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			var statsCalls, rawCalls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				var body string
				switch r.URL.Path {
				case statsQueryRangePath:
					statsCalls.Add(1)
					if !strings.Contains(r.FormValue("query"), "pack_json as "+mixedClassOriginalField) {
						t.Error("known backend did not attempt typed class stats")
					}
					body = fmt.Sprintf(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","__vlmc_target_k":"body","padding":%q},"values":[[%d,"1"]]}]}}`, strings.Repeat("x", 2048), at.Add(-time.Minute).Unix())
				case "/select/logsql/query":
					rawCalls.Add(1)
					if strings.Contains(r.FormValue("query"), "pack_json") {
						t.Error("bounded stats failure did not return to the original raw route")
					}
					body = fmt.Sprintf(`{"_time":%q,"_stream":"{app=\"api\"}","_msg":"{\"k\":\"body\"}"}`+"\n", at.Format(time.RFC3339Nano))
				default:
					t.Errorf("unexpected backend path %s", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				var encoded bytes.Buffer
				var writer io.WriteCloser
				if encoding == "gzip" {
					writer = gzip.NewWriter(&encoded)
				} else {
					var err error
					writer, err = zstd.NewWriter(&encoded, zstd.WithEncoderConcurrency(1))
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
				_, _ = io.WriteString(writer, body)
				_ = writer.Close()
				w.Header().Set("Content-Encoding", encoding)
				_, _ = w.Write(encoded.Bytes())
			}))
			t.Cleanup(srv.Close)
			p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), BackendCompression: encoding, LogLevel: "error", MetadataInventoryParallelism: -1, ExecutionLimits: ExecutionLimitsConfig{BackendMaxBufferedResponseBytes: bodyCap}})
			if err != nil {
				t.Fatal(err)
			}
			p.storeBackendVersion("v1.54.0", "v1.54.0")
			t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
			form := fmt.Sprintf("/loki/api/v1/query_range?query=%s&start=%s&end=%s&step=30", url.QueryEscape(query), at.Format(time.RFC3339Nano), at.Add(30*time.Second).Format(time.RFC3339Nano))
			got := doCompatProxyRequest(p, form, nil)
			if err := recordedParserDifference(recordedParserCase{Status: http.StatusOK, Response: want}, got.Code, got.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			if statsCalls.Load() != 1 || rawCalls.Load() != 1 {
				t.Fatalf("native overflow/raw fallback calls=%d/%d, want1/1", statsCalls.Load(), rawCalls.Load())
			}
		})
	}
}

func TestOrderedRawMetricCutRowRetainsTypedBodyError(t *testing.T) {
	const cap = 512
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at := time.Unix(1700002800, 0)
	var rows []pushdownRow
	for i := range 10 {
		rows = append(rows, pushdownRow{ts: at.Add(time.Duration(i) * time.Second), stream: map[string]string{"app": "api"}, msg: `{"tenant_id":"blue","padding":"` + strings.Repeat("x", 80) + `"}`})
	}
	plan, ok := compileOrderedJSONMetric(`sum by (tenant_id)(count_over_time({app="api"} | json | logfmt | drop __error__, __error_details__ [1m]))`)
	if !ok || plan.projectedLabel != "" {
		t.Fatal("expected a general, unchunked mixed plan")
	}
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			p, _ := newOrderedRawChunkProxy(t, rows, encoding, cap)
			reader, err := p.newOrderedJSONRowReader()
			if err != nil {
				t.Fatal(err)
			}
			err = p.readOrderedJSONMetricRows(context.Background(), plan, reader, `app:="api"`, at, at.Add(10*time.Second), at.Add(10*time.Second), at.Add(10*time.Second), time.Minute)
			var bounded *backendBodyTooLargeError
			if !errors.As(err, &bounded) || bounded.limit != cap {
				t.Fatalf("cut NDJSON hid its actual size error: %T %v", err, err)
			}
		})
	}
}

func orderedRawChunkFixture() (time.Time, []pushdownRow) {
	at := time.Unix(1700002800, 0).UTC()
	var rows []pushdownRow
	for h := range 7 {
		for i, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond, 3 * time.Minute, 9 * time.Minute, 30 * time.Minute} {
			stamp := at.Add(time.Duration(h)*time.Hour + offset)
			if stamp.After(at.Add(6 * time.Hour)) {
				continue
			}
			rows = append(rows, pushdownRow{ts: stamp, stream: map[string]string{"app": "api"}, msg: fmt.Sprintf(`{"tenant_id":"value%d","note":"%s"}`, i%2, strings.Repeat("x", 40))})
		}
	}
	// Different original lines at the same timestamp both count.
	rows = append(rows, pushdownRow{ts: at.Add(time.Hour), stream: map[string]string{"app": "api"}, msg: `tenant_id=value1 note=duplicate-time`})
	return at, rows
}

func orderedRawChunkQuery(function string) string {
	return `sum by (tenant_id)(` + function + `({app="api"} | json tenant_id="[\"tenant_id\"]" | logfmt | drop __error__, __error_details__ | tenant_id!="" [1m]))`
}

func TestOrderedRawMetricSmallLongRangeUsesOneRead(t *testing.T) {
	const limit = 64 << 10
	decodedBackendBodyFloorOverride.Store(limit)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	plan, _ := compileOrderedJSONMetric(orderedRawChunkQuery("count_over_time"))
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			p, attempts := newOrderedRawChunkProxy(t, rows, encoding, limit)
			// Match the real CLI's default; it is not an explicit raw-body cap.
			p.queryRangeSplitInterval = time.Hour
			series, err := p.collectOrderedJSONMetric(context.Background(), plan, at, at.Add(6*time.Hour), time.Minute)
			reads := attempts()
			if err != nil || len(series) != 2 {
				t.Fatalf("complete small response failed: groups=%d error=%v", len(series), err)
			}
			if len(reads) != 1 || reads[0].bytes >= limit {
				t.Fatalf("small long-range response unnecessarily split: %v", reads)
			}
			if reads[0].from != at.Add(-plan.window).UnixNano() || reads[0].to != at.Add(6*time.Hour+time.Nanosecond).UnixNano() {
				t.Fatalf("first read changed the original request bounds: %v", reads)
			}
		})
	}
}

func TestOrderedRawMetricTransportPreservesConfiguredUpperSplits(t *testing.T) {
	const limit = 64 << 10
	decodedBackendBodyFloorOverride.Store(limit)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	plan, _ := compileOrderedJSONMetric(orderedRawChunkQuery("count_over_time"))
	for _, interval := range []time.Duration{0, 15 * time.Minute, time.Hour, 24 * time.Hour} {
		p, attempts := newOrderedRawChunkProxy(t, rows, "gzip", limit)
		p.queryRangeSplitInterval = interval
		_, err := p.collectOrderedJSONMetric(context.Background(), plan, at, at.Add(6*time.Hour), time.Minute)
		reads := attempts()
		if err != nil || len(reads) != 1 || p.queryRangeSplitInterval != interval {
			t.Fatalf("transport added splits or changed upper-level setting %v: attempts=%d setting=%v error=%v", interval, len(reads), p.queryRangeSplitInterval, err)
		}
		if reads[0].from != at.Add(-plan.window).UnixNano() || reads[0].to != at.Add(6*time.Hour+time.Nanosecond).UnixNano() {
			t.Fatalf("transport changed the requested span: %v", reads)
		}
	}
}

func TestOrderedRawMetricChunksAreComplete(t *testing.T) {
	const cap = 768
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	control, _ := newOrderedRawChunkProxy(t, rows, "", cap)
	for _, encoding := range []string{"gzip", "zstd"} {
		p, attempts := newOrderedRawChunkProxy(t, rows, encoding, cap)
		for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
			plan, ok := compileOrderedJSONMetric(orderedRawChunkQuery(function))
			if !ok || plan.projectedLabel == "" {
				t.Fatal("expected certified mixed projection")
			}
			full := *plan
			full.projectedLabel = ""
			end := at.Add(6 * time.Hour)
			wantRows, err := control.collectOrderedJSONMetric(context.Background(), &full, at, end, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			gotRows, err := p.collectOrderedJSONMetric(context.Background(), plan, at, end, time.Minute)
			if err != nil {
				t.Fatalf("%s/%s: chunked read: %v", encoding, function, err)
			}
			want, err := buildOrderedJSONMetric(context.Background(), &full, wantRows, at, end, time.Minute, true, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			got, err := buildOrderedJSONMetric(context.Background(), plan, gotRows, at, end, time.Minute, true, 1<<20)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s/%s: complete output differs: %v\n%s\n%s", encoding, function, err, got, want)
			}
		}
		var overflow bool
		for _, read := range attempts() {
			overflow = overflow || read.bytes > cap
		}
		if !overflow {
			t.Fatal("fixture did not exercise a decoded-body cap")
		}
	}
}

func TestOrderedRawMetricChunksKeepRequestBudgets(t *testing.T) {
	const cap = 768
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	plan, _ := compileOrderedJSONMetric(orderedRawChunkQuery("count_over_time"))
	for _, tc := range []struct {
		name string
		rows int
		body int64
		want error
	}{
		{"bytes", 1000, 1024, orderedJSONResponseLimitError(1024)},
		{"rows", 8, 1 << 20, manualRowLimitError(8)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, attempts := newOrderedRawChunkProxy(t, rows, "gzip", cap)
			p.rangeMetricRowLimit, p.orderedJSONMaxBytes = tc.rows, tc.body
			_, err := p.collectOrderedJSONMetric(context.Background(), plan, at, at.Add(6*time.Hour), time.Minute)
			if err == nil || err.Error() != tc.want.Error() {
				t.Fatalf("global budget was reset or reclassified: %v, want %v", err, tc.want)
			}
			if len(attempts()) > 20 {
				t.Fatal("over-budget request continued to fan out")
			}
		})
	}
}

func TestOrderedRawMetricChunkCancellationAndIndivisibleSpan(t *testing.T) {
	const cap = 512
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	plan, _ := compileOrderedJSONMetric(orderedRawChunkQuery("count_over_time"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, attempts := newOrderedRawChunkProxy(t, rows, "gzip", cap, cancel)
	_, err := p.collectOrderedJSONMetric(ctx, plan, at, at.Add(6*time.Hour), time.Minute)
	if !errors.Is(err, context.Canceled) || len(attempts()) != 1 {
		t.Fatalf("canceled read retried: attempts=%d, error=%v", len(attempts()), err)
	}
	for i := range rows {
		rows[i].ts = at
	}
	p, attempts = newOrderedRawChunkProxy(t, rows, "gzip", cap)
	reader, _ := p.newOrderedJSONRowReader()
	err = p.readOrderedJSONMetricRows(context.Background(), plan, reader, `app:="api"`, at, at, at, at, time.Minute)
	var bounded *backendBodyTooLargeError
	if !errors.As(err, &bounded) || len(attempts()) != 1 {
		t.Fatalf("indivisible span retried or lost its cap: attempts=%d error=%v", len(attempts()), err)
	}
}

func TestOrderedRawMetricChunkSplitsAdjacentNanoseconds(t *testing.T) {
	const cap = 512
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at := time.Unix(1700002800, 0)
	rows := []pushdownRow{
		{ts: at, stream: map[string]string{"app": "api"}, msg: `{"tenant_id":"one","padding":"` + strings.Repeat("x", 200) + `"}`},
		{ts: at.Add(time.Nanosecond), stream: map[string]string{"app": "api"}, msg: `{"tenant_id":"two","padding":"` + strings.Repeat("x", 200) + `"}`},
	}
	plan, _ := compileOrderedJSONMetric(orderedRawChunkQuery("count_over_time"))
	p, attempts := newOrderedRawChunkProxy(t, rows, "gzip", cap)
	reader, _ := p.newOrderedJSONRowReader()
	err := p.readOrderedJSONMetricRows(context.Background(), plan, reader, `app:="api"`, at, at.Add(time.Nanosecond), at.Add(time.Nanosecond), at.Add(time.Nanosecond), time.Minute)
	if err != nil {
		t.Fatalf("adjacent nanoseconds were not split: %v; attempts=%v", err, attempts())
	}
	var samples int
	for _, entry := range reader.series {
		samples += len(entry.Samples)
	}
	if samples != 2 || len(reader.series) != 2 || len(attempts()) != 3 {
		t.Fatalf("lost or duplicated a boundary/cut row: samples=%d groups=%d attempts=%d", samples, len(reader.series), len(attempts()))
	}
}

// conformance: semantics/two-parsers-keep-one, loki_api_v1_query_range
// conformance: semantics/two-parsers-keep-one
func TestOrderedRawMetricChunksMatchRecordedLoki(t *testing.T) {
	const cap = 768
	decodedBackendBodyFloorOverride.Store(cap)
	t.Cleanup(func() { decodedBackendBodyFloorOverride.Store(0) })
	at, rows := orderedRawChunkFixture()
	set := oraclerun.EngineDataSet{Streams: pushdownEngineStreams(rows)}
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		query := orderedRawChunkQuery(function)
		set.Requests = append(set.Requests, lokiMetricRequest(function, query, at.UnixNano(), at.Add(6*time.Hour).UnixNano(), time.Minute))
	}
	answers := lokiEngineAnswers(t, "ordered_mixed_raw_chunks", []oraclerun.EngineDataSet{set})
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			p, attempts := newOrderedRawChunkProxy(t, rows, encoding, cap)
			for _, request := range set.Requests {
				got := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+request.Form, nil)
				want := answers[request.ID]
				if err := recordedParserDifference(recordedParserCase{Status: want.Status, Response: want.Body}, got.Code, got.Body.Bytes()); err != nil {
					t.Fatalf("%s: %v", request.ID, err)
				}
			}
			var overflow bool
			for _, read := range attempts() {
				overflow = overflow || read.bytes > cap
			}
			if !overflow {
				t.Fatal("fixture never exercised adaptive retry of a cut row")
			}
		})
	}
}
