package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	p.storeBackendVersion("v1.54.0", "v1.54.0")
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p, func() []orderedRawChunkAttempt {
		mu.Lock()
		defer mu.Unlock()
		return append([]orderedRawChunkAttempt(nil), attempts...)
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
