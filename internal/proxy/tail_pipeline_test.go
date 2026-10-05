package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	"github.com/gorilla/websocket"
)

// tailTestEntry is one entry of a decoded tail frame.
type tailTestEntry struct {
	ts       string
	line     string
	stream   map[string]string
	metadata map[string]string
	parsed   map[string]string
}

func decodeTailFrame(t *testing.T, raw []byte) (entries []tailTestEntry, flags []string) {
	t.Helper()
	var frame struct {
		Streams []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"streams"`
		EncodingFlags []string `json:"encodingFlags"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("decode tail frame: %v: %s", err, raw)
	}
	for _, s := range frame.Streams {
		if len(s.Values) != 1 {
			t.Fatalf("tail frame stream holds %d entries, Loki sends one per stream: %s", len(s.Values), raw)
		}
		v := s.Values[0]
		e := tailTestEntry{stream: s.Stream}
		_ = json.Unmarshal(v[0], &e.ts)
		_ = json.Unmarshal(v[1], &e.line)
		if len(v) > 2 {
			var md struct {
				SM map[string]string `json:"structuredMetadata"`
				P  map[string]string `json:"parsed"`
			}
			if err := json.Unmarshal(v[2], &md); err != nil {
				t.Fatalf("decode entry metadata: %v", err)
			}
			e.metadata, e.parsed = md.SM, md.P
		}
		entries = append(entries, e)
	}
	return entries, frame.EncodingFlags
}

// tailEntriesOf runs VictoriaLogs rows through the tail conversion of query.
func tailEntriesOf(t *testing.T, p *Proxy, query string, levelAsMetadata bool, rows ...string) []tailTestEntry {
	t.Helper()
	frames, err := p.tailFrames(t.Context(), p.newTailPipeline(query, levelAsMetadata), []byte(strings.Join(rows, "\n")+"\n"))
	if err != nil {
		t.Fatalf("tail frames: %v", err)
	}
	var out []tailTestEntry
	for _, f := range frames {
		entries, _ := decodeTailFrame(t, f)
		out = append(out, entries...)
	}
	return out
}

// tailLineOf returns the line of the single tail entry of row.
func tailLineOf(t *testing.T, p *Proxy, query, row string) string {
	t.Helper()
	entries := tailEntriesOf(t, p, query, false, row)
	if len(entries) != 1 {
		t.Fatalf("%s: %d tail entries, want 1", query, len(entries))
	}
	return entries[0].line
}

func newLokiProfileTailProxy(t *testing.T, backend string) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:             backend,
		Cache:                  cache.NewDisabled(),
		LogLevel:               "error",
		LabelStyle:             LabelStyleUnderscores,
		MetadataFieldMode:      MetadataFieldModeTranslated,
		EmitStructuredMetadata: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// VictoriaLogs rows of the Loki push of
// {app="api"} {"level":"warn","msg":"json two","user":"u2"} with structured
// metadata service.version=1.3 (as stored, and as | json returns it with the
// line's keys unpacked), a logfmt line (stored, unpacked), and a JSON line
// VictoriaLogs stored with its keys as fields (jsonline ingestion).
const (
	tailRowJSON           = `{"_time":"2026-01-01T00:00:01Z","_msg":"{\"level\":\"warn\",\"msg\":\"json two\",\"user\":\"u2\"}","_stream":"{app=\"api\"}","app":"api","service.version":"1.3"}`
	tailRowJSONUnpacked   = `{"_time":"2026-01-01T00:00:01Z","_msg":"{\"level\":\"warn\",\"msg\":\"json two\",\"user\":\"u2\"}","_stream":"{app=\"api\"}","app":"api","service.version":"1.3","level":"warn","msg":"json two","user":"u2"}`
	tailRowLogfmt         = `{"_time":"2026-01-01T00:00:02Z","_msg":"level=info msg=\"logfmt four\" user=u4","_stream":"{app=\"api\"}","app":"api"}`
	tailRowLogfmtUnpacked = `{"_time":"2026-01-01T00:00:02Z","_msg":"level=info msg=\"logfmt four\" user=u4","_stream":"{app=\"api\"}","app":"api","level":"info","msg":"logfmt four","user":"u4"}`
	tailRowFields         = `{"_time":"2026-01-01T00:00:03Z","_msg":"{\"user\":\"u1\",\"msg\":\"stored\"}","_stream":"{app=\"api\"}","app":"api","user":"u1","msg":"stored"}`
	tailRowLabeled        = `{"_time":"2026-01-01T00:00:04Z","_msg":"hello","_stream":"{app=\"api\",detected_level=\"warn\"}","app":"api","detected_level":"warn"}`
)

// Loki's tailer runs the pipeline per entry: a label filter on a key of the
// line that no stage exposes matches no label in Loki, while VictoriaLogs
// matched the stored field.
// conformance: loki_api_v1_tail
func TestTailPipeline_LabelFilterOnUnexposedLineKeyDropsRow(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	if got := tailEntriesOf(t, p, `{app="api"} | user="u1"`, false, tailRowFields); len(got) != 0 {
		t.Fatalf("label filter on a key no stage exposes kept %d entries: %+v", len(got), got)
	}
	got := tailEntriesOf(t, p, `{app="api"} | json | user="u1"`, false, tailRowFields)
	if len(got) != 1 || got[0].stream["user"] != "u1" {
		t.Fatalf("| json | user=\"u1\" entries: %+v", got)
	}
}

// Loki's default tail encoding: with no pipeline stage the frame carries the
// index labels only; any stage makes it carry every label of the entry
// (structured metadata, parsed labels and detected_level), as a query
// response in the default encoding does.
// conformance: loki_api_v1_tail
func TestTailPipeline_DefaultEncodingStreamLabels(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	got := tailEntriesOf(t, p, `{app="api"}`, false, tailRowJSON)
	if want := map[string]string{"app": "api", "service_name": "api"}; len(got) != 1 || fmt.Sprint(got[0].stream) != fmt.Sprint(want) {
		t.Fatalf("no-stage tail stream: %+v, want %v", got, want)
	}
	got = tailEntriesOf(t, p, `{app="api"} |= "json"`, false, tailRowJSON)
	want := map[string]string{"app": "api", "service_name": "api", "service_version": "1.3", "detected_level": "warn"}
	if len(got) != 1 || fmt.Sprint(got[0].stream) != fmt.Sprint(want) {
		t.Fatalf("line filter tail stream: %+v, want %v", got, want)
	}
	got = tailEntriesOf(t, p, `{app="api"} | json`, false, tailRowJSONUnpacked)
	want = map[string]string{"app": "api", "service_name": "api", "service_version": "1.3", "detected_level": "warn", "level": "warn", "msg": "json two", "user": "u2"}
	if len(got) != 1 || fmt.Sprint(got[0].stream) != fmt.Sprint(want) {
		t.Fatalf("| json tail stream: %+v, want %v", got, want)
	}
}

// With categorize-labels the stream holds the index labels, the entry its
// structured metadata and, apart from them, the labels a parser added.
// conformance: loki_api_v1_tail
func TestTailPipeline_CategorizedParsedLabels(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	got := tailEntriesOf(t, p, `{app="api"} | json`, true, tailRowJSONUnpacked)
	if len(got) != 1 {
		t.Fatalf("entries: %+v", got)
	}
	e := got[0]
	if fmt.Sprint(e.stream) != fmt.Sprint(map[string]string{"app": "api", "service_name": "api"}) {
		t.Errorf("stream %v", e.stream)
	}
	if fmt.Sprint(e.metadata) != fmt.Sprint(map[string]string{"detected_level": "warn", "service_version": "1.3"}) {
		t.Errorf("structuredMetadata %v", e.metadata)
	}
	if fmt.Sprint(e.parsed) != fmt.Sprint(map[string]string{"level": "warn", "msg": "json two", "user": "u2"}) {
		t.Errorf("parsed %v", e.parsed)
	}
}

// line_format renders the entry's labels into the line, as in a query
// response; in the Loki-compatible profile VictoriaLogs returns the stored
// line and the proxy renders the template.
// conformance: loki_api_v1_tail
func TestTailPipeline_LineFormat(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	got := tailEntriesOf(t, p, `{app="api"} | json | line_format "{{.user}}: {{.msg}}"`, false, tailRowJSONUnpacked, tailRowLogfmt)
	if len(got) != 2 || got[0].line != "u2: json two" || got[1].line != ": " {
		t.Fatalf("line_format lines: %+v", got)
	}
	got = tailEntriesOf(t, p, `{app="api"} | logfmt | line_format "{{.user}}"`, false, tailRowLogfmtUnpacked)
	if len(got) != 1 || got[0].line != "u4" {
		t.Fatalf("logfmt line_format lines: %+v", got)
	}
}

// A stream stored with a detected_level label: Loki's tail encoder keeps the
// value it derives from the line under the detected_level name in the entry
// metadata and leaves the stream label out, for a query with stages as for
// one without.
// conformance: loki_api_v1_tail
func TestTailPipeline_StoredDetectedLevelLabel(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	for _, q := range []string{`{app="api"}`, `{app="api"} |= "hello"`} {
		got := tailEntriesOf(t, p, q, true, tailRowLabeled)
		if len(got) != 1 {
			t.Fatalf("%s entries: %+v", q, got)
		}
		if _, ok := got[0].stream["detected_level"]; ok {
			t.Errorf("%s: detected_level stream label kept: %v", q, got[0].stream)
		}
		if got[0].metadata["detected_level"] != "unknown" || got[0].metadata["detected_level_extracted"] != "" {
			t.Errorf("%s: metadata %v", q, got[0].metadata)
		}
	}
}

// A batch of rows goes out as frames of at most maxTailFrameEntries entries,
// one stream per entry, in timestamp order across streams.
// conformance: loki_api_v1_tail
func TestTailPipeline_FramesBatchEntriesInTimeOrder(t *testing.T) {
	p := newLokiProfileTailProxy(t, "http://unused")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows []string
	for i := 0; i < maxTailFrameEntries+5; i++ {
		app := []string{"a", "b"}[i%2]
		rows = append(rows, fmt.Sprintf(`{"_time":%q,"_msg":"line %d","_stream":"{app=\"%s\"}","app":%q}`, base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano), i, app, app))
	}
	frames, err := p.tailFrames(t.Context(), p.newTailPipeline(`{app=~"a|b"} |= "line"`, false), []byte(strings.Join(rows, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames %d, want 2", len(frames))
	}
	first, _ := decodeTailFrame(t, frames[0])
	second, _ := decodeTailFrame(t, frames[1])
	if len(first) != maxTailFrameEntries || len(second) != 5 {
		t.Fatalf("frame sizes %d, %d", len(first), len(second))
	}
	for i, e := range append(first, second...) {
		if e.line != fmt.Sprintf("line %d", i) {
			t.Fatalf("entry %d is %q: entries out of timestamp order", i, e.line)
		}
	}
}

// The native tail sends VictoriaLogs the query of a query_range request and
// streams the entries of the rows Loki's pipeline keeps, in Loki's frames.
// conformance: loki_api_v1_tail
func TestTailPipeline_NativeTailAppliesPipeline(t *testing.T) {
	var mu sync.Mutex
	var tailQuery string
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
		case "/select/logsql/tail":
			mu.Lock()
			tailQuery = r.URL.Query().Get("query")
			mu.Unlock()
			w.Header().Set("Content-Type", "application/x-ndjson")
			fmt.Fprintln(w, tailRowJSONUnpacked)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer vl.Close()
	p := newLokiProfileTailProxy(t, vl.URL)
	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	query := `{app="api"} | json | user="u2" | line_format "{{.msg}}"`
	ws, _, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"?query="+url.QueryEscape(query), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	entries, _ := decodeTailFrame(t, msg)
	if len(entries) != 1 || entries[0].line != "json two" || entries[0].stream["user"] != "u2" {
		t.Fatalf("tail entries %+v", entries)
	}
	mu.Lock()
	defer mu.Unlock()
	// line_format renders on the proxy, so VictoriaLogs returns the stored
	// line the label filters are checked against.
	if strings.Contains(tailQuery, "format") {
		t.Errorf("VictoriaLogs tail query keeps line_format: %s", tailQuery)
	}
}

func tailBenchRows(n int) []byte {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"_time":%q,"_msg":"{\"level\":\"info\",\"msg\":\"request %d\",\"user\":\"u%d\"}","_stream":"{app=\"api\",env=\"prod\"}","app":"api","env":"prod","service.version":"1.2"}`+"\n",
			base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano), i, i%10)
	}
	return []byte(b.String())
}

// BenchmarkTailFrames converts one batch of rows; ns/op and allocs/op are
// per batch (1 or 100 rows), so the per-entry cost of a frame is visible.
func BenchmarkTailFrames(b *testing.B) {
	p, err := New(Config{BackendURL: "http://unused", Cache: cache.NewDisabled(), LogLevel: "error", LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, EmitStructuredMetadata: true})
	if err != nil {
		b.Fatal(err)
	}
	for _, bc := range []struct {
		name  string
		query string
		rows  int
	}{
		{"plain/1", `{app="api"}`, 1},
		{"plain/100", `{app="api"}`, 100},
		{"json/1", `{app="api"} | json`, 1},
		{"json/100", `{app="api"} | json`, 100},
	} {
		rows := tailBenchRows(bc.rows)
		tp := p.newTailPipeline(bc.query, false)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := p.tailFrames(context.Background(), tp, rows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A burst of rows from VictoriaLogs' tail goes out in full frames, not one
// or two entries a frame: Loki batches up to 100 entries a frame, and the
// per-entry cost of a frame drops with its size.
// conformance: loki_api_v1_tail
func TestTailPipeline_NativeTailBurstFillsFrames(t *testing.T) {
	const burst = 5000
	rows := tailBenchRows(burst)
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/x-ndjson")
		case "/select/logsql/tail":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write(rows)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer vl.Close()
	p := newLokiProfileTailProxy(t, vl.URL)
	srv := httptest.NewServer(http.HandlerFunc(p.handleTail))
	defer srv.Close()

	ws, _, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"?query="+url.QueryEscape(`{app="api"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	_ = ws.SetReadDeadline(time.Now().Add(20 * time.Second))
	entries, frames := 0, 0
	for entries < burst {
		_, msg, err := ws.ReadMessage()
		if err != nil {
			t.Fatalf("after %d entries in %d frames: %v", entries, frames, err)
		}
		got, _ := decodeTailFrame(t, msg)
		if len(got) > maxTailFrameEntries {
			t.Fatalf("frame holds %d entries, Loki's maximum is %d", len(got), maxTailFrameEntries)
		}
		entries += len(got)
		frames++
	}
	if avg := entries / frames; avg < 50 {
		t.Fatalf("%d entries in %d frames (%d a frame): the burst was not batched", entries, frames, avg)
	}
}
