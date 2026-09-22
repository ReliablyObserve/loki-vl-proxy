package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	fj "github.com/valyala/fastjson"
)

// detectedLevelRows are VictoriaLogs rows for the log-response encoding tests:
// one stream with a body keyword, a line without a level, a stored level
// field, and a stream with a detected_level stream field. Loki keeps such a
// stream label as an index label and exposes the entry's derived value as
// detected_level_extracted.
const detectedLevelRows = `{"_time":"2026-01-01T00:00:01Z","_msg":"request failed with error","_stream":"{app=\"api\",service.name=\"checkout\"}","app":"api","service.name":"checkout"}
{"_time":"2026-01-01T00:00:02Z","_msg":"hello","_stream":"{app=\"api\",service.name=\"checkout\"}","app":"api","service.name":"checkout"}
{"_time":"2026-01-01T00:00:03Z","_msg":"hello","_stream":"{app=\"api\",service.name=\"checkout\"}","app":"api","service.name":"checkout","level":"Warning","trace_id":"t1"}
{"_time":"2026-01-01T00:00:04Z","_msg":"hello","_stream":"{app=\"api\",detected_level=\"WARNING\"}","app":"api","detected_level":"WARNING"}
`

type dlTestEntry struct {
	stream   map[string]string
	metadata map[string]string
}

// dlTestEntriesFromStreams indexes converted streams by entry timestamp.
func dlTestEntriesFromStreams(t *testing.T, streams []map[string]interface{}) map[string]dlTestEntry {
	t.Helper()
	raw, err := json.Marshal(streams)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []struct {
		Stream map[string]string   `json:"stream"`
		Values [][]json.RawMessage `json:"values"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode streams: %v: %s", err, raw)
	}
	out := map[string]dlTestEntry{}
	for _, s := range decoded {
		for _, v := range s.Values {
			var ts string
			_ = json.Unmarshal(v[0], &ts)
			e := dlTestEntry{stream: s.Stream}
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
				}
				_ = json.Unmarshal(v[2], &meta)
				e.metadata = meta.StructuredMetadata
			}
			out[ts] = e
		}
	}
	return out
}

func newDetectedLevelTestProxy(t *testing.T, style LabelStyle, disableBodyScan bool) *Proxy {
	t.Helper()
	p, err := New(Config{
		BackendURL:                   "http://127.0.0.1:1",
		Cache:                        cache.NewDisabled(),
		LogLevel:                     "error",
		LabelStyle:                   style,
		EmitStructuredMetadata:       true,
		DisableDetectedLevelBodyScan: disableBodyScan,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

const (
	dlTS1 = "1767225601000000000"
	dlTS2 = "1767225602000000000"
	dlTS3 = "1767225603000000000"
	dlTS4 = "1767225604000000000"
)

// dlExpectedEntries is Loki's encoding of detectedLevelRows.
func dlExpectedEntries(style LabelStyle, categorized bool) map[string]dlTestEntry {
	service := "service.name"
	if style == LabelStyleUnderscores {
		service = "service_name"
	}
	checkout := map[string]string{"app": "api", service: "checkout"}
	if style != LabelStyleUnderscores {
		checkout["service_name"] = "checkout"
	}
	with := func(base map[string]string, extra map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	plain := map[string]string{"app": "api", "detected_level": "WARNING", "service_name": "api"}
	traceID := "trace_id"
	if !categorized {
		return map[string]dlTestEntry{
			dlTS1: {stream: with(checkout, map[string]string{"detected_level": "error"})},
			dlTS2: {stream: with(checkout, map[string]string{"detected_level": "unknown"})},
			dlTS3: {stream: with(checkout, map[string]string{"detected_level": "warn", "level": "Warning"})},
			dlTS4: {stream: with(plain, map[string]string{"detected_level_extracted": "unknown"})},
		}
	}
	return map[string]dlTestEntry{
		dlTS1: {stream: checkout, metadata: map[string]string{"detected_level": "error"}},
		dlTS2: {stream: checkout, metadata: map[string]string{"detected_level": "unknown"}},
		dlTS3: {stream: checkout, metadata: map[string]string{"detected_level": "warn", "level": "Warning", traceID: "t1"}},
		dlTS4: {stream: plain, metadata: map[string]string{"detected_level_extracted": "unknown"}},
	}
}

func dlAssertEntries(t *testing.T, got, want map[string]dlTestEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("entries = %d, want %d: %+v", len(got), len(want), got)
	}
	for ts, w := range want {
		g, ok := got[ts]
		if !ok {
			t.Fatalf("missing entry %s", ts)
		}
		if !reflect.DeepEqual(g.stream, w.stream) {
			t.Errorf("%s stream labels = %v, want %v", ts, g.stream, w.stream)
		}
		if len(w.metadata) > 0 || len(g.metadata) > 0 {
			if !reflect.DeepEqual(g.metadata, w.metadata) {
				t.Errorf("%s structuredMetadata = %v, want %v", ts, g.metadata, w.metadata)
			}
		}
	}
}

func dlStreamCount(streams []map[string]interface{}) int {
	return len(streams)
}

func TestDetectedLevelLogResponseEncoding(t *testing.T) {
	for _, style := range []LabelStyle{LabelStylePassthrough, LabelStyleUnderscores} {
		for _, categorized := range []bool{false, true} {
			name := string(style)
			if categorized {
				name += "/categorize-labels"
			}
			t.Run(name, func(t *testing.T) {
				p := newDetectedLevelTestProxy(t, style, false)
				want := dlExpectedEntries(style, categorized)

				streams, _, err := p.vlReaderToLokiStreams(strings.NewReader(detectedLevelRows), `{app="api"}`, "", categorized, categorized, false)
				if err != nil {
					t.Fatal(err)
				}
				dlAssertEntries(t, dlTestEntriesFromStreams(t, streams), want)
				// Loki splits entries of one stream by detected_level only in the
				// default encoding.
				wantStreams := 4
				if categorized {
					wantStreams = 2
				}
				if dlStreamCount(streams) != wantStreams {
					t.Errorf("streams = %d, want %d", dlStreamCount(streams), wantStreams)
				}

				entries := p.vlLogsToLokiWindowEntries([]byte(detectedLevelRows), `{app="api"}`, categorized, categorized)
				windowed := groupQueryRangeWindowEntries(entries, "forward", categorized, categorized)
				dlAssertEntries(t, dlTestEntriesFromStreams(t, windowed), want)
				if len(windowed) != wantStreams {
					t.Errorf("windowed streams = %d, want %d", len(windowed), wantStreams)
				}
			})
		}
	}
}

func TestDetectedLevelStreamingResponseEncoding(t *testing.T) {
	for _, categorized := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "categorize-labels"}[categorized], func(t *testing.T) {
			p := newDetectedLevelTestProxy(t, LabelStyleUnderscores, false)
			rec := httptest.NewRecorder()
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(detectedLevelRows))}
			p.streamLogQuery(rec, resp, `{app="api"}`, categorized, categorized)
			var out struct {
				Data struct {
					Result []map[string]interface{} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatalf("decode: %v: %s", err, rec.Body.String())
			}
			dlAssertEntries(t, dlTestEntriesFromStreams(t, out.Data.Result), dlExpectedEntries(LabelStyleUnderscores, categorized))
		})
	}
}

func TestDetectedLevelDropAndKeepStages(t *testing.T) {
	p := newDetectedLevelTestProxy(t, LabelStyleUnderscores, false)
	for _, categorized := range []bool{false, true} {
		for _, query := range []string{`{app="api"} | drop detected_level`, `{app="api"} | keep app`} {
			streams, _, err := p.vlReaderToLokiStreams(strings.NewReader(detectedLevelRows), query, "", categorized, categorized, false)
			if err != nil {
				t.Fatal(err)
			}
			for ts, e := range dlTestEntriesFromStreams(t, streams) {
				if ts == dlTS4 {
					// The stored detected_level stream label is deleted upstream by
					// VictoriaLogs; the derived detected_level_extracted is only
					// removed by a stage that names it (| keep app does).
					_, inStream := e.stream["detected_level_extracted"]
					_, inMetadata := e.metadata["detected_level_extracted"]
					if want := strings.Contains(query, "drop"); (inStream || inMetadata) != want {
						t.Errorf("%s categorized=%v: detected_level_extracted kept=%v, want %v", query, categorized, inStream || inMetadata, want)
					}
					continue
				}
				if _, ok := e.stream["detected_level"]; ok {
					t.Errorf("%s categorized=%v %s: detected_level stream label kept: %v", query, categorized, ts, e.stream)
				}
				if _, ok := e.metadata["detected_level"]; ok {
					t.Errorf("%s categorized=%v %s: detected_level metadata kept: %v", query, categorized, ts, e.metadata)
				}
			}
		}
	}
}

func TestDetectedLevelBodyScanDisabled(t *testing.T) {
	p := newDetectedLevelTestProxy(t, LabelStyleUnderscores, true)
	streams, _, err := p.vlReaderToLokiStreams(strings.NewReader(detectedLevelRows), `{app="api"}`, "", false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	got := dlTestEntriesFromStreams(t, streams)
	for ts, want := range map[string]string{dlTS1: "unknown", dlTS2: "unknown", dlTS3: "warn"} {
		if level := got[ts].stream["detected_level"]; level != want {
			t.Errorf("%s detected_level = %q, want %q", ts, level, want)
		}
	}
	if level := got[dlTS4].stream["detected_level_extracted"]; level != "unknown" {
		t.Errorf("%s detected_level_extracted = %q, want unknown", dlTS4, level)
	}
	if p.detectedLevelCacheKey() == newDetectedLevelTestProxy(t, LabelStyleUnderscores, false).detectedLevelCacheKey() {
		t.Error("cached responses must be keyed by the body-scan setting")
	}
}

func TestDetectedLevelTailFrameEncoding(t *testing.T) {
	p := newDetectedLevelTestProxy(t, LabelStyleUnderscores, false)
	decodeRow := func(line string) map[string]interface{} {
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		return row
	}
	rows := strings.Split(strings.TrimSpace(detectedLevelRows), "\n")

	// Loki's default tail frames carry the index labels only.
	frame := p.vlLineToTailFrame(decodeRow(rows[2]), nil, false)
	raw, _ := json.Marshal(frame)
	want := `{"streams":[{"stream":{"app":"api","service_name":"checkout"},"values":[["1767225603000000000","hello"]]}]}`
	if string(raw) != want {
		t.Errorf("default tail frame\n got %s\nwant %s", raw, want)
	}

	frame = p.vlLineToTailFrame(decodeRow(rows[2]), nil, true)
	raw, _ = json.Marshal(frame)
	want = `{"encodingFlags":["categorize-labels"],"streams":[{"stream":{"app":"api","service_name":"checkout"},"values":[["1767225603000000000","hello",{"structuredMetadata":{"detected_level":"warn","level":"Warning","trace_id":"t1"}}]]}]}`
	if string(raw) != want {
		t.Errorf("categorize-labels tail frame\n got %s\nwant %s", raw, want)
	}

	frame = p.vlLineToTailFrame(decodeRow(rows[3]), nil, true)
	raw, _ = json.Marshal(frame)
	// Loki's tail encoder keeps the derived detected_level in the metadata and
	// leaves a detected_level stream label out of the frame, unlike its query
	// encoder, which renames the derived value instead.
	want = `{"encodingFlags":["categorize-labels"],"streams":[{"stream":{"app":"api","service_name":"api"},"values":[["1767225604000000000","hello",{"structuredMetadata":{"detected_level":"unknown"}}]]}]}`
	if string(raw) != want {
		t.Errorf("stored detected_level stream field\n got %s\nwant %s", raw, want)
	}
}

func TestDetectedLevelPatternLevels(t *testing.T) {
	rows := `{"_time":"2026-01-01T00:00:01Z","_msg":"GET /orders failed with error","_stream":"{app=\"api\"}","app":"api"}
{"_time":"2026-01-01T00:00:02Z","_msg":"GET /health served in 3ms","_stream":"{app=\"api\"}","app":"api"}
{"_time":"2026-01-01T00:00:03Z","_msg":"cache miss for key orders","_stream":"{app=\"api\"}","app":"api","level":"Warning"}
{"_time":"2026-01-01T00:00:04Z","_msg":"reindex started by operator","_stream":"{app=\"api\"}","app":"api","level":"NOTICE"}
`
	patterns := extractLogPatterns([]byte(rows), "60s", 50, defaultLogRowLevels())
	levels := map[string]bool{}
	for _, pattern := range patterns {
		level, _ := pattern["level"].(string)
		levels[level] = true
	}
	for _, want := range []string{"error", "unknown", "warn", "notice"} {
		if !levels[want] {
			t.Errorf("pattern levels %v lack %q", levels, want)
		}
	}
	if levels["Warning"] || levels["NOTICE"] {
		t.Errorf("pattern levels must be lowercased detected_level values: %v", levels)
	}
}

func dlMetadataBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/select/logsql/query":
			_, _ = w.Write([]byte(detectedLevelRows))
		case "/select/logsql/streams":
			_, _ = w.Write([]byte(`{"values":[{"value":"{app=\"api\",detected_level=\"WARNING\"}","hits":1},{"value":"{app=\"api\",level=\"info\"}","hits":1}]}`))
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			_, _ = w.Write([]byte(`{"values":[{"value":"app","hits":2},{"value":"detected_level","hits":1},{"value":"level","hits":1}]}`))
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			_, _ = w.Write([]byte(`{"values":[{"value":"WARNING","hits":1}]}`))
		default:
			_, _ = w.Write([]byte(`{"values":[]}`))
		}
	}))
}

func TestDetectedLevelLabelValuesAnswersLikeLoki(t *testing.T) {
	for _, tc := range []struct {
		name         string
		streamValues string
		want         string
	}{
		// The derived value is not indexed: no data at all, as Loki answers.
		{name: "no detected_level stream label", streamValues: `{"values":[]}`, want: `{"status":"success"}`},
		// A detected_level stream label is an index label with its raw values.
		{name: "detected_level stream label", streamValues: `{"values":[{"value":"WARNING","hits":1}]}`, want: `{"data":["WARNING"],"status":"success"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fields []string
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				if r.URL.Path != "/select/logsql/stream_field_values" {
					t.Errorf("unexpected backend call %s", r.URL.Path)
				}
				fields = append(fields, r.Form.Get("field"))
				_, _ = w.Write([]byte(tc.streamValues))
			}))
			defer backend.Close()
			p := newTestProxy(t, backend.URL)
			rec := httptest.NewRecorder()
			p.handleLabelValues(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/detected_level/values?query=%7Bapp%3D%22api%22%7D", nil))
			if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != tc.want {
				t.Fatalf("label values = %d %s, want %s", rec.Code, rec.Body.String(), tc.want)
			}
			if !reflect.DeepEqual(fields, []string{"detected_level"}) {
				t.Fatalf("queried fields %v, want only the detected_level stream field (never level)", fields)
			}
		})
	}
}

// conformance: severity-exposure-surfaces
func TestDetectedLevelLabelValuesSortsAndCaches(t *testing.T) {
	var calls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		calls++
		// VictoriaLogs orders field values by hits, Loki by value.
		_, _ = w.Write([]byte(`{"values":[{"value":"warn","hits":9},{"value":"error","hits":3},{"value":"debug","hits":1}]}`))
	}))
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	const want = `{"data":["debug","error","warn"],"status":"success"}`
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		p.handleLabelValues(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/label/detected_level/values?query=%7Bapp%3D%22api%22%7D&start=1&end=2", nil))
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != want {
			t.Fatalf("request %d: label values = %d %s, want %s", i, rec.Code, rec.Body.String(), want)
		}
	}
	if calls != 1 {
		t.Fatalf("backend calls = %d, want 1: the second request must be served from the read cache", calls)
	}
}

func TestDetectedLevelSeriesKeepsStoredStreamLabel(t *testing.T) {
	backend := dlMetadataBackend(t)
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/series?match[]=%7Bapp%3D%22api%22%7D&start=1&end=2", nil)
	p.handleSeries(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("series status %d: %s", rec.Code, rec.Body.String())
	}
	// A detected_level stream field is an index label, as in Loki.
	if !strings.Contains(rec.Body.String(), `"detected_level":"WARNING"`) {
		t.Fatalf("series lost the detected_level stream label: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"level":"info"`) {
		t.Fatalf("series lost the level stream label: %s", rec.Body.String())
	}
}

func TestDetectedLevelDetectedFieldsAndValues(t *testing.T) {
	backend := dlMetadataBackend(t)
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	params := url.Values{"query": {`{app="api"}`}, "start": {"1"}, "end": {"2"}}

	rec := httptest.NewRecorder()
	p.handleDetectedFields(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_fields?"+params.Encode(), nil))
	var fields struct {
		Fields []struct {
			Label       string   `json:"label"`
			Type        string   `json:"type"`
			Cardinality int      `json:"cardinality"`
			Parsers     []string `json:"parsers"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fields); err != nil {
		t.Fatalf("decode detected_fields: %v: %s", err, rec.Body.String())
	}
	found := false
	for _, f := range fields.Fields {
		if f.Label != "detected_level" {
			continue
		}
		found = true
		if f.Type != "string" || f.Parsers != nil || f.Cardinality != 3 {
			t.Errorf("detected_level field = %+v, want type string, parsers null, cardinality 3 (error, unknown, warn)", f)
		}
	}
	extracted := false
	for _, f := range fields.Fields {
		if f.Label == "detected_level_extracted" {
			extracted = f.Parsers == nil && f.Cardinality == 1
		}
	}
	if !extracted {
		t.Errorf("entries of a stream with a detected_level label must list detected_level_extracted: %s", rec.Body.String())
	}
	if !found {
		t.Fatalf("detected_fields lacks detected_level: %s", rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"parsers":null`)) {
		t.Errorf("parsers must encode as null: %s", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	p.handleDetectedFieldValues(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/detected_field/detected_level/values?"+params.Encode(), nil))
	var values struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &values); err != nil {
		t.Fatalf("decode values: %v: %s", err, rec.Body.String())
	}
	sort.Strings(values.Values)
	if want := []string{"error", "unknown", "warn"}; !reflect.DeepEqual(values.Values, want) {
		t.Errorf("detected_level values = %v, want %v (derived, not the stored WARNING)", values.Values, want)
	}
}

func TestDetectedLevelDetectedLabelsFollowStreamLabels(t *testing.T) {
	backend := dlMetadataBackend(t)
	defer backend.Close()
	p := newTestProxy(t, backend.URL)
	summaries, err := p.detectNativeLabels(context.Background(), `{app="api"}`, "1", "2")
	if err != nil {
		t.Fatal(err)
	}
	// Only a detected_level stream field is a label; the derived value never is.
	if summary := summaries["detected_level"]; summary == nil || len(summary.values) != 1 {
		t.Fatalf("detected_labels = %v, want the detected_level stream label", summaries)
	}
}

// TestDetectedLevelMissingMessageRows covers rows whose _msg is VictoriaLogs'
// missing-message value: their fields came from a JSON body, so an unknown
// level word falls through to the keyword scan of the line returned for the
// row. A customised -backend-default-msg-value marks such rows too; an empty
// _msg is an empty line.
func TestDetectedLevelMissingMessageRows(t *testing.T) {
	const custom = "<none>"
	cases := []struct {
		name, msg, defaultMsg, want string
		fields                      map[string]string
	}{
		{name: "placeholder-unknown-word", msg: vlDefaultMsgValue, fields: map[string]string{"level": "notice", "msg": "error later"}, want: "error"},
		{name: "placeholder-known-word", msg: vlDefaultMsgValue, fields: map[string]string{"level": "WARN", "msg": "error later"}, want: "warn"},
		{name: "placeholder-no-level", msg: vlDefaultMsgValue, fields: map[string]string{"msg": "hello"}, want: "unknown"},
		{name: "custom-default-unknown-word", msg: custom, defaultMsg: custom, fields: map[string]string{"level": "notice", "msg": "error later"}, want: "error"},
		{name: "custom-default-not-configured", msg: custom, fields: map[string]string{"level": "notice", "msg": "error later"}, want: "notice"},
		// An empty _msg is a row VictoriaLogs stored without a message: the
		// line returned for it is rebuilt from its fields, so the fields are
		// read as JSON keys, as for the placeholder.
		{name: "empty-msg-reads-fields-as-json", msg: "", fields: map[string]string{"level": "notice", "msg": "error later"}, want: "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry := map[string]interface{}{"_time": "2026-01-01T00:00:01Z", "_msg": tc.msg, "_stream": `{app="api"}`, "app": "api"}
			for k, v := range tc.fields {
				entry[k] = v
			}
			raw, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			p := &Proxy{detectedLevelBodyScan: true, backendDefaultMsgValue: tc.defaultMsg}
			rows := p.newLogRowLevels()
			row, err := fj.ParseBytes(raw)
			if err != nil {
				t.Fatal(err)
			}
			obj, err := row.Object()
			if err != nil {
				t.Fatal(err)
			}
			stream := rows.stream(row.GetStringBytes("_stream"))
			if got := rows.fjRow(row, obj, stream).String(); got != tc.want {
				t.Fatalf("fjRow = %q, want %q", got, tc.want)
			}
			if got := rows.mapRow(entry, tc.msg, stream).String(); got != tc.want {
				t.Fatalf("mapRow = %q, want %q", got, tc.want)
			}
		})
	}
}
