package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// parsedStreamsFixtureRow is one log line the fake VictoriaLogs backend serves.
type parsedStreamsFixtureRow struct {
	ts     int64
	env    string
	fields [][2]string // parser-visible fields in line order
}

func (row parsedStreamsFixtureRow) line(logfmt bool) string {
	parts := make([]string, 0, len(row.fields))
	for _, f := range row.fields {
		if logfmt {
			parts = append(parts, f[0]+"="+f[1])
			continue
		}
		value := strconv.Quote(f[1])
		if f[0] == "status" || f[0] == "duration_ms" {
			value = f[1]
		}
		parts = append(parts, strconv.Quote(f[0])+":"+value)
	}
	if logfmt {
		return strings.Join(parts, " ")
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (row parsedStreamsFixtureRow) field(name string) string {
	for _, f := range row.fields {
		if f[0] == name {
			return f[1]
		}
	}
	return ""
}

// newParsedStreamsVLBackend emulates /select/logsql/query for the fixture: it
// honours start/end, the parser pipes (unpack_json / unpack_logfmt expose the
// line fields as top-level fields), the numeric filters used by the tests, the
// _time sort direction and the row limit.
func newParsedStreamsVLBackend(t *testing.T, rows []parsedStreamsFixtureRow, logfmt bool, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/query" {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		calls.Add(1)
		_ = r.ParseForm()
		query := r.Form.Get("query")
		startNs, _ := strconv.ParseInt(r.Form.Get("start"), 10, 64)
		endNs, _ := strconv.ParseInt(r.Form.Get("end"), 10, 64)
		limit, _ := strconv.Atoi(r.Form.Get("limit"))
		unpack := strings.Contains(query, "unpack_json") || strings.Contains(query, "unpack_logfmt")
		extract := strings.Contains(query, "extract_regexp")

		selected := make([]parsedStreamsFixtureRow, 0, len(rows))
		for _, row := range rows {
			if row.ts < startNs || row.ts > endNs {
				continue
			}
			if strings.Contains(query, "status:>=400") {
				if status, _ := strconv.Atoi(row.field("status")); status < 400 {
					continue
				}
			}
			if strings.Contains(query, "duration_ms:>5000") {
				if duration, _ := strconv.Atoi(row.field("duration_ms")); duration <= 5000 {
					continue
				}
			}
			selected = append(selected, row)
		}
		desc := strings.Contains(query, "_time desc")
		sort.SliceStable(selected, func(i, j int) bool {
			if desc {
				return selected[i].ts > selected[j].ts
			}
			return selected[i].ts < selected[j].ts
		})
		if limit > 0 && len(selected) > limit {
			selected = selected[:limit]
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, row := range selected {
			obj := map[string]string{
				"_time":        time.Unix(0, row.ts).UTC().Format(time.RFC3339Nano),
				"_msg":         row.line(logfmt),
				"_stream":      `{app="checkout",env="` + row.env + `",service.name="checkout"}`,
				"app":          "checkout",
				"env":          row.env,
				"service.name": "checkout",
			}
			if unpack {
				for _, f := range row.fields {
					obj[f[0]] = f[1]
				}
			}
			if extract {
				obj["route"] = row.field("path")
			}
			encoded, _ := json.Marshal(obj)
			_, _ = w.Write(append(encoded, '\n'))
		}
	}))
}

func parsedStreamsFixture(start int64, logfmt bool) []parsedStreamsFixtureRow {
	statuses := []string{"200", "404", "500", "503"}
	levels := []string{"info", "warn", "error"}
	rows := make([]parsedStreamsFixtureRow, 0, 30)
	for i := 0; i < 30; i++ {
		fields := [][2]string{
			{"level", levels[i%3]},
			{"msg", "req" + strconv.Itoa(i%3)},
			{"path", []string{"/a", "/b"}[i%2]},
			{"status", statuses[i%4]},
		}
		if logfmt {
			fields = append(fields, [2]string{"duration_ms", strconv.Itoa(1000 * (i % 9))})
		}
		rows = append(rows, parsedStreamsFixtureRow{
			ts:     start + int64(i)*int64(5*time.Minute),
			env:    []string{"prod", "dev"}[i%2],
			fields: fields,
		})
	}
	return rows
}

type parsedStreamsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType    string   `json:"resultType"`
		EncodingFlags []string `json:"encodingFlags"`
		Result        []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func runParsedStreamsQuery(t *testing.T, p *Proxy, query string, start, end int64, direction string, limit int, categorized bool) parsedStreamsResponse {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start, 10))
	params.Set("end", strconv.FormatInt(end, 10))
	params.Set("limit", strconv.Itoa(limit))
	if direction != "" {
		params.Set("direction", direction)
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("query %q: status=%d body=%s", query, rec.Code, rec.Body.String())
	}
	var resp parsedStreamsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v body=%s", query, err, rec.Body.String())
	}
	if resp.Status != "success" || resp.Data.ResultType != "streams" {
		t.Fatalf("query %q: unexpected envelope: %s", query, rec.Body.String())
	}
	return resp
}

// streamsByLabels flattens a streams response into label-set key -> entries,
// which is how Loki clients (Grafana's Loki datasource) consume stream objects.
func streamsByLabels(resp parsedStreamsResponse) map[string][]string {
	out := make(map[string][]string, len(resp.Data.Result))
	for _, stream := range resp.Data.Result {
		key := canonicalLabelsKey(stream.Stream)
		for _, value := range stream.Values {
			raw, _ := json.Marshal(value)
			out[key] = append(out[key], string(raw))
		}
	}
	return out
}

// TestQueryRangeWindow_ParsedLabelStreamsMatchSingleWindow guards the windowed
// log-query path against collapsing parser-extracted labels. Loki keys log
// streams by stream labels plus labels extracted by | json / | logfmt (for
// responses without categorize-labels metadata), so a range that crosses the
// split interval must return exactly the streams the single-request path does.
func TestQueryRangeWindow_ParsedLabelStreamsMatchSingleWindow(t *testing.T) {
	start := time.Now().Add(-26 * time.Hour).UTC().Truncate(time.Hour).Add(30 * time.Minute).UnixNano()
	end := start + int64(150*time.Minute) - 1

	cases := []struct {
		name   string
		query  string
		logfmt bool
		// wantStreams is Loki's stream count for the full range without
		// categorize-labels (distinct stream + extracted label sets).
		wantStreams int
		wantEntries int
	}{
		{name: "json", query: `{app="checkout"} | json`, wantStreams: 12, wantEntries: 30},
		{name: "json_status_filter", query: `{app="checkout"} | json | status >= 400`, wantStreams: 9, wantEntries: 22},
		{name: "json_drop_matcher", query: `{app="checkout"} | json | drop level="info"`, wantStreams: 12, wantEntries: 30},
		{name: "json_bare_drop_stream_label", query: `{app="checkout"} | json | drop env`, wantStreams: 12, wantEntries: 30},
		{name: "json_regexp_capture", query: `{app="checkout"} | json | regexp "(?P<route>/[a-z]+)"`, wantStreams: 12, wantEntries: 30},
		{name: "json_line_format", query: `{app="checkout"} | json | line_format "{{.path}}"`, wantStreams: 12, wantEntries: 30},
		{name: "logfmt", query: `{app="checkout"} | logfmt`, logfmt: true, wantStreams: 30, wantEntries: 30},
		{name: "logfmt_duration_filter", query: `{app="checkout"} | logfmt | duration_ms > 5000`, logfmt: true, wantStreams: 9, wantEntries: 9},
	}
	for _, tc := range cases {
		rows := parsedStreamsFixture(start, tc.logfmt)
		for _, style := range []LabelStyle{LabelStylePassthrough, LabelStyleUnderscores} {
			for _, categorized := range []bool{false, true} {
				for _, direction := range []string{"backward", "forward"} {
					for _, limit := range []int{1000, 7} {
						name := fmt.Sprintf("%s/%s/categorized=%t/%s/limit=%d", tc.name, style, categorized, direction, limit)
						t.Run(name, func(t *testing.T) {
							var windowCalls, singleCalls atomic.Int64
							windowBackend := newParsedStreamsVLBackend(t, rows, tc.logfmt, &windowCalls)
							defer windowBackend.Close()
							singleBackend := newParsedStreamsVLBackend(t, rows, tc.logfmt, &singleCalls)
							defer singleBackend.Close()

							windowed := newWindowingTestProxy(t, windowBackend.URL)
							single, err := New(Config{BackendURL: singleBackend.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
							if err != nil {
								t.Fatalf("create single-request proxy: %v", err)
							}
							windowed.emitStructuredMetadata = categorized
							single.emitStructuredMetadata = categorized
							windowed.labelTranslator = NewLabelTranslator(style, nil)
							single.labelTranslator = NewLabelTranslator(style, nil)

							got := runParsedStreamsQuery(t, windowed, tc.query, start, end, direction, limit, categorized)
							want := runParsedStreamsQuery(t, single, tc.query, start, end, direction, limit, categorized)
							if windowCalls.Load() < 2 {
								t.Fatalf("expected the windowed path to split the range, backend calls=%d", windowCalls.Load())
							}
							if singleCalls.Load() != 1 {
								t.Fatalf("expected one backend call on the single-request path, got %d", singleCalls.Load())
							}
							if !reflect.DeepEqual(got.Data, want.Data) {
								gotJSON, _ := json.Marshal(got.Data)
								wantJSON, _ := json.Marshal(want.Data)
								t.Fatalf("windowed response differs from single-request response\nwindowed: %s\nsingle:   %s", gotJSON, wantJSON)
							}

							wantEntries := min(tc.wantEntries, limit)
							entries := 0
							for _, stream := range got.Data.Result {
								entries += len(stream.Values)
							}
							if entries != wantEntries {
								t.Fatalf("entries=%d want=%d", entries, wantEntries)
							}
							if categorized {
								// Loki keeps extracted labels in the per-entry metadata and
								// keys categorize-labels streams by the stream labels only.
								for _, stream := range got.Data.Result {
									for _, parsed := range []string{"msg", "path", "status", "duration_ms"} {
										if _, ok := stream.Stream[parsed]; ok {
											t.Fatalf("categorize-labels stream must not carry extracted label %q: %v", parsed, stream.Stream)
										}
									}
								}
								return
							}
							if limit >= tc.wantEntries && len(got.Data.Result) != tc.wantStreams {
								t.Fatalf("streams=%d want=%d", len(got.Data.Result), tc.wantStreams)
							}
							for _, stream := range got.Data.Result {
								for _, parsed := range []string{"msg", "path", "status"} {
									if stream.Stream[parsed] == "" {
										t.Fatalf("stream is missing extracted label %q: %v", parsed, stream.Stream)
									}
								}
								if tc.logfmt && stream.Stream["duration_ms"] == "" {
									t.Fatalf("stream is missing extracted logfmt label duration_ms: %v", stream.Stream)
								}
								if strings.Contains(tc.query, "regexp") && stream.Stream["route"] != stream.Stream["path"] {
									t.Fatalf("stream is missing regexp capture label route: %v", stream.Stream)
								}
								if strings.Contains(tc.query, "drop env") && stream.Stream["env"] != "" {
									t.Fatalf("bare drop left stream label env: %v", stream.Stream)
								}
							}
						})
					}
				}
			}
		}
	}
}

// TestStreamResponse_ParsedLabelStreamsMatchBufferedResponse covers
// -stream-response: the chunked writer emits one stream object per entry, so
// it is compared by label set, which is how Loki clients merge stream objects.
func TestStreamResponse_ParsedLabelStreamsMatchBufferedResponse(t *testing.T) {
	start := time.Now().Add(-26 * time.Hour).UTC().Truncate(time.Hour).Add(30 * time.Minute).UnixNano()
	end := start + int64(150*time.Minute) - 1
	for _, tc := range []struct {
		query  string
		logfmt bool
	}{
		{query: `{app="checkout"} | json`},
		{query: `{app="checkout"} | json | status >= 400`},
		{query: `{app="checkout"} | logfmt`, logfmt: true},
		{query: `{app="checkout"} | logfmt | duration_ms > 5000`, logfmt: true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			rows := parsedStreamsFixture(start, tc.logfmt)
			var calls atomic.Int64
			backend := newParsedStreamsVLBackend(t, rows, tc.logfmt, &calls)
			defer backend.Close()
			buffered, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error"})
			if err != nil {
				t.Fatal(err)
			}
			streaming, err := New(Config{BackendURL: backend.URL, Cache: cache.NewDisabled(), LogLevel: "error", StreamResponse: true})
			if err != nil {
				t.Fatal(err)
			}
			want := streamsByLabels(runParsedStreamsQuery(t, buffered, tc.query, start, end, "backward", 1000, false))
			got := streamsByLabels(runParsedStreamsQuery(t, streaming, tc.query, start, end, "backward", 1000, false))
			if len(want) < 2 {
				t.Fatalf("expected extracted labels to split streams, got %d label sets", len(want))
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("stream-response label sets differ from buffered response\nstreaming: %v\nbuffered:  %v", got, want)
			}
		})
	}
}

// TestQueryRangeWindowCacheKey_SeparatesParsedStreamShapes ensures cached window
// fragments, which hold resolved stream labels, are only shared by queries
// with the same LogQL pipeline, even when the queries translate to the same
// LogsQL (conditional | drop and | keep emit none).
func TestQueryRangeWindowCacheKey_SeparatesParsedStreamShapes(t *testing.T) {
	p := newWindowingTestProxy(t, "http://127.0.0.1:1")
	window := queryRangeWindow{startNs: 1, endNs: 2}
	keyFor := func(logql string) string {
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?query="+url.QueryEscape(logql), nil)
		return p.queryRangeWindowCacheKey(req, `app:="checkout"`, "100", window, newLogQueryShape(logql), false, false)
	}
	base := keyFor(`{app="checkout"} | json`)
	for _, other := range []string{
		`{app="checkout"}`,
		`{app="checkout"} | json | drop level="debug"`,
		`{app="checkout"} | json | keep level="error"`,
		`{app="checkout"} | json | drop level`,
	} {
		if keyFor(other) == base {
			t.Fatalf("%q shares a window cache key with | json", other)
		}
	}
	if keyFor(`{app="checkout"}  |  json`) != base {
		t.Fatal("queries that differ only in whitespace must share a window cache key")
	}
	if strings.Contains(base, "parsed-streams=") {
		t.Fatalf("window cache key still uses the boolean shape marker: %s", base)
	}
}

// TestQueryRangeWindow_CachedFragmentsNotReusedAcrossDropKeep runs a | json
// query and then the same query with a conditional | drop or | keep on one
// caching proxy. A conditional | drop translates to identical LogsQL; the
// second query must still fetch its own fragments and match an uncached
// proxy's response.
func TestQueryRangeWindow_CachedFragmentsNotReusedAcrossDropKeep(t *testing.T) {
	start := time.Now().Add(-26 * time.Hour).UTC().Truncate(time.Hour).Add(30 * time.Minute).UnixNano()
	end := start + int64(150*time.Minute) - 1
	rows := parsedStreamsFixture(start, false)
	const first = `{app="checkout"} | json`
	for _, tc := range []struct {
		second     string
		sameLogsQL bool // conditional | keep still emits a LogsQL fields pipe
	}{
		{second: `{app="checkout"} | json | drop level="info"`, sameLogsQL: true},
		{second: `{app="checkout"} | json | keep level="error"`},
	} {
		second := tc.second
		t.Run(second, func(t *testing.T) {
			var cachedCalls, freshCalls atomic.Int64
			cachedBackend := newParsedStreamsVLBackend(t, rows, false, &cachedCalls)
			defer cachedBackend.Close()
			freshBackend := newParsedStreamsVLBackend(t, rows, false, &freshCalls)
			defer freshBackend.Close()

			cached := newWindowingTestProxy(t, cachedBackend.URL)
			firstLogsQL, err := cached.translateQueryWithContext(context.Background(), first)
			if err != nil {
				t.Fatal(err)
			}
			secondLogsQL, err := cached.translateQueryWithContext(context.Background(), second)
			if err != nil {
				t.Fatal(err)
			}
			if (firstLogsQL == secondLogsQL) != tc.sameLogsQL {
				t.Fatalf("precondition: LogsQL equality=%t want %t: %q and %q", firstLogsQL == secondLogsQL, tc.sameLogsQL, firstLogsQL, secondLogsQL)
			}

			runParsedStreamsQuery(t, cached, first, start, end, "backward", 1000, false)
			callsAfterFirst := cachedCalls.Load()
			if callsAfterFirst < 2 {
				t.Fatalf("expected windowed fetches for the first query, got %d", callsAfterFirst)
			}
			// Fragments are cached: repeating the first query must not refetch.
			runParsedStreamsQuery(t, cached, first, start, end, "backward", 1000, false)
			if cachedCalls.Load() != callsAfterFirst {
				t.Fatalf("expected the repeated query to be served from cached fragments, calls %d -> %d", callsAfterFirst, cachedCalls.Load())
			}

			got := runParsedStreamsQuery(t, cached, second, start, end, "backward", 1000, false)
			if cachedCalls.Load() != 2*callsAfterFirst {
				t.Fatalf("second query reused cached fragments: calls after first=%d, after second=%d", callsAfterFirst, cachedCalls.Load())
			}
			fresh := newWindowingTestProxy(t, freshBackend.URL)
			want := runParsedStreamsQuery(t, fresh, second, start, end, "backward", 1000, false)
			if !reflect.DeepEqual(got.Data, want.Data) {
				gotJSON, _ := json.Marshal(got.Data)
				wantJSON, _ := json.Marshal(want.Data)
				t.Fatalf("second query differs from an uncached proxy\ncached: %s\nfresh:  %s", gotJSON, wantJSON)
			}
			if firstResp := runParsedStreamsQuery(t, fresh, first, start, end, "backward", 1000, false); reflect.DeepEqual(firstResp.Data, want.Data) {
				t.Fatal("precondition: the drop/keep stage must change the response streams")
			}
		})
	}
}

// parsedStreamsBenchBody returns 500 VictoriaLogs rows of a | json query
// response (unpacked fields present) for the conversion benchmarks.
func parsedStreamsBenchBody() []byte {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	rows := parsedStreamsFixture(start, false)
	var body []byte
	for i := 0; i < 500; i++ {
		row := rows[i%len(rows)]
		obj := map[string]string{
			"_time":   time.Unix(0, start+int64(i)*int64(time.Second)).UTC().Format(time.RFC3339Nano),
			"_msg":    row.line(false),
			"_stream": `{app="checkout",env="` + row.env + `"}`,
			"app":     "checkout",
			"env":     row.env,
		}
		for _, f := range row.fields {
			obj[f[0]] = f[1]
		}
		encoded, _ := json.Marshal(obj)
		body = append(append(body, encoded...), '\n')
	}
	return body
}

// BenchmarkQueryRangeWindowEntries_ParsedJSON measures one window fragment of
// a | json log query: entry conversion plus the final stream grouping.
func BenchmarkQueryRangeWindowEntries_ParsedJSON(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	body := parsedStreamsBenchBody()
	query := `{app="checkout"} | json`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := p.vlLogsToLokiWindowEntries(body, query, false, false)
		_ = groupQueryRangeWindowEntries(entries, "backward", false, false)
	}
}

// BenchmarkLogQueryStreams_ParsedJSON is the single-request counterpart of
// BenchmarkQueryRangeWindowEntries_ParsedJSON over the same rows.
func BenchmarkLogQueryStreams_ParsedJSON(b *testing.B) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", Cache: cache.NewDisabled(), LogLevel: "error"})
	if err != nil {
		b.Fatal(err)
	}
	body := parsedStreamsBenchBody()
	query := `{app="checkout"} | json`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := p.vlReaderToLokiStreams(bytes.NewReader(body), query, "", false, false, false); err != nil {
			b.Fatal(err)
		}
	}
}
