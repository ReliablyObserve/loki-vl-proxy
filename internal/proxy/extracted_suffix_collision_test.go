package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// Rows as VictoriaLogs answers the translated query. A parser pipe has
// already overwritten a stored field named like a stream label with the
// line's value, so the stream label survives only in _stream.
const (
	// JSON line holding keys named like three stream labels, after unpack_json.
	extractedJSONRow = `{"_time":"2026-10-01T17:21:10Z","_msg":"{\"level\":\"debug\",\"app\":\"inner\",\"service_name\":\"inner\",\"msg\":\"m1\"}",` +
		`"_stream":"{app=\"col\",env=\"prod\",level=\"info\",service_name=\"svc\"}","app":"inner","env":"prod","level":"debug","service_name":"inner","msg":"m1"}`
	// logfmt line whose level equals the stream label's value, after unpack_logfmt.
	extractedLogfmtRow = `{"_time":"2026-10-01T17:21:11Z","_msg":"level=info app=inner msg=m2",` +
		`"_stream":"{app=\"col\",env=\"prod\",level=\"info\",service_name=\"svc\"}","app":"inner","env":"prod","level":"info","service_name":"svc","msg":"m2"}`
	// The same logfmt line after extract_regexp: only the capture's field is
	// written, and it holds the stream label's value.
	extractedRegexpRow = `{"_time":"2026-10-01T17:21:11Z","_msg":"level=info app=inner msg=m2",` +
		`"_stream":"{app=\"col\",env=\"prod\",level=\"info\",service_name=\"svc\"}","app":"col","env":"prod","level":"info","service_name":"svc"}`
	// A line with no keys of its own: every stored field repeats a stream label.
	extractedPlainRow = `{"_time":"2026-10-01T17:21:12Z","_msg":"hello",` +
		`"_stream":"{app=\"col\",env=\"prod\",level=\"info\",service_name=\"svc\"}","app":"col","env":"prod","level":"info","service_name":"svc"}`
	// Structured metadata service.name (OTel) beside the service_name the
	// proxy derives for a stream without one.
	extractedMetadataRow = `{"_time":"2026-10-01T17:21:13Z","_msg":"hello",` +
		`"_stream":"{app=\"smcol\",env=\"prod\"}","app":"smcol","env":"prod","service.name":"smsvc","trace_id":"t1"}`
)

type extractedEntry struct {
	stream, sm, parsed map[string]string
}

// extractedEntries runs query_range on the path (buffered, streamed or
// windowed) and returns each entry's labels by line.
func extractedEntries(t *testing.T, path, query string, categorize bool, rows ...string) map[string]extractedEntry {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(strings.Join(rows, "\n") + "\n"))
	}))
	defer backend.Close()
	p := lineFieldsProxy(t, backend.URL, path)
	q := url.Values{"query": {query}, "start": {"1790871720000000000"}, "end": {"1790875320000000000"}, "limit": {"10"}}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil)
	if categorize {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%s %s: status %d: %s", path, query, w.Code, w.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s %s: decode: %v", path, query, err)
	}
	out := map[string]extractedEntry{}
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			var line string
			_ = json.Unmarshal(v[1], &line)
			var meta struct {
				StructuredMetadata map[string]string `json:"structuredMetadata"`
				Parsed             map[string]string `json:"parsed"`
			}
			if len(v) > 2 {
				_ = json.Unmarshal(v[2], &meta)
			}
			delete(meta.StructuredMetadata, "detected_level")
			out[line] = extractedEntry{stream: r.Stream, sm: meta.StructuredMetadata, parsed: meta.Parsed}
		}
	}
	return out
}

// TestLogQuery_ParsedKeyNamedLikeStreamLabelGetsExtractedSuffix: Loki leaves
// a stream label alone when a parser stage reads a key of the same name and
// exposes the parsed value as name_extracted (parser.go duplicateSuffix); a
// structured metadata key named like the stream label's gets the suffix too.
// VictoriaLogs holds one stored field of each name, which unpack_* overwrote
// with the line's value, so the proxy reads the stream label from _stream and
// renames the stored field. A repeated stream label is no entry label. On the
// buffered, streamed and windowed paths, in both encodings.
//
// conformance: semantics/extracted-suffix-collision, profiles/structured-metadata-label-collision, loki_api_v1_query_range, loki-compatible-profile
func TestLogQuery_ParsedKeyNamedLikeStreamLabelGetsExtractedSuffix(t *testing.T) {
	stream := map[string]string{"app": "col", "env": "prod", "level": "info", "service_name": "svc"}
	merged := func(extra map[string]string) map[string]string {
		out := map[string]string{"detected_level": "info"}
		for k, v := range stream {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	jsonParsed := map[string]string{"app_extracted": "inner", "level_extracted": "debug", "service_name_extracted": "inner", "msg": "m1"}
	logfmtParsed := map[string]string{"app_extracted": "inner", "level_extracted": "info", "msg": "m2"}

	for _, path := range []string{"buffered", "streamed", "windowed"} {
		t.Run(path, func(t *testing.T) {
			cases := []struct {
				name, query string
				rows        []string
				want        map[string]extractedEntry
			}{
				{"json", `{app="col"} | json`, []string{extractedJSONRow, extractedPlainRow}, map[string]extractedEntry{
					`{"level":"debug","app":"inner","service_name":"inner","msg":"m1"}`: {stream: stream, parsed: jsonParsed},
					"hello": {stream: stream},
				}},
				{"logfmt value equal to the stream label", `{app="col"} | logfmt`, []string{extractedLogfmtRow}, map[string]extractedEntry{
					"level=info app=inner msg=m2": {stream: stream, parsed: logfmtParsed},
				}},
				{"regexp capture equal to the stream label", `{app="col"} | regexp "level=(?P<level>\\w+)"`, []string{extractedRegexpRow}, map[string]extractedEntry{
					"level=info app=inner msg=m2": {stream: stream, parsed: map[string]string{"level_extracted": "info"}},
				}},
				{"extraction list", `{app="col"} | json level, app`, []string{extractedJSONRow}, map[string]extractedEntry{
					`{"level":"debug","app":"inner","service_name":"inner","msg":"m1"}`: {stream: stream, parsed: map[string]string{"app_extracted": "inner", "level_extracted": "debug"}},
				}},
				{"drop of the renamed label", `{app="col"} | json | drop level_extracted`, []string{extractedJSONRow}, map[string]extractedEntry{
					`{"level":"debug","app":"inner","service_name":"inner","msg":"m1"}`: {stream: stream, parsed: map[string]string{"app_extracted": "inner", "service_name_extracted": "inner", "msg": "m1"}},
				}},
				{"no parser", `{app="col"}`, []string{extractedJSONRow, extractedPlainRow}, map[string]extractedEntry{
					`{"level":"debug","app":"inner","service_name":"inner","msg":"m1"}`: {stream: stream},
					"hello": {stream: stream},
				}},
				{"structured metadata against the derived service_name", `{app="smcol"}`, []string{extractedMetadataRow}, map[string]extractedEntry{
					"hello": {stream: map[string]string{"app": "smcol", "env": "prod", "service_name": "smcol"}, sm: map[string]string{"service_name_extracted": "smsvc", "trace_id": "t1"}},
				}},
			}
			for _, c := range cases {
				for _, categorize := range []bool{true, false} {
					got := extractedEntries(t, path, c.query, categorize, c.rows...)
					if len(got) != len(c.want) {
						t.Fatalf("%s categorize=%v: %d entries, want %d: %+v", c.name, categorize, len(got), len(c.want), got)
					}
					for line, w := range c.want {
						g, ok := got[line]
						if !ok {
							t.Fatalf("%s categorize=%v: no entry %q in %+v", c.name, categorize, line, got)
						}
						wantStream, wantSM, wantParsed := w.stream, w.sm, w.parsed
						if !categorize {
							// Without categorize-labels every label of an entry joins its stream.
							wantStream = merged(nil)
							if strings.HasPrefix(c.name, "structured") {
								wantStream = map[string]string{"app": "smcol", "env": "prod", "service_name": "smcol", "detected_level": "unknown"}
							}
							for _, m := range []map[string]string{w.sm, w.parsed} {
								for k, v := range m {
									wantStream[k] = v
								}
							}
							wantSM, wantParsed = nil, nil
						}
						if !reflect.DeepEqual(g.stream, wantStream) || len(g.sm) != len(wantSM) || len(g.parsed) != len(wantParsed) ||
							(len(wantSM) > 0 && !reflect.DeepEqual(g.sm, wantSM)) || (len(wantParsed) > 0 && !reflect.DeepEqual(g.parsed, wantParsed)) {
							t.Errorf("%s categorize=%v %q:\n got stream %v sm %v parsed %v\nwant stream %v sm %v parsed %v",
								c.name, categorize, line, g.stream, g.sm, g.parsed, wantStream, wantSM, wantParsed)
						}
					}
				}
			}
		})
	}
}

// A translated `| keep x_extracted` also keeps the stored field x, for the
// proxy to rename; when the entry has no collision x is no label Loki keeps.
//
// conformance: semantics/extracted-suffix-collision, loki-compatible-profile
func TestKeepAndDropEntryFieldsOfRenamedLabels(t *testing.T) {
	pf := map[string]string{"level": "debug", "level_extracted": "debug", "msg": "m"}
	sm := map[string]string{"level": "x"}
	keepEntryFields([]string{"level_extracted"}, sm, pf)
	if !reflect.DeepEqual(pf, map[string]string{"level_extracted": "debug", "msg": "m"}) || len(sm) != 0 {
		t.Errorf("keep level_extracted left pf %v sm %v", pf, sm)
	}
	pf = map[string]string{"level": "debug", "level_extracted": "debug"}
	keepEntryFields([]string{"level_extracted", "level"}, nil, pf)
	if len(pf) != 2 {
		t.Errorf("keep level_extracted, level removed a kept label: %v", pf)
	}
	pf = map[string]string{"level_extracted": "debug", "msg": "m"}
	dropEntryFields([]string{"level_extracted"}, nil, pf)
	if !reflect.DeepEqual(pf, map[string]string{"msg": "m"}) {
		t.Errorf("drop level_extracted left %v", pf)
	}
}

// The live tail converts rows with the same function as a query response, so
// a parsed key named like a stream label is renamed there too: into the
// stream's labels in the default frame, into the entry's parsed labels with
// categorize-labels.
//
// conformance: semantics/extracted-suffix-collision, loki_api_v1_tail
func TestTail_ParsedKeyNamedLikeStreamLabelGetsExtractedSuffix(t *testing.T) {
	p := lineFieldsProxy(t, "http://127.0.0.1:1", "buffered")
	rows := []byte(extractedJSONRow + "\n")

	entries, err := p.tailEntries(t.Context(), p.newTailPipeline(`{app="col"} | json`, false), rows)
	if err != nil || len(entries) != 1 {
		t.Fatalf("default tail entries %v, %v", entries, err)
	}
	if got := entries[0].stream; got["level"] != "info" || got["level_extracted"] != "debug" || got["app_extracted"] != "inner" || got["service_name_extracted"] != "inner" {
		t.Errorf("default tail stream %v", got)
	}

	entries, err = p.tailEntries(t.Context(), p.newTailPipeline(`{app="col"} | json`, true), rows)
	if err != nil || len(entries) != 1 {
		t.Fatalf("categorized tail entries %v, %v", entries, err)
	}
	tuple, _ := entries[0].value.([]interface{})
	meta, _ := tuple[2].(map[string]interface{})
	parsed, _ := meta["parsed"].(map[string]string)
	if entries[0].stream["level"] != "info" || parsed["level_extracted"] != "debug" || parsed["service_name_extracted"] != "inner" || parsed["level"] != "" {
		t.Errorf("categorized tail stream %v parsed %v", entries[0].stream, parsed)
	}
}

// A label the query sets itself under the name a collision would take wins:
// the stored field of the label_format target is kept, the renamed parsed key
// is not written over it.
//
// conformance: semantics/extracted-suffix-collision, loki-compatible-profile
func TestLogQuery_LabelSetByTheQueryWinsOverTheRename(t *testing.T) {
	row := strings.Replace(extractedJSONRow, `"msg":"m1"}`, `"msg":"m1","level_extracted":"z"}`, 1)
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		for i := 0; i < 20; i++ { // map order must not decide
			got := extractedEntries(t, path, `{app="col"} | json | label_format level_extracted="z"`, true, row)
			for _, e := range got {
				if e.parsed["level_extracted"] != "z" || e.parsed["app_extracted"] != "inner" {
					t.Fatalf("%s: parsed %v, want the query's level_extracted=z beside app_extracted=inner", path, e.parsed)
				}
			}
		}
	}
}
