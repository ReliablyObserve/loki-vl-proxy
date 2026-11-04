package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

// keepLogRow is the row VictoriaLogs returns for a keep query: the stored
// line, the stream and the fields the translated `| fields` left.
type keepLogRow struct {
	stream string
	msg    string
	fields map[string]string
}

func (r keepLogRow) json() string {
	row := map[string]string{"_time": "2026-10-01T17:21:10Z", "_msg": r.msg, "_stream": r.stream}
	for k, v := range r.fields {
		row[k] = v
	}
	b, _ := json.Marshal(row)
	return string(b)
}

// keepLogLabels runs a log query on route ("query_range" or "query") and
// returns the labels of its entries: by category (I stream, S structured
// metadata, P parsed) with categorize-labels, else the stream labels.
func keepLogLabels(t *testing.T, p *Proxy, route, query string, categorized bool) (map[string]string, map[string]string) {
	t.Helper()
	q := url.Values{"query": {query}, "limit": {"10"}}
	if route == "query" {
		q.Set("time", "1790875320000000000")
	} else {
		q.Set("start", "1790871720000000000")
		q.Set("end", "1790875320000000000")
	}
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/"+route+"?"+q.Encode(), nil)
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	w := httptest.NewRecorder()
	if route == "query" {
		p.handleQuery(w, req)
	} else {
		p.handleQueryRange(w, req)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("%s %s: status %d: %s", route, query, w.Code, w.Body.String())
	}
	var resp struct {
		Data struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || len(resp.Data.Result) == 0 {
		t.Fatalf("%s %s: want entries: %v %s", route, query, err, w.Body.String())
	}
	// The windowed path reads the fake's one row once per window: every
	// entry is that row.
	types := map[string]string{}
	stream := resp.Data.Result[0].Stream
	for _, r := range resp.Data.Result {
		if !reflect.DeepEqual(r.Stream, stream) {
			t.Fatalf("%s %s: entries in different streams: %s", route, query, w.Body.String())
		}
		for k := range r.Stream {
			types[k] = "I"
		}
		for _, v := range r.Values {
			if len(v) < 3 {
				continue
			}
			var meta struct {
				StructuredMetadata map[string]string `json:"structuredMetadata"`
				Parsed             map[string]string `json:"parsed"`
			}
			_ = json.Unmarshal(v[2], &meta)
			for k := range meta.StructuredMetadata {
				types[k] = "S"
			}
			for k := range meta.Parsed {
				types[k] = "P"
			}
		}
	}
	return types, stream
}

// Loki's keep stage keeps a label only when its exact name is listed
// (pkg/logql/log/keep_labels.go:15-45): a parsed key renamed name_extracted
// after a collision with a stream label (pkg/logql/log/parser.go
// duplicateSuffix) is gone unless the keep names name_extracted. The labels
// of each entry equal Loki's on the buffered, streamed and windowed paths, in
// both encodings.
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range
func TestLogQuery_KeepDropsUnlistedExtractedLabel(t *testing.T) {
	const (
		prodError = `{level="error",namespace="prod"}`
		jsonLine  = `{"level":"error","msg":"boom"}`
		payment   = `{app="payment-service",env="production",level="error"}`
		logfmt    = `level=error msg="payment failed"`
	)
	for _, tc := range []struct {
		name  string
		query string
		row   keepLogRow
		// want are the label categories with categorize-labels, plain the
		// stream labels without.
		want, plain map[string]string
	}{
		{"json key colliding with the stream label",
			`{namespace="prod"} | json | keep level`,
			keepLogRow{prodError, jsonLine, map[string]string{"level": "error"}},
			map[string]string{"level": "I"}, map[string]string{"level": "error"}},
		{"logfmt key colliding with the stream label",
			`{app="payment-service",env="production"} | logfmt | keep level, msg`,
			keepLogRow{payment, logfmt, map[string]string{"level": "error", "msg": "payment failed"}},
			map[string]string{"level": "I", "msg": "P"}, map[string]string{"level": "error", "msg": "payment failed"}},
		{"label filter before the keep",
			`{app="payment-service",env="production"} | logfmt | level="error" | keep level, msg`,
			keepLogRow{payment, logfmt, map[string]string{"level": "error", "msg": "payment failed"}},
			map[string]string{"level": "I", "msg": "P"}, map[string]string{"level": "error", "msg": "payment failed"}},
		{"error labels named next to it",
			`{namespace="prod"} | json | keep level, __error__`,
			keepLogRow{prodError, jsonLine, map[string]string{"level": "error"}},
			map[string]string{"level": "I"}, map[string]string{"level": "error"}},
		{"the renamed label named",
			`{namespace="prod"} | json | keep level_extracted`,
			keepLogRow{prodError, jsonLine, map[string]string{"level": "error"}},
			map[string]string{"level_extracted": "P"}, map[string]string{"level_extracted": "error"}},
		{"no collision: the parsed key is kept",
			`{namespace="prod"} | json | keep level`,
			keepLogRow{`{namespace="prod"}`, jsonLine, map[string]string{"level": "error"}},
			map[string]string{"level": "P"}, map[string]string{"level": "error"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/select/logsql/query" {
					_, _ = w.Write([]byte(tc.row.json() + "\n"))
					return
				}
				_, _ = w.Write([]byte(`{"values":[]}`))
			}))
			defer backend.Close()
			for _, path := range []string{"buffered", "streamed", "windowed"} {
				// Loki answers a log query on query_range only (an instant log
				// query is a 400 on both).
				for _, route := range []string{"query_range"} {
					p := lineFieldsProxy(t, backend.URL, path)
					got, _ := keepLogLabels(t, p, route, tc.query, true)
					if !reflect.DeepEqual(got, tc.want) {
						t.Errorf("%s %s categorized: %v, want %v", path, route, got, tc.want)
					}
					_, plain := keepLogLabels(t, p, route, tc.query, false)
					if !reflect.DeepEqual(plain, tc.plain) {
						t.Errorf("%s %s: stream %v, want %v", path, route, plain, tc.plain)
					}
				}
			}
		})
	}
}

// A stage after the keep: a parser adds labels again, so a key colliding
// with the kept stream label is renamed and kept (Loki parses after the
// keep), a label_format adds only its own target, and a later keep cannot
// bring back what an earlier one removed.
// conformance: semantics/keep-leaves-labels-loki-drops, loki_api_v1_query_range
func TestLogQuery_KeepThenStageAfterIt(t *testing.T) {
	const prodError = `{level="error",namespace="prod"}`
	for _, tc := range []struct {
		query  string
		row    keepLogRow
		absent string
		has    string
	}{
		{`{namespace="prod"} | keep level | json`,
			keepLogRow{prodError, `{"level":"error","msg":"boom"}`, map[string]string{"level": "error", "msg": "boom"}},
			"", "level_extracted"},
		{`{namespace="prod"} | json | keep level | label_format a="b"`,
			keepLogRow{prodError, `{"level":"error","msg":"boom"}`, map[string]string{"level": "error"}},
			"level_extracted", ""},
		{`{namespace="prod"} | json | keep msg | keep level_extracted, msg`,
			keepLogRow{prodError, `{"level":"error","msg":"boom"}`, map[string]string{"msg": "boom"}},
			"level_extracted", "msg"},
	} {
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/select/logsql/query" {
				_, _ = w.Write([]byte(tc.row.json() + "\n"))
				return
			}
			_, _ = w.Write([]byte(`{"values":[]}`))
		}))
		p := lineFieldsProxy(t, backend.URL, "buffered")
		got, _ := keepLogLabels(t, p, "query_range", tc.query, true)
		backend.Close()
		if tc.absent != "" && got[tc.absent] != "" {
			t.Errorf("%s: %s present in %v", tc.query, tc.absent, got)
		}
		if tc.has != "" && got[tc.has] == "" {
			t.Errorf("%s: %s missing from %v", tc.query, tc.has, got)
		}
	}
}
