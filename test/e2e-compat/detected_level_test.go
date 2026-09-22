//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// detected_level on log responses, tail, detected_fields and the metadata
// endpoints that must not expose it. One fixture is written to Loki and to
// VictoriaLogs through every ingestion route that stores a level differently,
// and each entry's detected_level, stream labels and structured metadata are
// compared with Loki's answer.

// dlRoute is how a fixture entry reaches both backends.
type dlRoute string

const (
	// Loki push API. VictoriaLogs keeps the line as _msg
	// (disable_message_parsing=1) and stores structured metadata as fields.
	dlRouteRaw dlRoute = "raw"
	// Loki push API with VictoriaLogs' default message parsing: JSON lines are
	// unpacked into fields and _msg becomes VictoriaLogs' placeholder.
	dlRouteUnpacked dlRoute = "unpacked"
	// VictoriaLogs jsonline with extra fields; Loki gets them as structured
	// metadata.
	dlRouteJSONLine dlRoute = "jsonline"
	// VictoriaLogs jsonline with level and detected_level stream fields; Loki
	// gets the same stream labels.
	dlRouteStreamFields dlRoute = "streamfields"
	// OTLP protobuf to both backends.
	dlRouteOTLP dlRoute = "otlp"
)

type dlCase struct {
	name   string
	route  dlRoute
	labels map[string]string // extra stream labels (raw, unpacked, streamfields)
	line   string
	sm     map[string]string // structured metadata / VictoriaLogs fields
	// OTLP record severity.
	severityNumber int
	severityText   string
	// proxyLevel is set when VictoriaLogs' stored form cannot reproduce
	// Loki's value: the expected proxy value, asserted explicitly.
	proxyLevel string
	// exact compares stream labels and structured metadata byte for byte.
	// Routes whose stored fields differ from Loki's metadata (unpacked JSON,
	// OTLP) compare detected_level only.
	exact bool
}

var dlCases = []dlCase{
	{name: "plain-nolevel", route: dlRouteRaw, line: "hello world", exact: true},
	{name: "plain-keyword", route: dlRouteRaw, line: "request failed with error code 500", exact: true},
	{name: "plain-keyword-earliest", route: dlRouteRaw, line: "warn: error happened", exact: true},
	{name: "plain-colon-left", route: dlRouteRaw, line: "misc:error something", exact: true},
	{name: "plain-uppercase-bracket", route: dlRouteRaw, line: "2026-01-01 10:00:00 [CRITICAL] disk full", exact: true},
	{name: "multiline", route: dlRouteRaw, line: "first line\nlevel=error second", exact: true},
	{name: "json-level", route: dlRouteRaw, line: `{"level":"WARN","msg":"x"}`, exact: true},
	{name: "json-nested-first", route: dlRouteRaw, line: `{"a":{"lvl":"dbg"},"level":"info"}`, exact: true},
	{name: "json-too-deep", route: dlRouteRaw, line: `{"a":{"b":{"level":"error"}},"msg":"ok"}`, exact: true},
	{name: "json-number", route: dlRouteRaw, line: `{"level":30,"msg":"pino"}`, exact: true},
	{name: "json-null-key", route: dlRouteRaw, line: `{"method":"GET","error":null,"status":200}`, exact: true},
	{name: "json-unknown-word", route: dlRouteRaw, line: `{"level":"notice","msg":"error later"}`, exact: true},
	{name: "json-severitytext", route: dlRouteRaw, line: `{"SeverityText":"Critical","msg":"x"}`, exact: true},
	{name: "logfmt-level", route: dlRouteRaw, line: `ts=1 level=Information msg="hi"`, exact: true},
	{name: "logfmt-case-insensitive", route: dlRouteRaw, line: `LEVEL=err msg=x`, exact: true},
	{name: "logfmt-priority", route: dlRouteRaw, line: `severity=info level=warn`, exact: true},
	{name: "logfmt-syntax-error", route: dlRouteRaw, line: `a=b=c level=warn`, exact: true},
	{name: "logfmt-empty-value", route: dlRouteRaw, line: `level= msg="fatal thing"`, exact: true},
	{name: "logfmt-quoted", route: dlRouteRaw, line: `level="trc" msg=x`, exact: true},
	{name: "logfmt-nolevel", route: dlRouteRaw, line: `v=3 msg=a`, exact: true},
	{name: "sm-level", route: dlRouteRaw, line: "hello", sm: map[string]string{"level": "Warning"}, exact: true},
	{name: "sm-severity", route: dlRouteRaw, line: "hello", sm: map[string]string{"severity": "ERR"}, exact: true},
	{name: "sm-severity-text", route: dlRouteRaw, line: "hello", sm: map[string]string{"severity_text": "INFO"}, exact: true},
	{name: "sm-detected-raw", route: dlRouteRaw, line: "hello", sm: map[string]string{"detected_level": "Weird"}, exact: true},
	{name: "sm-detected-normalised", route: dlRouteRaw, line: "hello", sm: map[string]string{"detected_level": "WARNING"}, exact: true},
	{name: "sm-severity-number", route: dlRouteRaw, line: "hello", sm: map[string]string{"severity_number": "13"}, exact: true},
	{name: "sm-severity-number-invalid", route: dlRouteRaw, line: "hello", sm: map[string]string{"severity_number": "abc"}, exact: true},
	{name: "sm-level-unknown-word", route: dlRouteRaw, line: "hello error", sm: map[string]string{"level": "notice"}, exact: true},
	{name: "sm-level-empty", route: dlRouteRaw, line: "error here", sm: map[string]string{"level": ""}, exact: true},
	{name: "sm-level-over-body", route: dlRouteRaw, line: `{"level":"error"}`, sm: map[string]string{"lvl": "debug"}, exact: true},
	{name: "label-level", route: dlRouteRaw, labels: map[string]string{"level": "Debug"}, line: "error text", exact: true},
	{name: "label-lvl", route: dlRouteRaw, labels: map[string]string{"lvl": "x"}, line: "error text", exact: true},
	// A pushed detected_level stream label is an index label: the entry's
	// derived value is exposed as detected_level_extracted.
	{name: "label-detected-level", route: dlRouteRaw, labels: map[string]string{"detected_level": "Warning"}, line: "hello label", exact: true},
	{name: "label-detected-level-body", route: dlRouteRaw, labels: map[string]string{"detected_level": "info"}, line: "request error", exact: true},
	// Two entries of one stream with different levels.
	{name: "split-a", route: dlRouteRaw, labels: map[string]string{"grp": "split"}, line: "level=error a", exact: true},
	{name: "split-b", route: dlRouteRaw, labels: map[string]string{"grp": "split"}, line: "level=info b", exact: true},

	{name: "unpacked-level", route: dlRouteUnpacked, line: `{"level":"WARN","msg":"x"}`},
	{name: "unpacked-unknown-word", route: dlRouteUnpacked, line: `{"level":"notice","msg":"error later"}`},
	{name: "unpacked-number", route: dlRouteUnpacked, line: `{"level":30,"msg":"pino"}`},
	{name: "unpacked-too-deep", route: dlRouteUnpacked, line: `{"a":{"b":{"level":"error"}},"msg":"ok"}`},
	{name: "unpacked-nolevel", route: dlRouteUnpacked, line: `{"msg":"hello"}`},
	// VictoriaLogs returns unpacked fields sorted by name, so the JSON
	// document order Loki reads is gone: the top-level level key wins.
	{name: "unpacked-document-order", route: dlRouteUnpacked, line: `{"a":{"lvl":"dbg"},"level":"info"}`, proxyLevel: "info"},
	// VictoriaLogs drops null values, so the "error" key never reaches the
	// line the keyword scan reads.
	{name: "unpacked-null-dropped", route: dlRouteUnpacked, line: `{"method":"GET","error":null,"status":200}`, proxyLevel: "unknown"},

	{name: "jsonline-level", route: dlRouteJSONLine, line: "hello jsonline", sm: map[string]string{"level": "ERR"}, exact: true},
	{name: "jsonline-nolevel", route: dlRouteJSONLine, line: "debug: hello jsonline", sm: map[string]string{"user": "u1"}, exact: true},
	{name: "jsonline-detected", route: dlRouteJSONLine, line: "hello jsonline", sm: map[string]string{"detected_level": "Error"}, exact: true},

	{name: "streamfields-level", route: dlRouteStreamFields, labels: map[string]string{"level": "warn"}, line: "error text", exact: true},
	{name: "streamfields-detected", route: dlRouteStreamFields, labels: map[string]string{"level": "info", "detected_level": "Error"}, line: "fatal: hello", exact: true},

	{name: "otlp-number", route: dlRouteOTLP, line: "hello", severityNumber: 13},
	{name: "otlp-number-text", route: dlRouteOTLP, line: "hello", severityNumber: 17, severityText: "Error"},
	{name: "otlp-number-fine", route: dlRouteOTLP, line: "hello", severityNumber: 10},
	{name: "otlp-unspecified", route: dlRouteOTLP, line: "request error"},
	{name: "otlp-text-only", route: dlRouteOTLP, line: "hello", severityText: "notice"},
}

type dlFixture struct {
	app     string
	corpus  string // app label of the generated edge-case corpus
	service string
	start   time.Time
	end     time.Time
	byTS    map[string]dlCase // entry timestamp (ns) -> case
}

var (
	dlFixtureOnce sync.Once
	dlFixtureData dlFixture
)

func (f dlFixture) appSelector() string     { return fmt.Sprintf(`{app=%q}`, f.app) }
func (f dlFixture) corpusSelector() string  { return fmt.Sprintf(`{app=%q}`, f.corpus) }
func (f dlFixture) serviceSelector() string { return fmt.Sprintf(`{service_name=%q}`, f.service) }

func ensureDetectedLevelFixture(t *testing.T) dlFixture {
	t.Helper()
	dlFixtureOnce.Do(func() {
		now := time.Now().UTC()
		f := dlFixture{
			app:     fmt.Sprintf("dlcompat%d", now.UnixNano()),
			corpus:  fmt.Sprintf("dlcorpus%d", now.UnixNano()),
			start:   now.Add(-2 * time.Minute),
			byTS:    map[string]dlCase{},
			service: "",
		}
		f.service = f.app + "-otel"
		base := f.start
		ts := func(i int) time.Time { return base.Add(time.Duration(i) * time.Millisecond) }

		var raw, unpacked []map[string]interface{}
		var jsonLines, streamFieldLines []string
		var lokiJSONLine []map[string]interface{}
		var otlpRecords [][]byte
		appCount, otelCount, corpusCount := 0, 0, 0
		// The hand-written cases first, then the generated corpus: same
		// ingestion, same comparison against live Loki.
		cases := append(append([]dlCase{}, dlCases...), generatedDetectedLevelCases()...)
		for i, c := range cases {
			at := ts(i)
			f.byTS[strconv.FormatInt(at.UnixNano(), 10)] = c
			app := f.app
			generated := strings.HasPrefix(c.name, "gen-")
			if generated {
				app = f.corpus
			}
			count := func() {
				if generated {
					corpusCount++
					return
				}
				appCount++
			}
			labels := map[string]string{"app": app, "case": c.name, "route": string(c.route)}
			if c.labels["grp"] != "" {
				labels["case"] = "split"
			}
			for k, v := range c.labels {
				labels[k] = v
			}
			value := []interface{}{strconv.FormatInt(at.UnixNano(), 10), c.line}
			if len(c.sm) > 0 {
				value = append(value, c.sm)
			}
			stream := map[string]interface{}{"stream": labels, "values": []interface{}{value}}
			switch c.route {
			case dlRouteRaw:
				raw = append(raw, stream)
				count()
			case dlRouteUnpacked:
				unpacked = append(unpacked, stream)
				count()
			case dlRouteJSONLine, dlRouteStreamFields:
				row := map[string]string{"_time": at.Format(time.RFC3339Nano), "_msg": c.line}
				for k, v := range labels {
					row[k] = v
				}
				for k, v := range c.sm {
					row[k] = v
				}
				encoded, _ := json.Marshal(row)
				lokiLabels := map[string]string{}
				for k, v := range labels {
					lokiLabels[k] = v
				}
				if c.route == dlRouteJSONLine {
					jsonLines = append(jsonLines, string(encoded))
				} else {
					streamFieldLines = append(streamFieldLines, string(encoded))
				}
				lokiJSONLine = append(lokiJSONLine, map[string]interface{}{"stream": lokiLabels, "values": []interface{}{value}})
				count()
			case dlRouteOTLP:
				otlpRecords = append(otlpRecords, otlpLogRecord(at, c.severityNumber, c.severityText, c.line, map[string]string{"case": c.name}))
				otelCount++
			}
		}
		f.end = ts(len(cases)).Add(time.Second)

		dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", dlJSON(map[string]interface{}{"streams": append(append(raw, unpacked...), lokiJSONLine...)}))
		dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", dlJSON(map[string]interface{}{"streams": raw}))
		dlPost(t, vlURL+"/insert/loki/api/v1/push", "application/json", dlJSON(map[string]interface{}{"streams": unpacked}))
		dlPost(t, vlURL+"/insert/jsonline?_stream_fields=app,case,route", "application/stream+json", []byte(strings.Join(jsonLines, "\n")))
		dlPost(t, vlURL+"/insert/jsonline?_stream_fields=app,case,route,level,detected_level", "application/stream+json", []byte(strings.Join(streamFieldLines, "\n")))
		otlp := otlpLogsData(map[string]string{"service.name": f.service}, otlpRecords)
		dlPost(t, lokiURL+"/otlp/v1/logs", "application/x-protobuf", otlp)
		dlPost(t, vlURL+"/insert/opentelemetry/v1/logs", "application/x-protobuf", otlp)

		dlWaitBothBackends(t, f, f.appSelector(), fmt.Sprintf(`{app=%q}`, f.app), appCount)
		dlWaitBothBackends(t, f, f.corpusSelector(), fmt.Sprintf(`{app=%q}`, f.corpus), corpusCount)
		dlWaitBothBackends(t, f, f.serviceSelector(), fmt.Sprintf(`{service.name=%q}`, f.service), otelCount)
		dlFixtureData = f
	})
	if dlFixtureData.app == "" {
		t.Fatal("detected_level fixture was not ingested")
	}
	return dlFixtureData
}

func dlJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func dlPost(t *testing.T, target, contentType string, body []byte) {
	t.Helper()
	resp, err := http.Post(target, contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("push %s: %v", target, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("push %s: status %d: %s", target, resp.StatusCode, msg)
	}
}

// dlWaitBothBackends proves both sides hold exactly the fixture before any
// comparison: Loki's range query and VictoriaLogs' native count agree with
// the pushed entry count.
func dlWaitBothBackends(t *testing.T, f dlFixture, lokiSelector, vlSelector string, want int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lokiLines, vlLines int
	for time.Now().Before(deadline) {
		resp := dlQueryRange(t, lokiURL, lokiSelector, f, nil)
		lokiLines = dlCountEntries(resp)
		vlLines = dlVLCount(t, vlSelector, f)
		if lokiLines == want && vlLines == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("fixture %s not ingested on both backends: loki=%d victorialogs=%d want %d", lokiSelector, lokiLines, vlLines, want)
}

func dlVLCount(t *testing.T, selector string, f dlFixture) int {
	t.Helper()
	params := url.Values{}
	params.Set("query", selector+" | stats count() as c")
	params.Set("start", f.start.Add(-time.Second).Format(time.RFC3339Nano))
	params.Set("end", f.end.Format(time.RFC3339Nano))
	resp, err := http.PostForm(vlURL+"/select/logsql/query", params)
	if err != nil {
		t.Fatalf("victorialogs count: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("victorialogs count status %d: %s", resp.StatusCode, body)
	}
	var row struct {
		C string `json:"c"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(body), &row); err != nil {
		return 0
	}
	n, _ := strconv.Atoi(row.C)
	return n
}

type dlStreamsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType    string   `json:"resultType"`
		EncodingFlags []string `json:"encodingFlags"`
		Result        []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Warnings []string `json:"warnings"`
}

type dlEntry struct {
	stream   map[string]string
	metadata map[string]string // structuredMetadata
	parsed   map[string]string
	stamp    string
}

func dlQueryRange(t *testing.T, baseURL, selector string, f dlFixture, headers map[string]string) dlStreamsResponse {
	t.Helper()
	params := url.Values{}
	params.Set("query", selector)
	params.Set("start", strconv.FormatInt(f.start.Add(-time.Second).UnixNano(), 10))
	params.Set("end", strconv.FormatInt(f.end.UnixNano(), 10))
	params.Set("limit", "1000")
	params.Set("direction", "forward")
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/loki/api/v1/query_range?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query_range %s: %v", baseURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query_range %s %s: status %d: %s", baseURL, selector, resp.StatusCode, body)
	}
	var out dlStreamsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode query_range %s: %v: %s", baseURL, err, body)
	}
	if out.Status != "success" || out.Data.ResultType != "streams" || len(out.Warnings) > 0 {
		t.Fatalf("query_range %s %s: status=%s type=%s warnings=%v", baseURL, selector, out.Status, out.Data.ResultType, out.Warnings)
	}
	return out
}

func dlCountEntries(resp dlStreamsResponse) int {
	n := 0
	for _, s := range resp.Data.Result {
		n += len(s.Values)
	}
	return n
}

// dlEntries indexes a streams response by entry timestamp.
func dlEntries(t *testing.T, resp dlStreamsResponse) map[string]dlEntry {
	t.Helper()
	out := map[string]dlEntry{}
	for si, s := range resp.Data.Result {
		for _, v := range s.Values {
			var stamp string
			_ = json.Unmarshal(v[0], &stamp)
			e := dlEntry{stream: s.Stream, stamp: strconv.Itoa(si)}
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				if err := json.Unmarshal(v[2], &meta); err != nil {
					t.Fatalf("decode entry metadata %s: %v", v[2], err)
				}
				e.metadata, e.parsed = meta.StructuredMetadata, meta.Parsed
			}
			if _, dup := out[stamp]; dup {
				t.Fatalf("duplicate entry timestamp %s", stamp)
			}
			out[stamp] = e
		}
	}
	return out
}

// dlLevel returns an entry's derived level: detected_level, or
// detected_level_extracted when the stream carries a detected_level label.
func dlLevel(e dlEntry, categorized bool) string {
	source := e.stream
	if categorized {
		source = e.metadata
	}
	if v, ok := source["detected_level_extracted"]; ok {
		return v
	}
	return source["detected_level"]
}

// dlDefaultEncodingLabels drops the structured metadata keys that Loki merges
// into the default-encoding stream labels and the proxy does not (it merges
// only level and detected_level there).
func dlDefaultEncodingLabels(labels map[string]string, c dlCase) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		if _, isMetadata := c.sm[k]; isMetadata && k != "level" && k != "detected_level" {
			continue
		}
		out[k] = v
	}
	return out
}

func TestCompat_DetectedLevelQueryRange(t *testing.T) {
	f := ensureDetectedLevelFixture(t)
	for _, categorized := range []bool{false, true} {
		name := "default"
		var headers map[string]string
		if categorized {
			name = "categorize-labels"
			headers = map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"}
		}
		t.Run(name, func(t *testing.T) {
			for _, selector := range []string{f.appSelector(), f.serviceSelector()} {
				loki := dlQueryRange(t, lokiURL, selector, f, headers)
				proxy := dlQueryRange(t, proxyURL, selector, f, headers)
				if categorized {
					if !reflect.DeepEqual(loki.Data.EncodingFlags, proxy.Data.EncodingFlags) {
						t.Fatalf("encodingFlags: loki %v proxy %v", loki.Data.EncodingFlags, proxy.Data.EncodingFlags)
					}
				}
				lokiEntries, proxyEntries := dlEntries(t, loki), dlEntries(t, proxy)
				if len(lokiEntries) == 0 || len(lokiEntries) != len(proxyEntries) {
					t.Fatalf("%s: entry count loki=%d proxy=%d", selector, len(lokiEntries), len(proxyEntries))
				}
				for stamp, le := range lokiEntries {
					c, ok := f.byTS[stamp]
					if !ok {
						t.Fatalf("unexpected Loki entry %s", stamp)
					}
					pe, ok := proxyEntries[stamp]
					if !ok {
						t.Fatalf("%s: proxy lacks entry %s", c.name, stamp)
					}
					lokiLevel, proxyLevel := dlLevel(le, categorized), dlLevel(pe, categorized)
					if lokiLevel == "" {
						t.Fatalf("%s: Loki returned no detected_level: %+v", c.name, le)
					}
					want := lokiLevel
					if c.proxyLevel != "" {
						if lokiLevel == c.proxyLevel {
							t.Fatalf("%s: documented difference no longer holds, Loki now returns %q", c.name, lokiLevel)
						}
						want = c.proxyLevel
					}
					if proxyLevel != want {
						t.Errorf("%s: detected_level proxy=%q want %q (Loki %q)", c.name, proxyLevel, want, lokiLevel)
					}
					if !c.exact {
						continue
					}
					if categorized {
						if !reflect.DeepEqual(le.stream, pe.stream) {
							t.Errorf("%s: stream labels\n loki  %v\n proxy %v", c.name, le.stream, pe.stream)
						}
						if !reflect.DeepEqual(le.metadata, pe.metadata) {
							t.Errorf("%s: structuredMetadata\n loki  %v\n proxy %v", c.name, le.metadata, pe.metadata)
						}
						continue
					}
					if got, wantLabels := pe.stream, dlDefaultEncodingLabels(le.stream, c); !reflect.DeepEqual(got, wantLabels) {
						t.Errorf("%s: stream labels\n loki  %v\n proxy %v", c.name, wantLabels, got)
					}
				}
				// Entries of one stream with different levels: same grouping as Loki.
				if selector == f.appSelector() {
					if l, p := dlSplitStreams(loki), dlSplitStreams(proxy); l != p {
						t.Errorf("stream count for the split case: loki=%d proxy=%d", l, p)
					}
				}
			}
		})
	}
}

func dlSplitStreams(resp dlStreamsResponse) int {
	n := 0
	for _, s := range resp.Data.Result {
		if s.Stream["case"] == "split" {
			n++
		}
	}
	return n
}

// dlTailReader forwards a tail connection's frames to a channel, closed when
// the connection fails; the read loop never hits a deadline, which would
// break a gorilla websocket connection for good.
func dlTailReader(conn *websocket.Conn) <-chan []byte {
	frames := make(chan []byte, 256)
	go func() {
		defer close(frames)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			frames <- msg
		}
	}()
	return frames
}

// dlTail is one tail websocket that can be redialled: a subscription can be
// closed by either side before the sentinel arrives (a backend hiccup, an
// upstream reconnect), which is not what this test is about.
type dlTail struct {
	url    string
	params url.Values
	header http.Header
	conn   *websocket.Conn
	frames <-chan []byte
}

func dlTailDial(t *testing.T, baseURL string, params url.Values, header http.Header) *dlTail {
	t.Helper()
	tail := &dlTail{url: baseURL, params: params, header: header}
	tail.open(t)
	t.Cleanup(func() {
		if tail.conn != nil {
			tail.conn.Close()
		}
	})
	return tail
}

func (s *dlTail) open(t *testing.T) {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(s.url, "http")+"/loki/api/v1/tail?"+s.params.Encode(), s.header)
	if err != nil {
		t.Fatalf("%s tail dial: %v", s.url, err)
	}
	s.conn = conn
	s.frames = dlTailReader(conn)
}

func (s *dlTail) redial(t *testing.T) {
	t.Helper()
	if s.conn != nil {
		s.conn.Close()
	}
	s.open(t)
}

// dlTailWaitSubscribed pushes sentinel lines until every tail delivers one,
// so the fixture is only pushed once all subscriptions are live.
func dlTailWaitSubscribed(t *testing.T, app string, tails ...*dlTail) {
	t.Helper()
	readers := make([]<-chan []byte, len(tails))
	for i, tail := range tails {
		readers[i] = tail.frames
	}
	pending := map[int]bool{}
	for i := range readers {
		pending[i] = true
	}
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 0; len(pending) > 0; attempt++ {
		if time.Now().After(deadline) {
			t.Fatalf("tail subscriptions not live after 30s (%d pending)", len(pending))
		}
		line := fmt.Sprintf("tail sentinel %s %d", app, attempt)
		payload := dlJSON(map[string]interface{}{"streams": []interface{}{map[string]interface{}{
			"stream": map[string]string{"app": app, "case": "sentinel"},
			"values": []interface{}{[]interface{}{strconv.FormatInt(time.Now().UnixNano(), 10), line}},
		}}})
		dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", payload)
		dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", payload)
		wait := time.After(2 * time.Second)
		for i := range readers {
			if !pending[i] {
				continue
			}
		drain:
			for {
				select {
				case msg, ok := <-readers[i]:
					if !ok {
						// The connection dropped before the sentinel; dial again
						// and keep pushing sentinels until the deadline.
						tails[i].redial(t)
						readers[i] = tails[i].frames
						break drain
					}
					if strings.Contains(string(msg), "tail sentinel "+app) {
						delete(pending, i)
						break drain
					}
				case <-wait:
					break drain
				}
			}
		}
	}
}

// dlTailFrames reads tail frames until every fixture entry is seen; sentinel
// entries are skipped.
func dlTailFrames(t *testing.T, frames <-chan []byte, want int) map[string]dlEntry {
	t.Helper()
	out := map[string]dlEntry{}
	timeout := time.After(45 * time.Second)
	for len(out) < want {
		var msg []byte
		select {
		case m, ok := <-frames:
			if !ok {
				t.Fatalf("tail connection closed after %d/%d entries", len(out), want)
			}
			msg = m
		case <-timeout:
			t.Fatalf("tail delivered %d/%d entries in 45s", len(out), want)
		}
		var frame struct {
			Streams []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(msg, &frame); err != nil {
			t.Fatalf("decode tail frame: %v: %s", err, msg)
		}
		var resp dlStreamsResponse
		for _, st := range frame.Streams {
			if st.Stream["case"] == "sentinel" {
				continue
			}
			resp.Data.Result = append(resp.Data.Result, struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			}{st.Stream, st.Values})
		}
		for stamp, e := range dlEntries(t, resp) {
			out[stamp] = e
		}
	}
	return out
}

func TestCompat_DetectedLevelTail(t *testing.T) {
	for _, categorized := range []bool{false, true} {
		name := "default"
		header := http.Header{}
		if categorized {
			name = "categorize-labels"
			header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			app := fmt.Sprintf("dltail%d", now.UnixNano())
			cases := []dlCase{}
			for _, c := range dlCases {
				if c.route == dlRouteRaw && c.labels["grp"] == "" {
					cases = append(cases, c)
				}
			}
			params := url.Values{}
			params.Set("query", fmt.Sprintf(`{app=%q}`, app))
			params.Set("start", strconv.FormatInt(now.UnixNano(), 10))
			header.Set("X-Scope-OrgID", "0")
			proxyTail := dlTailDial(t, proxyURL, params, header)
			lokiTail := dlTailDial(t, lokiURL, params, header)
			dlTailWaitSubscribed(t, app, proxyTail, lokiTail)
			proxyFrames, lokiFrames := proxyTail.frames, lokiTail.frames

			// Loki's tail drops an entry older than one it already sent, so the
			// entries go out one push at a time in timestamp order.
			byTS := map[string]dlCase{}
			for _, c := range cases {
				at := time.Now()
				byTS[strconv.FormatInt(at.UnixNano(), 10)] = c
				labels := map[string]string{"app": app, "case": c.name}
				for k, v := range c.labels {
					labels[k] = v
				}
				value := []interface{}{strconv.FormatInt(at.UnixNano(), 10), c.line}
				if len(c.sm) > 0 {
					value = append(value, c.sm)
				}
				payload := dlJSON(map[string]interface{}{"streams": []interface{}{map[string]interface{}{"stream": labels, "values": []interface{}{value}}}})
				dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", payload)
				dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", payload)
				time.Sleep(150 * time.Millisecond)
			}

			lokiEntries := dlTailFrames(t, lokiFrames, len(cases))
			proxyEntries := dlTailFrames(t, proxyFrames, len(cases))
			for stamp, le := range lokiEntries {
				c := byTS[stamp]
				pe, ok := proxyEntries[stamp]
				if !ok {
					t.Fatalf("%s: proxy tail lacks entry %s", c.name, stamp)
				}
				// Loki's tail sends structured metadata, detected_level included,
				// only with categorize-labels; default frames carry the index labels.
				if categorized {
					if l, p := le.metadata["detected_level"], pe.metadata["detected_level"]; l == "" || l != p {
						t.Errorf("%s: tail detected_level loki=%q proxy=%q", c.name, l, p)
					}
				} else if _, ok := le.stream["detected_level"]; ok != (c.labels["detected_level"] != "") {
					t.Errorf("%s: Loki default tail frame labels %v", c.name, le.stream)
				}
				if !reflect.DeepEqual(le.stream, pe.stream) || !reflect.DeepEqual(le.metadata, pe.metadata) {
					t.Errorf("%s: tail entry\n loki  %v %v\n proxy %v %v", c.name, le.stream, le.metadata, pe.stream, pe.metadata)
				}
			}
		})
	}
}

func dlGet(t *testing.T, baseURL, path string, params url.Values) map[string]interface{} {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, baseURL+path+"?"+params.Encode(), nil)
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s%s: %v", baseURL, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s%s: status %d: %s", baseURL, path, resp.StatusCode, body)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s%s: %v: %s", baseURL, path, err, body)
	}
	return out
}

func dlRangeParams(f dlFixture, query string) url.Values {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(f.start.Add(-time.Minute).UnixNano(), 10))
	params.Set("end", strconv.FormatInt(f.end.Add(time.Minute).UnixNano(), 10))
	return params
}

// dlRawSelector selects the entries both backends store identically.
func dlRawSelector(f dlFixture) string {
	return fmt.Sprintf(`{app=%q, route="raw", case!="label-level"}`, f.app)
}

func TestCompat_DetectedLevelDetectedFields(t *testing.T) {
	f := ensureDetectedLevelFixture(t)
	params := dlRangeParams(f, dlRawSelector(f))
	fieldsByLabel := func(resp map[string]interface{}) map[string]map[string]interface{} {
		out := map[string]map[string]interface{}{}
		for _, raw := range extractArray(resp, "fields") {
			field, _ := raw.(map[string]interface{})
			if label, _ := field["label"].(string); strings.HasPrefix(label, "detected_level") {
				out[label] = field
			}
		}
		return out
	}
	lokiFields := fieldsByLabel(dlGet(t, lokiURL, "/loki/api/v1/detected_fields", params))
	proxyFields := fieldsByLabel(dlGet(t, proxyURL, "/loki/api/v1/detected_fields", params))
	for _, label := range []string{"detected_level", "detected_level_extracted"} {
		lokiField, proxyField := lokiFields[label], proxyFields[label]
		if lokiField == nil {
			t.Fatalf("Loki /detected_fields lists no %s: %v", label, lokiFields)
		}
		if proxyField == nil {
			t.Fatalf("proxy /detected_fields lists no %s: %v", label, proxyFields)
		}
		for _, key := range []string{"type", "cardinality", "parsers"} {
			if !reflect.DeepEqual(lokiField[key], proxyField[key]) {
				t.Errorf("%s %s: loki %v proxy %v", label, key, lokiField[key], proxyField[key])
			}
		}
		if proxyField["parsers"] != nil {
			t.Errorf("%s parsers = %v, want null", label, proxyField["parsers"])
		}
	}

	values := func(resp map[string]interface{}) []string {
		var out []string
		for _, v := range extractArray(resp, "values") {
			out = append(out, fmt.Sprint(v))
		}
		sort.Strings(out)
		return out
	}
	lokiValues := values(dlGet(t, lokiURL, "/loki/api/v1/detected_field/detected_level/values", params))
	proxyValues := values(dlGet(t, proxyURL, "/loki/api/v1/detected_field/detected_level/values", params))
	if len(lokiValues) < 5 || !reflect.DeepEqual(lokiValues, proxyValues) {
		t.Errorf("detected_level values:\n loki  %v\n proxy %v", lokiValues, proxyValues)
	}
}

// TestCompat_DetectedLevelIndexEndpoints checks the endpoints backed by the
// index: the derived detected_level never appears there, while a pushed
// detected_level stream label does, exactly as in Loki.
func TestCompat_DetectedLevelIndexEndpoints(t *testing.T) {
	f := ensureDetectedLevelFixture(t)
	for _, tc := range []struct {
		name           string
		selector       string
		hasStreamLabel bool
	}{
		{name: "derived only", selector: fmt.Sprintf(`{app=%q, route="raw", case!~"label-detected-level.*"}`, f.app)},
		{name: "detected_level stream label", selector: f.appSelector(), hasStreamLabel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := dlRangeParams(f, tc.selector)

			for _, base := range []string{lokiURL, proxyURL} {
				names := map[string]bool{}
				for _, v := range extractArray(dlGet(t, base, "/loki/api/v1/labels", params), "data") {
					names[fmt.Sprint(v)] = true
				}
				if names["detected_level"] != tc.hasStreamLabel || !names["app"] || !names["level"] {
					t.Errorf("%s /labels = %v, want detected_level=%v with app and level", base, names, tc.hasStreamLabel)
				}
			}

			lokiValues := dlGet(t, lokiURL, "/loki/api/v1/label/detected_level/values", params)
			proxyValues := dlGet(t, proxyURL, "/loki/api/v1/label/detected_level/values", params)
			if !reflect.DeepEqual(lokiValues, proxyValues) {
				t.Errorf("/label/detected_level/values: loki %v proxy %v", lokiValues, proxyValues)
			}
			if _, hasData := lokiValues["data"]; hasData != tc.hasStreamLabel {
				t.Fatalf("precondition: Loki values %v, want data=%v", lokiValues, tc.hasStreamLabel)
			}

			seriesParams := url.Values{}
			seriesParams.Set("match[]", tc.selector)
			seriesParams.Set("start", params.Get("start"))
			seriesParams.Set("end", params.Get("end"))
			collect := func(base string) []string {
				var out []string
				for _, raw := range extractArray(dlGet(t, base, "/loki/api/v1/series", seriesParams), "data") {
					encoded, _ := json.Marshal(raw)
					out = append(out, string(encoded))
				}
				sort.Strings(out)
				return out
			}
			if l, p := collect(lokiURL), collect(proxyURL); len(l) == 0 || !reflect.DeepEqual(l, p) {
				t.Errorf("/series:\n loki  %v\n proxy %v", l, p)
			}

			detectedLabels := func(base string) map[string]float64 {
				out := map[string]float64{}
				for _, raw := range extractArray(dlGet(t, base, "/loki/api/v1/detected_labels", params), "detectedLabels") {
					label, _ := raw.(map[string]interface{})
					cardinality, _ := label["cardinality"].(float64)
					out[fmt.Sprint(label["label"])] = cardinality
				}
				return out
			}
			lokiLabels, proxyLabels := detectedLabels(lokiURL), detectedLabels(proxyURL)
			_, lokiHas := lokiLabels["detected_level"]
			_, proxyHas := proxyLabels["detected_level"]
			if lokiHas != tc.hasStreamLabel || proxyHas != tc.hasStreamLabel {
				t.Errorf("/detected_labels detected_level: loki %v proxy %v", lokiLabels, proxyLabels)
			}
		})
	}
}

// otlpLogRecord encodes one OTLP LogRecord (protobuf wire format).
func otlpLogRecord(at time.Time, severityNumber int, severityText, body string, attrs map[string]string) []byte {
	var b []byte
	b = binary.AppendUvarint(b, 1<<3|1)
	b = binary.LittleEndian.AppendUint64(b, uint64(at.UnixNano()))
	if severityNumber != 0 {
		b = binary.AppendUvarint(b, 2<<3)
		b = binary.AppendUvarint(b, uint64(severityNumber))
	}
	if severityText != "" {
		b = otlpBytes(b, 3, []byte(severityText))
	}
	b = otlpBytes(b, 5, otlpBytes(nil, 1, []byte(body)))
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b = otlpBytes(b, 6, otlpKeyValue(k, attrs[k]))
	}
	return b
}

// otlpLogsData wraps records in LogsData{ResourceLogs{Resource, ScopeLogs}}.
func otlpLogsData(resourceAttrs map[string]string, records [][]byte) []byte {
	var resource []byte
	for k, v := range resourceAttrs {
		resource = otlpBytes(resource, 1, otlpKeyValue(k, v))
	}
	var scope []byte
	for _, r := range records {
		scope = otlpBytes(scope, 2, r)
	}
	resourceLogs := otlpBytes(nil, 1, resource)
	resourceLogs = otlpBytes(resourceLogs, 2, scope)
	return otlpBytes(nil, 1, resourceLogs)
}

func otlpKeyValue(k, v string) []byte {
	kv := otlpBytes(nil, 1, []byte(k))
	return otlpBytes(kv, 2, otlpBytes(nil, 1, []byte(v)))
}

func otlpBytes(dst []byte, field int, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(field)<<3|2)
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}
