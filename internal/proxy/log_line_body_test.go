package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

// logLineBodyCase is a VictoriaLogs row and the line the proxy must return.
type logLineBodyCase struct {
	name string
	row  string
	// want is the expected line; empty means the row's stored _msg.
	want string
	// wantEmpty expects an empty line.
	wantEmpty bool
	// defaultMsgValue configures BackendDefaultMsgValue.
	defaultMsgValue string
	// hasFields marks rows with non-stream fields, which reach clients as tuple
	// metadata when categorize-labels is emitted.
	hasFields bool
}

var logLineBodyCases = []logLineBodyCase{
	{
		// Captured from the e2e stack: log-generator.py pushes a JSON line to VL
		// with _msg set to the original line, so VL also stores every JSON key
		// (flattened to service.name) as a top-level field.
		name:      "json line with extracted fields",
		row:       `{"_msg":"{\"service\": {\"name\": \"api-gateway\"}, \"method\": \"PUT\", \"path\": \"/api/v1/cart\", \"status\": 200, \"duration_ms\": 70, \"trace_id\": \"b09df04f98497226c5a6a00bf1798b7b\", \"span_id\": \"9f48a2f97ff9c01e\", \"request_id\": \"5010f05206f77710\", \"ip\": \"10.5.250.177\", \"user_id\": \"usr_888\", \"level\": \"info\", \"region\": \"eu-west-1\", \"version\": \"v1\"}","_stream":"{app=\"api-gateway\",cluster=\"us-east-1\",container=\"api\",env=\"production\",level=\"info\",namespace=\"prod\",pod=\"api-gateway-62cb5-ca38\",service_name=\"api-gateway\",version=\"v2\"}","_stream_id":"0000000000000000c951a8edcf768e3e8aa0ef1e03a99c80","_time":"2026-09-15T07:41:14.828187136Z","app":"api-gateway","cluster":"us-east-1","container":"api","duration_ms":"70","env":"production","ip":"10.5.250.177","level":"info","method":"PUT","namespace":"prod","path":"/api/v1/cart","pod":"api-gateway-62cb5-ca38","region":"eu-west-1","request_id":"5010f05206f77710","service.name":"api-gateway","service_name":"api-gateway","span_id":"9f48a2f97ff9c01e","status":"200","trace_id":"b09df04f98497226c5a6a00bf1798b7b","user_id":"usr_888","version":"v1"}`,
		hasFields: true,
	},
	{
		// Loki push of a plain line with structured metadata.
		name:      "plain line with structured metadata",
		row:       `{"_msg":"plain text line","_stream":"{app=\"api-gateway\",case=\"c\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","case":"c","service.name":"svc","trace_id":"abc"}`,
		hasFields: true,
	},
	{
		// jsonline insert with an explicit _msg and extra fields.
		name:      "short message with extra fields",
		row:       `{"_msg":"PUT /api/v1/users 401 233ms","_stream":"{app=\"api-gateway\",namespace=\"prod\"}","_time":"2026-01-01T10:00:00Z","app":"api-gateway","namespace":"prod","method":"PUT","path":"/api/v1/users","status":"401","trace_id":"abc123","k8s.pod.name":"api-1"}`,
		hasFields: true,
	},
	{
		// VL v1.50.0 Loki push of {"method":"GET","status":200,"msg":"hello json",
		// "http":{"route":"/a<b"}} without _msg (the Promtail/Alloy route): the
		// keys become fields and _msg holds -defaultMsgValue.
		name:      "loki push json without _msg",
		row:       `{"_msg":"` + vlDefaultMsgValue + `","_stream":"{app=\"api-gateway\",case=\"b\"}","_stream_id":"0000000000000000189ac8f5710d9952fc84aca8157f7bee","_time":"2026-09-15T07:44:02.000000001Z","app":"api-gateway","case":"b","method":"GET","msg":"hello json","status":"200","http.route":"/a<b"}`,
		want:      `{"http.route":"/a<b","method":"GET","msg":"hello json","status":"200"}`,
		hasFields: true,
	},
	{
		// /insert/jsonline row without _msg.
		name:      "jsonline without _msg",
		row:       `{"_msg":"` + vlDefaultMsgValue + `","_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","user":"u1","event":"login"}`,
		want:      `{"event":"login","user":"u1"}`,
		hasFields: true,
	},
	{
		// VL without -defaultMsgValue returns no _msg for a row without one.
		name:      "absent _msg",
		row:       `{"_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","user":"u2"}`,
		want:      `{"user":"u2"}`,
		hasFields: true,
	},
	{
		// Loki push of an empty line: VL stores the placeholder and nothing else.
		name:      "loki push empty line",
		row:       `{"_msg":"` + vlDefaultMsgValue + `","_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway"}`,
		wantEmpty: true,
	},
	{
		name:            "custom default msg value",
		row:             `{"_msg":"<no message>","_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","user":"u3"}`,
		want:            `{"user":"u3"}`,
		defaultMsgValue: "<no message>",
		hasFields:       true,
	},
	{
		// The same row without the flag keeps its stored line.
		name:      "custom placeholder without flag",
		row:       `{"_msg":"<no message>","_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","user":"u3"}`,
		hasFields: true,
	},
}

// logLineBodyRowAt returns row with _time moved to ts and the expected line.
func logLineBodyRowAt(t testing.TB, tc logLineBodyCase, ts time.Time) (string, string) {
	t.Helper()
	var fields map[string]interface{}
	if err := json.Unmarshal([]byte(tc.row), &fields); err != nil {
		t.Fatalf("decode row: %v", err)
	}
	fields["_time"] = ts.UTC().Format(time.RFC3339Nano)
	out, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode row: %v", err)
	}
	want := tc.want
	if want == "" && !tc.wantEmpty {
		want, _ = fields["_msg"].(string)
	}
	return string(out), want
}

type logLineBodyPath struct {
	name     string
	stream   bool
	windowed bool
}

// TestLogLineBody_MatchesLoki checks that every log query path returns the
// stored _msg as the line, as Loki returns the pushed line, and rebuilds the
// line only for rows VictoriaLogs stored without a message, for every label
// style, metadata field mode and tuple encoding.
func TestLogLineBody_MatchesLoki(t *testing.T) {
	paths := []logLineBodyPath{
		{name: "single"},
		{name: "stream-response", stream: true},
		{name: "windowed", windowed: true},
	}
	styles := []LabelStyle{LabelStylePassthrough, LabelStyleUnderscores}
	modes := []MetadataFieldMode{MetadataFieldModeNative, MetadataFieldModeTranslated, MetadataFieldModeHybrid}
	queries := []string{
		`{app="api-gateway"}`,
		`{app="api-gateway"} | drop __error__`,
		`{app="api-gateway"} |= "a"`,
		`{app="api-gateway"} | json`,
		`{app="api-gateway"} | logfmt`,
		`{app="api-gateway"} | detected_level="info"`,
	}

	for _, tc := range logLineBodyCases {
		for _, path := range paths {
			for _, style := range styles {
				for _, mode := range modes {
					for _, emit := range []bool{false, true} {
						for _, categorize := range []bool{false, true} {
							name := fmt.Sprintf("%s/%s/%s/%s/emit=%t/categorize=%t", tc.name, path.name, style, mode, emit, categorize)
							t.Run(name, func(t *testing.T) {
								for _, query := range queries {
									assertLogLineBody(t, tc, path, style, mode, emit, categorize, query)
								}
							})
						}
					}
				}
			}
		}
	}
}

func assertLogLineBody(t *testing.T, tc logLineBodyCase, path logLineBodyPath, style LabelStyle, mode MetadataFieldMode, emit, categorize bool, query string) {
	t.Helper()
	start := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)
	_, wantLine := logLineBodyRowAt(t, tc, start)

	var queryCalls atomic.Int64
	vl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.URL.Path != "/select/logsql/query" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"hits":[]}`))
			return
		}
		queryCalls.Add(1)
		ts := start.Add(time.Minute)
		if raw := r.Form.Get("start"); raw != "" {
			if ns, err := strconv.ParseInt(raw, 10, 64); err == nil {
				ts = time.Unix(0, ns).Add(time.Minute)
			}
		}
		body, _ := logLineBodyRowAt(t, tc, ts)
		_, _ = w.Write([]byte(body + "\n"))
	}))
	defer vl.Close()

	cfg := Config{
		BackendURL:             vl.URL,
		Cache:                  cache.New(time.Minute, 1000),
		LogLevel:               "error",
		LabelStyle:             style,
		MetadataFieldMode:      mode,
		EmitStructuredMetadata: emit,
		StreamResponse:         path.stream,
		BackendDefaultMsgValue: tc.defaultMsgValue,
	}
	if path.windowed {
		cfg.QueryRangeWindowingEnabled = true
		cfg.QueryRangeSplitInterval = time.Hour
		cfg.QueryRangeMaxParallel = 2
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	defer p.cache.Close()

	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(end.UnixNano()-1, 10))
	params.Set("limit", "100")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	if categorize {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d body=%s", query, rec.Code, rec.Body.String())
	}
	if path.windowed && queryCalls.Load() < 2 {
		t.Fatalf("%s: expected windowed fetches, got %d VL query calls", query, queryCalls.Load())
	}

	var resp struct {
		Data struct {
			Result []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("%s: decode response: %v body=%s", query, err, rec.Body.String())
	}
	lines := 0
	sawMetadata := false
	for _, stream := range resp.Data.Result {
		for _, value := range stream.Values {
			if len(value) < 2 {
				t.Fatalf("%s: short value %s", query, rec.Body.String())
			}
			var line string
			if err := json.Unmarshal(value[1], &line); err != nil {
				t.Fatalf("%s: decode line: %v", query, err)
			}
			if line != wantLine {
				t.Fatalf("%s: unexpected line\n got: %s\nwant: %s", query, line, wantLine)
			}
			if len(value) > 2 && len(value[2]) > 2 {
				sawMetadata = true
			}
			lines++
		}
	}
	if lines == 0 {
		t.Fatalf("%s: no lines in response %s", query, rec.Body.String())
	}
	// Non-stream fields keep reaching clients as tuple metadata.
	if categorize && emit && tc.hasFields && !sawMetadata {
		t.Fatalf("%s: expected structured metadata or parsed fields in tuples, got %s", query, rec.Body.String())
	}
}

// TestLogLineBody_TailPathsMatchLoki covers the websocket tail frame (native
// and synthetic tail) and the buffered tail conversion.
func TestLogLineBody_TailPathsMatchLoki(t *testing.T) {
	for _, tc := range logLineBodyCases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{BackendURL: "http://unused", Cache: cache.New(time.Minute, 10), LogLevel: "error", BackendDefaultMsgValue: tc.defaultMsgValue})
			if err != nil {
				t.Fatal(err)
			}
			defer p.cache.Close()
			body, wantLine := logLineBodyRowAt(t, tc, time.Now())

			var entry map[string]interface{}
			if err := json.Unmarshal([]byte(body), &entry); err != nil {
				t.Fatalf("decode row: %v", err)
			}
			frame := p.vlLineToTailFrame(entry, nil, false)
			streams := frame["streams"].([]map[string]interface{})
			if got := streams[0]["values"].([][]string)[0][1]; got != wantLine {
				t.Fatalf("tail frame line\n got: %s\nwant: %s", got, wantLine)
			}

			// The buffered conversion has no proxy and knows only VictoriaLogs'
			// default placeholder.
			if tc.defaultMsgValue != "" {
				return
			}
			for _, stream := range vlLogsToLokiStreams([]byte(body + "\n")) {
				for _, value := range stream["values"].([][]string) {
					if got := value[1]; got != wantLine {
						t.Fatalf("buffered tail line\n got: %s\nwant: %s", got, wantLine)
					}
				}
			}
		})
	}
}

func TestIsVLMissingMsg(t *testing.T) {
	for _, tc := range []struct {
		msg, custom string
		want        bool
	}{
		{"", "", true},
		{vlDefaultMsgValue, "", true},
		{"missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/", "", false},
		{"missing _msg field", "", false},
		{"<none>", "<none>", true},
		{vlDefaultMsgValue, "<none>", true},
		{"<none>", "", false},
		{"missing", "", false},
		{"GET /api 200", "", false},
		{`{"_msg":"x"}`, "", false},
	} {
		if got := isVLMissingMsg(tc.msg, tc.custom); got != tc.want {
			t.Errorf("isVLMissingMsg(%q, %q) = %v, want %v", tc.msg, tc.custom, got, tc.want)
		}
	}
}

func TestEncodeLogLine_RoundTrips(t *testing.T) {
	fields := []logLineField[string]{
		{key: "a.b", value: "caf\u00e9 \u2028"},
		{key: "error", value: "gateway \"timeout\"\nretry\tnow\\path\x01<&>"},
		{key: "z", value: "last"},
	}
	line := encodeLogLine(fields)
	if want := `{"a.b":"café ` + "\u2028" + `","error":"gateway \"timeout\"\nretry\tnow\\path\u0001<&>","z":"last"}`; line != want {
		t.Fatalf("line\n got: %s\nwant: %s", line, want)
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("rebuilt line is not JSON: %v", err)
	}
	for _, field := range fields {
		if decoded[field.key] != field.value {
			t.Fatalf("%s: got %q, want %q", field.key, decoded[field.key], field.value)
		}
	}
}

// TestLogLineBody_PipelineFieldsStayOutOfRebuiltLines checks that fields the
// LogQL pipeline writes in VictoriaLogs (captures, explicit extractions,
// label_format targets) do not change a rebuilt line, as Loki's line never
// changes with the pipeline.
func TestLogLineBody_PipelineFieldsStayOutOfRebuiltLines(t *testing.T) {
	row := `{"_msg":"` + vlDefaultMsgValue + `","_stream":"{app=\"api-gateway\"}","_time":"2026-09-15T07:44:02Z","app":"api-gateway","user":"u1","foo":"bar","cap":"missing","word":"missing","status":"200"}`
	for _, tc := range []struct {
		query string
		want  string
	}{
		{`{app="api-gateway"}`, `{"cap":"missing","foo":"bar","status":"200","user":"u1","word":"missing"}`},
		{`{app="api-gateway"} | label_format foo="bar"`, `{"cap":"missing","status":"200","user":"u1","word":"missing"}`},
		{`{app="api-gateway"} | label_format foo=user`, `{"cap":"missing","status":"200","user":"u1","word":"missing"}`},
		{`{app="api-gateway"} | regexp "(?P<cap>\\w+)"`, `{"foo":"bar","status":"200","user":"u1","word":"missing"}`},
		{`{app="api-gateway"} | pattern "<word> <_>"`, `{"cap":"missing","foo":"bar","status":"200","user":"u1"}`},
		{`{app="api-gateway"} | json foo="x", status`, `{"cap":"missing","user":"u1","word":"missing"}`},
		{`{app="api-gateway"} | logfmt cap`, `{"foo":"bar","status":"200","user":"u1","word":"missing"}`},
	} {
		lineCase := logLineBodyCase{name: tc.query, row: row, want: tc.want, hasFields: true}
		for _, path := range []logLineBodyPath{{name: "single"}, {name: "stream-response", stream: true}, {name: "windowed", windowed: true}} {
			t.Run(path.name+"/"+tc.query, func(t *testing.T) {
				assertLogLineBody(t, lineCase, path, LabelStyleUnderscores, MetadataFieldModeTranslated, true, true, tc.query)
			})
		}
		t.Run("tail/"+tc.query, func(t *testing.T) {
			p := newTestProxy(t, "http://unused")
			var entry map[string]interface{}
			if err := json.Unmarshal([]byte(row), &entry); err != nil {
				t.Fatal(err)
			}
			frame := p.vlLineToTailFrame(entry, logQueryLineFields(tc.query), false)
			if got := frame["streams"].([]map[string]interface{})[0]["values"].([][]string)[0][1]; got != tc.want {
				t.Fatalf("tail line\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

func BenchmarkStoredLogLine(b *testing.B) {
	stored := []byte(`{"_msg":"GET /api/v1/users 200","_stream":"{app=\"api\"}","_time":"2026-09-15T07:44:02Z","app":"api","method":"GET","status":"200"}`)
	missing := []byte(`{"_msg":"` + vlDefaultMsgValue + `","_stream":"{app=\"api\"}","_time":"2026-09-15T07:44:02Z","app":"api","method":"GET","status":"200","msg":"hello"}`)
	for _, bc := range []struct {
		name string
		row  []byte
	}{{"stored", stored}, {"missing", missing}} {
		b.Run(bc.name, func(b *testing.B) {
			parser := vlFJParserPool.Get()
			defer vlFJParserPool.Put(parser)
			row, err := parser.ParseBytes(bc.row)
			if err != nil {
				b.Fatal(err)
			}
			streamLabels := parseStreamLabels(string(row.GetStringBytes("_stream")))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = storedLogLineFromFJ(row, streamLabels, nil, "")
			}
		})
	}
}
