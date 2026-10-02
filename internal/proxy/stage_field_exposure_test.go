package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Rows as VictoriaLogs stores three Loki pushes of the same events:
//   - stageJSONRow: a JSON line (nested object included) whose keys
//     VictoriaLogs also holds as fields, next to OTel structured metadata;
//   - stageLogfmtRow: a logfmt line with structured metadata (VictoriaLogs
//     holds the line's keys as fields only after | logfmt);
//   - stageMissingLineRow: a JSON line pushed without _msg, which
//     VictoriaLogs unpacks into fields and stores without the line.
const (
	stageJSONRow = `{"_time":"2026-10-01T17:21:10Z","_msg":"{\"msg\":\"login ok\",\"user\":\"u1\",\"status\":200,\"svc\":{\"name\":\"api\"}}",` +
		`"_stream":"{app=\"gen\",env=\"ev\"}","app":"gen","env":"ev","msg":"login ok","user":"u1","status":"200","svc.name":"api",` +
		`"k8s.pod.name":"pod-1","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`
	stageLogfmtRow = `{"_time":"2026-10-01T17:21:11Z","_msg":"msg=\"login ok\" user=u1 status=200",` +
		`"_stream":"{app=\"lf\",env=\"ev\"}","app":"lf","env":"ev","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`
	stageMissingLineRow = `{"_time":"2026-10-01T17:21:12Z","_msg":"missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field",` +
		`"_stream":"{app=\"pl\",env=\"ev\"}","app":"pl","env":"ev","msg":"login ok","user":"u1","status":"200","svc.name":"api","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`
)

// stageFieldsBackend answers like VictoriaLogs for the rows above: the
// fields a LogsQL pipe writes are added to the row (VictoriaLogs filters are
// not evaluated, so the proxy's own row handling is what the tests see).
type stageFieldsBackend struct{}

func (stageFieldsBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	q := r.Form.Get("query")
	row := stageJSONRow
	switch {
	case strings.Contains(q, `"lf"`):
		row = stageLogfmtRow
		if strings.Contains(q, "unpack_logfmt") {
			row += `,"msg":"login ok","user":"u1","status":"200"`
		}
	case strings.Contains(q, `"pl"`):
		row = stageMissingLineRow
	}
	extra := map[string]string{
		"extract_regexp":         `,"who":"u1"`,
		"| extract ":             `,"m":"login ok"`,
		`as x`:                   `,"x":"gen"`,
		`copy "user" as u`:       `,"u":"u1"`,
		`copy "svc.name" as n`:   `,"n":"api"`,
		`format "<msg>"`:         ``,
		`rename status as code`:  `,"code":"200"`,
		`format "<user>" as who`: `,"who":"u1"`,
	}
	for marker, fields := range extra {
		if strings.Contains(q, marker) {
			row += fields
		}
	}
	if strings.Contains(q, `format "<msg>"`) && !strings.Contains(q, " as ") {
		// line_format rewrites the stored line.
		row = strings.Replace(row, `"_msg":"{\"msg\":\"login ok\",\"user\":\"u1\",\"status\":200,\"svc\":{\"name\":\"api\"}}"`, `"_msg":"login ok"`, 1)
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write([]byte(row + "}\n"))
}

type stageRow struct {
	line   string
	stream []string
	sm     []string
	parsed []string
}

// stageRows runs a log query and returns each entry's line, stream label
// names and (with categorize-labels) structuredMetadata and parsed keys,
// detected_level left out.
func stageRows(t *testing.T, p *Proxy, query string, categorize bool) []stageRow {
	t.Helper()
	q := url.Values{}
	q.Set("query", query)
	// One window of the windowed path (15m split interval), so each row is
	// fetched once.
	q.Set("start", "1790871720000000000")
	q.Set("end", "1790872320000000000")
	q.Set("limit", "10")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil)
	if categorize {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	w := httptest.NewRecorder()
	p.handleQueryRange(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", query, w.Code, w.Body.String())
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
		t.Fatalf("%s: decode: %v %s", query, err, w.Body.String())
	}
	keys := func(m map[string]string) []string {
		out := []string{}
		for k := range m {
			if k != detectedLevelLabel && k != "env" {
				out = append(out, k)
			}
		}
		sort.Strings(out)
		return out
	}
	var rows []stageRow
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			var row stageRow
			_ = json.Unmarshal(v[1], &row.line)
			row.stream = keys(r.Stream)
			if len(v) > 2 {
				var meta struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
					Parsed             map[string]string `json:"parsed"`
				}
				_ = json.Unmarshal(v[2], &meta)
				row.sm, row.parsed = keys(meta.StructuredMetadata), keys(meta.Parsed)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// TestLogQuery_StageFieldExposureLikeLoki: each LogQL stage exposes only the
// labels Loki's stage adds (pkg/logql/log, Loki v3.7.7), as parsed labels;
// fields of the JSON line no stage exposes are left out and structured
// metadata stays structured metadata. Checked on the buffered, streamed and
// windowed paths, with and without categorize-labels. The expected values are
// Loki's answers for the same pushed data.
//
// conformance: profiles/parsed-fields-without-parser, profiles/stage-field-exposure, loki_api_v1_query_range, loki-compatible-profile
func TestLogQuery_StageFieldExposureLikeLoki(t *testing.T) {
	sm := []string{"k8s_pod_name", "trace_id"}
	none := []string{}
	cases := []struct {
		query  string
		line   string // "" keeps the stored line
		sm     []string
		parsed []string
		// stream holds the label names of a response without categorize-labels
		// (parsed labels join the stream labels; structured metadata does not).
		stream []string
	}{
		{`{app="gen"}`, "", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | json`, "", sm, []string{"msg", "status", "svc_name", "user"}, []string{"app", "msg", "service_name", "status", "svc_name", "user"}},
		{`{app="gen"} | json user`, "", sm, []string{"user"}, []string{"app", "service_name", "user"}},
		{`{app="gen"} | json u="user", n="svc.name"`, "", sm, []string{"n", "u"}, []string{"app", "n", "service_name", "u"}},
		{`{app="gen"} | json nosuch, user`, "", sm, []string{"nosuch", "user"}, []string{"app", "nosuch", "service_name", "user"}},
		{`{app="gen"} | logfmt`, "", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | regexp "\"user\":\"(?P<who>[^\"]+)\""`, "", sm, []string{"who"}, []string{"app", "service_name", "who"}},
		{`{app="gen"} | pattern "{\"msg\":\"<m>\",<_>"`, "", sm, []string{"m"}, []string{"app", "m", "service_name"}},
		{`{app="gen"} | label_format x="{{.app}}"`, "", sm, []string{"x"}, []string{"app", "service_name", "x"}},
		{`{app="gen"} | unpack`, "", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | line_format "{{.msg}}"`, "\x00", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | json | line_format "{{.msg}}"`, "login ok", sm, []string{"msg", "status", "svc_name", "user"}, []string{"app", "msg", "service_name", "status", "svc_name", "user"}},
		{`{app="gen"} | json | user="u1"`, "", sm, []string{"msg", "status", "svc_name", "user"}, []string{"app", "msg", "service_name", "status", "svc_name", "user"}},
		{`{app="gen"} | k8s_pod_name="pod-1"`, "", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | user!="u1"`, "", sm, none, []string{"app", "service_name"}},
		{`{app="lf"} | logfmt`, "", []string{"trace_id"}, []string{"msg", "status", "user"}, []string{"app", "msg", "service_name", "status", "user"}},
		{`{app="lf"} | logfmt user`, "", []string{"trace_id"}, []string{"user"}, []string{"app", "service_name", "user"}},
		{`{app="lf"} | json`, "", []string{"trace_id"}, none, []string{"app", "service_name"}},
		// A row stored without its line keeps the earlier classification.
		{`{app="pl"}`, "", []string{"msg", "status", "svc_name", "trace_id", "user"}, none, []string{"app", "service_name"}},
		{`{app="pl"} | json`, "", none, []string{"msg", "status", "svc_name", "trace_id", "user"}, []string{"app", "msg", "service_name", "status", "svc_name", "trace_id", "user"}},
	}
	backend := &stageFieldsBackend{}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		p := lineFieldsProxy(t, srv.URL, path)
		for _, tc := range cases {
			rows := stageRows(t, p, tc.query, true)
			if len(rows) != 1 {
				t.Fatalf("%s %s: %d rows, want 1", path, tc.query, len(rows))
			}
			got := rows[0]
			if !reflect.DeepEqual(got.sm, tc.sm) || !reflect.DeepEqual(got.parsed, tc.parsed) {
				t.Errorf("%s %s: structuredMetadata %v parsed %v, want %v %v", path, tc.query, got.sm, got.parsed, tc.sm, tc.parsed)
			}
			if want := []string{"app", "service_name"}; !reflect.DeepEqual(got.stream, want) {
				t.Errorf("%s %s: categorize-labels stream labels %v, want only the stream labels %v", path, tc.query, got.stream, want)
			}
			switch tc.line {
			case "\x00":
				if got.line != "" {
					t.Errorf("%s %s: line %q, want the empty line Loki renders for a label it does not have", path, tc.query, got.line)
				}
			case "":
			default:
				if got.line != tc.line {
					t.Errorf("%s %s: line %q, want %q", path, tc.query, got.line, tc.line)
				}
			}
			plain := stageRows(t, p, tc.query, false)
			if len(plain) != 1 || !reflect.DeepEqual(plain[0].stream, tc.stream) {
				t.Errorf("%s %s: stream labels without categorize-labels %+v, want %v", path, tc.query, plain, tc.stream)
			}
		}
	}
}

// TestLogQuery_LabelFilterOnLineFieldLikeLoki: a label filter on a key of the
// JSON line that no earlier stage exposes is evaluated by Loki on a label the
// entry does not have, so a string matcher sees "" and a numeric comparison
// fails. VictoriaLogs holds the key as a field and matches it; the proxy
// drops such a row. Structured metadata, stream labels and keys a stage
// exposes keep their values.
//
// conformance: profiles/label-filter-on-line-field, profiles/parsed-fields-without-parser, loki_api_v1_query_range
func TestLogQuery_LabelFilterOnLineFieldLikeLoki(t *testing.T) {
	backend := &stageFieldsBackend{}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		p := lineFieldsProxy(t, srv.URL, path)
		for query, want := range map[string]int{
			`{app="gen"} | user="u1"`:                0,
			`{app="gen"} | user=~"u.*"`:              0,
			`{app="gen"} | status > 100`:             0,
			`{app="gen"} | svc_name="api"`:           0,
			`{app="gen"} | user="u1" | json`:         0,
			`{app="gen"} | json user | status="200"`: 0,
			`{app="gen"} | json | user="u1"`:         1,
			`{app="gen"} | json user | user="u1"`:    1,
			`{app="gen"} | user!="u1"`:               1,
			`{app="gen"} | user=""`:                  1,
			`{app="gen"} | k8s_pod_name="pod-1"`:     1,
			`{app="gen"} | app="gen"`:                1,
			`{app="gen"} | user="u1" or app="gen"`:   1,
			`{app="pl"} | user="u1"`:                 1,
		} {
			for _, categorize := range []bool{true, false} {
				if got := len(stageRows(t, p, query, categorize)); got != want {
					t.Errorf("%s %s (categorize-labels %v): %d rows, want %d", path, query, categorize, got, want)
				}
			}
		}
	}
}

// TestBackendLogQuery_LineFormatLeftToProxy: in the Loki-compatible profile
// VictoriaLogs is not asked to rewrite the line for a line_format nothing
// later reads; the proxy renders it from the labels Loki has.
//
// conformance: profiles/stage-field-exposure
func TestBackendLogQuery_LineFormatLeftToProxy(t *testing.T) {
	p := lineFieldsProxy(t, "http://127.0.0.1:1", "buffered")
	for query, want := range map[string]string{
		`{app="a"} | line_format "{{.msg}}"`:                                   `{app="a"}`,
		`{app="a"} | json | line_format "{{.msg}}" | level="info"`:             `{app="a"} | json | level="info"`,
		`{app="a"} | line_format "{{.msg}}" |= "x"`:                            `{app="a"} | line_format "{{.msg}}" |= "x"`,
		`{app="a"} | line_format "{{.msg}}" | logfmt`:                          `{app="a"} | line_format "{{.msg}}" | logfmt`,
		`{app="a"} | line_format "a" | line_format "b"`:                        `{app="a"} | line_format "a" | line_format "b"`,
		`count_over_time({app="a"} | line_format "{{.msg}}" [5m])`:             `count_over_time({app="a"} | line_format "{{.msg}}" [5m])`,
		`{app="a"} | line_format "{{.msg}}" | label_format l="{{ __line__ }}"`: `{app="a"} | line_format "{{.msg}}" | label_format l="{{ __line__ }}"`,
		`{app="a"} | msg="| line_format \"x\"" | line_format "{{.msg}}"`:       `{app="a"} | msg="| line_format \"x\""`,
		// The first textual match is inside a raw string value: kept as is.
		"{app=\"a\"} | msg=`| line_format \"x\"` | line_format \"{{.msg}}\"": "{app=\"a\"} | msg=`| line_format \"x\"` | line_format \"{{.msg}}\"",
	} {
		if got := p.backendLogQuery(query); got != want {
			t.Errorf("backendLogQuery(%s) = %s, want %s", query, got, want)
		}
	}
	other := lineFieldsProxyWithMode(t, "http://127.0.0.1:1", MetadataFieldModeHybrid)
	if q := `{app="a"} | line_format "{{.msg}}"`; other.backendLogQuery(q) != q {
		t.Fatal("the hybrid metadata mode is not the Loki-compatible profile and sends line_format to VictoriaLogs")
	}
}

// TestPipelineAddsLabels_PerStage: what each stage exposes (lineFieldExposure).
//
// conformance: profiles/stage-field-exposure
func TestPipelineAddsLabels_PerStage(t *testing.T) {
	for query, want := range map[string]string{
		`{app="a"}`: "json=false logfmt=false unpack=false names=[]",
		`{app="a"} |= "x" | level="info" | drop p`:  "json=false logfmt=false unpack=false names=[]",
		`{app="a"} | decolorize | line_format "x"`:  "json=false logfmt=false unpack=false names=[]",
		`{app="a"} | json`:                          "json=true logfmt=false unpack=false names=[]",
		`{app="a"} | json user, n="svc.name"`:       "json=false logfmt=false unpack=false names=[n user]",
		`{app="a"} | logfmt`:                        "json=false logfmt=true unpack=false names=[]",
		`{app="a"} | logfmt user`:                   "json=false logfmt=false unpack=false names=[user]",
		`{app="a"} | regexp "(?P<m>\\w+) (\\w+)"`:   "json=false logfmt=false unpack=false names=[m]",
		`{app="a"} | pattern "<m> <_>"`:             "json=false logfmt=false unpack=false names=[m]",
		`{app="a"} | unpack`:                        "json=false logfmt=false unpack=true names=[]",
		`{app="a"} | label_format x="y", z=user`:    "json=false logfmt=false unpack=false names=[x z]",
		`{app="a"} | json user | json`:              "json=true logfmt=false unpack=false names=[user]",
		`{app="a"} | logfmt | json | logfmt status`: "json=true logfmt=true unpack=false names=[status]",
	} {
		lq := mustParseLogQuery(t, query)
		e := pipelineAddsLabels(lq.Pipeline)
		names := make([]string, 0, len(e.names))
		for n := range e.names {
			names = append(names, n)
		}
		sort.Strings(names)
		got := "json=" + boolString(e.jsonAll) + " logfmt=" + boolString(e.logfmtAll) + " unpack=" + boolString(e.unpack) + " names=[" + strings.Join(names, " ") + "]"
		if got != want {
			t.Errorf("%s: %s, want %s", query, got, want)
		}
	}

	// Each label filter carries what the stages before it expose.
	e := pipelineAddsLabels(mustParseLogQuery(t, `{app="a"} | user="u1" | json user | status>=500 | json | level!="x" | a="1" or b="2"`).Pipeline)
	var got []string
	for _, f := range e.filters {
		got = append(got, f.name+":"+boolString(f.matchesEmpty)+":"+boolString(f.before.jsonAll)+":"+boolString(f.before.names["user"]))
	}
	if want := "user:false:false:false status:false:false:true level:true:true:true"; strings.Join(got, " ") != want {
		t.Errorf("filters %v, want %s", got, want)
	}
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func mustParseLogQuery(t *testing.T, query string) *logqlpkg.LogQuery {
	t.Helper()
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	return lq
}

func lineFieldsProxyWithMode(t *testing.T, backendURL string, mode MetadataFieldMode) *Proxy {
	t.Helper()
	p, err := New(Config{BackendURL: backendURL, Cache: cache.New(time.Second, 10), LogLevel: "error",
		LabelStyle: LabelStyleUnderscores, MetadataFieldMode: mode})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDetectedFields_NestedJSONPathLikeLoki: Loki's json parser names a
// nested key by its path joined with underscores and reports the whole path
// in jsonPath ({"svc":{"name":"api"}} -> svc_name, ["svc","name"]).
//
// conformance: profiles/detected-fields-dotted-json-keys, loki_api_v1_detected_fields
func TestDetectedFields_NestedJSONPathLikeLoki(t *testing.T) {
	srv := httptest.NewServer(&stageFieldsBackend{})
	defer srv.Close()
	p := lineFieldsProxy(t, srv.URL, "buffered")
	w := serve(p, http.MethodGet, "/loki/api/v1/detected_fields?start=1790871720000000000&end=1790872320000000000&query="+url.QueryEscape(`{app="gen"}`), nil)
	var df struct {
		Fields []struct {
			Label    string   `json:"label"`
			JSONPath []string `json:"jsonPath"`
			Parsers  []string `json:"parsers"`
		} `json:"fields"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &df) != nil {
		t.Fatalf("detected_fields: %d %.300s", w.Code, w.Body)
	}
	got := map[string]string{}
	for _, f := range df.Fields {
		got[f.Label] = strings.Join(f.Parsers, ",") + " " + strings.Join(f.JSONPath, ",")
	}
	for label, want := range map[string]string{
		"svc_name":     "json svc,name",
		"msg":          "json msg",
		"user":         "json user",
		"status":       "json status",
		"k8s_pod_name": " ",
		"trace_id":     " ",
	} {
		if got[label] != want {
			t.Errorf("detected_fields %s: parsers/jsonPath %q, want %q (all: %v)", label, got[label], want, got)
		}
	}
}

// TestLineFieldExposure_LokiProfileOnly: only the Loki-compatible profile
// classifies line fields per stage; the hybrid and native metadata modes keep
// every stored field, as before.
//
// conformance: profiles/parsed-fields-without-parser, loki-compatible-profile
func TestLineFieldExposure_LokiProfileOnly(t *testing.T) {
	if lineFieldsProxy(t, "http://127.0.0.1:1", "buffered").lineFieldExposure(`{app="a"}`) == nil {
		t.Fatal("the Loki-compatible profile has no line field exposure")
	}
	for _, mode := range []MetadataFieldMode{MetadataFieldModeHybrid, MetadataFieldModeNative} {
		if lineFieldsProxyWithMode(t, "http://127.0.0.1:1", mode).lineFieldExposure(`{app="a"}`) != nil {
			t.Fatalf("%s metadata mode classifies line fields per stage", mode)
		}
	}
	if lineFieldsProxy(t, "http://127.0.0.1:1", "buffered").lineFieldExposure(`{app="a"`) != nil {
		t.Fatal("a query the parser rejects has a line field exposure")
	}
}

// TestLogQuery_ExtractionListValuesLikeLoki: every label of a json or logfmt
// extraction list has a value, read from the line as Loki reads it: a nested
// object as its JSON text, a logfmt rename from the renamed key, and an empty
// value for a key the line lacks. | json gives none for a line that does not
// start like JSON.
//
// conformance: profiles/stage-field-exposure, loki_api_v1_query_range
func TestLogQuery_ExtractionListValuesLikeLoki(t *testing.T) {
	srv := httptest.NewServer(&stageFieldsBackend{})
	defer srv.Close()
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		p := lineFieldsProxy(t, srv.URL, path)
		for query, want := range map[string]string{
			`{app="gen"} | json s="svc", nosuch, first="svc.name"`: `{"first":"api","nosuch":"","s":"{\"name\":\"api\"}"}`,
			`{app="lf"} | logfmt code="status", nosuch`:            `{"code":"200","nosuch":""}`,
			`{app="lf"} | json nosuch`:                             `null`,
		} {
			q := url.Values{}
			q.Set("query", query)
			q.Set("start", "1790871720000000000")
			q.Set("end", "1790872320000000000")
			req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil)
			req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
			w := httptest.NewRecorder()
			p.handleQueryRange(w, req)
			var resp struct {
				Data struct {
					Result []struct {
						Values [][]json.RawMessage `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil || len(resp.Data.Result) != 1 || len(resp.Data.Result[0].Values[0]) < 3 {
				t.Fatalf("%s %s: %d %s", path, query, w.Code, w.Body.String())
			}
			var meta struct {
				Parsed map[string]string `json:"parsed"`
			}
			_ = json.Unmarshal(resp.Data.Result[0].Values[0][2], &meta)
			got, _ := json.Marshal(meta.Parsed)
			if string(got) != want {
				t.Errorf("%s %s: parsed %s, want %s", path, query, got, want)
			}
		}
	}
}

// TestLokiJSONExpressionPath reads Loki's json expression forms.
//
// conformance: profiles/stage-field-exposure
func TestLokiJSONExpressionPath(t *testing.T) {
	for expr, want := range map[string]string{
		`user`:             "user",
		`svc.name`:         "svc|name",
		`items[0]`:         "items|[0]",
		`["a b"].c`:        "a b|c",
		`["a"]["b"][2]`:    "a|b|[2]",
		`bad[x]`:           "<nil>",
		`unterminated["a"`: "<nil>",
	} {
		path := lokiJSONExpressionPath(expr)
		got := strings.Join(path, "|")
		if path == nil {
			got = "<nil>"
		}
		if got != want {
			t.Errorf("lokiJSONExpressionPath(%s) = %s, want %s", expr, got, want)
		}
	}
}
