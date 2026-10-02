package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
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
	stageArrayRow = `{"_time":"2026-10-01T17:21:13Z","_msg":"{\"msg\":\"tagged\",\"tags\":[\"a\",\"b\"],\"svc\":{\"zones\":[1,2]}}",` +
		`"_stream":"{app=\"arr\",env=\"ev\"}","app":"arr","env":"ev","msg":"tagged","tags":"[\"a\",\"b\"]","svc.zones":"[1,2]","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`
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
	src := stageJSONRow
	switch {
	case strings.Contains(q, `"lf"`):
		src = stageLogfmtRow
	case strings.Contains(q, `"pl"`):
		src = stageMissingLineRow
	case strings.Contains(q, `"arr"`):
		src = stageArrayRow
	}
	row := map[string]string{}
	_ = json.Unmarshal([]byte(src+"}"), &row)
	if src == stageLogfmtRow && strings.Contains(q, "unpack_logfmt") {
		row["msg"], row["user"], row["status"] = "login ok", "u1", "200"
	}
	for marker, fields := range map[string][2]string{
		"extract_regexp":                        {"who", "u1"},
		"| extract ":                            {"m", "login ok"},
		`as x`:                                  {"x", "gen"},
		`" as l`:                                {"l", "{{ __line__ }}"},
		`copy "user" as u`:                      {"u", "u1"},
		`copy "svc.name" as n`:                  {"n", "api"},
		`"<user>" as u skip_empty_results`:      {"u", "u1"},
		`"<user>" as who skip_empty_results`:    {"who", "u1"},
		`"<status>" as code skip_empty_results`: {"code", "200"},
	} {
		if strings.Contains(q, marker) {
			row[fields[0]] = fields[1]
		}
	}
	for _, field := range []string{"user", "status"} {
		if strings.Contains(q, "| delete "+field) {
			delete(row, field)
		}
	}
	if strings.Contains(q, "copy _msg as "+translator.StoredLineField) {
		row[translator.StoredLineField] = row["_msg"]
	}
	if i := strings.Index(q, `| format "`); i >= 0 && !strings.Contains(q[i:], `" as `) {
		// line_format rewrites the stored line: the first placeholder's value.
		if m := regexp.MustCompile(`<([\w.]+)>`).FindStringSubmatch(q[i:]); m != nil {
			row["_msg"] = row[m[1]]
		}
	}
	body, _ := json.Marshal(row)
	w.Header().Set("Content-Type", "application/x-ndjson")
	_, _ = w.Write(append(body, '\n'))
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
		// stream holds the stream and parsed label names of a response without
		// categorize-labels; structured metadata joins them as well.
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
		// A line_format a later stage reads stays in the VictoriaLogs query,
		// which copies the stored line aside first.
		{`{app="gen"} | json | line_format "{{.user}}" |= "u"`, "u1", sm, []string{"msg", "status", "svc_name", "user"}, []string{"app", "msg", "service_name", "status", "svc_name", "user"}},
		{`{app="gen"} | line_format "{{.msg}}" | decolorize`, "\x00", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | json user | line_format "{{.user}}" | label_format l="{{ __line__ }}"`, "", sm, []string{"l", "user"}, []string{"app", "l", "service_name", "user"}},
		// label_format reading a key no stage exposed: nothing to rename, an
		// empty value in a template.
		{`{app="gen"} | label_format u=user`, "", sm, none, []string{"app", "service_name"}},
		{`{app="gen"} | label_format x="{{.user}}"`, "", sm, []string{"x"}, []string{"app", "service_name", "x"}},
		{`{app="gen"} | json | label_format who=user`, "", sm, []string{"msg", "status", "svc_name", "who"}, []string{"app", "msg", "service_name", "status", "svc_name", "who"}},
		// Arrays give no label.
		{`{app="arr"} | json`, "", []string{"trace_id"}, []string{"msg"}, []string{"app", "msg", "service_name"}},
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
			// Without categorize-labels Loki merges structured metadata into
			// the stream labels too.
			wantPlain := append(append([]string{}, tc.stream...), tc.sm...)
			sort.Strings(wantPlain)
			wantPlain = slices.Compact(wantPlain)
			plain := stageRows(t, p, tc.query, false)
			if len(plain) != 1 || !reflect.DeepEqual(plain[0].stream, wantPlain) {
				t.Errorf("%s %s: stream labels without categorize-labels %+v, want %v", path, tc.query, plain, wantPlain)
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
// later reads; the proxy renders it from the labels Loki has. A line_format
// that stays makes VictoriaLogs return the stored line as well.
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
		got, keepLine := p.backendLogQuery(query)
		if got != want {
			t.Errorf("backendLogQuery(%s) = %s, want %s", query, got, want)
		}
		// A line_format VictoriaLogs still runs needs the stored line too.
		if wantKeep := got == query && !strings.HasPrefix(got, "count_over_time"); keepLine != wantKeep {
			t.Errorf("backendLogQuery(%s) keeps the stored line %v, want %v", query, keepLine, wantKeep)
		}
	}
	other := lineFieldsProxyWithMode(t, "http://127.0.0.1:1", MetadataFieldModeHybrid)
	if q := `{app="a"} | line_format "{{.msg}}"`; func() bool { got, keep := other.backendLogQuery(q); return got != q || keep }() {
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
			`{app="gen"} | label_format x="{{.user}}-{{.app}}"`:    `{"x":"-gen"}`,
			`{app="gen"} | json | label_format x="{{.user}}"`:      `{"msg":"login ok","status":"200","svc_name":"api","user":"u1","x":"gen"}`,
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

// TestLogfmtLineHasKey covers keys at the start, middle and end of a line,
// quoted values with spaces, tab separators and keys that only sanitize to
// the label name.
//
// conformance: profiles/stage-field-exposure
func TestLogfmtLineHasKey(t *testing.T) {
	line := []byte("level=info msg=\"a b=c\" user=u1\tstatus=200")
	for key, want := range map[string]bool{"level": true, "msg": true, "user": true, "status": true, "b": false, "nosuch": false} {
		if got := logfmtLineHasKey(line, key); got != want {
			t.Errorf("logfmtLineHasKey(%q) = %v, want %v", key, got, want)
		}
	}
	if !logfmtLineHasKey([]byte("last=1"), "last") || logfmtLineHasKey([]byte(""), "x") || logfmtLineHasKey([]byte("bare"), "bare") {
		t.Error("single-token and empty lines")
	}
	if !logfmtLineHasLabel([]byte("a=1 http.method=GET"), "http_method") || logfmtLineHasLabel([]byte("http.method"), "http_method") {
		t.Error("sanitized key at the end of the line")
	}
}

// pagedRowsBackend answers like VictoriaLogs for a fixed set of rows: the
// start/end bounds (inclusive), the requested sort direction and the limit
// are applied; filters are not. It counts the requests.
type pagedRowsBackend struct {
	rows     []string // NDJSON rows, oldest first
	requests atomic.Int64
}

func (b *pagedRowsBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	b.requests.Add(1)
	bound := func(v string, def time.Time) time.Time {
		if v == "" {
			return def
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(0, n)
		}
		return def
	}
	start := bound(r.Form.Get("start"), time.Unix(0, 0))
	end := bound(r.Form.Get("end"), time.Unix(1<<40, 0))
	limit, _ := strconv.Atoi(r.Form.Get("limit"))
	var picked []string
	for _, row := range b.rows {
		var v struct {
			Time string `json:"_time"`
		}
		_ = json.Unmarshal([]byte(row), &v)
		ts, _ := time.Parse(time.RFC3339Nano, v.Time)
		if !ts.Before(start) && !ts.After(end) {
			picked = append(picked, row)
		}
	}
	if !strings.Contains(r.Form.Get("query"), "sort by (_time)") {
		for i, j := 0, len(picked)-1; i < j; i, j = i+1, j-1 {
			picked[i], picked[j] = picked[j], picked[i]
		}
	}
	if limit > 0 && len(picked) > limit {
		picked = picked[:limit]
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, row := range picked {
		_, _ = w.Write([]byte(row + "\n"))
	}
}

// TestLogQuery_LabelFilterOnLineFieldFillsLimit: rows the proxy drops after
// VictoriaLogs applied the limit are replaced by reading further pages, so a
// filter on user (structured metadata on older plain lines, a key of newer
// JSON lines) returns Loki's three plain lines, not an empty page; the
// refetch stops at its page budget.
//
// conformance: profiles/label-filter-on-line-field, loki_api_v1_query_range
func TestLogQuery_LabelFilterOnLineFieldFillsLimit(t *testing.T) {
	base := time.Date(2026, 10, 1, 17, 21, 0, 0, time.UTC)
	row := func(i int, msg string) string {
		ts := base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano)
		b, _ := json.Marshal(map[string]string{"_time": ts, "_msg": msg, "_stream": `{app="mix"}`, "app": "mix", "user": "u1"})
		return string(b)
	}
	backend := &pagedRowsBackend{}
	for i := 0; i < 3; i++ {
		backend.rows = append(backend.rows, row(i, fmt.Sprintf("plain line %d", i)))
	}
	for i := 3; i < 6; i++ {
		backend.rows = append(backend.rows, row(i, fmt.Sprintf(`{"msg":"json line %d","user":"u1"}`, i)))
	}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	run := func(p *Proxy, query, limit, direction string) []string {
		q := url.Values{}
		q.Set("query", query)
		q.Set("start", strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10))
		q.Set("end", strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10))
		q.Set("limit", limit)
		q.Set("direction", direction)
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
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil {
			t.Fatalf("%s: %d %s", query, w.Code, w.Body.String())
		}
		var lines []string
		for _, r := range resp.Data.Result {
			for _, v := range r.Values {
				var line string
				_ = json.Unmarshal(v[1], &line)
				lines = append(lines, line)
			}
		}
		sort.Strings(lines)
		return lines
	}
	for _, path := range []string{"buffered", "streamed", "windowed"} {
		p := lineFieldsProxy(t, srv.URL, path)
		for _, direction := range []string{"backward", "forward"} {
			got := run(p, `{app="mix"} | user="u1"`, "3", direction)
			if want := "[plain line 0 plain line 1 plain line 2]"; fmt.Sprint(got) != want {
				t.Errorf("%s %s: lines %v, want %s", path, direction, got, want)
			}
		}
		// A page budget bounds the refetch: one row kept per page of one.
		backend.requests.Store(0)
		got := run(p, `{app="mix"} | user="u1"`, "1", "backward")
		if fmt.Sprint(got) != "[plain line 2]" || backend.requests.Load() > 1+DefaultLabelFilterRefillMaxPages+2 {
			t.Errorf("%s limit 1: lines %v after %d requests", path, got, backend.requests.Load())
		}
		// Without a filter on a line key nothing is refetched.
		backend.requests.Store(0)
		if got := run(p, `{app="mix"} | user!="u1"`, "3", "backward"); len(got) != 3 || backend.requests.Load() > 2 {
			t.Errorf("%s negative filter: %v after %d requests", path, got, backend.requests.Load())
		}
	}
}

// TestLabelFilterRefillMaxPages: -label-filter-refill-max-pages bounds the
// further pages a filtered log page reads; 0 reads none, so the page keeps
// only the rows the first page held. The three newest rows are JSON lines
// the filter drops; a further page holds the rows read at its boundary plus
// the limit, so two further pages reach the newest plain line.
//
// conformance: profiles/label-filter-on-line-field
func TestLabelFilterRefillMaxPages(t *testing.T) {
	base := time.Date(2026, 10, 1, 17, 21, 0, 0, time.UTC)
	backend := &pagedRowsBackend{}
	for i := 0; i < 6; i++ {
		msg := fmt.Sprintf("plain line %d", i)
		if i >= 3 {
			msg = fmt.Sprintf(`{"msg":"json line %d","user":"u1"}`, i)
		}
		row, _ := json.Marshal(map[string]string{"_time": base.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
			"_msg": msg, "_stream": `{app="mix"}`, "app": "mix", "user": "u1"})
		backend.rows = append(backend.rows, string(row))
	}
	srv := httptest.NewServer(backend)
	defer srv.Close()
	for _, tc := range []struct {
		pages    int
		want     string
		requests int64
	}{
		{0, "[]", 1},
		{1, "[]", 2},
		{2, "[plain line 2]", 3},
		{-1, "[]", 1}, // a negative value reads no further page, like 0
	} {
		p, err := New(Config{BackendURL: srv.URL, Cache: cache.NewDisabled(), LogLevel: "error", EmitStructuredMetadata: true,
			LabelStyle: LabelStyleUnderscores, MetadataFieldMode: MetadataFieldModeTranslated, LabelFilterRefillMaxPages: tc.pages})
		if err != nil {
			t.Fatal(err)
		}
		backend.requests.Store(0)
		q := url.Values{}
		q.Set("query", `{app="mix"} | user="u1"`)
		q.Set("start", strconv.FormatInt(base.Add(-time.Minute).UnixNano(), 10))
		q.Set("end", strconv.FormatInt(base.Add(time.Minute).UnixNano(), 10))
		q.Set("limit", "1")
		w := httptest.NewRecorder()
		p.handleQueryRange(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+q.Encode(), nil))
		var resp struct {
			Data struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil {
			t.Fatalf("pages %d: %d %s", tc.pages, w.Code, w.Body.String())
		}
		lines := []string{}
		for _, r := range resp.Data.Result {
			for _, v := range r.Values {
				lines = append(lines, v[1])
			}
		}
		if got := fmt.Sprint(lines); got != tc.want || backend.requests.Load() != tc.requests {
			t.Errorf("pages %d: lines %s after %d requests, want %s after %d", tc.pages, got, backend.requests.Load(), tc.want, tc.requests)
		}
	}
}
