package vlemu

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// base is a whole hour, so the buckets of a step of up to an hour start at it.
const base = int64(1699999200) * int64(time.Second)

func sec(n int64) int64 { return base + n*int64(time.Second) }

// emuFailure is what the emulator reports through its testing.TB.
type emuFailure struct{ msg string }

// failingTB turns the emulator's Fatalf into a panic the test recovers.
type failingTB struct{ testing.TB }

func (failingTB) Fatalf(format string, args ...any) { panic(emuFailure{fmt.Sprintf(format, args...)}) }

// fatal runs f and returns the Fatalf message the emulator raised, "" when there was none.
func fatal(f func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			failure, ok := r.(emuFailure)
			if !ok {
				panic(r)
			}
			msg = failure.msg
		}
	}()
	f()
	return ""
}

func serve(h http.Handler, path string, form url.Values, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func newEmu(t *testing.T, rows ...Row) *Emulator {
	t.Helper()
	return &Emulator{T: failingTB{t}, Rows: rows, Field: "v"}
}

type sample struct {
	metric map[string]string
	values [][2]string // time in seconds, value
}

func decodeStats(t *testing.T, rec *httptest.ResponseRecorder) (kind string, out []sample) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var body struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	if body.Status != "success" {
		t.Fatalf("status %q", body.Status)
	}
	for _, r := range body.Data.Result {
		s := sample{metric: r.Metric}
		pairs := r.Values
		if body.Data.ResultType == "vector" {
			pairs = [][2]any{r.Value}
		}
		for _, p := range pairs {
			s.values = append(s.values, [2]string{fmt.Sprintf("%.0f", p[0].(float64)), p[1].(string)})
		}
		out = append(out, s)
	}
	return body.Data.ResultType, out
}

func rangeForm(query, step string) url.Values {
	return url.Values{"query": {query}, "start": {fmt.Sprint(sec(-600) / 1e9)}, "end": {fmt.Sprint(sec(600) / 1e9)}, "step": {step}}
}

// rel is an absolute second as an offset from base, the unit the assertions are written in.
func rel(abs string) string {
	var n int64
	fmt.Sscan(abs, &n)
	return fmt.Sprint(n - base/int64(time.Second))
}

func relSeries(out []sample) map[string][][2]string {
	m := map[string][][2]string{}
	for _, s := range out {
		key := s.metric["__name__"]
		for k, v := range s.metric {
			if k != "__name__" {
				key += "|" + k + "=" + v
			}
		}
		for _, p := range s.values {
			m[key] = append(m[key], [2]string{rel(p[0]), p[1]})
		}
	}
	return m
}

// The emulator answers the fixed endpoints and records the requests of the query endpoints.
func TestEmulatorEndpoints(t *testing.T) {
	e := newEmu(t)
	h := e.Handler()
	if rec := serve(h, "/health", nil); rec.Body.String() != "ok" {
		t.Errorf("/health = %q", rec.Body)
	}
	if rec := serve(h, "/metrics", nil); rec.Body.Len() != 0 {
		t.Errorf("/metrics without a version = %q", rec.Body)
	}
	e.Version = "v1.53.0"
	if rec := serve(h, "/metrics", nil); rec.Body.String() != `vm_app_version{short_version="v1.53.0",version="v1.53.0"} 1`+"\n" {
		t.Errorf("/metrics = %q", rec.Body)
	}
	if rec := serve(h, "/select/logsql/field_names", nil); rec.Body.String() != `{"values":[]}` {
		t.Errorf("unknown path = %q", rec.Body)
	}
	e.Rows = []Row{{TS: sec(0), Stream: `{app="a"}`, Value: 1}}
	serve(h, "/select/logsql/query", url.Values{"query": {`{app="a"}`}})
	serve(h, "/select/logsql/query", url.Values{"query": {`{app="a"} | stats count()`}})
	serve(h, "/select/logsql/stats_query", url.Values{"query": {`* | stats count()`}, "time": {"1"}})
	if got := e.EndpointCalls("/select/logsql/query"); !reflect.DeepEqual(got, []string{`{app="a"}`}) {
		t.Errorf("the stats probe is not a raw-row read: %q", got)
	}
	if got := e.EndpointCalls("/select/logsql/stats_query"); !reflect.DeepEqual(got, []string{`* | stats count()`}) {
		t.Errorf("stats_query calls = %q", got)
	}
	if got := e.EndpointCalls("/select/logsql/hits"); got != nil {
		t.Errorf("calls of an endpoint never called = %q", got)
	}
}

// Rows of the request's window come back oldest first as VictoriaLogs' JSON lines, each with its stream, its line (or
// <field>=<value>) and the labels a parser extracts.
func TestEmulatorRawRows(t *testing.T) {
	e := newEmu(t,
		Row{TS: sec(30), Stream: `{app="a"}`, Value: 2.5, Labels: map[string]string{"level": "warn"}},
		Row{TS: sec(0), Stream: `{app="a"}`, Value: 1, Msg: "custom line", Labels: map[string]string{"k": "v"}},
		Row{TS: sec(90), Stream: `{app="b"}`, Raw: "abc"},
	)
	h := e.Handler()
	lines := func(form url.Values) []map[string]string {
		rec := serve(h, "/select/logsql/query", form)
		if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
			t.Errorf("content type %q", ct)
		}
		var out []map[string]string
		for _, l := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n") {
			if l == "" {
				continue
			}
			var m map[string]string
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatalf("%v: %s", err, l)
			}
			out = append(out, m)
		}
		return out
	}
	all := lines(url.Values{"query": {"*"}})
	if len(all) != 3 || all[0]["_msg"] != "custom line" || all[1]["_msg"] != "v=2.5" || all[2]["_msg"] != "v=abc" {
		t.Fatalf("rows oldest first: %v", all)
	}
	first := all[0]
	if first["_time"] != time.Unix(0, sec(0)).UTC().Format(time.RFC3339Nano) || first["_stream"] != `{app="a"}` || first["_stream_id"] != `{app="a"}` || first["v"] != "1" || first["k"] != "v" {
		t.Errorf("fields of a row: %v", first)
	}
	// The window is [start, end): the end is exclusive. start and end take RFC 3339, whole seconds and nanoseconds.
	if got := lines(url.Values{"query": {"*"}, "start": {time.Unix(0, sec(0)).UTC().Format(time.RFC3339Nano)}, "end": {fmt.Sprint(sec(30) / 1e9)}}); len(got) != 1 || got[0]["_msg"] != "custom line" {
		t.Errorf("window [0, 30): %v", got)
	}
	if got := lines(url.Values{"query": {"*"}, "start": {fmt.Sprint(sec(30))}}); len(got) != 2 {
		t.Errorf("start in nanoseconds: %v", got)
	}
	if got := lines(url.Values{"query": {"*"}, "start": {fmt.Sprintf("%.1f", float64(sec(31))/1e9)}}); len(got) != 1 || got[0]["v"] != "abc" {
		t.Errorf("start in fractional seconds: %v", got)
	}
	// detected_level is Loki's value of the row where the query formats it.
	dl := lines(url.Values{"query": {`* | format "<__dl>" as detected_level`}})
	if dl[0]["detected_level"] != "unknown" || dl[1]["detected_level"] != "warn" {
		t.Errorf("default level derivation: %v", dl)
	}
	e.DeriveLevel = func(level, msg string) string { return level + ":" + msg }
	if dl = lines(url.Values{"query": {`* | format "<__dl>" as detected_level`}}); dl[1]["detected_level"] != "warn:v=2.5" {
		t.Errorf("DeriveLevel: %v", dl)
	}
	// The conversion check reads the rows the counting gate flags 1: a value that is no number and not nan.
	if bad := lines(url.Values{"query": {`* | filter __lvp_bad:="1"`}}); len(bad) != 1 || bad[0]["v"] != "abc" {
		t.Errorf("bad rows: %v", bad)
	}
	// The stored-row read of the conversion check names a stream.
	if byStream := lines(url.Values{"query": {"_stream_id:" + strconv.Quote(`{app="a"}`)}}); len(byStream) != 2 {
		t.Errorf("rows of the stream: %v", byStream)
	}
	if msg := fatal(func() { serve(h, "/select/logsql/query", url.Values{"query": {"*"}, "start": {"yesterday"}}) }); !strings.Contains(msg, `unparsable time "yesterday"`) {
		t.Errorf("unparsable start: %q", msg)
	}
}

// stats_query_range groups by the time bucket [edge, edge+step), the edges at k*step-offset; stats_query answers one
// value at the request's time.
func TestEmulatorStatsRangeBuckets(t *testing.T) {
	rows := []Row{
		{TS: sec(0), Stream: `{app="a"}`, Value: 1},
		{TS: sec(10), Stream: `{app="a"}`, Value: 2},
		{TS: sec(59), Stream: `{app="b"}`, Value: 4},
		{TS: sec(60), Stream: `{app="a"}`, Value: 8},
		{TS: sec(130), Stream: `{app="b"}`, Value: 16},
	}
	for _, c := range []struct {
		name   string
		query  string
		step   string
		offset string
		want   map[string][][2]string
	}{
		{"count per bucket", "* | stats count()", "1m", "", map[string][][2]string{"count()": {{"0", "3"}, {"60", "1"}, {"120", "1"}}}},
		{"a bucket holds the edge and not the next one", "* | stats count() as n", "60", "", map[string][][2]string{"n": {{"0", "3"}, {"60", "1"}, {"120", "1"}}}},
		{"sum by a label", "* | stats by (app) sum(__lvp_v) as s", "1m", "", map[string][][2]string{
			"s|app=a": {{"0", "3"}, {"60", "8"}}, "s|app=b": {{"0", "4"}, {"120", "16"}}}},
		{"avg, min, max and sum_len", "* | stats avg(__lvp_v) as a, min(__lvp_v) as lo, max(__lvp_v) as hi, sum_len(_msg) as len", "5m", "", map[string][][2]string{
			"a": {{"0", "6.2"}}, "lo": {{"0", "1"}}, "hi": {{"0", "16"}}, "len": {{"0", "16"}}}}, // four lines of 3 bytes and one of 4
		{"an offset moves the edges back", "* | stats count() as n", "1m", "10s", map[string][][2]string{"n": {{"-10", "2"}, {"50", "2"}, {"110", "1"}}}},
		{"a math pipe after the stats adds a metric", "* | stats count() as n | math n/2 as half", "1m", "", map[string][][2]string{
			"n": {{"0", "3"}, {"60", "1"}, {"120", "1"}}, "half": {{"0", "1.5"}, {"60", "0.5"}, {"120", "0.5"}}}},
		{"two stats pipes: the rate", "* | stats by (app) sum(__lvp_v) as __lvp_inner | math __lvp_inner/60 as __lvp_rate | stats by (app) sum(__lvp_rate)", "1m", "", map[string][][2]string{
			"sum(__lvp_rate)|app=a": {{"0", "0.05"}, {"60", "0.13333333333333333"}}, "sum(__lvp_rate)|app=b": {{"0", "0.06666666666666667"}, {"120", "0.26666666666666666"}}}},
		{"a presence filter keeps the groups with the field", `* | stats by (app) count() as c | filter c:* | stats count() as n`, "1m", "", map[string][][2]string{"n": {{"0", "2"}, {"60", "1"}, {"120", "1"}}}},
		{"a presence filter drops the groups without it", `* | stats by (app) count() as c | filter nope:* | stats count() as n`, "1m", "", map[string][][2]string{}},
		{"first and last row", "* | stats row_min(_time, __lvp_v) as first, count() as n", "5m", "", map[string][][2]string{"n": {{"0", "5"}}, "first": {{"0", `{"__lvp_v":"1"}`}}}},
		{"last row", "* | stats row_max(_time, __lvp_v) as last, count() as n", "5m", "", map[string][][2]string{"n": {{"0", "5"}}, "last": {{"0", `{"__lvp_v":"16"}`}}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEmu(t, rows...)
			form := rangeForm(c.query, c.step)
			if c.offset != "" {
				form.Set("offset", c.offset)
			}
			kind, out := decodeStats(t, serve(e.Handler(), "/select/logsql/stats_query_range", form))
			if kind != "matrix" {
				t.Fatalf("result type %q", kind)
			}
			got := relSeries(out)
			for name, want := range c.want {
				if !reflect.DeepEqual(got[name], want) {
					t.Errorf("series %s:\n got  %v\n want %v", name, got[name], want)
				}
			}
			for name := range got {
				if _, ok := c.want[name]; !ok {
					t.Errorf("unexpected series %s: %v", name, got[name])
				}
			}
		})
	}
}
