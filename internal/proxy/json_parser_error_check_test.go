package proxy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// Which range aggregations of a query fail on a line Loki's JSON parser rejects, as Loki's engine answers them
// (recorded with the Loki oracle: internal/querycorpus testdata/parser-error-wrappers-loki-diff.jsonl.gz). Every
// function keeps the error; only a sum without grouping directly over rate or count_over_time spares Loki the parse,
// and absent_over_time, a filter on __error__ and a drop of __error__ never reach it.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONErrorProbes(t *testing.T) {
	const leaf = `rate({job="sp"} | json [1m])`
	for _, tc := range []struct {
		query  string
		probes int
		offset time.Duration
	}{
		{leaf, 1, 0},
		{`sort(rate({job="sp"} | JSON [1m]))`, 1, 0},
		{`sort(rate({job="sp"} | Json [1m]))`, 1, 0},
		{`sort(sum by (detected_level) (count_over_time({job="sp"} | json [1m])))`, 1, 0},
		{`sort(` + leaf + `)`, 1, 0},
		{`sort_desc(` + leaf + `)`, 1, 0},
		{`topk(2, ` + leaf + `)`, 1, 0},
		{`bottomk(2, ` + leaf + `)`, 1, 0},
		{`sum by (app) (` + leaf + `)`, 1, 0},
		{`max by (app) (` + leaf + `)`, 1, 0},
		{`avg(` + leaf + `)`, 1, 0},
		{`count by (pod) (` + leaf + `)`, 1, 0},
		{`label_replace(` + leaf + `, "dst", "$1", "app", "(.*)")`, 1, 0},
		{leaf + ` * 2`, 1, 0},
		{`2 * ` + leaf, 1, 0},
		{leaf + ` > 0`, 1, 0},
		{leaf + ` + ` + leaf, 2, 0},
		{leaf + ` and count_over_time({job="ok"}[1m])`, 1, 0},
		{`count_over_time({job="ok"}[1m]) or ` + leaf, 1, 0},
		{`sort(sum by (app) (` + leaf + `))`, 1, 0},
		{`sum(sum by (app) (` + leaf + `))`, 1, 0},
		{`max(sort(` + leaf + `))`, 1, 0},
		{`rate({job="sp"} | json [1m] offset 40m)`, 1, 40 * time.Minute},
		{`sort(count_over_time({job="sp"} | json [5m] offset 1h))`, 1, time.Hour},
		{`sum by (app) (bytes_over_time({job="sp"} | json [1m]))`, 1, 0},
		// Loki does not parse: no label is required.
		{`sum(` + leaf + `)`, 0, 0},
		{`sort(sum(` + leaf + `))`, 0, 0},
		{`topk(1, sum(` + leaf + `))`, 0, 0},
		{`sum(` + leaf + `) + sum(` + leaf + `)`, 0, 0},
		{`sum(count_over_time({job="sp"} | json [1m]))`, 0, 0},
		// No error reaches the result.
		{`absent_over_time({job="sp"} | json [1m])`, 0, 0},
		{`rate({job="sp"} | json | __error__="" [1m])`, 0, 0},
		{`sort(rate({job="sp"} | json | __error__!="JSONParserErr" [1m]))`, 0, 0},
		{`rate({job="sp"} | json | drop __error__, __error_details__ [1m])`, 0, 0},
		{`rate({job="sp"} | logfmt [1m])`, 0, 0},
		{`rate({job="sp"} [1m])`, 0, 0},
		{`count_over_time({job="sp"} | regexp "(?P<a>x)" [1m])`, 0, 0},
		// A shape the evaluator does not compile is not checked (it answers as before).
		{`sum_over_time({job="sp"} | json | unwrap n [1m])`, 0, 0},
		{`rate({job="sp"} | json | line_format "{{.a}}" [1m])`, 0, 0},
	} {
		expr, err := logqlpkg.Parse(tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		probes := jsonErrorProbes(expr)
		if len(probes) != tc.probes {
			t.Errorf("%s: %d probes, want %d", tc.query, len(probes), tc.probes)
			continue
		}
		for _, p := range probes {
			if p.offset != tc.offset || p.window <= 0 {
				t.Errorf("%s: offset %s window %s, want offset %s", tc.query, p.offset, p.window, tc.offset)
			}
		}
	}
}

// The proxy's mark (logsql.JSONMark: literal replacements, no regular expression on the lines) marks the lines a
// query's own unpack_json reads, and leaves alone a query it cannot follow.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONErrorMarkSpec(t *testing.T) {
	got, ok := jsonErrorMarkSpec.MarkStats(`a:=b | unpack_json | stats by (_stream) count() as c`)
	if !ok || !strings.Contains(got, "stats by (_stream, "+jsonValidField+") count() as c") || strings.Contains(got, "_msg:~") {
		t.Fatalf("the mark (with no regular expression on the lines): %v %s", ok, got)
	}
	var c *jsonErrorCheck
	if got := c.rawRowsQuery(`a:=b | unpack_json`); got != `a:=b | unpack_json` {
		t.Errorf("a nil check rewrote %q", got)
	}
	c = &jsonErrorCheck{}
	for in, marked := range map[string]bool{
		`a:=b | unpack_json`:                                   true,
		`a:=b | unpack_json | fields _time, _msg`:              true, // a keep stage keeps the mark
		`a:=b | unpack_json | stats count()`:                   false,
		`a:=b | fields _time, _stream, _msg, level, app, app*`: false,
	} {
		if got := c.rawRowsQuery(in); strings.Contains(got, jsonErrorField) != marked {
			t.Errorf("rawRowsQuery(%q) = %q, marked %v", in, got, marked)
		}
	}
	if q, ok := jsonErrorMarkSpec.Lookup(`a:=b`, jsonLookupRows, "__lvp_window_phase"); !ok || !strings.Contains(q, "limit 8") {
		t.Errorf("lookup %v %s", ok, q)
	}
}

// A stats query is marked in every stats pipe, and a query whose pipes after the stats could mix or drop the marked
// groups is left alone; the strip takes the marked groups out and the empty mark off the others.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONCountingStatsQuery(t *testing.T) {
	c := &jsonErrorCheck{}
	params := func(q string) map[string][]string { return map[string][]string{"query": {q}, "step": {"60s"}} }
	for in, want := range map[string]string{
		`a:=b | unpack_json | stats by (_stream) count() as i | math i/60 as r | stats by (app) sum(r)`: `| stats by (_stream, __vlp) count() as i | math i/60 as r | stats by (app, __vlp) sum(r)`,
		`a:=b | unpack_json | stats count() as c`:                                                       `| stats by (__vlp) count() as c`,
		`a:=b | unpack_json | stats by (app) count() as c | sort by (c desc) | limit 2`:                 "",
		`a:=b | unpack_json | limit 9`:                                                                  "",
	} {
		sent, strip, ok := c.jsonCountingStatsQuery(t.Context(), "/select/logsql/stats_query_range", params(in))
		if ok != (want != "") {
			t.Errorf("%s: rewritten %v", in, ok)
			continue
		}
		if !ok {
			if sent.Get("query") != in || strip != nil {
				t.Errorf("%s: changed to %q", in, sent.Get("query"))
			}
			continue
		}
		if q := sent.Get("query"); !strings.HasSuffix(q, want) || !strings.Contains(q, "replace (") {
			t.Errorf("%s:\n got %q\nwant suffix %q", in, q, want)
		}
	}
	if _, _, ok := c.jsonCountingStatsQuery(t.Context(), "/select/logsql/query", params(`a:=b | unpack_json | stats count()`)); ok {
		t.Error("a raw-row read was rewritten as a stats query")
	}
	off := context.WithValue(t.Context(), jsonErrorMarkOffKey{}, true)
	if _, _, ok := c.jsonCountingStatsQuery(off, "/select/logsql/stats_query", params(`a:=b | unpack_json | stats count()`)); ok {
		t.Error("a read the evaluator owns was rewritten")
	}
	var nilCheck *jsonErrorCheck
	if _, _, ok := nilCheck.jsonCountingStatsQuery(t.Context(), "/select/logsql/stats_query", params(`a:=b | unpack_json | stats count()`)); ok {
		t.Error("a nil check rewrote a query")
	}

	_, strip, _ := c.jsonCountingStatsQuery(t.Context(), "/select/logsql/stats_query_range", params(`a:=b | unpack_json | stats by (app) count() as c`))
	clean := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","app":"api","__vlp":"1"},"values":[[60,"2"]]}]}}`)
	out, hits, err := strip(clean)
	if err != nil || len(hits) != 0 || string(out) != `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","app":"api"},"values":[[60,"2"]]}]}}` {
		t.Errorf("a clean answer: %s %v %v", out, hits, err)
	}
	marked := []byte(`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"c","app":"api","__vlp":"1"},"values":[[60,"2"]]},{"metric":{"__name__":"c","app":"api","__vlp":""},"values":[[120,"1"],[180,"1"]]}]}}`)
	out, hits, err = strip(marked)
	if err != nil || len(hits) != 2 {
		t.Fatalf("a marked answer: %s %v %v", out, hits, err)
	}
	var body struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &body); err != nil || len(body.Data.Result) != 1 || body.Data.Result[0].Metric[jsonValidField] != "" || body.Data.Result[0].Metric["app"] != "api" {
		t.Errorf("a marked answer kept %s", out)
	}
	if got, want := hits[0], (jsonErrorHit{from: time.Unix(60, 0), to: time.Unix(240, 0)}); !got.from.Equal(want.from) || !got.to.Equal(want.to) {
		t.Errorf("first hit %v, want %v", got, want)
	}
	for _, broken := range []string{
		`{"__vlp":""`,
		`{"data":"x","__vlp":""}`,
		`{"data":{"result":"x"},"__vlp":""}`,
		`{"data":{"result":[{"metric":"x"}]},"__vlp":""}`,
	} {
		if _, _, err := strip([]byte(broken)); err == nil {
			t.Errorf("a broken body was read: %s", broken)
		}
	}
	instant := []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"c","__vlp":""},"value":[300,"1"]}]}}`)
	_, strip, _ = c.jsonCountingStatsQuery(t.Context(), "/select/logsql/stats_query", params(`a:=b | unpack_json | stats count() as c`))
	if _, hits, err := strip(instant); err != nil || len(hits) != 1 || !hits[0].from.IsZero() {
		t.Errorf("an instant marked answer: %v %v", hits, err)
	}
}

// A raw row's mark is taken off the row and its time recorded.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONTakeRawRow(t *testing.T) {
	c := &jsonErrorCheck{}
	entry := map[string]interface{}{"_msg": "x", jsonErrorField: "1"}
	c.takeRawRowEntry(entry, 5)
	c.takeRawRowEntry(map[string]interface{}{"_msg": "{}"}, 6)
	if _, ok := entry[jsonErrorField]; ok || len(c.rawHits) != 1 || c.rawHits[0].from.UnixNano() != 5 {
		t.Errorf("entry %v hits %v", entry, c.rawHits)
	}
	var nilCheck *jsonErrorCheck
	nilCheck.takeRawRowEntry(entry, 1)
	for i := 0; i < jsonMaxHits+5; i++ {
		c.takeRawRowEntry(map[string]interface{}{jsonErrorField: "1"}, int64(i))
	}
	if len(c.rawHits) != jsonMaxHits {
		t.Errorf("%d hits kept, want the bound %d", len(c.rawHits), jsonMaxHits)
	}
}

// The one-pass read of a raw row finds the mark wherever VictoriaLogs places the field (first, middle, last,
// absent), reads the time, stream and level as the lookups do (first of a repeated key, a non-string field as
// absent, a mark of any value type), and takes the mark off the row.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONRawRowHeadFindsMarkAnywhere(t *testing.T) {
	const (
		timeField   = `"_time":"2024-01-01T00:00:00Z"`
		streamField = `"_stream":"{app=\"a\"}"`
		levelField  = `"level":"info"`
		msgField    = `"_msg":"x"`
		mark        = `"` + jsonErrorField + `":"1"`
	)
	row := func(fields ...string) string { return "{" + strings.Join(fields, ",") + "}" }
	c := &jsonErrorCheck{}
	var p fj.Parser
	for _, tc := range []struct {
		name   string
		line   string
		marked bool
		stream string
		level  string
		ts     string
	}{
		{"mark first", row(mark, timeField, streamField, levelField, msgField), true, `{app="a"}`, "info", "2024-01-01T00:00:00Z"},
		{"mark middle", row(timeField, streamField, mark, levelField, msgField), true, `{app="a"}`, "info", "2024-01-01T00:00:00Z"},
		{"mark last", row(msgField, levelField, streamField, timeField, mark), true, `{app="a"}`, "info", "2024-01-01T00:00:00Z"},
		{"mark absent", row(timeField, streamField, levelField, msgField), false, `{app="a"}`, "info", "2024-01-01T00:00:00Z"},
		{"mark of another type", row(timeField, `"`+jsonErrorField+`":true`), true, "", "", "2024-01-01T00:00:00Z"},
		{"no level, no stream", row(timeField, mark), true, "", "", "2024-01-01T00:00:00Z"},
		{"first of a repeated key wins", row(`"level":"warn"`, timeField, levelField, `"level":"error"`), false, "", "warn", "2024-01-01T00:00:00Z"},
		{"a non-string field reads as absent", row(`"level":5`, `"_time":7`, `"_stream":null`, mark), true, "", "", ""},
		{"an escaped key is read unescaped", row(`"_ti\u006de":"t"`, `"__lvp_p\u0065rr":"1"`), true, "", "", "t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := p.Parse(tc.line)
			if err != nil {
				t.Fatal(err)
			}
			gotTS, gotStream, gotLevel, marked := c.rawRowHead(v)
			if marked != tc.marked || string(gotTS) != tc.ts || string(gotStream) != tc.stream || string(gotLevel) != tc.level {
				t.Fatalf("got time %q stream %q level %q marked %v, want %q %q %q %v", gotTS, gotStream, gotLevel, marked, tc.ts, tc.stream, tc.level, tc.marked)
			}
			// The same fields read by lookup: the one-pass read changes no answer.
			if string(v.GetStringBytes("_time")) != tc.ts || string(v.GetStringBytes("_stream")) != tc.stream || string(v.GetStringBytes("level")) != tc.level || (v.Get(jsonErrorField) != nil) != tc.marked {
				t.Fatalf("the lookups read other fields than the one-pass read on %s", tc.line)
			}
			before := len(c.rawHits)
			if marked {
				c.takeRawRowMark(v, 9)
				if v.Get(jsonErrorField) != nil || len(c.rawHits) != before+1 || c.rawHits[before].from.UnixNano() != 9 {
					t.Errorf("the mark stays on the row or no hit recorded: %v", c.rawHits)
				}
			}
		})
	}
	// A request without a check reads the fields by lookup and never reports a mark.
	var none *jsonErrorCheck
	v, err := p.Parse(row(timeField, mark, levelField))
	if err != nil {
		t.Fatal(err)
	}
	if ts, _, level, marked := none.rawRowHead(v); marked || string(ts) != "2024-01-01T00:00:00Z" || string(level) != "info" {
		t.Errorf("no check: time %q level %q marked %v", ts, level, marked)
	}
	// A line that is not an object holds no field.
	v, err = p.Parse(`[1,2]`)
	if err != nil {
		t.Fatal(err)
	}
	if ts, stream, level, marked := c.rawRowHead(v); ts != nil || stream != nil || level != nil || marked {
		t.Errorf("an array line read as %q %q %q %v", ts, stream, level, marked)
	}
}

// The windows a probe reads follow Loki's evaluation: left-open, right-closed, moved back by the offset, and the
// step grid when the step is wider than the range.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONWindows(t *testing.T) {
	start := time.Unix(3600, 0)
	probe := &jsonErrorProbe{window: time.Minute, offset: 10 * time.Minute}
	instant := &unwrapCheck{instant: true, start: start, end: start}
	from, to, _ := instant.jsonWindows(probe)
	if want := start.Add(-11 * time.Minute).Add(time.Nanosecond); !from.Equal(want) {
		t.Errorf("from %s, want %s", from, want)
	}
	if want := start.Add(-10 * time.Minute).Add(time.Nanosecond); !to.Equal(want) {
		t.Errorf("to %s, want %s", to, want)
	}
	_, _, last := instant.jsonWindows(probe)
	for ts, visible := range map[time.Time]bool{
		start.Add(-10 * time.Minute):                      true,
		start.Add(-11 * time.Minute):                      false,
		start.Add(-11 * time.Minute).Add(time.Second):     true,
		start.Add(-10 * time.Minute).Add(time.Nanosecond): false,
	} {
		if got := instant.jsonVisible(ts.UnixNano(), probe, last); got != visible {
			t.Errorf("instant: %s visible %v, want %v", ts, got, visible)
		}
	}
	wide := &unwrapCheck{start: start, end: start.Add(10 * time.Minute), step: 3 * time.Minute}
	_, _, last = wide.jsonWindows(probe)
	if want := start.Add(9 * time.Minute); !last.Equal(want) {
		t.Errorf("last evaluation %s, want %s", last, want)
	}
	// Between two windows of a step wider than the range: no evaluation reads the line.
	if wide.jsonVisible(start.Add(-10*time.Minute).Add(90*time.Second).UnixNano(), probe, last) {
		t.Error("a line between two windows is visible")
	}
	if !wide.jsonVisible(start.Add(-10*time.Minute).Add(3*time.Minute).UnixNano(), probe, last) {
		t.Error("a line at an evaluation time is not visible")
	}
}

// A binary operation, a vector aggregation or label_replace is no bare range aggregation, whatever its text looks
// like: the text pattern read `count_over_time({job="ok"}[1m]) or rate({job="sp"} | json [1m])` as one selector
// running through the operator, and VictoriaLogs rejected the query it made (500 where Loki answers).
//
// conformance: semantics/json-parser-error-under-wrappers
func TestBareParserMetricSpecRejectsWrappedQueries(t *testing.T) {
	for _, q := range []string{
		`count_over_time({job="ok"}[1m]) or rate({job="sp", enc="json"} | json [1m])`,
		`rate({job="ok", enc="json"} | json [1m]) + rate({job="ok", enc="json"} | json [1m])`,
		`count_over_time({a="b"} | regexp "(?P<s>x)" [1m]) and count_over_time({a="b"} | regexp "(?P<s>x)" [1m])`,
		`label_replace(rate({a="b"} | json [1m]), "d", "$1", "a", "(.*)")`,
	} {
		if spec, ok := parseBareParserMetricCompatSpec(q); ok {
			t.Errorf("%s: read as a bare parser metric over %q", q, spec.baseQuery)
		}
	}
	if _, ok := parseBareParserMetricCompatSpec(`rate({a="b"} | json [1m])`); !ok {
		t.Error("a bare parser metric is no longer read")
	}
}

// FuzzJSONErrorProbesWrapperNesting nests fuzzed wrappers around error-producing and clean leaves and holds the
// decision to Loki's rule: every wrapper keeps a leaf's error, except a sum without grouping directly over a rate or
// count_over_time leaf (Loki does not parse), and the walk stays bounded.
func FuzzJSONErrorProbesWrapperNesting(f *testing.F) {
	for _, seed := range [][]byte{{0}, {1, 2, 3}, {4, 4, 4, 4}, {9, 0, 7}, {5, 6, 8, 1, 2}, make([]byte, 80)} {
		f.Add(seed, uint8(0))
		f.Add(seed, uint8(3))
	}
	leaves := []struct {
		text  string
		fails bool
	}{
		{`rate({job="sp"} | json [1m])`, true},
		{`count_over_time({job="sp"} | json [5m] offset 10m)`, true},
		{`rate({job="sp"} | json | __error__="" [1m])`, false},
		{`absent_over_time({job="sp"} | json [1m])`, false},
		{`rate({job="sp"} | logfmt [1m])`, false},
	}
	wrappers := []string{
		`sort(%s)`, `sort_desc(%s)`, `topk(2, %s)`, `bottomk(1, %s)`, `max by (app) (%s)`, `avg(%s)`,
		`label_replace(%s, "d", "$1", "app", "(.*)")`, `%s * 2`, `2 - %s`, `%s > 0`, `sum by (app) (%s)`,
	}
	f.Fuzz(func(t *testing.T, path []byte, leafIndex uint8) {
		if len(path) > 48 {
			path = path[:48]
		}
		leaf := leaves[int(leafIndex)%len(leaves)]
		query, binary := leaf.text, 1
		sumOverLeaf := false
		for i, b := range path {
			switch {
			case b == 255 && binary < 4:
				query = "(" + query + ") + (" + query + ")"
				binary *= 2
			case b == 254 && i == 0:
				query = "sum(" + query + ")"
				sumOverLeaf = true
			default:
				query = strings.Replace(wrappers[int(b)%len(wrappers)], "%s", query, 1)
			}
		}
		expr, err := logqlpkg.Parse(query)
		if err != nil {
			t.Skip()
		}
		start := time.Now()
		probes := jsonErrorProbes(expr)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("%s: %s", query, elapsed)
		}
		want := 0
		if leaf.fails && !sumOverLeaf {
			want = binary
		}
		if want > maxJSONErrorProbes {
			want = maxJSONErrorProbes
		}
		if len(probes) != want {
			t.Fatalf("%s: %d probes, want %d", query, len(probes), want)
		}
	})
}
