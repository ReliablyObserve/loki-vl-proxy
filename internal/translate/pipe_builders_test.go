package translate

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func pipesText(pipes []logsql.Pipe) []string {
	out := make([]string, len(pipes))
	for i, p := range pipes {
		out[i] = p.String()
	}
	return out
}

// mustParseLogsQL fails the test when the VictoriaLogs syntax package cannot parse the query.
func mustParseLogsQL(t *testing.T, query string) {
	t.Helper()
	if _, err := logsql.Parse(query); err != nil {
		t.Errorf("%q does not parse: %v", query, err)
	}
}

func eqStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\n got  %q\n want %q", what, got, want)
	}
}

// A binary metric expression is split at an arithmetic or comparison operator written with a blank on each side; the
// set operators are not among them.
// conformance: architecture-syntax-package-layering
func TestBinaryOperatorToken(t *testing.T) {
	for op, want := range map[string]string{
		"/": " / ", "*": " * ", "+": " + ", "-": " - ", "%": " % ", "^": " ^ ",
		"==": " == ", "!=": " != ", ">=": " >= ", "<=": " <= ", ">": " > ", "<": " < ",
		"and": "", "or": "", "unless": "", "": "", "=": "", "=~": "",
	} {
		if got := BinaryOperatorToken(op); got != want {
			t.Errorf("BinaryOperatorToken(%q) = %q, want %q", op, got, want)
		}
	}
	// A two-byte comparison is tried before the one-byte comparison it starts with.
	pos := map[string]int{}
	for i, tok := range BinaryOperatorTokens {
		pos[tok] = i
	}
	for _, pair := range [][2]string{{" >= ", " > "}, {" <= ", " < "}} {
		if pos[pair[0]] > pos[pair[1]] {
			t.Errorf("%q must be tried before %q", pair[0], pair[1])
		}
	}
}

// Loki renames a parsed label that shares its name with a stream label to name_extracted.
// conformance: semantics/extracted-suffix-collision
func TestExtractedBaseAndFieldName(t *testing.T) {
	for _, c := range []struct {
		label, base string
		ok          bool
	}{
		{"app_extracted", "app", true},
		{"my_app_extracted", "my_app", true},
		{"app9_extracted", "app9", true},
		{"_extracted", "", false},
		{"extracted", "", false},
		{"app", "", false},
		{"a-b_extracted", "", false},
		{"a.b_extracted", "", false},
		{"app_extracted_extracted", "app_extracted", true},
	} {
		base, ok := ExtractedBase(c.label)
		if base != c.base || ok != c.ok {
			t.Errorf("ExtractedBase(%q) = %q, %v; want %q, %v", c.label, base, ok, c.base, c.ok)
		}
	}
	for name, want := range map[string]string{
		"app": "app", "a.b_c": "a.b_c", "A1": "A1", "a b": `"a b"`, "a-b": `"a-b"`, `a"b`: `"a\"b"`, "ü": `"ü"`,
	} {
		if got := LogsQLFieldName(name); got != want {
			t.Errorf("LogsQLFieldName(%q) = %s, want %s", name, got, want)
		}
	}
	for field, want := range map[string]string{`"a b"`: "a b", "ab": "ab", `"`: `"`, `""`: ""} {
		if got := unquoted(field); got != want {
			t.Errorf("unquoted(%s) = %q, want %q", field, got, want)
		}
	}
	if got, want := dotOrUnderscore("my_app_x"), `my[._]app[._]x`; got != want {
		t.Errorf("dotOrUnderscore = %q, want %q", got, want)
	}
}

// A filter on name_extracted reads the parsed key where the entry has the stream label name, and the key
// name_extracted where it has not.
// conformance: semantics/extracted-suffix-collision
func TestExtractedFilter(t *testing.T) {
	eq := func(field, v string) logsql.FilterExpr {
		return logsql.FieldFilter{Field: field, Op: logsql.FieldOpExact, Value: v}
	}
	for _, c := range []struct {
		name        string
		base        string
		parsed, lit logsql.FilterExpr
		want        string
		wantOK      bool
	}{
		{
			name: "stream label other than service_name", base: "app", parsed: eq("__lxp_app", "1"), lit: eq("app_extracted", "1"),
			want:   `((__lxp_stream:~"[{,]app=\"" __lxp_app:* __lxp_app:="1") OR (NOT (__lxp_stream:~"[{,]app=\"" __lxp_app:*) (app_extracted:="1")))`,
			wantOK: true,
		},
		{
			name: "underscore may be a dot in the stream", base: "my_app", parsed: eq("x", "1"), lit: eq("app_extracted", "1"),
			want:   `((__lxp_stream:~"[{,]my[._]app=\"" __lxp_my_app:* x:="1") OR (NOT (__lxp_stream:~"[{,]my[._]app=\"" __lxp_my_app:*) (app_extracted:="1")))`,
			wantOK: true,
		},
		{
			name: "service_name is every stream's label", base: "service_name", parsed: eq("x", "1"), lit: eq("y", "1"),
			want:   `((__lxp_service_name:* x:="1") OR (NOT (__lxp_service_name:*) (y:="1")))`,
			wantOK: true,
		},
		{
			name: "an alternation is the translator's to join", base: "app",
			parsed: logsql.AnyOf{eq("x", "1"), eq("y", "2")}, lit: eq("z", "1"), wantOK: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, ok := ExtractedFilter(c.base, c.parsed, c.lit)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !ok {
				if f != nil {
					t.Errorf("filter = %v, want nil", f)
				}
				return
			}
			if got := f.String(); got != c.want {
				t.Errorf("filter:\n got  %s\n want %s", got, c.want)
			}
			mustParseLogsQL(t, "* "+f.String())
		})
	}
}

// The scratch unpack and copy pipes that carry a parsed key past the stream label of the same name.
// conformance: semantics/extracted-suffix-collision
func TestExtractedScratchPipes(t *testing.T) {
	if got, want := ExtractedScratchField(1, "a-b"), `"__lxp1_a-b"`; got != want {
		t.Errorf("ExtractedScratchField = %s, want %s", got, want)
	}
	if got, want := ExtractedScratchField(0, "app"), "__lxp0_app"; got != want {
		t.Errorf("ExtractedScratchField = %s, want %s", got, want)
	}
	if got, want := ExtractedUnpack(true, 0, []string{"app", "a-b"}).String(), `| unpack_logfmt from _msg fields (app, "a-b") result_prefix "__lxp0_"`; got != want {
		t.Errorf("logfmt unpack = %s, want %s", got, want)
	}
	if got, want := ExtractedUnpack(false, 2, []string{"app"}).String(), `| unpack_json from _msg fields (app) result_prefix "__lxp2_"`; got != want {
		t.Errorf("json unpack = %s, want %s", got, want)
	}
	if got, want := ExtractedDelete().String(), "| delete __lxp*"; got != want {
		t.Errorf("delete = %s, want %s", got, want)
	}

	// Two parsers: the first one's value wins, so the last parser is copied and each earlier one overrides it where it
	// holds the key. _stream is copied aside once, by the first base that is not service_name.
	copied := false
	eqStrings(t, "two parsers, two bases", pipesText(ExtractedCoalesce([]string{"app", "service_name"}, 2, &copied)), []string{
		"| copy _stream as __lxp_stream",
		"| copy __lxp1_app as __lxp_app",
		`| format if (__lxp0_app:*) "<__lxp0_app>" as __lxp_app`,
		"| copy __lxp1_service_name as __lxp_service_name",
		`| format if (__lxp0_service_name:*) "<__lxp0_service_name>" as __lxp_service_name`,
	})
	if !copied {
		t.Error("streamCopied was not set")
	}
	copied = true
	eqStrings(t, "stream already copied", pipesText(ExtractedCoalesce([]string{"app"}, 1, &copied)), []string{"| copy __lxp0_app as __lxp_app"})
	copied = false
	eqStrings(t, "service_name alone needs no stream copy", pipesText(ExtractedCoalesce([]string{"service_name"}, 1, &copied)), []string{"| copy __lxp0_service_name as __lxp_service_name"})
	if copied {
		t.Error("service_name alone must not copy _stream")
	}
}

// ExtractedStatsText writes the whole collision rename of a stats pipe over a query whose parsers extract every key.
// conformance: semantics/extracted-suffix-collision
func TestExtractedStatsText(t *testing.T) {
	for _, c := range []struct {
		name       string
		logfmt     []bool
		bases      []string
		labels     []string
		want       string
		wantCopies bool
	}{
		{
			name: "json parser, one base", logfmt: []bool{false}, bases: []string{"app"}, labels: []string{"app_extracted"},
			want: ` | unpack_json from _msg fields (app) result_prefix "__lxp0_"` +
				` | copy _stream as __lxp_stream` +
				` | copy __lxp0_app as __lxp_app` +
				` | format if (__lxp_stream:~"[{,]app=\"" __lxp_app:*) "<__lxp_app>" as app_extracted` +
				` | delete __lxp*`,
			wantCopies: true,
		},
		{
			name: "logfmt then json, service_name", logfmt: []bool{true, false}, bases: []string{"service_name"}, labels: []string{"service_name_extracted"},
			want: ` | unpack_logfmt from _msg fields (service_name) result_prefix "__lxp0_"` +
				` | unpack_json from _msg fields (service_name) result_prefix "__lxp1_"` +
				` | copy __lxp1_service_name as __lxp_service_name` +
				` | format if (__lxp0_service_name:*) "<__lxp0_service_name>" as __lxp_service_name` +
				` | format if (__lxp_service_name:*) "<__lxp_service_name>" as service_name_extracted` +
				` | delete __lxp*`,
			wantCopies: true,
		},
		{
			name: "two bases, dotted stream label", logfmt: []bool{false}, bases: []string{"my_app", "env"}, labels: []string{"my_app_extracted", "env_extracted"},
			want: ` | unpack_json from _msg fields (my_app, env) result_prefix "__lxp0_"` +
				` | copy _stream as __lxp_stream` +
				` | copy __lxp0_my_app as __lxp_my_app` +
				` | copy __lxp0_env as __lxp_env` +
				` | format if (__lxp_stream:~"[{,]my[._]app=\"" __lxp_my_app:*) "<__lxp_my_app>" as my_app_extracted` +
				` | format if (__lxp_stream:~"[{,]env=\"" __lxp_env:*) "<__lxp_env>" as env_extracted` +
				` | delete __lxp*`,
			wantCopies: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			var b strings.Builder
			copies := ExtractedStatsText(&b, c.logfmt, c.bases, c.labels)
			if b.String() != c.want {
				t.Errorf("text:\n got  %s\n want %s", b.String(), c.want)
			}
			if copies != c.wantCopies {
				t.Errorf("copies = %v, want %v", copies, c.wantCopies)
			}
			mustParseLogsQL(t, "* "+b.String())
		})
	}
}

// A json expression defines a label from the key it names; every other spelling is the translator's to read.
// conformance: architecture-syntax-package-layering
func TestJSONAliasCopies(t *testing.T) {
	f := func(name, expr string) logql.ExtractionField {
		return logql.ExtractionField{Name: name, Expression: expr}
	}
	for _, c := range []struct {
		name   string
		fields []logql.ExtractionField
		param  string
		want   []string
		ok     bool
	}{
		{"no fields", nil, "", nil, true},
		{"alias copies in name order, bare names copy nothing", []logql.ExtractionField{f("b", "x.y"), f("a", "m"), f("c", "c"), f("d", "")}, `b="x.y", a="m", c, d`,
			[]string{`| copy "m" as a`, `| copy "x.y" as b`}, true},
		{"a dotted or hyphenated alias is quoted", []logql.ExtractionField{f("http.m", "m"), f("x-y", "k")}, `http.m="m", x-y="k"`,
			[]string{`| copy "m" as "http.m"`, `| copy "k" as "x-y"`}, true},
		{"backslash", []logql.ExtractionField{f("a", `x\y`)}, `a="x\\y"`, nil, false},
		{"raw string", []logql.ExtractionField{f("a", "x")}, "a=`x`", nil, false},
		{"line break", []logql.ExtractionField{f("a", "x")}, "a=\"x\"\n", nil, false},
		{"tab", []logql.ExtractionField{f("a", "x")}, "a=\"x\"\t", nil, false},
		{"name written twice", []logql.ExtractionField{f("a", "x"), f("a", "y")}, `a="x", a="y"`, nil, false},
		{"leading space", []logql.ExtractionField{f("a", " x")}, `a=" x"`, nil, false},
		{"trailing space", []logql.ExtractionField{f("a", "x ")}, `a="x "`, nil, false},
		{"bracket path", []logql.ExtractionField{f("a", "[0]")}, `a="[0]"`, nil, false},
		{"comma", []logql.ExtractionField{f("a", "x,y")}, `a="x,y"`, nil, false},
		{"quote", []logql.ExtractionField{f("a", `x"y`)}, `a="x"y"`, nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			pipes, ok := JSONAliasCopies(c.fields, c.param)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			eqStrings(t, "pipes", pipesText(pipes), c.want)
		})
	}
	if got := JSONUnpack().String(); got != "| unpack_json" {
		t.Errorf("JSONUnpack = %s", got)
	}
	if got := LogfmtUnpack().String(); got != "| unpack_logfmt" {
		t.Errorf("LogfmtUnpack = %s", got)
	}
}

// drop deletes its bare labels except the ones detected_level is derived from, which stay in the row.
// conformance: architecture-syntax-package-layering
func TestDropDeleteKeepsLevelSources(t *testing.T) {
	pipe, ok := DropDelete([]string{"level", "foo", "bar", "severity_number"})
	if !ok || pipe.String() != "| delete foo, bar" {
		t.Errorf("DropDelete = %v, %v", pipe, ok)
	}
	for _, labels := range [][]string{nil, {}, {"level"}, {"level", "severity_number", "lvl"}} {
		if pipe, ok := DropDelete(labels); ok || pipe != nil {
			t.Errorf("DropDelete(%v) = %v, %v; want no pipe", labels, pipe, ok)
		}
	}
	got := LevelSourceFields()
	got[0] = "changed"
	if LevelSourceFields()[0] != "level" {
		t.Error("LevelSourceFields returned the shared slice")
	}
}

// keep lists the row's time, line and stream, the names, the stored fields behind a collision rename and a level.
// conformance: architecture-syntax-package-layering
func TestKeepFields(t *testing.T) {
	label := func(s string) string {
		switch s {
		case streamLabelsProbe:
			return streamLabelsProbe + "=app,env"
		case "http_status":
			return "http.status"
		}
		return s
	}
	base := []string{"_time", "_msg", "_stream"}
	for _, c := range []struct {
		name  string
		names []string
		label func(string) string
		want  []string
	}{
		{"plain names", []string{"app"}, nil, []string{"app"}},
		{"detected_level also keeps its source", []string{"detected_level"}, nil, []string{"detected_level", "level"}},
		{"level already kept", []string{"detected_level", "level"}, nil, []string{"detected_level", "level"}},
		{"collision rename keeps the stored name", []string{"app_extracted"}, nil, []string{"app_extracted", "app"}},
		{"stored name already kept", []string{"app", "app_extracted"}, nil, []string{"app", "app_extracted"}},
		{"not a bare identifier", []string{"a b_extracted", "_extracted"}, nil, []string{"a b_extracted", "_extracted"}},
		{"label translation adds the stored name", []string{"http_status", "service_name", "detected_level", "app"}, label,
			[]string{"http_status", "service_name", "detected_level", "app", "level", "http.status"}},
		{"stored name already present", []string{"http_status", "http.status"}, label, []string{"http_status", "http.status"}},
	} {
		eqStrings(t, c.name, KeepFields(c.names, c.label).(logsql.PipeFields).Labels, append(append([]string{}, base...), c.want...))
	}
}

// The label function carries the stream label names behind a probe name.
// conformance: architecture-syntax-package-layering
func TestStreamLabelNames(t *testing.T) {
	carrying := func(s string) string {
		if s == streamLabelsProbe {
			return streamLabelsProbe + "=app,env"
		}
		return s
	}
	if list, ok := StreamLabelList(carrying); !ok || list != "app,env" {
		t.Errorf("StreamLabelList = %q, %v", list, ok)
	}
	eqStrings(t, "names", StreamLabelNames(carrying), []string{"app", "env"})
	for name, fn := range map[string]func(string) string{
		"nil function":       nil,
		"identity":           func(s string) string { return s },
		"probe echoed alone": func(string) string { return streamLabelsProbe },
		"wrong separator":    func(string) string { return streamLabelsProbe + ":app" },
	} {
		if list, ok := StreamLabelList(fn); ok || list != "" {
			t.Errorf("%s: StreamLabelList = %q, %v", name, list, ok)
		}
		if names := StreamLabelNames(fn); names != nil {
			t.Errorf("%s: StreamLabelNames = %v", name, names)
		}
	}
	if names := StreamLabelNames(func(string) string { return streamLabelsProbe + "=" }); len(names) != 1 || names[0] != "" {
		t.Errorf("an empty list holds one empty name, got %q", names)
	}
}

// A range aggregation over a log query without an unwrapped value counts the rows, or sums the bytes of their lines.
// conformance: architecture-syntax-package-layering
func TestRangeStatsPipes(t *testing.T) {
	for op, want := range map[logql.RangeOp]string{
		logql.RangeCountOverTime:  "| stats count()",
		logql.RangeAbsentOverTime: "| stats count()",
		logql.RangeBytesOverTime:  "| stats sum_len(_msg)",
	} {
		pipe, ok := RangeStatsPipe(op)
		if !ok || pipe.String() != want {
			t.Errorf("RangeStatsPipe(%s) = %s, %v; want %s", op, pipe, ok, want)
		}
	}
	for _, op := range []logql.RangeOp{logql.RangeRate, logql.RangeBytesRate, logql.RangeSumOverTime, ""} {
		if _, ok := RangeStatsPipe(op); ok {
			t.Errorf("RangeStatsPipe(%s) must refuse", op)
		}
	}
	for op, want := range map[logql.RangeOp]string{
		logql.RangeCountOverTime: "count()", logql.RangeAbsentOverTime: "count()", logql.RangeRate: "count()",
		logql.RangeBytesOverTime: "sum_len(_msg)", logql.RangeBytesRate: "sum_len(_msg)",
	} {
		fn, ok := RangeRowFunc(op)
		if !ok || fn.String() != want {
			t.Errorf("RangeRowFunc(%s) = %v, %v; want %s", op, fn, ok, want)
		}
	}
	if _, ok := RangeRowFunc(logql.RangeSumOverTime); ok {
		t.Error("RangeRowFunc must refuse sum_over_time")
	}
}

// rate and bytes_rate divide the per-series count (or bytes) by the range's seconds, then sum per series.
// conformance: architecture-syntax-package-layering
func TestRateStatsPipes(t *testing.T) {
	series := "by (_stream, level, detected_level)"
	for op, fn := range map[logql.RangeOp]string{logql.RangeRate: "count()", logql.RangeBytesRate: "sum_len(_msg)"} {
		pipes, ok := RateStatsPipes(op, 60)
		if !ok {
			t.Fatalf("RateStatsPipes(%s) refused", op)
		}
		eqStrings(t, string(op), pipesText(pipes[:]), []string{
			"| stats " + series + " " + fn + " as __lvp_inner",
			"| math __lvp_inner/60 as __lvp_rate",
			"| stats " + series + " sum(__lvp_rate)",
		})
	}
	for _, op := range []logql.RangeOp{logql.RangeCountOverTime, logql.RangeBytesOverTime, logql.RangeAbsentOverTime} {
		if pipes, ok := RateStatsPipes(op, 60); ok || pipes != [3]logsql.Pipe{} {
			t.Errorf("RateStatsPipes(%s) = %v, %v; want a refusal", op, pipes, ok)
		}
	}
	// A parser's query is per stream only.
	pipes := RateStatsPipesBy(logsql.Count{}, 2.5, []logsql.GroupKey{{Field: "_stream"}})
	eqStrings(t, "by stream", pipesText(pipes[:]), []string{
		"| stats by (_stream) count() as __lvp_inner",
		"| math __lvp_inner/2.5 as __lvp_rate",
		"| stats by (_stream) sum(__lvp_rate)",
	})
	if got, want := RateDivision(0.5).String(), "| math __lvp_inner/0.5 as __lvp_rate"; got != want {
		t.Errorf("RateDivision = %s, want %s", got, want)
	}
	chain := RateDetectedLevelChain()
	if chain == "" || chain != RateDetectedLevelChain() {
		t.Error("RateDetectedLevelChain must be a stable non-empty chain")
	}
}

// A filter on a stream label after a parser reads the stream's value, not the parsed key written over it.
// conformance: semantics/extracted-suffix-collision
func TestStreamValueScratchPipes(t *testing.T) {
	eqStrings(t, "copies", pipesText(StreamValueCopies([]string{"app", "x"})), []string{"| copy app as __lxsv_app", "| copy x as __lxsv_x"})
	eqStrings(t, "fills", pipesText(StreamValueFills([]string{"my_app"})), []string{
		`| format if (NOT __lxsv_my_app:* "my_app":*) "<my_app>" as __lxsv_my_app`,
		`| format if (NOT __lxsv_my_app:* "my.app":*) "<my.app>" as __lxsv_my_app`,
		`| format if (NOT __lxsv_my_app:* "my-app":*) "<my-app>" as __lxsv_my_app`,
	})
	eqStrings(t, "fills of a name without underscore", pipesText(StreamValueFills([]string{"app"})), []string{
		`| format if (NOT __lxsv_app:* "app":*) "<app>" as __lxsv_app`,
	})
	eqStrings(t, "restores", pipesText(StreamValueRestores([]string{"app"})), []string{`| format if (__lxsv_app:*) "<__lxsv_app>" as app`})
	if got := StreamValueDelete().String(); got != "| delete __lxsv_*" {
		t.Errorf("StreamValueDelete = %s", got)
	}
	if got := len(StreamValueCopies(nil)) + len(StreamValueFills(nil)) + len(StreamValueRestores(nil)); got != 0 {
		t.Errorf("no names write no pipes, got %d", got)
	}
}
