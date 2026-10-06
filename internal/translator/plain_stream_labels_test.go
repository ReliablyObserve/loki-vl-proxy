package translator

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func translateWithStreamLabels(t *testing.T, query string, names ...string) string {
	t.Helper()
	out, err := TranslateLogQLWithCapabilities(query, WithStreamLabels(nil, names), nil, logsql.Capabilities{})
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

// A plain name that is a stream label of the tenant reads the value stored
// before the parser: a log query filters a scratch field (the stored field
// stays the parsed value the response exposes), a metric query puts the stored
// value back into the field.
func TestPlainStreamLabel_LogFilterReadsScratch(t *testing.T) {
	got := translateWithStreamLabels(t, `{app="x"} | json | level="debug"`, "level")
	want := `app:="x" | copy level as __lxsv_level | unpack_json | format if (NOT __lxsv_level:* "level":*) "<level>" as __lxsv_level | filter __lxsv_level:="debug" | delete __lxsv_*`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestPlainStreamLabel_FilterOperators(t *testing.T) {
	for _, q := range []string{
		`{app="x"} | json | level="debug"`, `{app="x"} | logfmt | level!="debug"`, `{app="x"} | json | level=~"a|b"`,
		`{app="x"} | json | level!~"a|b"`, `{app="x"} | json | level!=""`, `{app="x"} | json | status="1" or level="a"`,
		`{app="x"} | regexp "(?P<level>\\w+)" | level="a"`, `{app="x"} | pattern "<level> <_>" | level="a"`,
	} {
		got := translateWithStreamLabels(t, q, "level")
		if !strings.Contains(got, "copy level as __lxsv_level") || !strings.Contains(got, "__lxsv_level:") || strings.Contains(got, " level:") {
			t.Errorf("%s: filter does not read the scratch field: %s", q, got)
		}
		if !strings.HasSuffix(got, "| delete __lxsv_*") {
			t.Errorf("%s: scratch not deleted: %s", q, got)
		}
	}
}

func TestPlainStreamLabel_MetricRestoresStoredValue(t *testing.T) {
	got := translateWithStreamLabels(t, `sum by (level) (count_over_time({app="x"} | logfmt | level="a" [5m]))`, "level")
	want := `app:="x" | copy level as __lxsv_level | unpack_logfmt | format if (__lxsv_level:*) "<__lxsv_level>" as level | filter level:="a" | delete __lxsv_* | stats by (level) count()`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// Each parser stage restores (a later parser would overwrite again), and only
// the first copies.
func TestPlainStreamLabel_EveryParserStage(t *testing.T) {
	got := translateWithStreamLabels(t, `sum by (level) (count_over_time({app="x"} | json | logfmt [5m]))`, "level")
	if strings.Count(got, "copy level as __lxsv_level") != 1 || strings.Count(got, `as level`) != 2 {
		t.Fatalf("one copy and a restore per parser expected: %s", got)
	}
}

// A query without a parser stage and a name label_format sets keep their plain
// translation (the proxy passes only the stream labels a query names, so a
// name that is no stream label never reaches the translator and the Drilldown
// fast paths still match).
func TestPlainStreamLabel_OtherQueriesUnchanged(t *testing.T) {
	for _, tc := range []struct{ q, name string }{
		{`{app="x", level="info"} |= "boom"`, "level"},
		{`{app="x"} | level="info"`, "level"},
		{`sum by (level) (count_over_time({app="x"} [5m]))`, "level"},
		{`{app="x"} | json | label_format level="z" | level="z"`, "level"},
		{`{app="x"} | json level, user`, "level"},
		{`{app="x"} | json |= "level=info"`, "level"},
	} {
		with := translateWithStreamLabels(t, tc.q, tc.name)
		plain, err := TranslateLogQL(tc.q)
		if err != nil {
			t.Fatal(err)
		}
		if tc.q == `{app="x"} | json | label_format level="z" | level="z"` {
			if strings.Contains(with, "filter __lxsv_level") {
				t.Errorf("a name label_format sets must keep its plain filter: %s", with)
			}
			continue
		}
		if with != plain {
			t.Errorf("%s changed\nwith  %s\nplain %s", tc.q, with, plain)
		}
	}
}

func TestPlainStreamLabel_NoNamesNoProbe(t *testing.T) {
	if got := plainStreamLabels(nil); got != nil {
		t.Fatalf("nil function: %v", got)
	}
	fn := func(l string) string { return l }
	if got := plainStreamLabels(fn); got != nil {
		t.Fatalf("plain function: %v", got)
	}
	if WithStreamLabels(nil, nil) != nil {
		t.Fatal("no names must keep the function as is")
	}
	w := WithStreamLabels(func(l string) string { return "x_" + l }, []string{"a", "b"})
	if got := plainStreamLabels(w); len(got) != 2 || w("c") != "x_c" {
		t.Fatalf("wrapper: %v %s", got, w("c"))
	}
}

// A label_format source and a label_format or line_format template that read
// a stream label's plain name read its scratch field in a log query.
func TestPlainStreamLabel_LogFormatsReadScratch(t *testing.T) {
	for q, want := range map[string]string{
		`{app="x"} | json | label_format x=level | x="info"`:          `format if (__lxsv_level:*) "<__lxsv_level>" as x`,
		`{app="x"} | json | label_format x="{{.level}}-a" | x="info"`: `format "<__lxsv_level>-a" as x`,
		`{app="x"} | json | line_format "{{.level}}" |= "info"`:       `format "<__lxsv_level>"`,
	} {
		if got := translateWithStreamLabels(t, q, "level"); !strings.Contains(got, want) {
			t.Errorf("%s\nwant %s in %s", q, want, got)
		}
	}
	// A name label_format set earlier is that label, not the stream's.
	if got := translateWithStreamLabels(t, `{app="x"} | json | label_format level="z" | label_format x=level | x="z"`, "level"); strings.Contains(got, "<__lxsv_level>") {
		t.Errorf("label set by label_format read as the stream label: %s", got)
	}
}
