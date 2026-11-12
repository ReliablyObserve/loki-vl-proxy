package logql

import "testing"

func TestMayHaveNonFilterStage(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want bool
	}{
		{`{app="a"}`, false},
		{`{app="a"} |= "x" != "y" |~ "z.+" !~ "w" |> "<_> a" !> "b<_>"`, false},
		{`{app="a"} | status >= 500 | level="error" and path =~ "/a"`, false},
		{`{app="a"} | json="x"`, false},
		{`{app="a"} | drop != "x"`, false},
		{`{app="a"} |= "a | json b"`, false},
		{"{app=\"a\"} |= `a | json b`", false},
		{`{app="a"} |= "a\" | json"`, false},
		{`{app="a"} |= "x" | status="200" |> "<_> y"`, false},
		{`{a="b|json"}`, false},
		{`{service_name="a", detected_level="b"}`, false},
		{`{app="a"} |= "service_name" != "detected_level"`, false},
		{`{app="a"} | my_service_name="x" | detected_level_x="y"`, false},
		{`{app="a"} | detected_level="error"`, true},
		{`{app="a"} | service_name=~"a|b"`, true},
		{`{app="a"} |= "x" | status="200" and service_name="x"`, true},
		{`{app="a"} | json`, true},
		{`{app="a"}|json`, true},
		{`{app="a"} |  logfmt | level="x"`, true},
		{`{app="a"} | json status="x"`, true},
		{`{app="a"} | regexp "(?P<a>.)"`, true},
		{`{app="a"} | pattern "<_> x"`, true},
		{`{app="a"} | unpack`, true},
		{`{app="a"} | line_format "{{.a}}"`, true},
		{`{app="a"} | label_format a=b`, true},
		{`{app="a"} | decolorize`, true},
		{`{app="a"} | drop a`, true},
		{`{app="a"} | keep a`, true},
		{`{app="a"} |= "x" | json | a="b"`, true},
		{`sum(rate({app="a"} | unwrap n [5m]))`, true},
		{"{app=\"a\"} |\vjson", true},
		{"{app=\"a\"} |\fjson", true},
		{"{app=\"a\"} |\u00a0json", true},
		{"{app=\"a\"} |\u3000logfmt", true},
		{"{app=\"a\"} | # c\n json", true},
		{"{app=\"a\"} # \"\n | json", true},
		{"{app=\"a\"} # | json", false},
		{`{app="a"} | distinct a`, true},
		{`{app="a"} | JSON`, true},
		{"{}|jsOn", true},
		{`{app="a"} | Logfmt | level="x"`, true},
		{`{app="a"} | DROP x`, true},
		{`{app="a"} | Line_Format "x"`, true},
		{`{app="a"} | JSON_x="1"`, false},
		{``, false},
		{`{app="a`, false},
		{"{app=`a", false},
		{`"\`, false},
	} {
		if got := MayHaveNonFilterStage(tc.q); got != tc.want {
			t.Errorf("MayHaveNonFilterStage(%q) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

// The screen allocates nothing.
func TestMayHaveNonFilterStageDoesNotAllocate(t *testing.T) {
	for _, q := range []string{
		`{app="api", namespace="prod"} |= "error" != "debug" | status >= 500 | json`,
		`{app="api", namespace="prod"} |= "error" != "debug" | Status >= 500 | Label_Format a="b"`,
	} {
		if n := testing.AllocsPerRun(100, func() { _ = MayHaveNonFilterStage(q) }); n != 0 {
			t.Fatalf("%q: %v allocations", q, n)
		}
	}
}

func FuzzMayHaveNonFilterStage(f *testing.F) {
	for _, s := range []string{`{a="b"} | json`, `{a="b"} |= "x\"y" | status>5`, "{a=`b`} | drop", `"|json`, `|`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, q string) {
		// No panic, and a query the parser reads as filters only is never reported as having a stage.
		got := MayHaveNonFilterStage(q)
		if lq, err := ParseLogQuery(q); err == nil && !got {
			for _, st := range lq.Pipeline {
				if _, ok := st.(*ParserStage); ok {
					t.Fatalf("%q has a parser stage the screen missed", q)
				}
			}
		}
	})
}
