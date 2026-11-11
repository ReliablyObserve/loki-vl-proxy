package logsql

import (
	"reflect"
	"testing"
)

// The guard rewrites a filter on _time, _stream or _stream_id in another letter case to a copy into a scratch field,
// a filter on it and its delete, wherever the filter stands (the query's first part, a filter pipe, a pipe's if
// condition, a stats function's if), and leaves every other text as it is, byte for byte.
//
// conformance: semantics/reserved-field-name-case-outbound
func TestGuardReservedFieldCase(t *testing.T) {
	for in, want := range map[string]string{
		`app:="a" "_TIME":="up"`:                   `app:="a" | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:="up" | delete __lxrf_TIME`,
		`app:="a" _TIME:="up"`:                     `app:="a" | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:="up" | delete __lxrf_TIME`,
		`app:="a" _TIME:>=5s`:                      `app:="a" | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:>=5s | delete __lxrf_TIME`,
		`"_Stream_Id":="a"`:                        `* | copy _Stream_Id as __lxrf_Stream_Id | filter __lxrf_Stream_Id:="a" | delete __lxrf_Stream_Id`,
		`app:="a" -"_STREAM":="x"`:                 `app:="a" | copy _STREAM as __lxrf_STREAM | filter -__lxrf_STREAM:="x" | delete __lxrf_STREAM`,
		`app:="a" ("_TIME":="x" or b:="y") c:="z"`: `app:="a" c:="z" | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:="x" OR b:="y" | delete __lxrf_TIME`,
		`app:="a" "_TIME":="x" "_Time":>1`:         `app:="a" | copy _TIME as __lxrf_TIME, _Time as __lxrf_Time | filter __lxrf_TIME:="x" __lxrf_Time:>1 | delete __lxrf_TIME, __lxrf_Time`,
		`app:="a" | unpack_logfmt | filter "_TIME":~"^[0-9]+$" | copy "_TIME" as __lvp_n | math __lvp_n as __lvp_v | stats sum(__lvp_v)`: `app:="a" | unpack_logfmt | copy _TIME as __lxrf_TIME | filter __lxrf_TIME:~"^[0-9]+$" | delete __lxrf_TIME | copy "_TIME" as __lvp_n | math __lvp_n as __lvp_v | stats sum(__lvp_v)`,
		`app:="a" | format if ("_Stream":*) "<_Stream>" as x skip_empty_results`:                                                         `app:="a" | copy _Stream as __lxrf_Stream | format if (__lxrf_Stream:*) "<_Stream>" as x skip_empty_results | delete __lxrf_Stream`,
		`app:="a" | stats count() if ("_TIME":=x) as c`:                                                                                  `app:="a" | copy _TIME as __lxrf_TIME | stats count() if (__lxrf_TIME:="x") as c`,
	} {
		got := GuardReservedFieldCase(in)
		if got != want {
			t.Errorf("%s:\n got %s\nwant %s", in, got, want)
			continue
		}
		q, err := Parse(got)
		if err != nil {
			t.Errorf("%s: the guard's %s does not parse: %v", in, got, err)
			continue
		}
		if hasReservedCaseFilter(q) {
			t.Errorf("%s: %s still filters a name in another case", in, got)
		}
	}
	// Nothing to rewrite: the text itself comes back.
	for _, in := range []string{
		`app:="a"`, `app:="a" _time:5m`, `app:="a" "_time":="x"`, `app:="a" {_TIME="x"}`, `app:="a" _stream:{_TIME="x"}`,
		`app:="a" | copy _TIME as x | delete _TIME | fields _TIME | stats by (_TIME) count()`, `app:="a" "x _TIME y"`,
		`app:="a" __lxrf_TIME:="x"`, `app:="a" x_TIME:="x"`, `app:="a" _TIMES:="x"`, `not a (query`, `app:="a" _TIME:(`,
		`app:="a"  "_TIME" `, ``,
	} {
		if got := GuardReservedFieldCase(in); got != in {
			t.Errorf("%q: changed to %q", in, got)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = GuardReservedFieldCase(`app:="a" _time:[2025-01-01T00:00:00Z, 2025-01-02T00:00:00Z] | unpack_json | filter level:="error" | stats by (_stream) count()`)
	}); allocs != 0 {
		t.Errorf("a query without such a name allocates %v times", allocs)
	}
}

// hasReservedCaseFilter reports a filter of q that names a field in another case: what the guard's output never has.
func hasReservedCaseFilter(q *Query) bool {
	var names []string
	v := reflect.ValueOf(q).Elem()
	if q.Filter != nil {
		rewriteFilters(v.FieldByName("Filter"), true, &names)
	}
	for _, p := range q.Pipes {
		rewriteFilters(reflect.ValueOf(&p).Elem(), false, &names)
	}
	return len(names) > 0
}

// FuzzOutboundGuard holds the guard to its contract on any text: a text without such a name comes back as it is; a
// text the parser reads comes back as a text it reads, with no filter left on a name in another case, and a second
// pass changes nothing.
//
// conformance: semantics/reserved-field-name-case-outbound
func FuzzOutboundGuard(f *testing.F) {
	for _, s := range []string{
		`app:="a" "_TIME":="up"`, `app:="a" | unpack_logfmt | filter "_TIME":~"x" | stats count()`, `app:="a" (_Stream:x or b:y)`,
		`app:="a" | format if ("_STREAM_ID":*) "<a>" as b`, `app:="a" | stats count() if (_TiMe:>5) as c`, `_TIME:5m`, `app:="a"`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		if len(in) > 2000 {
			return
		}
		out := GuardReservedFieldCase(in)
		if !mayNameReservedOtherCase(in) {
			if out != in {
				t.Fatalf("%q: changed to %q without such a name", in, out)
			}
			return
		}
		if _, err := Parse(in); err != nil {
			if out != in {
				t.Fatalf("%q: an unreadable text changed to %q", in, out)
			}
			return
		}
		q, err := Parse(out)
		if err != nil {
			t.Fatalf("%q: the guard's %q does not parse: %v", in, out, err)
		}
		if hasReservedCaseFilter(q) {
			t.Fatalf("%q: the guard's %q still filters a name in another case", in, out)
		}
		if again := GuardReservedFieldCase(out); again != out {
			t.Fatalf("%q: a second pass changes %q to %q", in, out, again)
		}
	})
}

// The guard on a query without such a name, the text of every request the proxy sends but these: a byte scan.
func BenchmarkGuardReservedFieldCasePlain(b *testing.B) {
	q := `app:="a" _time:[2025-01-01T00:00:00Z, 2025-01-02T00:00:00Z] | unpack_json | filter level:="error" | stats by (_stream) count()`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if GuardReservedFieldCase(q) != q {
			b.Fatal("changed")
		}
	}
}

// The guard on a query it rewrites: a parse, the rewrite and a print.
func BenchmarkGuardReservedFieldCaseRewrite(b *testing.B) {
	q := `app:="a" | unpack_logfmt | filter "_TIME":~"^[0-9]+$" | copy "_TIME" as __lvp_n | math __lvp_n as __lvp_v | stats sum(__lvp_v)`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = GuardReservedFieldCase(q)
	}
}
