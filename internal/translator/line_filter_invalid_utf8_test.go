package translator

import "testing"

// A substring line filter whose text is not UTF-8 matches as Loki's byte match does on lines every ingestion API
// can store (UTF-8 strings only): |= keeps no line, != keeps every line, and an `or` chain keeps its other
// alternatives. A regexp that is not UTF-8 stays Loki's parse error, raised before translation.
//
// conformance: semantics/line-filter-invalid-utf8
func TestTranslateLineFilterInvalidUTF8(t *testing.T) {
	for _, tc := range []struct{ logql, want string }{
		{`{app="a"} |= "\xff\xfe"`, `app:="a" _stream_id:in()`},
		{`{app="a"} != "\xff"`, `app:="a"`},
		{`{app="a"} |= "\xff" or "b"`, `app:="a" ~"b"`},
		{`{app="a"} != "\xff" or "b"`, `app:="a" NOT ~"b"`},
		{`{app="a"} |= "ok" != "\xff" |= "x"`, `app:="a" ~"ok" ~"x"`},
		{`sum(count_over_time({app="a"} |= "\xff\xfe" [5m]))`, `app:="a" _stream_id:in() | stats count()`},
	} {
		got, err := TranslateLogQL(tc.logql)
		if err != nil {
			t.Fatalf("%s: %v", tc.logql, err)
		}
		if len(got) < len(tc.want) || got[:len(tc.want)] != tc.want {
			t.Errorf("%s:\n got  %s\n want %s…", tc.logql, got, tc.want)
		}
	}
}
