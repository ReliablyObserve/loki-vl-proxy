package logsql_test

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

var testMark = logsql.JSONMark{Sentinel: "__vlp", Flag: "__lvp_perr"}

// A stats query gets the splice before its first unpack_json and the sentinel in every stats grouping, the line
// restored only when a later pipe reads it; a shape the mark cannot follow is left alone, and every text it writes
// parses back to the same query.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONMarkStats(t *testing.T) {
	const splice = `replace ("{", "{\"__vlp\":1,") at _msg limit 1 | replace ("{\"__vlp\":1,}", "{\"__vlp\":1}") at _msg limit 1`
	const restore = `replace ("{\"__vlp\":1,", "{") at _msg limit 1 | replace ("{\"__vlp\":1}", "{}") at _msg limit 1`
	for _, tc := range []struct {
		in, want string
		restored bool
	}{
		{`job:="sp" | unpack_json | stats by (_stream) count() as i | math i/60 as r | stats by (app) sum(r)`, `stats by (_stream, __vlp) count() as i | math i/60 as r | stats by (app, __vlp) sum(r)`, false},
		{`job:="sp" | unpack_json | stats count() as c`, `| unpack_json | stats by (__vlp) count() as c`, false},
		{`job:="sp" | unpack_json | stats by (_stream) sum_len(_msg) as b | stats count()`, `stats by (_stream, __vlp) sum_len(_msg) as b`, true},
		{`job:="sp" | unpack_json | filter _msg:~"x" | stats count() as c`, `stats by (__vlp) count() as c`, true},
		{`job:="sp" | math ((_time - 1) % 3) as p | filter p:>1 | unpack_json fields (app) keep_original_fields | stats by (app) count() as c`, `unpack_json fields (app, __vlp) keep_original_fields`, false},
		// A level derivation's conditional unpack comes first: the mark goes around the leaf's plain one.
		{`job:="sp" | unpack_json if (-x:*) from _msg fields (level) result_prefix "__j_" | format if (-x:*) "<__j_level>" as y | unpack_json | stats by (y) count() as c`, `| unpack_json | stats by (y, __vlp) count() as c`, false},
		{`job:="sp" | unpack_json | stats (app) count() as c`, "", false},
		{`job:="sp" | unpack_json | stats by () count() as c`, `stats by (__vlp) count() as c`, false},
		{`job:="sp" | filter _msg:"a | b" | unpack_json | stats count() as c`, `| filter _msg:"a | b" | replace`, false},
		{`job:="sp" | unpack_json | stats by (app) count() as c | sort by (c desc) | limit 2`, "", false},
		{`job:="sp" | unpack_json | limit 9`, "", false},
		{`job:="sp" | unpack_json from x | stats count()`, "", false},
		{`job:="sp" | unpack_logfmt | stats count()`, "", false},
		{`job:="sp" | unpack_json | fields _time, app | stats count()`, `| fields _time, app, __vlp | stats by (__vlp) count()`, false},
		{`job:="sp" | unpack_json | stats count() | fields x`, "", false},
		{`not a query (`, "", false},
	} {
		got, ok := testMark.MarkStats(tc.in)
		if ok != (tc.want != "") {
			t.Errorf("%s: rewritten %v: %s", tc.in, ok, got)
			continue
		}
		if !ok {
			if got != tc.in {
				t.Errorf("%s: changed to %s", tc.in, got)
			}
			continue
		}
		if !strings.Contains(got, tc.want) || !strings.Contains(got, splice) || strings.Contains(got, restore) != tc.restored {
			t.Errorf("%s:\n got %s\nwant %s (restored %v)", tc.in, got, tc.want, tc.restored)
		}
		if _, err := logsql.Parse(got); err != nil {
			t.Errorf("%s: the marked text does not parse: %v", got, err)
		}
		if !keepsText(tc.in, got) {
			t.Errorf("%s: the marked text %s does not keep the query's own text", tc.in, got)
		}
		if _, ok := testMark.MarkStats(got); ok {
			t.Errorf("%s: marked twice", got)
		}
	}
}

// A raw-row read is marked when its rows carry the mark; the lookup reads the stored rows of base that the mark sets,
// oldest first, without the sentinel, the copy or the dropped fields.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONMarkRowsAndLookup(t *testing.T) {
	for in, marked := range map[string]bool{
		`job:="sp" | unpack_json`:                        true,
		`job:="sp" | unpack_json | fields _time, _msg`:   true,
		`job:="sp" | unpack_json | stats count()`:        false,
		`job:="sp" | fields _time, _stream, _msg, level`: false,
	} {
		got, ok := testMark.MarkRows(in)
		if ok != marked || ok && !strings.Contains(got, `format if (NOT __vlp:*) "1" as __lvp_perr | delete __vlp`) || !ok && got != in {
			t.Errorf("MarkRows(%s) = %s, %v", in, got, ok)
		}
	}
	got, ok := testMark.Lookup(`job:="sp" | math ((_time - 1) % 3) as __lvp_window_phase | filter __lvp_window_phase:>1`, 8, "__lvp_window_phase")
	if !ok {
		t.Fatal("no lookup")
	}
	for _, part := range []string{`copy _msg as __vlm`, `replace ("{", "{\"__vlp\":1,") at __vlm limit 1`, `unpack_json from __vlm fields (__vlp)`, `filter NOT __vlp:*`, `delete __vlm, __vlp, __lvp_window_phase`, `sort by (_time)`, `limit 8`} {
		if !strings.Contains(got, part) {
			t.Errorf("lookup %s: no %q", got, part)
		}
	}
	if _, err := logsql.Parse(got); err != nil {
		t.Errorf("the lookup does not parse: %v", err)
	}
	if _, ok := testMark.Lookup(`not a query (`, 8); ok {
		t.Error("a lookup of a broken base")
	}
}

// FuzzJSONMark marks fuzzed queries: it never panics, a marked text parses, keeps the query's own text, holds the
// sentinel, and is not marked again.
func FuzzJSONMark(f *testing.F) {
	for _, seed := range []string{
		`a:=b | unpack_json | stats by (_stream) count() as c`,
		`a:=b | unpack_json fields (x, y) keep_original_fields | stats count() | math c/2 as d`,
		`a:=b | math ((_time - 1) % 3) as p | filter p:>1 | unpack_json | limit 9`,
		`a:=b | unpack_json | stats by (_stream) sum_len(_msg) as b | stats count()`,
		`a:=b | unpack_json from x`,
		`* | unpack_json | stats by (a) count() as c | sort by (c) | limit 1`,
		`"x" | unpack_json | fields _msg`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, query string) {
		if len(query) > 4096 {
			return
		}
		for _, mark := range []func(string) (string, bool){testMark.MarkStats, testMark.MarkRows} {
			got, ok := mark(query)
			if !ok {
				if got != query {
					t.Fatalf("an unmarked query changed: %q -> %q", query, got)
				}
				continue
			}
			if !strings.Contains(got, testMark.Sentinel) {
				t.Fatalf("%q: marked without the sentinel: %q", query, got)
			}
			if _, err := logsql.Parse(got); err != nil || !keepsText(query, got) {
				t.Fatalf("%q: the marked text %q does not parse or keep the query's text: %v", query, got, err)
			}
			if _, twice := mark(got); twice {
				t.Fatalf("%q: marked twice", got)
			}
		}
		if lookup, ok := testMark.Lookup(query, 3); ok {
			if _, err := logsql.Parse(lookup); err != nil {
				t.Fatalf("%q: the lookup %q does not parse: %v", query, lookup, err)
			}
		}
	})
}

// keepsText reports whether marked holds every byte of query, in order: the mark only inserts.
func keepsText(query, marked string) bool {
	i := 0
	for j := 0; j < len(marked) && i < len(query); j++ {
		if marked[j] == query[i] {
			i++
		}
	}
	return i == len(query)
}
