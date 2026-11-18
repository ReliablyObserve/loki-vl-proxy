package proxy

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Under a parser's series identity, the counting gate's rewrite flags as nan (2) the rows whose value a later stats
// pipe sums or averages is NaN: VictoriaLogs' sum keeps NaN as its empty value, so a series of NaN left the outer sum
// at a range up to the step, and a series that keeps its hash came out of the fold's second stats pipe as 0. The
// nan group makes the series NaN where it sits, as Loki's sum and average are. Max and min, the first stats pipe and
// a query without the series hash are written as before.
//
// conformance: semantics/unwrap-identity-nan-partials, semantics/unwrap-gate-parsefloat-divergences
func TestUnwrapNaNMarkUnderSeriesIdentity(t *testing.T) {
	gate := translator.UnwrapGateFor("d", "")
	hash := " | unpack_json from _msg result_prefix \"__lvp_p_\" | delete __lvp_p_d | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid | math __lvp_v as __lvp_v"
	mark := func(fields ...string) string {
		conds := make([]string, len(fields))
		for i, f := range fields {
			conds[i] = "-" + f + ":<=inf"
		}
		return ` | format if (__lvp_bad:="0" (` + strings.Join(conds, " or ") + `)) "2" as __lvp_bad | `
	}
	for _, tc := range []struct {
		name, rest string
		want       string // the mark the rewrite writes before the last stats pipe, "" none
	}{
		{"sum over the series of a range up to the step",
			hash + " | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)",
			mark("__lvp_inner")},
		{"avg by a label",
			hash + " | stats by (app, __lvp_sid) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (app) avg(__lvp_inner)",
			mark("__lvp_inner")},
		{"sum of the squared deviations",
			hash + " | stats by (__lvp_sid) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats sum(__lvp_inner_var)",
			mark("__lvp_inner_var")},
		{"the fold's second stats pipe",
			hash + " | stats by (__lvp_sid, __lvp_c) sum(__lvp_v) as __lvp_sum, count() as __sample_count | math __lvp_sum as __lvp_sum | math __sample_count as __sample_count | math __lvp_sum / __sample_count as __lvp_fold | stats by (__lvp_sid) sum(__lvp_sum) as __lvp_sum, sum(__sample_count) as __sample_count",
			mark("__lvp_sum", "__sample_count")},
		{"max over the series", hash + " | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats max(__lvp_inner)", ""},
		{"one stats pipe", hash + " | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner", ""},
		{"no series hash", " | stats by (_stream) avg(__lvp_v) as __lvp_inner | stats sum(__lvp_inner)", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, _, _, _, ok := unwrapCountingRewrite(`app:="a"`+gate+tc.rest, func(string, string) (bool, bool) { return false, true })
			if !ok {
				t.Fatalf("not rewritten: %s", tc.rest)
			}
			last := strings.LastIndex(got, " | stats ")
			hasMark := strings.Contains(got, `__lvp_bad:="0" (-`)
			if tc.want == "" {
				if hasMark {
					t.Fatalf("a nan mark where none belongs: %s", got)
				}
				return
			}
			if !strings.HasSuffix(got[:last+3], tc.want) || strings.Count(got, `__lvp_bad:="0" (-`) != 1 {
				t.Fatalf("want the mark %q before the last stats pipe:\n%s", tc.want, got)
			}
			if !strings.Contains(got[last:], "__lvp_bad)") {
				t.Fatalf("the last stats pipe does not group by the flag: %s", got[last:])
			}
		})
	}
	// The functions' arguments are read as written.
	if fs, ok := statsFunctions("sum(a) as x, count() as n, avg(b)"); !ok || fs[0].arg != "a" || fs[1].arg != "" || fs[2].arg != "b" {
		t.Fatalf("arguments: %+v %v", fs, ok)
	}
	if unwrapNaNMark("max(a) as x, count()") != "" || unwrapNaNMark("sum(a b)") != "" {
		t.Fatal("a mark for a list with no plain sum or average")
	}
}
