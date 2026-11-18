package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// dynamicHashAhead is the identity pipes of a json parser over the unwrapped label n written before the unwrap gate
// (Profile IdentityAhead): no math pipe reads the value again, the gate writes it last.
const dynamicHashAhead = `| unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | set_stream_fields __lvp_p_*, level, detected_level, _stream | rename _stream as __lvp_sid | hash(__lvp_sid) as __lvp_sid | filter "n":~`

// With IdentityAhead the series hash stands before the unwrap gate, so the gate ends the pipes before the first stats
// pipe (translate.SplitUnwrapGate reads it there) and that pipe groups by the clause's labels and the hash: the text
// the window evaluator reads for a range longer than the step. The stats pipes, the series and the text without the
// profile are the same as the default's, every unwrap conversion included, and the text parses.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicUnwrapIdentityAhead(t *testing.T) {
	for _, tc := range []struct{ q, by string }{
		{`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`, "by (__lvp_sid)"},
		{`sum by (app) (max_over_time({app="a"} | json | unwrap n [5m]))`, "by (app, __lvp_sid)"},
		{`max by (app) (stdvar_over_time({app="a"} | json | unwrap n [5m]))`, "by (app, __lvp_sid)"},
		{`avg(stddev_over_time({app="a"} | json | n > 5 | unwrap n [5m]))`, "by (__lvp_sid)"},
		{`max by (route) (min_over_time({app="a"} |= "x" | json | unwrap duration(n) [1m]))`, "by (route, __lvp_sid)"},
		{`sum(avg_over_time({app="a"} | json | unwrap bytes(n) [2m]))`, "by (__lvp_sid)"},
	} {
		def := loweredText(t, tc.q, Profile{})
		ahead := loweredText(t, tc.q, Profile{IdentityAhead: true})
		if !strings.Contains(ahead, dynamicHashAhead) || strings.Contains(ahead, "| math __lvp_v as __lvp_v") {
			t.Errorf("%s: the hash does not stand before the gate:\n%s", tc.q, ahead)
		}
		first := strings.Index(ahead, " | stats ")
		base, field, ok := translate.SplitUnwrapGate(ahead[:first])
		if !ok || field != "n" || !strings.HasSuffix(base, "| hash(__lvp_sid) as __lvp_sid") {
			t.Errorf("%s: the gate does not end the pipes before the first stats pipe (%v %q):\n%s", tc.q, ok, field, ahead)
		}
		if !strings.HasPrefix(ahead[first:], " | stats "+tc.by+" ") {
			t.Errorf("%s: the first stats pipe is %.80s, want it grouped %s", tc.q, ahead[first:], tc.by)
		}
		// The same stats pipes as the default text, and the same text once the hash is moved back after the gate.
		if dFirst := strings.Index(def, " | stats "); def[dFirst:] != ahead[first:] {
			t.Errorf("%s: the stats pipes differ:\n default %s\n   ahead %s", tc.q, def[dFirst:], ahead[first:])
		}
		if _, err := logsql.Parse(ahead); err != nil {
			t.Errorf("%s: %s does not parse: %v", tc.q, ahead, err)
		}
	}
	// A query without the dynamic identity writes the same text with the profile or without it.
	for _, q := range []string{
		`sum(sum_over_time({app="a"} | json | unwrap n [5m]))`,
		`sum(avg_over_time({app="a"} | json | keep n | unwrap n [5m]))`,
		`sum by (app) (avg_over_time({app="a"} | unwrap n [5m]))`,
		`sum(quantile_over_time(0.9, {app="a"} | json | unwrap n [5m]))`,
	} {
		p := rangeAggPlan(q)
		def, err := PlanQuery(q, p, Profile{})
		if err != nil {
			continue
		}
		ahead, err := PlanQuery(q, p, Profile{IdentityAhead: true})
		if err != nil || physical.Encode(def) != physical.Encode(ahead) {
			t.Errorf("%s: the profile changed the text:\n%s\n%v", q, physical.Encode(def), err)
		}
	}
	// An unwrapped label named like a reserved field in another case: the gate is written in the outbound guard's
	// form, which no gate split reads, so the hash ahead is not offered for it.
	for q, want := range map[string]bool{
		`sum(max_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`:   false,
		`sum(max_over_time({app="a"} | logfmt | unwrap _Stream [5m]))`: false,
		`sum(max_over_time({app="a"} | logfmt | unwrap time [5m]))`:    true,
		`sum(max_over_time({app="a"} | logfmt | unwrap n [5m]))`:       true,
		`sum(sum_over_time({app="a"} | logfmt | unwrap n [5m]))`:       false,
		`sum(max_over_time({app="a"} | logfmt | unwrap n [5m])) * 2`:   false,
		// A fold of rows writes no hash, and an extraction list's series are fields of the row.
		`max(max_over_time({app="a"} | logfmt | unwrap n [5m]))`:      false,
		`sum(max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: false,
	} {
		if got := DynamicIdentityAhead(rangeAggPlan(q)); got != want {
			t.Errorf("%s: DynamicIdentityAhead %v, want %v", q, got, want)
		}
	}
	// The stream identity profile wins: no hash at all.
	const q = `sum(avg_over_time({app="a"} | logfmt | unwrap n [5m]))`
	if got := loweredText(t, q, Profile{IdentityAhead: true, StreamIdentity: true}); strings.Contains(got, "__lvp_sid") {
		t.Errorf("stream identity and ahead: %s", got)
	}
}

// loweredText is the text a query lowers to under prof, the unwrap gate written out.
func loweredText(t *testing.T, q string, prof Profile) string {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		t.Fatalf("%s: does not parse", q)
	}
	op, err := PlanQuery(q, p, prof)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return physical.Encode(op)
}
