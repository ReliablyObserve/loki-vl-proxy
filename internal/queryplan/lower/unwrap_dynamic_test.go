package lower

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
)

// dynamicHash is the identity pipes of a json parser over the unwrapped label n, as the lowering writes them.
const dynamicHash = `| unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | pack_json fields (__lvp_p_*, level, detected_level, _stream) as __lvp_sid | hash(__lvp_sid) as __lvp_sid | math __lvp_v as __lvp_v`

// The plan's series of a range function over an unwrapped label after a json or logfmt parser with no keep: the
// stream and every parsed label less the unwrapped one (and the stored level), whatever the clause of the outer
// aggregation; a query with a keep, a drop, a format, a named extraction, two parsers or a range by keeps the
// translator's identity.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/stdvar-grouped-pooled-variance
func TestDynamicUnwrapIdentity(t *testing.T) {
	const j = `{app="a"} | json | unwrap n [5m]`
	for _, tc := range []struct {
		q      string
		ok     bool
		writes bool
	}{
		{`sum(avg_over_time(` + j + `))`, true, true},
		{`sum by (app) (max_over_time(` + j + `))`, true, true},
		{`max by (app) (stdvar_over_time({app="a"} | logfmt | unwrap n [5m]))`, true, true},
		{`avg(quantile_over_time(0.9, ` + j + `))`, false, false},
		{`min(rate_counter(` + j + `))`, false, false},
		{`sum(min_over_time({app="a"} |= "x" | json | unwrap n [5m]))`, true, true},
		{`sum(stddev_over_time({app="a"} | json | n > 5 | unwrap n [5m]))`, true, true},
		{`sum(stddev_over_time({app="a"} | json | unwrap n | __error__="" [5m]))`, true, true},
		// Not taken per series (additive, or first and last), or not under a vector aggregation of the four.
		{`sum(sum_over_time(` + j + `))`, false, false},
		{`sum(first_over_time(` + j + `))`, false, false},
		{`sum(rate(` + j + `))`, false, false},
		{`count(avg_over_time(` + j + `))`, false, false},
		{`topk(1, avg_over_time(` + j + `))`, false, false},
		{`avg_over_time(` + j + `)`, false, false},
		{`sum without (app) (avg_over_time(` + j + `))`, false, false},
		{`sum(avg_over_time(` + j + `) by (app))`, false, false},
		// The series are not every parsed label, or the unpack would read another line.
		{`sum(avg_over_time({app="a"} | json | keep app, n | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json | drop pod | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json n | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json | logfmt | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt --strict | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | regexp "(?P<n>[0-9]+)" | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json | line_format "{{.n}}" | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json | label_format m=n | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json | decolorize | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | unwrap n [5m]))`, false, false},
		{`sum(count_over_time({app="a"} | json [5m]))`, false, false},
		// At depth the lowering writes it too; the proxy serves the root shapes only.
		{`sum(avg_over_time(` + j + `)) * 2`, false, true},
		{`topk(1, sum by (app) (avg_over_time(` + j + `)))`, false, true},
		{`label_replace(sum(avg_over_time(` + j + `)), "b", "$1", "app", "(.*)")`, false, true},
		{`sum(avg_over_time(` + j + `)) / sum(count_over_time({app="a"}[5m]))`, false, true},
	} {
		p := rangeAggPlan(tc.q)
		if p == nil {
			t.Fatalf("%s: does not parse", tc.q)
		}
		if got := DynamicUnwrapIdentity(p); got != tc.ok {
			t.Errorf("%s: DynamicUnwrapIdentity is %v, want %v", tc.q, got, tc.ok)
		}
		if got := WritesDynamicUnwrapIdentity(p); got != tc.writes {
			t.Errorf("%s: WritesDynamicUnwrapIdentity is %v, want %v", tc.q, got, tc.writes)
		}
	}
}

// The stats of the dynamic identity: the identity pipes after the unwrap gate, a value per series (the stream and
// the hash), then the clause's aggregation over them; stdvar squares the deviation of each series in VictoriaLogs.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/stdvar-grouped-pooled-variance
func TestDynamicUnwrapIdentityLowering(t *testing.T) {
	const j = `{app="a"} | json | unwrap n [5m]`
	for q, want := range map[string]string{
		`sum(avg_over_time(` + j + `))`:                        dynamicHash + ` | stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`,
		`sum by (app) (max_over_time(` + j + `))`:              dynamicHash + ` | stats by (app, __lvp_sid) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (app) sum(__lvp_inner)`,
		`max by (app) (stdvar_over_time(` + j + `))`:           dynamicHash + ` | stats by (app, __lvp_sid) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats by (app) max(__lvp_inner_var)`,
		`sum(stdvar_over_time(` + j + `))`:                     dynamicHash + ` | stats by (__lvp_sid) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats sum(__lvp_inner_var)`,
		`avg(stddev_over_time(` + j + `))`:                     dynamicHash + ` | stats by (__lvp_sid) stddev(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats avg(__lvp_inner)`,
		`sum(min_over_time({app="a"} | json | unwrap n [5m]))`: dynamicHash + ` | stats by (__lvp_sid) min(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`,
	} {
		if got := keepStats(t, q); got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
	// A logfmt parser unpacks the line as logfmt, and every spelling of the unwrapped label leaves the hash.
	got := unwrapText(t, `sum(avg_over_time({app="a"} | logfmt | unwrap a_b [5m]))`, Profile{})
	for _, part := range []string{
		`| unpack_logfmt from _msg result_prefix "__lvp_p_" | delete __lvp_p_a_b, "__lvp_p_a.b", "__lvp_p_a-b" |`,
		`| stats by (__lvp_sid) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`,
	} {
		if !strings.Contains(got, part) {
			t.Errorf("logfmt: %s holds no %s", got, part)
		}
	}
	// The unwrapped value is read again as a number after the identity pipes, and each series' value before the outer
	// aggregation: VictoriaLogs' stats answered NaN over "+Inf" or 1e308 written out (TestUnwrapDynamicValueFormsMatchLoki).
	// A label filter between the parser and the unwrap, the shape of the value-forms e2e row, keeps both.
	got = unwrapText(t, `sum by (service_name) (max_over_time({app="a"} | logfmt | form="inf" | unwrap u [1m]))`, Profile{})
	for _, part := range []string{
		`| hash(__lvp_sid) as __lvp_sid | math __lvp_v as __lvp_v | stats by (service_name, __lvp_sid) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (service_name) sum(__lvp_inner)`,
	} {
		if !strings.Contains(got, part) {
			t.Errorf("value forms: %s holds no %s", got, part)
		}
	}
	// The text VictoriaLogs reads: every pipe parses.
	const q = `sum(avg_over_time({app="a"} | logfmt | unwrap a_b [5m]))`
	op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := logsql.Parse(op.(physical.VLQuery).Text); err != nil {
		t.Errorf("logfmt: %s does not parse: %v", op.(physical.VLQuery).Text, err)
	}
}

// dynamicIdentity reads the parser of the one stage that extracts every key, and refuses a pipeline whose unwrap
// comes before any parser.
func TestDynamicIdentityParser(t *testing.T) {
	p := rangeAggPlan(`sum(avg_over_time({app="a"} | logfmt | unwrap n [5m]))`)
	va := p.Root.(*queryplan.VectorAgg)
	parser, ok := dynamicIdentity(va, va.In.(*queryplan.RangeAgg))
	if !ok || parser == nil || parser.String() != "| logfmt" {
		t.Fatalf("parser %v %v", parser, ok)
	}
	// The unwrap stands before the parser: nothing the parser extracts reaches its value.
	ra := &queryplan.RangeAgg{Op: va.In.(*queryplan.RangeAgg).Op, In: &queryplan.LogSelect{Stages: []queryplan.Stage{&queryplan.Unwrap{Label: "n"}, parser}}}
	if _, ok := dynamicIdentity(va, ra); ok {
		t.Error("an unwrap before the parser has a dynamic identity")
	}
	// A query that is no vector aggregation over a range function has none.
	if DynamicUnwrapIdentity(rangeAggPlan(`sum(vector(1))`)) {
		t.Error("a vector literal has a dynamic identity")
	}
}

// FuzzDynamicUnwrapIdentity holds the decision on any query: two readings agree, the identity is claimed only
// where the stages leave every parsed label (queryplan.SeriesLabels), the lowered text of such a query holds the
// identity pipes and parses, and every query that has the identity writes it.
func FuzzDynamicUnwrapIdentity(f *testing.F) {
	for _, q := range []string{
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`,
		`sum by (app) (quantile_over_time(0.9, {app="a"} | logfmt | unwrap duration(n) [5m]))`,
		`max by (app) (stdvar_over_time({app="a"} | json | n > 1 | unwrap n [1m]))`,
		`sum(avg_over_time({app="a"} | json | unwrap "a\"b" [5m]))`,
		`sum(avg_over_time({app="a"} | json | keep n | unwrap n [5m]))`,
		`topk(1, sum(rate_counter({app="a"} | json | unwrap n [5m])))`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		p := rangeAggPlan(q)
		if p == nil {
			return
		}
		got := DynamicUnwrapIdentity(p)
		if got != DynamicUnwrapIdentity(p) {
			t.Fatalf("%q: two readings differ", q)
		}
		if !got {
			return
		}
		if !WritesDynamicUnwrapIdentity(p) {
			t.Fatalf("%q: the identity is not written", q)
		}
		ra := p.Root.(*queryplan.VectorAgg).In.(*queryplan.RangeAgg)
		if out := queryplan.SeriesLabels(ra.In.Stages); !out.Dynamic || out.Kept || out.Opaque || len(out.Dropped) > 0 {
			t.Fatalf("%q: identity claimed for %+v", q, out)
		}
		op, err := PlanQuery(q, p, Profile{})
		if err != nil {
			return
		}
		vq, isQuery := op.(physical.VLQuery)
		if !isQuery {
			t.Fatalf("%q: lowered to %T, not one query", q, op)
		}
		text := vq.Text
		if !strings.Contains(text, "| hash(__lvp_sid) as __lvp_sid") {
			t.Fatalf("%q: %s holds no series hash", q, text)
		}
		if _, err := logsql.Parse(text); err != nil {
			t.Fatalf("%q: %s does not parse: %v", q, text, err)
		}
	})
}
