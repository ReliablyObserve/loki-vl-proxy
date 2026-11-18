package lower

import (
	"regexp"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
)

// dynamicHash is the identity pipes of a json parser over the unwrapped label n, as the lowering writes them.
const dynamicHash = `| unpack_json from _msg result_prefix "__lvp_p_" | delete __lvp_p_n | set_stream_fields __lvp_p_*, level, detected_level, _stream | rename _stream as __lvp_sid | hash(__lvp_sid) as __lvp_sid | math __lvp_v as __lvp_v`

// The plan's series of a range function over an unwrapped label after a json or logfmt parser with no keep: the
// stream and every parsed label less the unwrapped one (and the stored level), whatever the clause of the outer
// aggregation; after an extraction list of plain keys the stream and the labels it names. A query with a keep, a drop,
// a format, an alias or a json path in the list, a listed key named like a stream selector label or a level, two
// parsers or a range by keeps the translator's identity.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/stdvar-grouped-pooled-variance, semantics/unwrap-extraction-list-series-identity
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
		// An extraction list of plain keys: the stream and the labels it names.
		{`sum(avg_over_time({app="a"} | json n | unwrap n [5m]))`, true, true},
		{`sum(avg_over_time({app="a"} | json n, m | unwrap n [5m]))`, true, true},
		{`max by (app) (max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`, true, true},
		{`avg(stddev_over_time({app="a"} | logfmt m, n | unwrap n [5m]))`, true, true},
		{`sum(avg_over_time({app="a"} | json x="n" | unwrap x [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json n="a.b" | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt x="n" | unwrap x [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt n, app | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt n, level | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt n, service_name | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json n, detected_level | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | logfmt n, _m | unwrap n [5m]))`, false, false},
		{`sum(avg_over_time({app="a"} | json n | logfmt m | unwrap n [5m]))`, false, false},
		{`count(avg_over_time({app="a"} | json n, m | unwrap n [5m]))`, false, false},
		{`sum(first_over_time({app="a"} | json n, m | unwrap n [5m]))`, false, false},
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
// the hash; after an extraction list the stream, its stored level fields and the listed labels), then the clause's
// aggregation over them; stdvar squares the deviation of each series in VictoriaLogs.
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
	// An extraction list: the series are the stream, its stored level fields and the listed labels, which the query's
	// own unpack writes, so the stats group by them: no second unpack, no hash, no detected_level chain. The unpack
	// reads the listed keys alone (not the clause's labels, which Loki reads from the stream), and a clause that names
	// detected_level derives it as any grouping by it does.
	for q, want := range map[string]struct{ unpack, stats string }{
		`sum by (app) (max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (app, _stream, level, detected_level, m) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (app) sum(__lvp_inner)`},
		`sum(max_over_time({app="a"} | logfmt n | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n) skip_empty_results <gate n >`,
			`| stats by (_stream, level, detected_level) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`},
		`avg by (m) (avg_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (m, _stream, level, detected_level) avg(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (m) avg(__lvp_inner)`},
		`sum by (app) (stdvar_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (app, _stream, level, detected_level, m) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats by (app) sum(__lvp_inner_var)`},
		`max(stdvar_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (_stream, level, detected_level, m) stddev(__lvp_v) as __lvp_inner | math __lvp_inner*__lvp_inner as __lvp_inner_var | stats max(__lvp_inner_var)`},
		`sum(max_over_time({app="a"} | logfmt m | unwrap n [5m]))`: {
			`| unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (_stream, level, detected_level, m) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`},
		`sum(max_over_time({app="a"} | json n, m | unwrap n [5m]))`: {
			`app:="a" | unpack_json <gate n >`,
			`| stats by (_stream, level, detected_level, m) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats sum(__lvp_inner)`},
		`sum by (detected_level) (min_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`app:="a" <chain> | unpack_logfmt fields (n, m) skip_empty_results <gate n >`,
			`| stats by (detected_level, _stream, level, m) min(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (detected_level) sum(__lvp_inner)`},
	} {
		got := unwrapText(t, q, Profile{})
		for _, part := range []string{want.unpack, want.stats} {
			if !strings.Contains(got, part) {
				t.Errorf("extraction list %s: %s holds no %s", q, got, part)
			}
		}
		for _, part := range []string{"__lvp_sid", "__lvp_p_", "| set_stream_fields", "math __lvp_v as"} {
			if strings.Contains(got, part) {
				t.Errorf("extraction list %s: %s holds %s", q, got, part)
			}
		}
		if !strings.Contains(q, "detected_level") && strings.Contains(got, "<chain>") {
			t.Errorf("extraction list %s: %s derives detected_level", q, got)
		}
		op, err := PlanQuery(q, rangeAggPlan(q), Profile{})
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := logsql.Parse(physical.Encode(op)); err != nil {
			t.Errorf("extraction list %s: %s does not parse: %v", q, physical.Encode(op), err)
		}
	}
	// A listed label the clause names too, with an underscore: the unpack, the spellings it resolves into the label
	// and both stats read the one field, so a series and its group agree (the underscore-style proxy reads its
	// clause the same way).
	got = unwrapText(t, `sum by (user_id) (max_over_time({app="a"} | logfmt n, user_id | unwrap n [5m]))`, Profile{})
	for _, part := range []string{
		`| unpack_logfmt fields (n, user_id, "user.id", "user-id") skip_empty_results`,
		`"<user.id>" as user_id keep_original_fields`,
		`| stats by (user_id, _stream, level, detected_level) max(__lvp_v) as __lvp_inner | math __lvp_inner as __lvp_inner | stats by (user_id) sum(__lvp_inner)`,
	} {
		if !strings.Contains(got, part) {
			t.Errorf("listed clause label: %s holds no %s", got, part)
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

// The maximum of the series' maxima is the maximum of the group's rows, and the minimum of their minima the minimum
// (foldsRows): under a parser's series identity, of every key or of an extraction list, the lowering writes one stats
// pipe by the clause, with no second unpack, no hash, no per-series stats and no re-read of a series' value, and the
// query's logfmt unpack reads the unwrapped key and the clause labels the line can hold. The other pairs keep the
// identity (TestDynamicUnwrapIdentityLowering).
//
// conformance: semantics/unwrap-per-series-identity-raw-rows, semantics/unwrap-extraction-list-series-identity
func TestFoldsRowsLowering(t *testing.T) {
	for q, want := range map[string]struct{ unpack, stats string }{
		`max(max_over_time({app="a"} | logfmt | unwrap n [5m]))`: {
			`app:="a" | unpack_logfmt fields (n) skip_empty_results <gate n >`, `> | stats max(__lvp_v)`},
		`max by (app) (max_over_time({app="a"} | logfmt | unwrap n [5m]))`: {
			`app:="a" | unpack_logfmt fields (n, app) skip_empty_results <gate n >`, `> | stats by (app) max(__lvp_v)`},
		`min by (app) (min_over_time({app="a"} | json | unwrap n [5m]))`: {
			`app:="a" | unpack_json <gate n >`, `> | stats by (app) min(__lvp_v)`},
		`max(max_over_time({app="a"} | logfmt n | unwrap n [5m]))`: {
			`app:="a" | unpack_logfmt fields (n) skip_empty_results <gate n >`, `> | stats max(__lvp_v)`},
		// The clause's stream label is not read from the line; its listed label is.
		`max by (app) (max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`app:="a" | unpack_logfmt fields (n) skip_empty_results <gate n >`, `> | stats by (app) max(__lvp_v)`},
		`min by (m, app) (min_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`app:="a" | unpack_logfmt fields (n, m) skip_empty_results <gate n >`, `> | stats by (m, app) min(__lvp_v)`},
		`min(min_over_time({app="a"} | json n, m | unwrap n [5m]))`: {
			`app:="a" | unpack_json <gate n >`, `> | stats min(__lvp_v)`},
		// A label filter after the parser keeps the unpack bare, as before.
		`max by (app) (max_over_time({app="a"} | logfmt n, m | m="x" | unwrap n [1m]))`: {
			`app:="a" | unpack_logfmt | filter m:="x" <gate n >`, `> | stats by (app) max(__lvp_v)`},
		// A clause naming detected_level derives it, as any grouping by it does.
		`max by (detected_level) (max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`: {
			`app:="a" <chain> | unpack_logfmt fields (n) skip_empty_results <gate n >`, `> | stats by (detected_level) max(__lvp_v)`},
	} {
		got := unwrapText(t, q, Profile{})
		for _, part := range []string{want.unpack, want.stats} {
			if !strings.Contains(got, part) {
				t.Errorf("%s: %s holds no %s", q, got, part)
			}
		}
		if !strings.HasSuffix(got, want.stats[2:]) || strings.Count(got, "| stats ") != 1 {
			t.Errorf("%s: %s does not end in its one stats pipe %s", q, got, want.stats[2:])
		}
		for _, part := range []string{"__lvp_sid", "__lvp_p_", "| set_stream_fields", "__lvp_inner", "math __lvp_v as"} {
			if strings.Contains(got, part) {
				t.Errorf("%s: %s holds %s", q, got, part)
			}
		}
		if !strings.Contains(q, "detected_level") && strings.Contains(got, "<chain>") {
			t.Errorf("%s: %s derives detected_level", q, got)
		}
		p := rangeAggPlan(q)
		if !DynamicUnwrapIdentity(p) || !WritesDynamicUnwrapIdentity(p) {
			t.Errorf("%s: no longer served as a parser's series identity", q)
		}
		op, err := PlanQuery(q, p, Profile{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := logsql.Parse(physical.Encode(op)); err != nil {
			t.Errorf("%s: %s does not parse: %v", q, physical.Encode(op), err)
		}
	}
	// The other pairs read a value per series: the identity stays.
	for _, q := range []string{
		`max(min_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`,
		`min(max_over_time({app="a"} | logfmt | unwrap n [5m]))`,
		`sum(max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`,
		`max(avg_over_time({app="a"} | json | unwrap n [5m]))`,
		`max(stddev_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`,
		`avg by (app) (min_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`,
	} {
		got := unwrapText(t, q, Profile{})
		if !strings.Contains(got, "__lvp_inner") || !strings.Contains(got, "__lvp_sid") && !strings.Contains(got, "_stream, level, detected_level") {
			t.Errorf("%s: %s reads no value per series", q, got)
		}
	}
	// The stream profile (a request whose windows keep the stream's series) writes the string translator's text,
	// unchanged: a value per stream, then the clause.
	const q = `max by (app) (max_over_time({app="a"} | logfmt | unwrap n [5m]))`
	if got := unwrapText(t, q, Profile{StreamIdentity: true}); !strings.HasSuffix(got, "| stats by (app, _stream) max(__lvp_v) as __lvp_inner | stats by (app) max(__lvp_inner)") {
		t.Errorf("stream profile: %s is not the stream's value, then the clause", got)
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
		`sum(max_over_time({app="a"} | logfmt | unwrap _TIME [5m]))`,
		`max by (app) (max_over_time({app="a"} | logfmt | unwrap n [5m]))`,
		`min(min_over_time({app="a"} | json n, m | unwrap n [5m]))`,
		`max by (m, app) (max_over_time({app="a"} | logfmt n, m | n > 1 | unwrap n [1m]))`,
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
		// The proxy's text screen turns a query away whose root the metric screen names as another construct: it
		// never turns away a root identity.
		if root := ScreenMetric(logql.CanonicalizeLogRanges(q)); root != "" && root != Construct(p.Root) {
			t.Fatalf("%q: the metric screen names %s, the plan's root is %s", q, root, Construct(p.Root))
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
		va := p.Root.(*queryplan.VectorAgg)
		switch {
		case foldsRows(va.Op, ra.Op):
			// The maximum of maxima and the minimum of minima read the rows of each group: one stats pipe, no
			// identity, no hash.
			if strings.Count(text, "| stats ") != 1 || strings.Contains(text, "__lvp_sid") || strings.Contains(text, "__lvp_inner") {
				t.Fatalf("%q: %s is not one stats pipe over the rows", q, text)
			}
		case !strings.Contains(text, "| hash(__lvp_sid) as __lvp_sid") && !strings.Contains(text, "_stream, level, detected_level"):
			t.Fatalf("%q: %s holds no series identity", q, text)
		}
		if _, err := logsql.Parse(text); err != nil {
			t.Fatalf("%q: %s does not parse: %v", q, text, err)
		}
		// With the hash before the unwrap gate (Profile IdentityAhead), the text parses, the gate ends the pipes before
		// the first stats pipe and the stats pipes are the default's.
		ahead, err := PlanQuery(q, p, Profile{IdentityAhead: true})
		if err != nil {
			t.Fatalf("%q: the default lowers, the hash ahead does not: %v", q, err)
		}
		at := physical.Encode(ahead)
		first, dFirst := strings.Index(at, " | stats "), strings.Index(text, " | stats ")
		if first < 0 || dFirst < 0 || at[first:] != text[dFirst:] {
			t.Fatalf("%q: the stats pipes differ:\n%s\n%s", q, text, at)
		}
		if base, _, ok := translate.SplitUnwrapGate(at[:first]); DynamicIdentityAhead(p) && (!ok || !strings.HasSuffix(base, "| hash(__lvp_sid) as __lvp_sid")) {
			t.Fatalf("%q: the gate does not follow the hash: %s", q, at)
		}
		if _, err := logsql.Parse(at); err != nil {
			t.Fatalf("%q: %s does not parse: %v", q, at, err)
		}
	})
}

// StreamIdentity writes the series of a parser's identity shape as the string translator does: no hash, the stream
// and each line, the text the flip of the construct serves for a query the identity does not reach (nested under a
// binary operation, a window the handlers keep on the stream). The default profile writes the hash.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestDynamicUnwrapIdentityStreamProfile(t *testing.T) {
	for _, q := range []string{
		`sum(avg_over_time({app="a"} | json | unwrap n [5m]))`,
		`sum by (app) (max_over_time({app="a"} | logfmt | unwrap n [5m])) * 2`,
		`max by (app) (stdvar_over_time({app="a"} | logfmt | unwrap n [5m]))`,
	} {
		p := rangeAggPlan(q)
		hash, err := PlanQuery(q, p, Profile{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		stream, err := PlanQuery(q, p, Profile{StreamIdentity: true})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !strings.Contains(physical.Encode(hash), "__lvp_sid") || strings.Contains(physical.Encode(stream), "__lvp_sid") {
			t.Errorf("%s: default %s, stream identity %s", q, physical.Encode(hash), physical.Encode(stream))
		}
	}
}

// ListedUnwrapIdentity names the parser's series identity of an extraction list of plain keys (the stream, its stored
// level fields and the listed labels, fields of the row), which the window evaluator merges on a range longer than
// the step; a parser of every key (the series hash) and every shape DynamicUnwrapIdentity turns away are not.
//
// conformance: semantics/unwrap-over-time-range-windows-residual-forms
func TestListedUnwrapIdentity(t *testing.T) {
	for q, want := range map[string]bool{
		`sum(stdvar_over_time({app="a"} | logfmt n | unwrap n [5m]))`:             true,
		`sum by (app) (max_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`:   true,
		`avg(avg_over_time({app="a"} | json n, m | m="x" | unwrap n [1m]))`:       true,
		`min by (m) (min_over_time({app="a"} | logfmt n, m | unwrap n [5m]))`:     true,
		`sum(stdvar_over_time({app="a"} | logfmt | unwrap n [5m]))`:               false,
		`sum(stdvar_over_time({app="a"} | json a="n" | unwrap a [5m]))`:           false,
		`count by (app) (max_over_time({app="a"} | logfmt n | unwrap n [5m]))`:    false,
		`sum(quantile_over_time(0.5, {app="a"} | logfmt n | unwrap n [5m]))`:      false,
		`sum(stdvar_over_time({app="a"} | logfmt n | unwrap n [5m])) * 2`:         false,
		`stdvar_over_time({app="a"} | logfmt n | unwrap n [5m])`:                  false,
		`sum(stdvar_over_time({app="a"} | logfmt n | keep n | unwrap n [5m]))`:    false,
		`sum without (pod) (max_over_time({app="a"} | logfmt n | unwrap n [5m]))`: false,
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		if got := ListedUnwrapIdentity(p); got != want {
			t.Errorf("%s: ListedUnwrapIdentity %v, want %v", q, got, want)
		}
		if ListedUnwrapIdentity(p) && !DynamicUnwrapIdentity(p) {
			t.Errorf("%s: a listed identity that is no dynamic identity", q)
		}
	}
}

// streamWord is the stream field or its id as a word of a LogsQL text.
var streamWord = regexp.MustCompile(`(^|[^\w.])_stream(_id)?([^\w.]|$)`)

// set_stream_fields writes the series' canonical value into _stream and blanks _stream_id, and the rename moves it to
// the series field: no pipe after the identity may read either. Every grouping a client sends (none, a stream label,
// a parsed label, level, detected_level, service_name), with a label filter, json and logfmt, with the hash written
// after the gate and before it (Profile IdentityAhead), holds neither word after the hash.
//
// conformance: semantics/unwrap-identity-key-order
func TestDynamicIdentityLeavesNoStreamReader(t *testing.T) {
	const hash = "| hash(__lvp_sid) as __lvp_sid"
	n := 0
	for _, parser := range []string{"json", "logfmt"} {
		for _, by := range []string{"", "by (app) ", "by (pod) ", "by (route) ", "by (level) ", "by (detected_level) ", "by (service_name) ", "by (app, route) "} {
			for _, outer := range []string{"sum", "avg", "max", "min"} {
				for _, fn := range []string{"avg_over_time", "max_over_time", "stddev_over_time", "stdvar_over_time"} {
					for _, filter := range []string{"", ` | route="/x"`} {
						q := outer + " " + by + "(" + fn + `({app="a"} | ` + parser + filter + " | unwrap n [5m]))"
						for _, prof := range []Profile{{}, {IdentityAhead: true}} {
							p := rangeAggPlan(q)
							if p == nil || !DynamicUnwrapIdentity(p) {
								continue
							}
							op, err := PlanQuery(q, p, prof)
							if err != nil {
								continue
							}
							text := physical.Encode(op)
							at := strings.Index(text, hash)
							if at < 0 {
								continue
							}
							n++
							if m := streamWord.FindString(text[at:]); m != "" {
								t.Errorf("%s (ahead %v): %q after the identity: %s", q, prof.IdentityAhead, m, text[at:])
							}
						}
					}
				}
			}
		}
	}
	if n < 200 {
		t.Fatalf("only %d identity texts checked", n)
	}
}
