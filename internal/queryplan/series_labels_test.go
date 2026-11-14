package queryplan

import (
	"reflect"
	"slices"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
)

// seriesLabelsOf is the series identity of the pipeline of a query's first log selector.
func seriesLabelsOf(t testing.TB, q string) OutLabels {
	t.Helper()
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	p, err := Build(expr, Inputs{})
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	sel := firstSelect(p.Root)
	if sel == nil {
		t.Fatalf("%s: no log selector", q)
	}
	return SeriesLabels(sel.Stages)
}

// The labels a sample carries after the pipeline, as Loki's stages decide them: a keep deletes what it does not
// name (never the error labels), a drop what it names, a parser adds keys, the unwrap takes its label out, and a
// stage that changes labels by their values claims nothing.
//
// conformance: semantics/keep-before-unwrap-series-identity
func TestSeriesLabels(t *testing.T) {
	for _, tc := range []struct {
		q        string
		want     OutLabels
		identity []string
		decided  bool
	}{
		{q: `{a="x"}`, want: OutLabels{}},
		{q: `{a="x"} | json`, want: OutLabels{Dynamic: true}},
		{q: `{a="x"} | json | keep app, duration_ms`, want: OutLabels{Kept: true, Names: []string{"app", "duration_ms"}},
			identity: []string{"app", "duration_ms"}, decided: true},
		{q: `first_over_time({a="x"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
			want:     OutLabels{Kept: true, Names: []string{"app"}, Unwrapped: "duration_ms"},
			identity: []string{"app"}, decided: true},
		{q: `first_over_time({a="x"} | json | keep duration_ms | unwrap duration_ms [5m])`,
			want:     OutLabels{Kept: true, Names: []string{}, Unwrapped: "duration_ms"},
			identity: []string{}, decided: true},
		// The error labels stay whatever the keep says; they are not part of the named identity.
		{q: `{a="x"} | json | keep __error__, app, __error_details__, __preserve_error__`,
			want: OutLabels{Kept: true, Names: []string{"app"}}, identity: []string{"app"}, decided: true},
		{q: `{a="x"} | keep __error__`, want: OutLabels{Kept: true, Names: []string{}}, identity: []string{}, decided: true},
		// A parser after the keep adds the keys of each line.
		{q: `sum_over_time({a="x"} | keep app | json | unwrap d [5m])`, want: OutLabels{Kept: true, Names: []string{"app"}, Dynamic: true, Unwrapped: "d"}},
		// A drop after the keep removes its labels from the identity and is recorded.
		{q: `sum_over_time({a="x"} | json | keep app, pod, d | drop pod | unwrap d [5m])`,
			want: OutLabels{Kept: true, Names: []string{"app"}, Dropped: []string{"pod"}, Unwrapped: "d"}, identity: []string{"app"}, decided: true},
		{q: `sum_over_time({a="x"} | json d | drop pod, pod | unwrap d [5m])`,
			want: OutLabels{Dynamic: true, Dropped: []string{"pod"}, Unwrapped: "d"}},
		// A keep that deletes the unwrapped label: every sample is a conversion error, not modelled.
		{q: `sum_over_time({a="x"} | json | keep app | unwrap d [5m])`, want: OutLabels{Kept: true, Names: []string{"app"}, Opaque: true, Unwrapped: "d"}},
		// Stages that change labels by their values.
		{q: `{a="x"} | json | keep app, b | label_format c=b`, want: OutLabels{Kept: true, Names: []string{"app", "b"}, Opaque: true}},
		{q: `{a="x"} | json | keep app, b="y"`, want: OutLabels{Opaque: true}},
		{q: `{a="x"} | json | keep app | drop b="y"`, want: OutLabels{Kept: true, Names: []string{"app"}, Opaque: true}},
		// A later keep decides alone: what came before it is gone.
		{q: `{a="x"} | json | label_format c=b | keep app, b="y" | keep app`, want: OutLabels{Kept: true, Names: []string{"app"}},
			identity: []string{"app"}, decided: true},
		// Filters and formats of the line leave the labels alone.
		{q: `sum_over_time({a="x"} | json | keep app, d |= "x" | app="y" | line_format "{{.app}}" | decolorize | unwrap d [5m])`,
			want: OutLabels{Kept: true, Names: []string{"app"}, Unwrapped: "d"}, identity: []string{"app"}, decided: true},
	} {
		got := seriesLabelsOf(t, tc.q)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.q, got, tc.want)
		}
		identity, decided := got.KeptIdentity()
		if decided != tc.decided || decided && !reflect.DeepEqual(identity, tc.identity) {
			t.Errorf("%s: identity %v %v, want %v %v", tc.q, identity, decided, tc.identity, tc.decided)
		}
	}
}

// The keep's own label list is never written to: the identity is a copy where it differs.
func TestSeriesLabelsLeavesTheStagesAlone(t *testing.T) {
	expr, err := logql.Parse(`sum_over_time({a="x"} | json | keep __error__, app, d, pod | drop pod | unwrap d [5m])`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Build(expr, Inputs{})
	if err != nil {
		t.Fatal(err)
	}
	sel := firstSelect(p.Root)
	keep := sel.Stages[1].(*Keep)
	before := slices.Clone(keep.Labels)
	for range 2 {
		if got := SeriesLabels(sel.Stages); !reflect.DeepEqual(got.Names, []string{"app"}) {
			t.Fatalf("names %v", got.Names)
		}
	}
	if !reflect.DeepEqual(keep.Labels, before) {
		t.Fatalf("the keep's labels changed: %v, were %v", keep.Labels, before)
	}
}

// SeriesLabels is total and deterministic on every pipeline Build makes; the identity never names the unwrapped
// label or an error label, only labels the last keep names, and is claimed only after a keep with nothing after it
// that adds or changes labels.
func FuzzSeriesLabels(f *testing.F) {
	seedQueries(f)
	for _, q := range []string{
		`first_over_time({a="x"} | json | keep app, duration_ms | unwrap duration_ms [5m])`,
		`sum(stddev_over_time({a="x"} | json | keep __error__, app, d | drop app | unwrap d [5m]))`,
		`quantile_over_time(0.9, {a="x"} | keep app | json | unwrap d [5m])`,
		`{a="x"} | json | keep app, b="y" | label_format c=b | keep c`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		expr, err := parseCanon(q)
		if err != nil {
			return
		}
		p, err := Build(expr, Inputs{})
		if err != nil {
			return
		}
		sel := firstSelect(p.Root)
		if sel == nil {
			return
		}
		a, b := SeriesLabels(sel.Stages), SeriesLabels(sel.Stages)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%q: two readings differ: %+v %+v", q, a, b)
		}
		names, decided := a.KeptIdentity()
		if decided && (!a.Kept || a.Dynamic || a.Opaque) {
			t.Fatalf("%q: identity claimed for %+v", q, a)
		}
		var lastKeep *Keep
		for _, st := range sel.Stages {
			if k, ok := st.(*Keep); ok {
				lastKeep = k
			}
		}
		for _, n := range a.Names {
			if n == a.Unwrapped && n != "" {
				t.Fatalf("%q: the identity names the unwrapped label %s", q, n)
			}
			if slices.Contains(errorLabels[:], n) {
				t.Fatalf("%q: the identity names the error label %s", q, n)
			}
			if lastKeep == nil || !slices.Contains(lastKeep.Labels, n) {
				t.Fatalf("%q: the identity names %s, which no keep names", q, n)
			}
		}
		if decided && !reflect.DeepEqual(names, a.Names) {
			t.Fatalf("%q: identity %v is not the names %v", q, names, a.Names)
		}
	})
}

// BenchmarkSeriesLabels is the series identity of a pipeline, read on demand: a pipeline without a keep, one whose
// keep names the unwrapped label (a copy without it) and one with the error labels too.
func BenchmarkSeriesLabels(b *testing.B) {
	for _, q := range []string{
		`sum_over_time({a="x"} | json | status >= 500 | unwrap d [5m])`,
		`first_over_time({a="x"} | json | keep app, d | unwrap d [5m])`,
		`first_over_time({a="x"} | json | keep __error__, app, pod, d | unwrap d [5m])`,
	} {
		b.Run(q, func(b *testing.B) {
			stages := firstSelect(planOfQuery(b, q).Root).Stages
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = SeriesLabels(stages)
			}
		})
	}
}

func planOfQuery(tb testing.TB, q string) *Plan {
	tb.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		tb.Fatal(err)
	}
	p, err := Build(expr, Inputs{})
	if err != nil {
		tb.Fatal(err)
	}
	return p
}
