// internal/logsql/edge_cases_test.go
package logsql_test

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// TestFieldFilterAllOps tests all 13 FieldOp variants with edge-case values.
func TestFieldFilterAllOps(t *testing.T) {
	tests := []struct {
		name string
		ff   logsql.FieldFilter
		want string
	}{
		// FieldOpExact — empty value
		{"exact_empty_val", logsql.FieldFilter{Field: "f", Op: logsql.FieldOpExact, Value: ""}, `f:=""`},
		// FieldOpExact — unicode
		{"exact_unicode", logsql.FieldFilter{Field: "мsg", Op: logsql.FieldOpExact, Value: "日本語"}, `мsg:="日本語"`},
		// FieldOpRegexp — special chars
		{"regexp_special", logsql.FieldFilter{Field: "url", Op: logsql.FieldOpRegexp, Value: `^/api/v\d+/`}, `url:~"^/api/v\d+/"`},
		// FieldOpPrefix — empty prefix
		{"prefix_empty", logsql.FieldFilter{Field: "f", Op: logsql.FieldOpPrefix, Value: ""}, "f:*"},
		// FieldOpSubstring — unicode
		{"substr_unicode", logsql.FieldFilter{Field: "msg", Op: logsql.FieldOpSubstring, Value: "错误"}, "msg:*错误*"},
		// FieldOpEmpty
		{"empty", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpEmpty}, `level:""`},
		// FieldOpAny
		{"any", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny}, "level:*"},
		// FieldOpGT — negative value
		{"gt_neg", logsql.FieldFilter{Field: "temp", Op: logsql.FieldOpGT, Value: "-10"}, "temp:>-10"},
		// FieldOpGTE — zero
		{"gte_zero", logsql.FieldFilter{Field: "count", Op: logsql.FieldOpGTE, Value: "0"}, "count:>=0"},
		// FieldOpLT — float
		{"lt_float", logsql.FieldFilter{Field: "rate", Op: logsql.FieldOpLT, Value: "0.5"}, "rate:<0.5"},
		// FieldOpLTE
		{"lte", logsql.FieldFilter{Field: "p99", Op: logsql.FieldOpLTE, Value: "100"}, "p99:<=100"},
		// FieldOpRange — zero-width range
		{"range_same", logsql.FieldFilter{Field: "x", Op: logsql.FieldOpRange, Value: "5,5"}, "x:range(5,5)"},
		// FieldOpIn — single value
		{"in_single", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpIn, Value: "error"}, "level:in(error)"},
		// FieldOpIn — many values
		{"in_many", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpIn, Value: "error,warn,info,debug"}, "level:in(error,warn,info,debug)"},
		// FieldOpIPv4Range — requires v1.45+
		{"ipv4range", logsql.FieldFilter{Field: "ip", Op: logsql.FieldOpIPv4Range, Value: "10.0.0.0, 10.0.0.255"}, "ip:ipv4_range(10.0.0.0, 10.0.0.255)"},
		// Negate=true
		{"negate_any", logsql.FieldFilter{Field: "level", Op: logsql.FieldOpAny, Negate: true}, "NOT level:*"},
		{"negate_range", logsql.FieldFilter{Field: "lat", Op: logsql.FieldOpRange, Value: "0,100", Negate: true}, "NOT lat:range(0,100)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ff.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNestedLogicalExpressions tests deep nesting and operator precedence.
func TestNestedLogicalExpressions(t *testing.T) {
	tests := []struct {
		name string
		expr logsql.FilterExpr
		want string
	}{
		{
			"and_of_ors",
			logsql.AndExpr{
				Left:  logsql.OrExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}},
				Right: logsql.OrExpr{Left: logsql.Word{Value: "c"}, Right: logsql.Word{Value: "d"}},
			},
			"(a OR b) AND (c OR d)",
		},
		{
			"not_and",
			logsql.NotExpr{
				Expr: logsql.AndExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}},
			},
			"NOT (a AND b)",
		},
		{
			"not_not",
			logsql.NotExpr{Expr: logsql.NotExpr{Expr: logsql.Word{Value: "error"}}},
			"NOT NOT error",
		},
		{
			"triple_and",
			logsql.AndExpr{
				Left:  logsql.AndExpr{Left: logsql.Word{Value: "a"}, Right: logsql.Word{Value: "b"}},
				Right: logsql.Word{Value: "c"},
			},
			"a AND b AND c",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.expr.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSequenceEdgeCases tests Sequence with 1, 2, and many parts.
func TestSequenceEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{"one_part", []string{"error"}, `seq("error")`},
		{"two_parts", []string{"a", "b"}, `seq("a","b")`},
		{"many_parts", []string{"x", "y", "z", "w"}, `seq("x","y","z","w")`},
		{"empty_part", []string{"", "b"}, `seq("","b")`},
		{"unicode_parts", []string{"日本語", "错误"}, `seq("日本語","错误")`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := logsql.Sequence{Parts: tc.parts}
			if got := s.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestQuantilePhiEdgeCases tests Quantile with boundary phi values.
func TestQuantilePhiEdgeCases(t *testing.T) {
	tests := []struct {
		phi  float64
		want string
	}{
		{0.0, "quantile(0, latency)"},
		{0.5, "quantile(0.5, latency)"},
		{0.99, "quantile(0.99, latency)"},
		{0.9999, "quantile(0.9999, latency)"},
		{1.0, "quantile(1, latency)"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			q := logsql.Quantile{Phi: tc.phi, Field: "latency"}
			if got := q.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLimitZeroStats tests UniqValues and Values with Limit=0.
func TestLimitZeroStats(t *testing.T) {
	if got := (logsql.UniqValues{Field: "f", Limit: 0}).String(); got != "uniq_values(f, 0)" {
		t.Errorf("UniqValues zero limit: got %q", got)
	}
	if got := (logsql.Values{Field: "f", Limit: 0}).String(); got != "values(f, 0)" {
		t.Errorf("Values zero limit: got %q", got)
	}
}

// TestRowFuncEdgeCases tests RowAny and RowMax with 1 and many fields.
func TestRowFuncEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		fn   logsql.StatsFunc
		want string
	}{
		{"row_any_one", logsql.RowAny{Fields: []string{"_msg"}}, "row_any(_msg)"},
		{"row_any_many", logsql.RowAny{Fields: []string{"_msg", "level", "status"}}, "row_any(_msg, level, status)"},
		{"row_max_one_field", logsql.RowMax{By: "latency", Fields: []string{"_msg"}}, "row_max(latency, _msg)"},
		{"row_max_many", logsql.RowMax{By: "latency", Fields: []string{"_msg", "level", "status"}}, "row_max(latency, _msg, level, status)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPipeSortEdgeCases tests PipeSort with multi-field mixed asc/desc.
func TestPipeSortEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		pipe logsql.PipeSort
		want string
	}{
		{
			"multi_mixed",
			logsql.PipeSort{By: []logsql.SortField{
				{Field: "count", Desc: true},
				{Field: "ts", Desc: false},
				{Field: "host", Desc: true},
			}},
			"| sort by (count desc, ts, host desc)",
		},
		{
			"single_asc",
			logsql.PipeSort{By: []logsql.SortField{{Field: "ts"}}},
			"| sort by (ts)",
		},
		{
			"with_limit",
			logsql.PipeSort{By: []logsql.SortField{{Field: "count", Desc: true}}, Limit: 100},
			"| sort by (count desc) limit 100",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.pipe.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPipeStatsNoBy tests PipeStats without a By clause.
func TestPipeStatsNoBy(t *testing.T) {
	p := logsql.PipeStats{
		Funcs: []logsql.StatsFuncAlias{
			{Func: logsql.Count{}, Alias: "total"},
			{Func: logsql.Sum{Field: "bytes"}, Alias: "bytes_total"},
		},
	}
	want := "| stats count() as total, sum(bytes) as bytes_total"
	if got := p.String(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestCapabilitiesAllMinorVersions tests every minor version v1.40–v1.50.
func TestCapabilitiesAllMinorVersions(t *testing.T) {
	tests := []struct {
		semver string
		want   logsql.Capabilities
	}{
		{"v1.40.0", logsql.Capabilities{}},
		{"v1.41.0", logsql.Capabilities{}},
		{"v1.42.0", logsql.Capabilities{}},
		{"v1.43.0", logsql.Capabilities{}},
		// boundary: v1.43.9 vs v1.44.0
		{"v1.43.9", logsql.Capabilities{}},
		{"v1.44.0", logsql.Capabilities{StatsRateSum: true}},
		// boundary: v1.44.9 vs v1.45.0
		{"v1.44.9", logsql.Capabilities{StatsRateSum: true}},
		{"v1.45.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.46.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.47.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.48.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		// boundary: v1.48.9 vs v1.49.0
		{"v1.48.9", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true}},
		{"v1.49.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true}},
		// boundary: v1.49.9 vs v1.50.0
		{"v1.49.9", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true}},
		{"v1.50.0", logsql.Capabilities{StatsRateSum: true, FieldIPv4Range: true, MetadataSubstring: true, DensePatternWindowing: true}},
	}
	for _, tc := range tests {
		t.Run(tc.semver, func(t *testing.T) {
			got := logsql.CapabilitiesFor(tc.semver)
			if got != tc.want {
				t.Errorf("CapabilitiesFor(%q) = %+v, want %+v", tc.semver, got, tc.want)
			}
		})
	}
}

// TestCapabilitiesMalformedSemver tests graceful handling of malformed semver.
func TestCapabilitiesMalformedSemver(t *testing.T) {
	malformed := []string{
		"not-a-version",
		"1",
		"v1",
		".",
		"v.1.0",
		"1.x.0",
		"v1.50.0-beta.1", // pre-release: should still parse major.minor
	}
	for _, s := range malformed {
		t.Run(s, func(t *testing.T) {
			// Must not panic; result must be all-false (safe baseline)
			got := logsql.CapabilitiesFor(s)
			// pre-release tag on v1.50.0-beta.1 still parses as v1.50, so allow all-true
			if strings.HasPrefix(s, "v1.50") || strings.HasPrefix(s, "1.50") {
				return // pre-release accepted
			}
			// All other malformed must return zero Capabilities
			want := logsql.Capabilities{}
			if got != want {
				t.Errorf("CapabilitiesFor(%q) = %+v, want zero", s, got)
			}
		})
	}
}

// TestTimeFilterAbsoluteRange tests TimeFilter with bracket-range values.
func TestTimeFilterAbsoluteRange(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"5m", "_time:5m"},
		{"1h", "_time:1h"},
		{"[2024-01-01,2024-01-02]", "_time:[2024-01-01,2024-01-02]"},
		{"[2024-01-01T00:00:00Z,2024-01-02T00:00:00Z]", "_time:[2024-01-01T00:00:00Z,2024-01-02T00:00:00Z]"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			f := logsql.TimeFilter{Range: tc.input}
			if got := f.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestStreamFilterEdgeCases tests StreamFilter with all matcher operators.
func TestStreamFilterEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		sf   logsql.StreamFilter
		want string
	}{
		{
			"all_ops",
			logsql.StreamFilter{Matchers: []logsql.LabelMatcher{
				{Name: "app", Op: "=", Value: "nginx"},
				{Name: "env", Op: "!=", Value: "dev"},
				{Name: "host", Op: "=~", Value: "web-.*"},
				{Name: "dc", Op: "!~", Value: "us-.*"},
			}},
			`{app="nginx", env!="dev", host=~"web-.*", dc!~"us-.*"}`,
		},
		{
			"single_matcher",
			logsql.StreamFilter{Matchers: []logsql.LabelMatcher{{Name: "app", Op: "=", Value: "api"}}},
			`{app="api"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sf.String(); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
