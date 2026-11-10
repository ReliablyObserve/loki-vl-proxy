package lower

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// rangeProfile is one option set the range aggregation lowering is held to the string translator under.
type rangeProfile struct {
	name   string
	label  func(string) string
	stream map[string]bool
	caps   logsql.Capabilities
}

// rangeProfiles are the profiles that change the translator's text: the two VictoriaLogs versions of the matrix,
// the label styles, the stream fields and the stream labels of the tenant.
func rangeProfiles() []rangeProfile {
	underscores := func(s string) string { return strings.ReplaceAll(s, ".", "_") }
	labels := translator.WithStreamLabels(underscores, []string{"app", "namespace", "service_name", "env", "level", "pod"})
	fields := map[string]bool{"app": true, "namespace": true, "env": true, "service_name": true}
	v53, v49 := logsql.CapabilitiesFor("v1.53.0"), logsql.CapabilitiesFor("v1.49.0")
	return []rangeProfile{
		{"v1.53 identity", nil, nil, v53},
		{"v1.49 identity", nil, nil, v49},
		{"v1.53 underscores", underscores, nil, v53},
		{"v1.49 underscores", underscores, nil, v49},
		{"v1.53 stream fields", underscores, fields, v53},
		{"v1.49 stream fields", underscores, fields, v49},
		{"v1.53 stream labels", labels, nil, v53},
		{"v1.49 stream labels", labels, nil, v49},
		{"v1.53 stream fields and labels", labels, fields, v53},
	}
}

func (p rangeProfile) lowerProfile() Profile {
	return Profile{Caps: p.caps, Label: p.label, StreamFields: p.stream}
}

// rangeAggPlan is the resolved plan of q, nil when q does not parse or build.
func rangeAggPlan(q string) *queryplan.Plan {
	expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
	if err != nil {
		return nil
	}
	p, err := queryplan.Build(expr, queryplan.Inputs{})
	if err != nil {
		return nil
	}
	queryplan.Resolve(p)
	return p
}

// rangeAggLowering lowers q under prof and compares it with the string translator, both read by the LogsQL
// parser. delegated is true when the lowering answers ErrNotLowered; ok is false when q is not translated.
func rangeAggLowering(t testing.TB, q string, prof rangeProfile) (lowered, legacy string, same, delegated, ok bool) {
	t.Helper()
	p := rangeAggPlan(q)
	if p == nil {
		return "", "", false, false, false
	}
	legacy, terr := translator.TranslateLogQLWithCapabilities(logql.CanonicalizeLogRanges(q), prof.label, prof.stream, prof.caps)
	// A selector that keeps every row followed by pipes only is written without its `*`: the proxy puts it in
	// front (translateQueryOpts), as the lowering does.
	if strings.HasPrefix(legacy, "|") {
		legacy = "* " + legacy
	}
	op, lerr := Plan(p, prof.lowerProfile())
	if errors.Is(lerr, ErrNotLowered) {
		return "", legacy, false, true, terr == nil
	}
	if terr != nil || lerr != nil {
		if (terr == nil) != (lerr == nil) {
			t.Fatalf("%s: translator error %v, lowering error %v", q, terr, lerr)
		}
		return "", "", false, false, false
	}
	// A without clause is a fold around the query: the clean query is compared, and the fold with the marker.
	core, post := physical.Split(op)
	lowered = core.(physical.VLQuery).Text
	legacy, wantPost := translator.SplitPost(legacy)
	want, err := logsql.Parse(legacy)
	if err != nil {
		t.Fatalf("%s: the translator's %s does not parse: %v", q, legacy, err)
	}
	got, err := logsql.Parse(lowered)
	if err != nil {
		t.Fatalf("%s: the lowered %s does not parse: %v", q, lowered, err)
	}
	return lowered, legacy, reflect.DeepEqual(want, got) && reflect.DeepEqual(post, wantPost), false, true
}

// A range aggregation over a log query with no parser lowers to the query the string translator writes, for every
// function it lowers, every selector and filter shape, every range and offset spelling and every profile: the
// log query of the metric rows is written with the label names alone (no capabilities, no stream fields), then one
// stats pipe with no grouping, or for rate and bytes_rate the detected_level chain and the stats per series.
//
// conformance: architecture-syntax-package-layering
func TestPlanLowersRangeAggregations(t *testing.T) {
	selectors := []string{
		`{app="a"}`, `{app="a", env=~"prod|stg"}`, `{app!="a", env!~"x.*"}`, `{service_name="s"}`, `{service_name=~"s|t"}`,
		`{detected_level!="info"}`, `{detected_level="error"}`, `{k8s.pod.name="p"}`, `{app=""}`, `{app=~".*"}`,
	}
	filters := []string{
		``, ` |= "x"`, ` != "x" |~ "y.+"`, ` |= "x" or "y"`, ` |= ip("10.0.0.0/8")`, ` |> "<_> x <_>"`, ` !> "<_> a <_>" |= "k"`,
		` | level="error"`, ` | status>=500 and path=~"/a.*"`, ` |= "k" | a="1" |> "<_> x <_>" | b!=""`, ` | ip_a=ip("10.0.0.0/8")`,
	}
	ranges := []string{
		`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[300s]`, `[2w]`, `[5m] offset 1h`, `[5m] offset 0s`, `[1h] offset 24h`, `[30s] offset 1ms`,
	}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	for _, prof := range rangeProfiles() {
		for _, f := range funcs {
			for _, s := range selectors {
				for _, fl := range filters {
					for _, r := range ranges {
						q := f + "(" + s + fl + r + ")"
						lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
						if !ok || delegated {
							t.Errorf("%s: %s: not lowered (ok %v)", prof.name, q, ok)
							continue
						}
						if !same {
							t.Errorf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
						}
					}
				}
			}
			// The parenthesised inner query.
			for _, q := range []string{
				f + `(({app="a"} |= "x")[5m])`, f + `( {app="a"} [5m] offset 1h )`, f + `(({app="a"})[1m])`,
			} {
				lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
				if !ok || delegated || !same {
					t.Errorf("%s: %s: ok %v delegated %v\n lowered %s\n  string %s", prof.name, q, ok, delegated, lowered, legacy)
				}
			}
		}
	}
}

// What the lowering writes for the plain forms: the text itself, not only its agreement with the translator.
//
// conformance: architecture-syntax-package-layering
func TestPlanRangeAggregationText(t *testing.T) {
	for q, want := range map[string]string{
		`count_over_time({app="a"}[5m])`:                           `app:="a" | stats count()`,
		`count_over_time({app="a"} |= "x"[5m] offset 1h)`:          `app:="a" ~"x" | stats count()`,
		`absent_over_time({app="a"}[5m])`:                          `app:="a" | stats count()`,
		`bytes_over_time({app="a", env="p"} != "x"[1m])`:           `app:="a" env:="p" NOT ~"x" | stats sum_len(_msg)`,
		`count_over_time({detected_level!="info"} |= "x"[5m])`:     `~"x" | stats count()`,
		`count_over_time({detected_level!="info"}[5m])`:            `* | stats count()`,
		`count_over_time({app="a"} | level="error"[5m] offset 0s)`: `app:="a" level:="error" | stats count()`,
	} {
		p := rangeAggPlan(q)
		op, err := Plan(p, Profile{})
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		if got := op.(physical.VLQuery).Text; got != want {
			t.Errorf("%s:\n got %s\nwant %s", q, got, want)
		}
	}
}

// What the lowering writes for rate and bytes_rate: the log query's filters, the detected_level chain once, the
// restore of the stream labels and the query's other pipes, then the count (or the bytes) per series, its division
// by the range's seconds, and the sum per series.
//
// conformance: architecture-syntax-package-layering
func TestPlanRateText(t *testing.T) {
	chain := translator.BuildDetectedLevelChain(translator.DetectedLevelFieldNames, translator.DetectedLevelChainGrouping, nil)
	stream := ` | stats by (_stream, level, detected_level) `
	tail := func(fn, seconds string) string {
		return stream + fn + ` as __lvp_inner | math __lvp_inner/` + seconds + ` as __lvp_rate` + stream + `sum(__lvp_rate)`
	}
	restore, _ := streamTagRestore(benchStreamLabels)
	for _, c := range []struct {
		q, head, pipes, tail string
		label                func(string) string
	}{
		{`rate({app="a"}[5m])`, `app:="a"`, ``, tail(`count()`, `300`), nil},
		{`rate({app="a"} |= "x" [1h] offset 1h)`, `app:="a" ~"x"`, ``, tail(`count()`, `3600`), nil},
		{`bytes_rate({app="a"} | level="e" [90s])`, `app:="a" level:="e"`, ``, tail(`sum_len(_msg)`, `90`), nil},
		{`bytes_rate({app="a"}[1ms])`, `app:="a"`, ``, tail(`sum_len(_msg)`, `0.001`), nil},
		{`rate({detected_level!="info"}[1d])`, `*`, ``, tail(`count()`, `86400`), nil},
		{`rate({app="a"}[5m])`, `app:="a"`, ` ` + restore, tail(`count()`, `300`), benchStreamLabels},
	} {
		p := rangeAggPlan(c.q)
		op, err := Plan(p, Profile{Label: c.label})
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		want := c.head + ` ` + chain + c.pipes + c.tail
		if got := op.(physical.VLQuery).Text; got != want {
			t.Errorf("%s:\n got %.300s\nwant %.300s", c.q, got, want)
		}
	}
	// A pattern filter that extracts goes after the chain, with the pipes of the query.
	op, err := Plan(rangeAggPlan(`rate({app="a"} |> "<_> x <_> y" |= "k"[5m])`), Profile{})
	if err != nil {
		t.Fatal(err)
	}
	text := op.(physical.VLQuery).Text
	if i, j := strings.Index(text, chain), strings.Index(text, "extract_regexp"); i < 0 || j >= 0 && j < i || strings.Count(text, chain) != 1 {
		t.Errorf("the chain is not once before the first pipe: %.400s", text)
	}
}

// A selector that keeps every row (`{detected_level!="info"}`) is no filter beside the other filters of a log query
// either, as the translator writes it.
//
// conformance: architecture-syntax-package-layering
func TestPlanLogSelectDropsKeepAllSelector(t *testing.T) {
	for _, q := range []string{
		`{detected_level!="info"} |= "x"`, `{detected_level!="info"} | a="1"`, `{detected_level!="info"} |> "<_> x <_>"`,
		`{detected_level!="info"} !> "<_> x <_>" | a="1" |= "k"`, `{detected_level=~".*"} != "x"`, `{detected_level!="info"}`,
	} {
		for _, prof := range rangeProfiles() {
			lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof)
			if !ok || delegated || !same {
				t.Errorf("%s: %s: ok %v delegated %v\n lowered %s\n  string %s", prof.name, q, ok, delegated, lowered, legacy)
			}
		}
	}
	op, err := Plan(rangeAggPlan(`{detected_level!="info"} |= "x"`), Profile{})
	if err != nil || op.(physical.VLQuery).Text != `~"x"` {
		t.Errorf("got %v, %v; want ~\"x\" with no star", op, err)
	}
}

// The log query of a metric row is written without the capabilities and the stream fields of the profile: a
// regexp the floor and the pin write differently, and a declared stream field, give the same text under both.
//
// conformance: architecture-syntax-package-layering
func TestPlanRangeAggregationIgnoresCapsAndStreamFields(t *testing.T) {
	v53, v49 := logsql.CapabilitiesFor("v1.53.0"), logsql.CapabilitiesFor("v1.49.0")
	for _, q := range []string{
		`count_over_time({app="a"} | status=~"error|warn" [5m])`,
		`count_over_time({app="a"} | ip_a=ip("10.0.0.0/8") |= ip("10.0.0.0/8") [5m])`,
		`bytes_over_time({app="a"} |> "<_> x <_>" [5m])`,
		`absent_over_time({service_name="s", level=~"a.*"} [5m])`,
	} {
		base := ""
		for _, prof := range []Profile{
			{Caps: v53}, {Caps: v49}, {Caps: v53, StreamFields: map[string]bool{"app": true, "service_name": true}}, {},
		} {
			op, err := Plan(rangeAggPlan(q), prof)
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
			text := op.(physical.VLQuery).Text
			if base == "" {
				base = text
			} else if text != base {
				t.Errorf("%s: %s, and %s under another profile", q, text, base)
			}
		}
	}
}

// A kept line is for log responses: a range aggregation is not lowered under it, the translator does not write one.
//
// conformance: architecture-syntax-package-layering
func TestPlanDelegatesRangeAggregations(t *testing.T) {
	for q, construct := range map[string]string{
		`rate({app="a"}[0s])`:                                    "RangeAgg range",
		`bytes_rate({app="a"} |= "x" [0s] offset 1h)`:            "RangeAgg range",
		`rate({app="a"} | json [5m])`:                            "RangeAgg parser",
		`bytes_rate({app="a"} | unwrap n [5m])`:                  "RangeAgg unwrap",
		`rate({app="a"} | drop x [5m])`:                          "RangeAgg drop or keep",
		`bytes_rate({app="a"} | keep x [5m])`:                    "RangeAgg drop or keep",
		`rate({app="a"} | detected_level="e" [5m])`:              "RangeAgg LogSelect label filter detected_level",
		`bytes_rate({app="a"} | line_format "{{.a}}" [5m])`:      "RangeAgg LogSelect pipeline",
		`rate({app="a"}[5m]) by (x)`:                             "RangeAgg grouping",
		`sum_over_time({app="a"} | unwrap n [5m])`:               "RangeAgg unwrap",
		`quantile_over_time(0.9, {app="a"} | unwrap n [5m])`:     "RangeAgg unwrap",
		`first_over_time({app="a"} | unwrap n [5m])`:             "RangeAgg unwrap",
		`rate_counter({app="a"} | unwrap n [5m])`:                "RangeAgg unwrap",
		`count_over_time({app="a"} | unwrap n [5m])`:             "RangeAgg unwrap",
		`count_over_time({app="a"} | json [5m])`:                 "RangeAgg parser",
		`count_over_time({app="a"} | logfmt | level="e" [5m])`:   "RangeAgg parser",
		`bytes_over_time({app="a"} | regexp "(?P<a>x)" [5m])`:    "RangeAgg parser",
		`count_over_time({app="a"} | line_format "{{.a}}" [5m])`: "RangeAgg LogSelect pipeline",
		`count_over_time({app="a"} | drop level [5m])`:           "RangeAgg drop or keep",
		`count_over_time({app="a"} | keep detected_level [5m])`:  "RangeAgg drop or keep",
		`count_over_time({app="a"} | detected_level="e" [5m])`:   "RangeAgg LogSelect label filter detected_level",
		`count_over_time({app="a"}[5m]) by (x)`:                  "RangeAgg grouping",
		`count_over_time({app="a"}[5m]) by ()`:                   "RangeAgg grouping",
		`absent_over_time({app="a"}[5m]) without (x)`:            "RangeAgg grouping",
		`bytes_over_time({app="a"}[5m]) without (x, y)`:          "RangeAgg grouping",
	} {
		p := rangeAggPlan(q)
		if p == nil {
			t.Fatalf("%s: does not parse", q)
		}
		_, err := Plan(p, Profile{})
		var nl *NotLoweredError
		if !errors.As(err, &nl) || nl.Construct != construct {
			t.Errorf("%s: got %v, want not lowered: %s", q, err, construct)
		}
	}
	// A kept line, and a stream label name the string translator writes as it is.
	_, err := Plan(rangeAggPlan(`count_over_time({app="a"}[5m])`), Profile{KeepLine: true})
	var nl *NotLoweredError
	if !errors.As(err, &nl) || nl.Construct != "RangeAgg keepLine" {
		t.Errorf("keepLine: got %v", err)
	}
	odd := translator.WithStreamLabels(nil, []string{"a`b"})
	if _, err := Plan(rangeAggPlan(`count_over_time({app="a"}[5m])`), Profile{Label: odd}); !errors.As(err, &nl) || nl.Construct != "RangeAgg stream label name" {
		t.Errorf("stream label name: got %v", err)
	}
}

// The stream labels of the tenant add the restore of their stream values before the first pipe, once per name that
// the level derivation does not read as stored, numbered in order.
//
// conformance: architecture-syntax-package-layering
func TestPlanRangeAggregationRestoresStreamLabels(t *testing.T) {
	label := translator.WithStreamLabels(nil, []string{"app", "level", "k8s.pod", "env-x"})
	for _, q := range []string{
		`count_over_time({app="a"}[5m])`, `count_over_time({app="a"} |= "x" [5m])`, `bytes_over_time({app="a"} |> "<_> x <_>" [5m])`,
		`absent_over_time({app="a"} |> "<_> x <_>" |= "k" | a="1" [5m])`, `count_over_time({app="a"} | a="1" [5m])`,
	} {
		lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, rangeProfile{name: "labels", label: label, caps: logsql.CapabilitiesFor("v1.53.0")})
		if !ok || delegated || !same {
			t.Errorf("%s: ok %v delegated %v\n lowered %s\n  string %s", q, ok, delegated, lowered, legacy)
		}
		if strings.Count(lowered, "extract_regexp") != 3 || !strings.Contains(lowered, "__lxst_2") || strings.Contains(lowered, "level=") {
			t.Errorf("%s: the restore of three names (app, k8s.pod, env-x) is not in %s", q, lowered)
		}
	}
	if !slices.Equal(levelFieldNames, translator.DetectedLevelFieldNames) {
		t.Errorf("the level fields the restore skips drifted from the translator's: %v", levelFieldNames)
	}
}

// Text the string translator reads as syntax, though a quoted value holds it (a ")" that unbalances its parentheses,
// a "| unwrap x", the brackets of a range): the lowering keeps the value whole. A " bool " or "without (x)" the
// translator rewrites is left to it (TestPlanDelegatesVectorAggregations).
//
// conformance: architecture-syntax-package-layering
func TestPlanRangeAggregationKeepsQuotedValues(t *testing.T) {
	for q, want := range map[string]string{
		`bytes_over_time({app="a"} |~ "a) + (b" [5m])`:     "a) + (b",
		`absent_over_time({app="a"} |= "| unwrap x" [5m])`: "unwrap x",
		`count_over_time({app="a"} |= "[5m]" [5m])`:        "5m",
	} {
		op, err := Plan(rangeAggPlan(q), Profile{})
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		text := op.(physical.VLQuery).Text
		if !strings.Contains(text, want) || !strings.HasPrefix(text, `app:="a" `) {
			t.Errorf("%s: %s does not hold %s", q, text, want)
		}
		if _, err := logsql.Parse(text); err != nil {
			t.Errorf("%s: %s does not parse: %v", q, text, err)
		}
	}
}

// FuzzRangeAggLowering holds the lowering of range aggregations to the string translator on any selector values,
// line filters, label filters, functions, ranges and offsets: both write the same query, or the lowering leaves the
// query to the translator. Values the translator reads as syntax in a metric query (" bool ", "without", "unwrap",
// unbalanced parentheses, a pipe before a parser name) are left out: it rewrites those, the lowering does not.
//
// conformance: architecture-syntax-package-layering
func FuzzRangeAggLowering(f *testing.F) {
	f.Add("a", "x", "level", "error", uint8(0), uint8(0), uint8(0))
	f.Add("service_name", "é\n", "status", ".*", uint8(7), uint8(3), uint8(5))
	f.Add("k8s.pod", "[5m]", "a-b", "10.0.0.0/8", uint8(200), uint8(9), uint8(11))
	f.Fuzz(func(t *testing.T, sel, line, name, val string, fn, shape, rng uint8) {
		for _, s := range []string{sel, line, name, val} {
			if translatorReadsAsSyntax(s) {
				return
			}
		}
		ident := func(s string) string {
			var b strings.Builder
			for _, r := range s {
				if r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' && b.Len() > 0 {
					b.WriteRune(r)
				}
			}
			if b.Len() == 0 {
				return "x"
			}
			return b.String()
		}
		matchers := []string{`="`, `!="`, `=~"`, `!~"`}
		selector := `{` + ident(sel) + matchers[shape%4] + escapeValue(sel) + `"}`
		if shape&0x10 != 0 {
			selector = `{` + ident(sel) + matchers[shape%4] + escapeValue(sel) + `", env="p"}`
		}
		stage := []string{
			``, ` |= ` + strconv.Quote(line), ` != ` + strconv.Quote(line), ` |~ ` + strconv.Quote(line), ` !~ ` + strconv.Quote(line),
			` |> ` + strconv.Quote(line), ` | ` + ident(name) + `=` + strconv.Quote(val), ` | ` + ident(name) + `=~` + strconv.Quote(val) + ` |= ` + strconv.Quote(line),
			` | ` + ident(name) + `>=` + strconv.Itoa(len(val)), ` |= ` + strconv.Quote(line) + ` or ` + strconv.Quote(val),
		}[shape>>5%10]
		function := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}[fn%5]
		span := []string{`[5m]`, `[1ms]`, `[1d]`, `[1h30m]`, `[5m] offset 1h`, `[1m] offset 0s`}[rng%6]
		q := function + `(` + selector + stage + span + `)`
		for _, prof := range rangeProfiles() {
			if lowered, legacy, same, delegated, ok := rangeAggLowering(t, q, prof); ok && !delegated && !same {
				t.Fatalf("%s: %s:\n lowered %s\n  string %s", prof.name, q, lowered, legacy)
			}
		}
	})
}

// escapeValue is s as the inside of a double-quoted LogQL string.
func escapeValue(s string) string {
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}

// translatorReadsAsSyntax reports whether s holds text the string translator reads from a metric query's text as
// syntax though it is inside a quoted value: the words " bool ", "without" and "unwrap", parentheses that do not
// balance, a pipe before the name of a parser, and the brackets of a range.
func translatorReadsAsSyntax(s string) bool {
	if strings.Contains(s, "bool") || strings.Contains(s, "without") || strings.Contains(s, "unwrap") || strings.Contains(s, "offset") {
		return true
	}
	depth := 0
	for _, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			if depth--; depth < 0 {
				return true
			}
		}
	}
	if depth != 0 {
		return true
	}
	for _, kw := range []string{"json", "logfmt", "pattern", "regexp", "unpack", "by", "on", "ignoring", "group_"} {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// The restore of a set of stream labels is built once and shared: concurrent requests of a tenant read the same
// printed text, and a set past the memo's bound is built each time with the same result.
//
// conformance: architecture-syntax-package-layering
func TestPlanStreamTagRestoreMemo(t *testing.T) {
	label := translator.WithStreamLabels(nil, []string{"app", "env"})
	want, ok := streamTagRestore(label)
	if !ok || strings.Count(want, "extract_regexp") != 2 {
		t.Fatalf("restore = %q, %v", want, ok)
	}
	done := make(chan string, 16)
	for i := 0; i < 16; i++ {
		go func() {
			got, _ := streamTagRestore(translator.WithStreamLabels(nil, []string{"app", "env"}))
			done <- got
		}()
	}
	for i := 0; i < 16; i++ {
		if got := <-done; got != want {
			t.Errorf("concurrent restore differs: %q", got)
		}
	}
	if text, ok := streamTagRestore(nil); text != "" || !ok {
		t.Errorf("no label function: %q, %v", text, ok)
	}
	if text, ok := streamTagRestore(func(s string) string { return s }); text != "" || !ok {
		t.Errorf("no stream labels: %q, %v", text, ok)
	}
	if _, ok := streamTagRestore(translator.WithStreamLabels(nil, []string{"a b"})); ok {
		t.Error("a name with a space is lowered")
	}
	restoreMemoCount.Store(maxRestoreMemo)
	t.Cleanup(func() { restoreMemoCount.Store(0) })
	over := translator.WithStreamLabels(nil, []string{"over", "bound"})
	a, _ := streamTagRestore(over)
	b, _ := streamTagRestore(over)
	if a != b || strings.Count(a, "extract_regexp") != 2 {
		t.Errorf("past the bound: %q vs %q", a, b)
	}
}
