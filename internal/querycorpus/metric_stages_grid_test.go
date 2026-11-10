package querycorpus_test

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// metricStagesGrid is the generated grid of range and vector aggregations over log queries with drop, keep,
// decolorize, line_format, label_format and the regexp and pattern parsers (next to json and logfmt): a stride
// sample of the parser, extract, format and parsed label filter grids (json and logfmt with their flags and field
// lists, drop and keep, regexp, pattern, unpack, decolorize, line_format, label_format, label filters after a
// parser) inside each function the lowering writes natively, alone and under the seven vector aggregations with
// and without a clause, over an unwrapped label too, plus hand-written shapes the metric rows of the string
// translator write specially (a trailing keep, a stream label read after a parser, text it reads as a parser or an
// unwrap). Deterministic; the lowering and the string translator are held to each other on every row under every
// profile (TestMetricStagesGridShadow).
func metricStagesGrid(stride int) []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	funcs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	var logs []string
	for _, g := range [][]string{parserGrid(), extractGrid(), formatGrid(), labelFilterParsedGrid()} {
		for i := 0; i < len(g); i += stride {
			logs = append(logs, g[i])
		}
	}
	logs = validQueries(logs)
	for i, q := range logs {
		if !strings.HasPrefix(q, "{") {
			continue
		}
		for _, f := range funcs {
			add(fmt.Sprintf(`%s(%s [5m])`, f, q))
		}
		if i%11 == 0 {
			add(fmt.Sprintf(`rate(%s [1m] offset 5m)`, q))
			add(fmt.Sprintf(`count_over_time((%s)[30s])`, q))
		}
		// the aggregations: a few shapes per query, rotating through the functions, operators and clauses.
		ops := []string{"sum", "count", "min", "max", "avg"}
		clauses := []string{``, `by (a)`, `without (a)`, `by (level, a)`, `by (service_name)`, `by (detected_level)`}
		f := funcs[i%len(funcs)]
		op := ops[i%len(ops)]
		add(fmt.Sprintf(`%s(%s(%s [5m]))`, op, f, q))
		add(fmt.Sprintf(`%s by (a) (%s(%s [5m]))`, ops[(i+1)%len(ops)], funcs[(i+2)%len(funcs)], q))
		add(fmt.Sprintf(`%s %s (%s(%s [5m]))`, ops[(i+2)%len(ops)], clauses[(i/3)%len(clauses)], funcs[(i+3)%len(funcs)], q))
		add(fmt.Sprintf(`sum(rate(%s [5m]))`, q))
		add(fmt.Sprintf(`count(count_over_time(%s [5m]))`, q))
		if i%7 == 0 {
			add(fmt.Sprintf(`sum_over_time(%s | unwrap n [5m])`, q))
			add(fmt.Sprintf(`avg(max_over_time(%s | unwrap n [5m]))`, q))
		}
	}
	// hand-written shapes the metric rows of the string translator write specially: a trailing keep, a keep or drop
	// that names the level fields, a stream label read after a parser, a parser after a label filter, and quoted
	// text the translator reads as a parser, an unwrap, a clause or a stats pipe, in every stage that holds a value.
	shapes := []string{
		`{app="a"} | json | keep a`, `{app="a"} | logfmt | keep a, b | status > 5`, `{app="a"} | json | keep a | line_format "{{.a}}"`,
		`{app="a"} | regexp "(?P<a>x)" |= "| unpack_json"`, `{app="a"} | pattern "<a>" |= "x | unpack_logfmt"`, `{app="a"} | regexp "(?P<a>x| json)"`,
		`{app="a"} | pattern "| json <a>"`, `{app="a"} | json a="| logfmt"`, `{app="a"} | regexp "(?P<a_b>x)" | a_b="1"`,
		`{app="a"} | keep a`, `{app="a"} | keep a | json`, `{app="a"} | json | keep a | json`, `{app="a"} | keep a | keep b`,
		`{app="a"} | keep a="x"`, `{app="a"} | drop a | keep b`, `{app="a"} | keep __error__, a`, `{app="a"} | keep level`,
		`{app="a"} | keep detected_level`, `{app="a"} | keep detected_level, a`, `{app="a"} | keep detected_level, level`,
		`{app="a"} | json | drop level`, `{app="a"} | json | keep detected_level`, `{app="a"} | drop detected_level`,
		`{app="a"} | drop level, detected_level, a`, `{app="a"} | drop level="x", a`, `{app="a"} | keep detected_level="x"`,
		`{app="a"} | pod="p" | json`, `{app="a"} | pod="p" | json | pod="q"`, `{app="a", pod="b"} | json | pod="q"`,
		`{app="a"} | json | app="q"`, `{service_name="s"} | json | service_name="q"`, `{app="a"} | logfmt | pod=~"p.*" | drop pod`,
		`{app="a"} | json | line_format "{{.pod}}"`, `{app="a"} | json | label_format x=pod`, `{app="a"} | json | label_format pod=x`,
		`{app="a"} | regexp "(?P<pod>x)" | pod="p"`, `{app="a"} | pattern "<pod> <b>" | pod="p" | drop pod`, `{app="a"} | pattern "<_>" | keep a`,
		`{app="a"} | regexp "(?P<a>x)" | line_format "{{.a}}"`, `{app="a"} | regexp "(?P<a>x)" | label_format b=a | keep b`,
		`{app="a"} | pattern "<a> <b>" | decolorize | drop a`, `{app="a"} | decolorize | regexp "(?P<a>.*)"`,
		`{app="a"} | json | __error__=""`, `{app="a"} | json | __error__!=""`, `{app="a"} | json | drop __error__`,
		`{app="a"} | json | keep`, `{app="a"} | json | drop`, `{app="a"} | json | json`, `{app="a"} | logfmt | json`,
		`{app="a"} |= "a | json b"`, `{app="a"} |= "x |logfmt"`, `{app="a"} |~ "| pattern"`, `{app="a"} != "a|regexp"`, `{app="a"} |= "a | unpack"`,
		`{app="a"} | a="b | json"`, `{app="a"} | line_format "x | json"`, `{app="a"} | json | line_format "x | logfmt"`,
		`{app="a"} | label_format x="| json"`, `{app="a"} | label_format x="a | regexp b"`, `{app="a"} | drop a="| json"`, `{app="a"} | keep a="| pattern"`,
		`{app="a"} | drop a=~"x|json"`, `{app="a"} | drop a=" | unpack"`, `{app="a"} | a=~"| logfmt"`, `{app="a"} | a="| unwrap n"`,
		`{app="a"} | drop a="| unwrap n"`, `{app="a"} | label_format a="| unwrap n"`, `{app="a"} | line_format "| unwrap n"`,
		`{app="a"} | drop a="without (x)"`, `{app="a"} | label_format a="a bool b"`, `{app="a"} | line_format "x ||| y"`,
		`{app="a"} | drop a="| stats by (x)"`, `{app="a"} | label_format a="| stats by (x)"`, `{app="a"} | line_format "| stats count()"`,
		`{app="a"} | label_format a="| copy x"`, `{app="a"} | label_format a="as n"`, `{app="a"} | line_format "| unpack_json"`,
		`{app="a"} |= "x" | json | level="error" |= "y"`, `{app="a"} | decolorize | json`, `{app="a"} | unpack | level="e"`,
		`{app="a"} | keep a | line_format "x"`, `{app="a"} | keep a | decolorize`, `{app="a"} | keep a | a="x"`, `{app="a"} | keep a |= "x"`,
		`{app="a"} | keep a | label_format b=a`, `{app="a"} | keep a | unwrap a`, `{app="a"} | line_format "x" | keep a`,
		`{app="a"} | json | keep a | regexp "(?P<b>x)"`, `{app="a"} | json | keep a | pattern "<b>"`, `{app="a"} | regexp "(?P<b>x)" | keep b`,
	}
	for _, f := range funcs {
		for _, in := range shapes {
			add(fmt.Sprintf(`%s(%s [5m])`, f, in))
		}
	}
	for _, in := range shapes {
		for _, op := range []string{"sum", "count", "min", "max", "avg"} {
			for _, g := range []string{``, `by (a)`, `without (a)`, `by (a_b)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (pod)`} {
				for _, f := range []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"} {
					if g == `` {
						add(fmt.Sprintf(`%s(%s(%s [5m]))`, op, f, in))
					} else {
						add(fmt.Sprintf(`%s %s (%s(%s [5m]))`, op, g, f, in))
					}
				}
			}
		}
		for _, f := range []string{"sum_over_time", "avg_over_time", "last_over_time", "quantile_over_time", "rate", "rate_counter"} {
			// the unwrap last, and with stages after it, which the translator keeps where they stand.
			for _, uw := range []string{in + ` | unwrap n`, in + ` | unwrap n | keep a`, in + ` | unwrap n | keep n`, in + ` | unwrap n | drop a`,
				in + ` | unwrap n | line_format "x"`, in + ` | unwrap n | a="b"`} {
				if f == "quantile_over_time" {
					add(fmt.Sprintf(`%s(0.9, %s [5m])`, f, uw))
				} else {
					add(fmt.Sprintf(`%s(%s [5m])`, f, uw))
					add(fmt.Sprintf(`sum by (a) (%s(%s [5m]))`, f, uw))
					add(fmt.Sprintf(`count(%s(%s [5m]))`, f, uw))
					add(fmt.Sprintf(`avg without (a) (%s(%s [5m]))`, f, uw))
				}
			}
		}
	}
	return out
}

// metricStagesStride is the step through each source grid (about 109,000 queries together): 32 keeps the grid to
// seconds, LVP_METRIC_STAGES_STRIDE=1 takes every query.
func metricStagesStride() int {
	if v, err := strconv.Atoi(os.Getenv("LVP_METRIC_STAGES_STRIDE")); err == nil && v > 0 {
		return v
	}
	return 32
}

// TestMetricStagesGridShadow holds the lowering of range and vector aggregations over drop, keep, formatting stages
// and the regexp and pattern parsers to the string translator on the generated grid under every profile: 0
// mismatches, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestMetricStagesGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := metricStagesGrid(metricStagesStride())
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator (first 30):\n%s", len(problems), joinFirst(problems, 30))
	}
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
