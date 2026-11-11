// conformance: architecture-syntax-package-layering
package querycorpus_test

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// parserLabelsGrid is the generated grid of the metric queries whose labels a json, logfmt, regexp or pattern
// parser extracts: the labels the string translator reads from the text of the query and the plan has as typed data.
//
//   - the labels a stats pipe groups by or aggregates under their sanitized name (a name with an underscore:
//     http_method, duration_ms, user_id), on the vector aggregations, the range aggregations of an unwrapped label
//     and the aggregations over rows, with the names the query defines itself (an alias, a capture, a
//     label_format destination), stream labels, derived labels and `_extracted` ones;
//   - the series of a `without` fold over a parser: bare and listed extractions, aliases, captures, the names the
//     translator does not follow (level, a stream label, a flag, an unpack), the stages that end or change the
//     list (an unwrap, label_format, drop, keep), negative line filters glued to a stage;
//   - the series behind a trailing keep, with the stages that end it, matchers, error labels, resolved names;
//   - the bare logfmt unpack the translator narrows to the labels it reads.
//
// Deterministic; the lowering and the string translator are held to each other on every row under every profile
// (TestParserLabelsGridShadow), and the grid is mutation-checked (TestParserLabelsGridCatchesMutations).
func parserLabelsGrid() []string {
	bulk, hand := parserLabelsGridParts()
	return append(bulk[:len(bulk):len(bulk)], hand...)
}

// parserLabelsGridParts is the grid in two parts: the generated bulk (the first three groups above) and the
// hand-written shapes (the text the translator reads as syntax), which a sampled run keeps whole.
func parserLabelsGridParts() (bulk, hand []string) {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	wrap := func(op, g, in string) {
		if g == "" {
			add(fmt.Sprintf(`%s(%s)`, op, in))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, op, g, in))
		add(fmt.Sprintf(`%s(%s) %s`, op, in, g))
	}
	call := func(f, body, rng string) string {
		if f == "quantile_over_time" {
			return fmt.Sprintf(`%s(0.9, %s%s)`, f, body, rng)
		}
		return fmt.Sprintf(`%s(%s%s)`, f, body, rng)
	}
	rowFuncs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	unwrapFuncs := []string{"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time",
		"stddev_over_time", "stdvar_over_time", "quantile_over_time", "rate", "rate_counter"}
	ops := []string{"sum", "count", "min", "max", "avg"}
	sel := `{app="a"}`

	// 1. labels resolved under their sanitized name.
	parsers := []string{
		` | json`, ` | logfmt`, ` | json a_b`, ` | logfmt a_b, c`, ` | json a_b="x.y"`, ` | json c="a.b"`, ` | json --strict`, ` | logfmt --keep-empty`,
		` | json | logfmt`, ` | json | label_format a_b=c`, ` | json | label_format c=a_b`, ` | regexp "(?P<a_b>x)"`, ` | pattern "<a_b> <c>"`,
		` | json | line_format "<a_b>"`, ` | json a_b="x.y" | line_format "{{.a_b}}"`, ` | json x="a.b" | drop x`, ` | unpack`, ` | json | unpack`,
		` | logfmt | json`,
	}
	groupings := []string{
		``, `by ()`, `by (a)`, `by (a_b)`, `by (a_b, c_d)`, `by (a, a_b)`, `by (a_b, a_b)`, `by (user_id)`, `by (service_name, a_b)`,
		`by (detected_level, a_b)`, `by (level, a_b)`, `by (app, a_b)`, `by (_a_b)`, `by (a_extracted)`, `by (level_extracted, a_b)`,
		`by (A_B)`, `by (a_b1)`, `by (a_b_c_d_e_f_g_h)`, `by (k8s_pod_name)`, `by (k8s.pod.name)`, `by (a_b, bool)`,
		`without (a)`, `without (a_b)`, `without (pod)`, `without (a, a_b)`,
	}
	filters := []string{``, ` | a_b != ""`, ` | a_b="x"`, ` | a_b=~"p.*"`, ` | status >= 500`, ` | a_b != "" | c="d"`, ` | __error__=""`, ` | a_extracted="x"`}
	for _, p := range parsers {
		for _, g := range groupings {
			for _, op := range []string{"sum", "count"} {
				for _, fn := range []string{"count_over_time", "rate"} {
					wrap(op, g, call(fn, sel+p, `[5m]`))
				}
			}
		}
	}
	for _, p := range []string{` | json`, ` | logfmt`, ` | regexp "(?P<a_b>x)"`} {
		for _, f := range filters[1:] {
			for _, g := range []string{``, `by (a_b)`, `by (c_d)`, `by (a_extracted)`, `by (a_b, c_d)`, `without (pod)`, `by (app, a_b)`} {
				for _, op := range []string{"sum", "count"} {
					for _, fn := range []string{"count_over_time", "rate", "bytes_over_time"} {
						wrap(op, g, call(fn, sel+p+f, `[5m]`))
					}
				}
			}
		}
	}
	// every operation x function on the common parsers.
	for _, p := range []string{` | json`, ` | logfmt`, ` | json a_b`} {
		for _, g := range []string{``, `by (a_b)`, `by (app, a_b)`, `without (pod)`} {
			for _, op := range append(ops, "stddev") {
				for _, fn := range rowFuncs {
					wrap(op, g, call(fn, sel+p, `[5m]`))
				}
			}
		}
	}
	// the range aggregations of an unwrapped label.
	unwrapStages := []string{`unwrap n`, `unwrap a_b`, `unwrap bytes(a_b)`, `unwrap duration(a_b)`, `unwrap a_extracted`, `unwrap _a`}
	for _, p := range []string{` | json`, ` | logfmt a_b, n`, ` | json a_b="x.y"`, ` | logfmt | label_format a_b=n`, ` | regexp "(?P<a_b>.+)"`, ` | json x="a.b" | line_format "{{.x}}"`} {
		for _, u := range unwrapStages {
			for _, tail := range []string{``, ` | a_b != ""`} {
				for _, fn := range unwrapFuncs {
					body := sel + p + ` | ` + u + tail
					add(call(fn, body, `[5m]`))
					for _, by := range []string{`by ()`, `by (a_b)`} {
						add(call(fn, body, `[5m]`) + ` ` + by)
					}
					if fn != "sum_over_time" && fn != "first_over_time" && fn != "rate" && fn != "stdvar_over_time" {
						continue
					}
					for _, g := range []string{``, `by (a_b)`, `by (c_d)`, `by (service_name)`, `without (pod)`} {
						for _, op := range []string{"sum", "count"} {
							wrap(op, g, call(fn, body, `[5m]`))
						}
					}
				}
			}
		}
	}

	// 2. the series of a `without` fold over a parser.
	withoutParsers := []string{
		` | json`, ` | logfmt`, ` | json a, b`, ` | json a="x.y", b`, ` | json a="x.y.z", b="k"`, ` | json a="x[0]"`, ` | json a=` + "`x.y`", ` | logfmt a, b`, ` | logfmt a="b"`,
		` | logfmt a_b, c`, ` | logfmt --strict a`, ` | json --strict a`, ` | logfmt --keep-empty a`, ` | json a | json b`, ` | json a | logfmt b`, ` | json a, a`,
		` | json a | json`, ` | logfmt | json a`, ` | regexp "(?P<a>x)(?P<b>y)"`, ` | regexp "(x)(y)"`, ` | regexp "(?P<a>x"`, ` | regexp "(?P<level>x)"`,
		` | pattern "<a> <_> <b>"`, ` | pattern "<a><b>"`, ` | pattern "<_>"`, ` | json level`, ` | json detected_level`, ` | json service_name`, ` | json app`,
		` | json env`, ` | logfmt a, app`, ` | regexp "(?P<app>x)"`, ` | pattern "<app>"`, ` | unpack`, ` | json a | unpack`, ` | json a | label_format b=a`,
		` | json a | drop a`, ` | json a | keep a`, ` | json a | line_format "{{.a}}"`, ` | json a | decolorize`, ` | json a | a="x"`, ` | json a | a=~"x.*"`,
		` | json a != "x"`, ` | json a | json b != "x"`, ` | regexp "(?P<a>x)" !~ "y"`, ` | pattern "<a>" != "y"`, ` | json a |= "y" != "z"`, ` | json a !> "<_>"`,
		` | json a | json b |> "<_>"`, ` | json a | unwrap n`, ` | json a | unwrap n | json b`, ` | json a | unwrap n | label_format b=a`, ` | json a | unwrap a`,
		` | json a, n | unwrap n`, ` | json a_b`, ` | json a_b, c_d`, ` | json a="x.y" | json a`,
	}
	for _, p := range withoutParsers {
		for _, op := range []string{"sum", "count", "avg", "max"} {
			for _, g := range []string{`without (pod)`, `without (a)`, `without ()`} {
				for _, fn := range []string{"count_over_time", "bytes_over_time", "rate", "bytes_rate", "absent_over_time"} {
					wrap(op, g, call(fn, sel+p, `[5m]`))
				}
			}
		}
	}
	for _, p := range []string{` | json a | unwrap n`, ` | logfmt a, n | unwrap n`, ` | json | unwrap n`, ` | logfmt a, b | unwrap a`, ` | regexp "(?P<a>x)(?P<n>y)" | unwrap n`,
		` | json a, n | unwrap bytes(n)`, ` | json a_b, n | unwrap n`, ` | json a="x.y", n | unwrap n`, ` | logfmt | unwrap n`, ` | logfmt n | unwrap n | a="x"`,
		` | json a | unwrap n | keep a`, ` | pattern "<a> <n>" | unwrap duration(n)`, ` | json a | line_format "x" | unwrap n`} {
		for _, op := range []string{"sum", "count", "avg"} {
			for _, g := range []string{`without (pod)`, `without (a)`, `without (a, pod)`} {
				for _, fn := range unwrapFuncs {
					wrap(op, g, call(fn, sel+p, `[5m]`))
				}
				wrap(op, g, call("absent_over_time", sel+p, `[5m]`))
			}
		}
	}
	// the selector's own labels, and the parsed ones that collide with them.
	for _, s := range []string{`{app="a", env="p"}`, `{a="b"}`, `{service_name="s"}`, `{level="error"}`, `{a=~"x.*"}`, `{a!="x"}`} {
		for _, p := range []string{` | json a, b`, ` | logfmt a`, ` | json a, env`, ` | regexp "(?P<a>x)"`} {
			for _, op := range []string{"count", "sum", "avg"} {
				wrap(op, `without (pod)`, call("rate", s+p, `[5m]`))
				wrap(op, `without (pod)`, call("count_over_time", s+p, `[5m]`))
			}
		}
	}
	// nests with a `without` at some level, and a `by` of a resolved name.
	for _, p := range []string{` | json`, ` | logfmt a, b`, ` | json a_b`} {
		for _, fn := range []string{"count_over_time", "rate"} {
			in := call(fn, sel+p, `[5m]`)
			add(fmt.Sprintf(`sum(count without (pod) (%s))`, in))
			add(fmt.Sprintf(`count by (zone) (max without (pod) (%s))`, in))
			add(fmt.Sprintf(`max(avg(%s) without (pod)) by (zone)`, in))
			add(fmt.Sprintf(`sum(sum by (a_b) (%s))`, in))
			add(fmt.Sprintf(`sum by (a_b) (sum by (a_b, c) (%s))`, in))
			add(fmt.Sprintf(`min(count by (a_b, c_d) (%s))`, in))
			add(fmt.Sprintf(`sum without (a_b) (sum by (a_b, c) (%s))`, in))
			add(fmt.Sprintf(`sum without (pod) (sum without (x) (%s))`, in))
		}
	}

	// 3. the series behind a trailing keep.
	keeps := []string{
		` | keep a`, ` | keep a, b`, ` | keep a_b`, ` | keep a_b, c_d`, ` | keep level, a`, ` | keep service_name`, ` | keep detected_level`, ` | keep detected_level, level`,
		` | keep __error__`, ` | keep __error__, __error_details__`, ` | keep a, __error__`, ` | keep a="x"`, ` | keep a, b="x"`, ` | keep a="x" | json | keep b`,
		` | keep a, a`, ` | keep k8s_pod_name`, ` | keep k8s.pod.name`, ` | keep app`, ` | keep _stream`, ` | keep a_extracted`, ` | keep level_extracted, a`,
		` | keep a | keep b`, ` | keep a | json`, ` | keep a | logfmt b`, ` | keep a | label_format b=a`, ` | keep a | drop b`, ` | keep a | unwrap n`,
		` | keep a | line_format "x"`, ` | keep a | decolorize`, ` | keep a | a="1"`, ` | keep a |= "x" != "y"`, ` | keep a | regexp "(?P<b>x)"`,
		` | keep a | pattern "<b>"`, ` | keep a | unpack`, ` | keep a | keep b="x"`, ` | keep a | drop b="x"`, ` | keep a | a=~"x.*" | b>1`,
	}
	for _, k := range keeps {
		for _, pre := range []string{``, ` | json`, ` | logfmt a_b`} {
			for _, op := range []string{"sum", "count"} {
				for _, g := range []string{``, `by (a)`, `by (a_b)`, `without (pod)`} {
					for _, fn := range []string{"count_over_time", "rate", "bytes_rate"} {
						wrap(op, g, call(fn, sel+pre+k, `[5m]`))
					}
				}
			}
			for _, fn := range []string{"rate", "bytes_rate", "count_over_time"} {
				add(call(fn, sel+pre+k, `[5m]`))
			}
		}
	}
	for _, k := range []string{` | keep a`, ` | keep a_b`, ` | keep __error__`, ` | keep a="x"`, ` | keep a | json`} {
		for _, u := range []string{`unwrap n`, `unwrap a_b`} {
			for _, pre := range []string{``, ` | json`} {
				for _, body := range []string{sel + pre + k + ` | ` + u, sel + pre + ` | ` + u + k, sel + pre + ` | ` + u + k + ` | keep b`} {
					for _, op := range []string{"sum", "count", "avg"} {
						for _, g := range []string{``, `by (x)`, `without (pod)`} {
							for _, fn := range []string{"rate", "max_over_time"} {
								wrap(op, g, call(fn, body, `[5m]`))
							}
						}
					}
					add(call("rate", body, `[5m]`))
				}
			}
		}
	}

	bulkEnd := len(out)
	// 4. text the translator reads as syntax, which the lowering leaves to it.
	for _, line := range []string{` |= "| unpack_json"`, ` |= "as a_b"`, ` |= "<a_b>"`, ` |= "| filter a_b:!\"\""`, ` |= "| copy x as a_b"`, ` |= "| stats "`, ` |= "without (x)"`} {
		for _, g := range []string{``, `by (a_b)`, `without (pod)`} {
			wrap("sum", g, call("rate", sel+` | json`+line, `[5m]`))
			wrap("count", g, call("count_over_time", sel+` | json`+line, `[5m]`))
			wrap("sum", g, call("sum_over_time", sel+` | json`+line+` | unwrap a_b`, `[5m]`))
		}
	}
	return out[:bulkEnd:bulkEnd], out[bulkEnd:]
}

// parserLabelsShadow holds lowerFn to the string translator on the grid under every profile: the text byte for
// byte, the rows each side answers counted by construct, and every mismatch returned.
func parserLabelsShadow(t *testing.T, grid []string, lowerFn lowering) (rows, native int, delegated map[string]int, problems []string) {
	t.Helper()
	delegated = map[string]int{}
	profiles := planProfiles()
	for _, q := range grid {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, queryplan.Inputs{})
		if err != nil {
			continue
		}
		queryplan.Resolve(plan)
		for _, prof := range profiles {
			got, lerr := lowerFn(q, plan, prof)
			legacy, ok := prof.translate(q)
			if !ok {
				if lerr == nil {
					problems = append(problems, fmt.Sprintf("%s: %q: lowered, the string translator rejects it", prof.name, q))
				}
				continue
			}
			rows++
			if strings.HasPrefix(legacy, "|") {
				legacy = "* " + legacy
			}
			var nl *lower.NotLoweredError
			if errors.As(lerr, &nl) {
				delegated[nl.Construct]++
				continue
			}
			if lerr != nil {
				problems = append(problems, fmt.Sprintf("%s: %q: lowering failed: %v", prof.name, q, lerr))
				continue
			}
			// A root the proxy evaluates (stddev, stdvar, a ranking) is held to the proxy's route, as in TestPlanShadow.
			routed, divergence := evaluatedRoot(plan)
			if _, known := knownTypedDivergences()[queryID("logql", q)]; known || translatorMisreadsQuotedText(q) || (!routed && translatorMisreadsParenthesizedMetric(q)) || translatorReadsStageMarker(q) {
				delegated["known divergence"]++
				continue
			}
			if routed {
				if msg := checkProxyEvaluated(q, prof, got); msg != "" {
					problems = append(problems, msg)
					continue
				}
				native++
				continue
			}
			if divergence != "" {
				delegated["known divergence "+divergence]++
				continue
			}
			want, err := translator.DecodePhysical(legacy)
			if err != nil {
				t.Fatalf("%s: DecodePhysical(%q): %v", prof.name, legacy, err)
			}
			if text := physical.Encode(got); text != legacy || !physical.Equal(want, got) {
				problems = append(problems, fmt.Sprintf("%s: %q:\n lowered %s\n  string %s", prof.name, q, text, legacy))
				continue
			}
			native++
		}
	}
	return rows, native, delegated, problems
}

// parserLabelsStride is the step through the generated bulk of the grid: every query when the race detector and the
// coverage instrumentation are off (about 10 s), one in 24 under them, where the package would otherwise outrun its
// timeout (the CI shard runs it with both). LVP_PARSER_LABELS_STRIDE sets it.
func parserLabelsStride() int {
	if v, err := strconv.Atoi(os.Getenv("LVP_PARSER_LABELS_STRIDE")); err == nil && v > 0 {
		return v
	}
	if raceDetectorEnabled || testing.CoverMode() != "" {
		return 24
	}
	return 1
}

// parserLabelsSampledGrid is every stride-th query of the generated bulk and all the hand-written shapes.
func parserLabelsSampledGrid(stride int) []string {
	bulk, hand := parserLabelsGridParts()
	var out []string
	for i := 0; i < len(bulk); i += stride {
		out = append(out, bulk[i])
	}
	return append(out, hand...)
}

// TestParserLabelsGridShadow holds the lowering of the parser-label constructs to the string translator on the
// generated grid under every profile: 0 mismatches, the text byte for byte.
// conformance: architecture-syntax-package-layering
func TestParserLabelsGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := parserLabelsSampledGrid(parserLabelsStride())
	rows, native, delegated, problems := parserLabelsShadow(t, grid, nativeLowering)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 20))
	}
	if native < rows/3 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}

// mutateOp applies f to the text of the query an operator holds (under a fold too).
func mutateOp(op physical.Op, f func(string) string) physical.Op {
	switch o := op.(type) {
	case physical.VLQuery:
		o.Text = f(o.Text)
		return o
	case physical.Fold:
		o.In = mutateOp(o.In, f)
		return o
	}
	return op
}

// parserLabelsSample is up to n grid queries for each marker the constructs of the grid show in a query.
func parserLabelsSample(n int, markers ...string) []string {
	var out []string
	for _, m := range markers {
		taken := 0
		for _, q := range parserLabelsGrid() {
			if strings.Contains(q, m) {
				out = append(out, q)
				if taken++; taken == n {
					break
				}
			}
		}
	}
	return out
}

// keySpelling is a dotted or hyphenated key in the field list of a narrowed unpack.
var keySpelling = regexp.MustCompile(`, "[^"]*[.-][^"]*"`)

// TestParserLabelsGridCatchesMutations shows the grid fails when the lowering is wrong in each of the ways the step
// can be wrong: a resolved label's key pipe dropped, a fold's series not told apart by the parsed labels, a keep's
// labels replaced by the stream, a narrowed unpack that misses the spellings of a key, a collision rename without
// its scratch pipes. The unchanged lowering is held to 0 mismatches on the same sample.
// conformance: architecture-syntax-package-layering
func TestParserLabelsGridCatchesMutations(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	sample := parserLabelsSample(120, "by (a_b", "without (pod)", " | keep ", "_extracted", "unwrap a_b", "logfmt a_b, n | unwrap")
	if _, _, _, problems := parserLabelsShadow(t, sample, nativeLowering); len(problems) != 0 {
		t.Fatalf("the unchanged lowering has %d mismatches on the sample:\n%s", len(problems), joinFirst(problems, 3))
	}
	for name, mutate := range map[string]func(string) string{
		"a key pipe of a resolved label is dropped": func(s string) string {
			if i := strings.Index(s, `| format if ("`); i >= 0 {
				if j := strings.Index(s[i+1:], " | "); j >= 0 {
					return s[:i] + s[i+1+j+3:]
				}
			}
			return s
		},
		"a fold's series lose the parsed labels": func(s string) string {
			return strings.Replace(s, "stats by (_stream, ", "stats by (_stream, _msg, ", 1)
		},
		"a keep's labels become the stream": func(s string) string {
			if strings.Contains(s, "| fields _time, _msg, _stream, ") {
				return strings.Replace(s, "| stats by (", "| stats by (_stream, ", 1)
			}
			return s
		},
		"a narrowed logfmt unpack loses the key spellings": func(s string) string {
			i := strings.Index(s, "| unpack_logfmt fields (")
			if i < 0 {
				return s
			}
			j := strings.Index(s[i:], ")")
			return s[:i] + keySpelling.ReplaceAllString(s[i:i+j], "") + s[i+j:]
		},
		"a collision rename loses its scratch pipes": func(s string) string {
			if i := strings.Index(s, " | delete __lxp*"); i >= 0 {
				return s[:i] + s[i+len(" | delete __lxp*"):]
			}
			return s
		},
	} {
		wrong := func(q string, p *queryplan.Plan, prof planProfile) (physical.Op, error) {
			op, err := nativeLowering(q, p, prof)
			if err != nil {
				return nil, err
			}
			return mutateOp(op, mutate), nil
		}
		if _, _, _, problems := parserLabelsShadow(t, sample, wrong); len(problems) == 0 {
			t.Errorf("%s: the grid does not fail", name)
		}
	}
}

// TestParserLabelsVLOracle has VictoriaLogs' own parser read the LogsQL the lowering writes for the parser-label grid
// (the key pipes, the collision rename, the fold's series, the keep's labels) on every VictoriaLogs version of the
// matrix, as it reads the string translator's. It runs when LVP_RUN_ORACLES=1.
// conformance: architecture-syntax-package-layering
func TestParserLabelsVLOracle(t *testing.T) {
	// rate_counter is a marker function the proxy answers itself, a regexp with no named group and a pattern of two
	// adjacent captures or of none named are extracts VictoriaLogs rejects: all are rejected in the translator's text too, so none
	// says anything about the lowering.
	var grid []string
	for _, q := range parserLabelsGrid() {
		if !strings.Contains(q, "rate_counter") && !strings.Contains(q, `regexp "(x)(y)"`) && !strings.Contains(q, `regexp "(?P<a>x"`) &&
			!strings.Contains(q, `pattern "<a><b>"`) && !strings.Contains(q, `pattern "<_>"`) {
			grid = append(grid, q)
		}
	}
	checkLoweredGridWithVL(t, "parser-labels", grid)
}

// The oracle's typed pairs are made on the text the handlers translate (a quantile_over_time phi above 1 is
// clamped to 1, which VictoriaLogs accepts) and leave out rate_counter, whose stats function is a marker the proxy
// answers itself.
// conformance: architecture-syntax-package-layering
func TestTypedPairIsMadeOnTheTranslatedText(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	for _, v := range []string{"v1.49.0", "v1.53.0"} {
		typed, built, ok := typedQueryPair(`quantile_over_time(2.0, {app="a"} | json | unwrap duration_ms [5m])`, v)
		if !ok || typed != built || !strings.Contains(typed, "quantile(1,") {
			t.Errorf("%s: phi above 1: pair %v, typed %q, built %q", v, ok, typed, built)
		}
		if _, _, ok := typedQueryPair(`rate_counter({app="a"} | unwrap n [5m])`, v); ok {
			t.Errorf("%s: rate_counter makes a pair", v)
		}
	}
}
