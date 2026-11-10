package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// unwrapGrid is the generated grid of range aggregations over an unwrapped label, alone and under the vector
// aggregations: every function (sum, avg, min, max, first, last, stddev, stdvar, quantile with its spellings of the
// parameter, rate and rate_counter) with every conversion (none, bytes, duration, duration_seconds), unwrapped
// labels that are function names, selectors, line filters and label filters before and after the unwrap, the
// range and offset spellings, every grouping spelling of the range (by, by (), the derived names, repeated and
// dotted labels, without) and of the vector aggregation (none, by and without, in both positions of the clause),
// and the spellings and text the string translator reads unlike the plan (an unwrap that is not spelled with one
// blank on each side, quoted `| unwrap`, `| json` and `| stats`). Deterministic; the lowering and the string
// translator are held to each other on every row under every profile (TestUnwrapGridShadow).
func unwrapGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	// call writes a range aggregation over the log query body, with the quantile parameter when the function has one.
	call := func(f, body, rng, tail string) string {
		if f == "quantile_over_time" {
			return fmt.Sprintf(`%s(0.9, %s%s)%s`, f, body, rng, tail)
		}
		return fmt.Sprintf(`%s(%s%s)%s`, f, body, rng, tail)
	}
	funcs := []string{"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time",
		"stddev_over_time", "stdvar_over_time", "quantile_over_time", "rate", "rate_counter"}
	// the functions Loki accepts a by clause on.
	grouped := []string{"avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time", "quantile_over_time"}
	unwraps := []string{`| unwrap n`, `| unwrap bytes(n)`, `| unwrap duration(n)`, `| unwrap duration_seconds(n)`, `| unwrap max`, `| unwrap abs`,
		`| unwrap rand`, `| unwrap sum`, `| unwrap quantile`, `| unwrap a_b`, `| unwrap k8s.x`, `| unwrap _x`, `| unwrap Dur`, `| unwrap duration`,
		`| unwrap bytes`, `| unwrap n | __error__=""`, `| unwrap n | __error__!=""`, `| unwrap bytes(n) | __error__=""`, `| unwrap n | __error__="" | __error_details__=""`}
	selectors := []string{
		`{app="a"}`, `{app="a", env="prod"}`, `{app!="a"}`, `{app=~"a|b"}`, `{app=~".*"}`, `{service_name="s"}`, `{service_name=~"s|t"}`,
		`{detected_level="error"}`, `{detected_level!="info"}`, `{k8s.pod.name="p"}`, `{k8s_pod_name="p"}`, `{a="1", b=~"x.*", c!="y", d!~"z|w"}`,
		`{level="error"}`, `{namespace="prod", pod=~"api-.+"}`, `{app=""}`,
	}
	lines := []string{``, ` |= "x"`, ` != "x"`, ` |~ "x.+y"`, ` !~ "x|y"`, ` |= "a" |= "b" != "c"`, ` |= "x" or "y"`, ` |= ip("10.0.0.0/8")`,
		` |> "<_> x <_> y"`, ` !> "<_> x"`, ` |= "k" |> "<_> x <_>" != "j"`, ` |= ""`, ` |= "é\n"`, " |= `raw\\d`", ` |~ "(?i)error"`}
	labelFilters := []string{``, ` | status="200"`, ` | status!="200"`, ` | status=~"5.."`, ` | status!~"5.."`, ` | status>=500`, ` | status<10`, ` | status==3`,
		` | status="200" | foo!="bar"`, ` | duration>1s`, ` | duration<=2m`, ` | size>1KB`, ` | size<=2MiB`, ` | ok=true`, ` | a="b" or c="d"`,
		` | a="b" and c!="d"`, ` | ip("10.0.0.0/8")`, ` | addr=ip("10.0.0.0/8")`, ` | addr!=ip("10.0.0.1")`, ` | detected_level="error"`,
		` | service_name="s"`, ` | n>0`, ` | __error__=""`, ` | n!=""`}
	// every function and conversion, on a plain selector, with and without a range-level by where Loki takes one.
	for _, f := range funcs {
		for _, u := range unwraps {
			add(call(f, `{app="a"} `+u+` `, `[5m]`, ``))
		}
	}
	// every selector, line filter and label filter, one function per kind of statistics.
	for _, f := range []string{"sum_over_time", "last_over_time", "rate"} {
		for _, s := range selectors {
			for _, l := range lines {
				add(call(f, s+l+` | unwrap n `, `[5m]`, ``))
			}
		}
		for _, lf := range labelFilters {
			add(call(f, `{app="a"}`+lf+` | unwrap n `, `[5m]`, ``))
			add(call(f, `{app="a"} | unwrap n`+lf+` `, `[5m]`, ``))
			add(call(f, `{app="a"} |= "x"`+lf+` | unwrap duration(n)`+lf+` `, `[1m]`, ``))
		}
	}
	// the range and offset spellings (rate divides by the seconds), parenthesised and padded.
	for _, f := range funcs {
		for _, r := range []string{`[5m]`, `[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[1d]`, `[1ms]`, `[1h30m]`, `[300s]`, `[2w]`, `[1m30s]`, `[1w1d]`,
			`[1y]`, `[3d12h]`, `[0s]`, `[999ms]`, `[1500ms]`, `[1.5h]`, `[1.1d]`, `[1h1m1s1ms]`, `[10000h]`, `[1ns]`, `[1us]`, `[2.5s]`} {
			add(call(f, `{app="a"} | unwrap n `, r, ``))
			add(call(f, `{app="a"} |= "x" | unwrap bytes(n) `, r, ``))
		}
		add(call(f, `({app="a"} | unwrap n) `, `[5m]`, ``))
		add(call(f, ` {app="a"} | unwrap n `, `[5m] `, ``))
	}
	// the spellings of quantile's parameter.
	for _, phi := range []string{`0`, `1`, `0.5`, `0.50`, `.5`, `0.95`, `0.999`, `5e-1`, `1e0`, `0.0`, `00.5`, `1.`, `0x1p-1`, `-0`, `0.5000000001`, `1_0`, ` 0.5`, `0.5 `, `(0.5)`, `2`, `-0.5`} {
		add(fmt.Sprintf(`quantile_over_time(%s, {app="a"} | unwrap n [5m])`, phi))
		add(fmt.Sprintf(`quantile_over_time(%s,{app="a"} | unwrap n [5m]) by (pod)`, phi))
		add(fmt.Sprintf(`sum by (app) (quantile_over_time(%s, {app="a"} | unwrap n [5m]))`, phi))
	}
	groupings := []string{
		`by (app)`, `by ()`, `by (app, env)`, `by (app, app)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (app, service_name)`,
		`by (service_name, detected_level)`, `by (k8s.pod.name)`, `by (k8s_pod_name)`, `by (_stream)`, `by (_stream, level)`, `by (app, level, detected_level)`,
		`by (__error__)`, `by (a,b,c)`, `by ( app )`, `by(app)`, `by (bool)`, `by ( bool )`, `by (a, bool, b)`, `without (app)`, `without (a, b)`, `without ()`, `by (_msg)`,
	}
	// every grouping on the range, over every function Loki accepts one on, and over those it does not.
	for _, f := range funcs {
		for _, g := range groupings {
			add(call(f, `{app="a"} | unwrap n `, `[5m]`, ` `+g))
			if g == `by (app)` || g == `by ()` {
				add(call(f, `{app="a"} |= "x" | unwrap bytes(n) `, `[5m]`, ` `+g))
			}
		}
	}
	// the vector aggregations over every function: both positions of the clause, every grouping spelling.
	ops := []string{"sum", "count", "min", "max", "avg", "stddev", "stdvar"}
	vgroups := []string{
		``, `by ()`, `by (app)`, `by (app, app)`, `by (app, env)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (app, service_name)`,
		`by (service_name, detected_level)`, `by (k8s.pod.name)`, `by (k8s_pod_name)`, `by (_stream)`, `by (app, level, detected_level)`, `by (__error__)`,
		`by ( app )`, `without (app)`, `without (a, b)`, `without (a, a)`, `without ()`, `without (k8s.pod.name)`, `without (service_name)`,
		`without (detected_level)`, `without (level)`,
	}
	wrap := func(op, g, in string) {
		if g == "" {
			add(fmt.Sprintf(`%s(%s)`, op, in))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, op, g, in))
		add(fmt.Sprintf(`%s(%s) %s`, op, in, g))
	}
	vfuncs := []string{"sum_over_time", "max_over_time", "stddev_over_time", "stdvar_over_time", "first_over_time", "last_over_time", "quantile_over_time", "rate", "rate_counter"}
	for _, op := range ops {
		for _, g := range vgroups {
			for _, f := range vfuncs {
				wrap(op, g, call(f, `{app="a"} | unwrap n `, `[5m]`, ``))
			}
		}
	}
	for _, op := range ops {
		for _, f := range vfuncs {
			for _, u := range []string{`| unwrap bytes(n)`, `| unwrap duration(n)`, `| unwrap duration_seconds(n)`, `| unwrap max`, `| unwrap n | __error__=""`} {
				for _, g := range []string{``, `by (app)`, `without (app)`} {
					wrap(op, g, call(f, `{app="a"} `+u+` `, `[5m]`, ``))
				}
			}
		}
	}
	for _, op := range []string{"sum", "count", "avg"} {
		for _, f := range []string{"sum_over_time", "last_over_time", "quantile_over_time", "rate"} {
			for _, g := range []string{``, `by (app)`, `by (service_name)`, `by (detected_level)`, `without (app)`} {
				for _, s := range []string{selectors[3], selectors[5], selectors[9]} {
					for _, l := range []string{``, ` != "x" |~ "y.+"`, ` | status="200" |= "k"`} {
						wrap(op, g, call(f, s+l+` | unwrap n `, `[5m]`, ``))
					}
				}
			}
		}
		for _, f := range []string{"sum_over_time", "last_over_time", "quantile_over_time", "rate", "rate_counter"} {
			for _, r := range []string{`[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[2w]`, `[0s]`, `[1.5h]`} {
				for _, g := range []string{``, `by (app)`, `without (app)`} {
					wrap(op, g, call(f, `{app="a"} | unwrap n `, r, ``))
				}
			}
		}
	}
	// a vector aggregation over a range aggregation with its own clause: the lowering leaves it to the translator.
	for _, f := range grouped {
		for _, g := range []string{`by (pod)`, `by ()`, `without (pod)`} {
			add(fmt.Sprintf(`sum(%s)`, call(f, `{app="a"} | unwrap n `, `[5m]`, ` `+g)))
			add(fmt.Sprintf(`avg by (app) (%s)`, call(f, `{app="a"} | unwrap n `, `[5m]`, ` `+g)))
		}
	}
	// spellings of the unwrap stage and of the calls around it.
	for _, in := range []string{
		`{app="a"} |unwrap n [5m]`, `{app="a"} |  unwrap n [5m]`, `{app="a"} | unwrap  n [5m]`, `{app="a"} | unwrap n[5m]`, `{app="a"} | unwrap n |__error__="" [5m]`,
		"{app=\"a\"} |\nunwrap n [5m]", "{app=\"a\"} | unwrap\tn [5m]", `{app="a"}|unwrap n [5m]`, `{app="a"} | unwrap bytes( n ) [5m]`, `{app="a"} | unwrap bytes (n) [5m]`,
		`{app="a"} | unwrap  bytes(n) [5m]`, `{app="a"} | unwrap BYTES(n) [5m]`, `{app="a"} | unwrap duration_seconds(n)[5m]`, `{app="a"} | unwrap n | __error__="" | unwrap m [5m]`,
		`{app="a"} | unwrap n | unwrap m [5m]`, `{app="a"} | unwrap [5m]`, `{app="a"} | unwrap bytes() [5m]`, `{app="a"} | unwrap n |= "x" [5m]`,
		`{app="a"} | unwrap n != "x" [5m]`, `{app="a"} | unwrap n | json [5m]`, `{app="a"} | json | unwrap n [5m]`, `{app="a"} | logfmt | unwrap n [5m]`,
		`{app="a"} | unwrap n | drop a [5m]`, `{app="a"} | unwrap n | keep a [5m]`, `{app="a"} | unwrap n | line_format "x" [5m]`, `{app="a"} | unwrap n | label_format a=b [5m]`,
		`{app="a"} | drop a | unwrap n [5m]`, `{app="a"} | keep n | unwrap n [5m]`, `{app="a"} | line_format "{{.x}}" | unwrap n [5m]`, `{app="a"} | decolorize | unwrap n [5m]`,
		`{app="a"} | regexp "(?P<n>x)" | unwrap n [5m]`, `{app="a"} | pattern "<n> x" | unwrap n [5m]`, `{app="a"} | unpack | unwrap n [5m]`, `{app="a"} | json n="x" | unwrap n [5m]`,
	} {
		for _, f := range []string{"sum_over_time", "quantile_over_time", "last_over_time", "rate", "absent_over_time"} {
			add(call(f, in, ``, ``))
			add(fmt.Sprintf(`sum by (app) (%s)`, call(f, in, ``, ``)))
		}
	}
	// text the translator reads as syntax: a quoted unwrap, parser, stats pipe, clause or modifier, in a selector, a line
	// filter, or a label filter value.
	for _, v := range []string{
		`| unwrap x`, `| unwrap bytes(x)`, `| unwrap duration(y)`, `| json`, `|json`, `| logfmt`, `| pattern`, `| regexp`, `| unpack`, `| unpack_json`, `| stats by (x)`,
		`| stats `, `without (x)`, `a bool b`, `|||`, `a|||b`, `service_name`, `detected_level`, `__without__:sum:app`, `__rate_counter__(x)`, `| unwrap`, `|  json`, `a | jsonx`,
		`a | json_x`, `x |logfmt y`, `a|unwrap n `,
	} {
		// the same text in the queries that read no unwrapped value: the translator reads the parser it holds
		// into the series identity of a rate and of a count, min, max and avg.
		for _, f := range []string{"rate", "count_over_time", "absent_over_time", "bytes_rate"} {
			for _, g := range []string{``, `by (app)`, `without (app)`} {
				for _, op := range []string{"sum", "count", "avg"} {
					wrap(op, g, call(f, fmt.Sprintf(`{app="a"} |= %q `, v), `[5m]`, ``))
				}
			}
			add(call(f, fmt.Sprintf(`{app="a"} |= %q `, v), `[5m]`, ``))
			add(call(f, fmt.Sprintf(`{app="a"} |= %q | unwrap n `, v), `[5m]`, ``))
		}
		for _, f := range []string{"sum_over_time", "last_over_time", "quantile_over_time", "rate"} {
			add(call(f, fmt.Sprintf(`{app="a"} |= %q | unwrap n `, v), `[5m]`, ``))
			add(call(f, fmt.Sprintf(`{app="a"} |~ %q | unwrap n `, v), `[5m]`, ``))
			add(call(f, fmt.Sprintf(`{app=%q} | unwrap n `, v), `[5m]`, ``))
			// the translator reads these two names out of a label filter value (semantics/translator-reads-quoted-text-as-syntax).
			labelValue := v != "service_name" && v != "detected_level"
			if labelValue {
				add(call(f, fmt.Sprintf(`{app="a"} | unwrap n | s=%q `, v), `[5m]`, ``))
			}
			for _, g := range []string{``, `by (app)`, `without (app)`} {
				wrap("sum", g, call(f, fmt.Sprintf(`{app="a"} |= %q | unwrap n `, v), `[5m]`, ``))
				if labelValue {
					wrap("avg", g, call(f, fmt.Sprintf(`{app="a"} | unwrap n | s=%q `, v), `[5m]`, ``))
				}
			}
		}
	}
	// values the string translator sends in the client's spelling (a regexp line filter with an escape).
	for _, in := range []string{
		`{app="a"} |~ "\x41b"`, `{app="a"} !~ "\x41b"`, `{app="a"} |~ "\101b"`, `{app="\x41"}`, "{app=`a`}", `{app="a"} |= "\x41b"`, `{app="a"} | b="\x41"`, `{app="a\tb"}`,
	} {
		for _, f := range []string{"sum_over_time", "last_over_time", "rate"} {
			add(call(f, in+` | unwrap n `, `[5m]`, ``))
			wrap("sum", `by (app)`, call(f, in+` | unwrap n `, `[5m]`, ``))
		}
	}
	// the shapes that stay with the translator, and the ones around them.
	for _, q := range []string{
		`sum_over_time({app="a"}[5m])`, `quantile_over_time(0.9, {app="a"}[5m])`, `rate_counter({app="a"}[5m])`, `absent_over_time({app="a"} | unwrap n [5m])`,
		`count_over_time({app="a"} | unwrap n [5m])`, `bytes_over_time({app="a"} | unwrap n [5m])`, `bytes_rate({app="a"} | unwrap n [5m])`,
		`sum(absent_over_time({app="a"} | unwrap n [5m]))`, `sum by (app) (absent_over_time({app="a"} | unwrap n [5m]))`,
		`sum_over_time({app="a"} | unwrap n [5m]) > 5`, `sum(sum_over_time({app="a"} | unwrap n [5m])) / 2`, `topk(2, sum_over_time({app="a"} | unwrap n [5m]))`,
		`sum(sum by (app) (sum_over_time({app="a"} | unwrap n [5m])))`, `sum_over_time({app="a"} | unwrap n [5m] offset 1h)`, `sum_over_time({app="a"} | unwrap n [5m]) offset 1h`,
		`label_replace(sum_over_time({app="a"} | unwrap n [5m]), "x", "$1", "app", "(.*)")`, `max_over_time({app="a"} | unwrap n [5m] offset 5m) by (pod)`,
		`sum without (x) (max_over_time({x!="  "} | unwrap n [5m]))`, `count without (x) (sum_over_time({app="a"} |= "a  b" | unwrap n [5m]))`,
		`sum without (x) (rate({app="a"} |= "without(0)\xa2" | unwrap n [5m]))`, `sum by (x) (sum_over_time({x!="  "} | unwrap n [5m]))`,
		`sum(sum_over_time({app="a"} | unwrap n [5m])) by (app) by (env)`, `avg(avg_over_time({app="a"} | unwrap n [5m]) by (app)) by (env)`,
	} {
		add(q)
	}
	return out
}

// TestUnwrapGridShadow holds the lowering of range and vector aggregations over an unwrapped label to the string
// translator on the generated grid under every profile: 0 mismatches, the text byte for byte, and the rows each
// side answers are counted.
// conformance: architecture-syntax-package-layering
func TestUnwrapGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := unwrapGrid()
	rows, native, delegated, problems := labelFilterShadow(t, grid)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 40))
	}
	if native < rows/4 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
