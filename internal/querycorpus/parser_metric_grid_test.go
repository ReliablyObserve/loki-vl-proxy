package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// parserMetricGrid is the generated grid of range and vector aggregations over a log query with a json or logfmt
// parser: the parsers with their flags, extraction lists and aliases, in front of and behind each other, label
// filters after them (parsed keys with and without underscores, a stream label, the error labels), every
// function that reads rows (count, bytes, absent, rate, bytes_rate) and every function over an unwrapped label
// (sum, avg, min, max, first, last, stddev, stdvar, quantile, rate_counter, rate) with the conversions, grouped by
// labels the translator resolves by text (with and without an underscore, derived ones, a stream label) on the
// range and on the seven vector aggregations, with and without a clause. Deterministic; the lowering and the
// string translator are held to each other on every row under every profile (TestParserMetricGridShadow).
func parserMetricGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	call := func(f, body, rng, tail string) string {
		if f == "quantile_over_time" {
			return fmt.Sprintf(`%s(0.9, %s%s)%s`, f, body, rng, tail)
		}
		return fmt.Sprintf(`%s(%s%s)%s`, f, body, rng, tail)
	}
	wrap := func(op, g, in string) {
		if g == "" {
			add(fmt.Sprintf(`%s(%s)`, op, in))
			return
		}
		add(fmt.Sprintf(`%s %s (%s)`, op, g, in))
		add(fmt.Sprintf(`%s(%s) %s`, op, in, g))
	}
	parsers := []string{
		` | json`, ` | logfmt`, ` | json a, b`, ` | json a="x.y", b="z"`, ` | json a="x", b_c="y"`, ` | logfmt --strict`, ` | logfmt --keep-empty a, b="k"`,
		` | logfmt a, b_c`, ` | json | logfmt`, ` | logfmt | json`, ` | json | json`, ` | json --strict`, ` | json a="x.y" | logfmt`, ` | logfmt n="k"`,
	}
	after := []string{
		``, ` | status>=500`, ` | status="200"`, ` | app="a"`, ` | level="error"`, ` | a_b="x"`, ` | __error__=""`, ` | x!=""`, ` | status=~"5.." | method="GET"`,
		` | duration>1s`, ` | detected_level="error"`, ` | service_name="s"`, ` | a_extracted="x"`, ` | k8s_pod_name=~"p.*"`,
	}
	rowFuncs := []string{"count_over_time", "bytes_over_time", "absent_over_time", "rate", "bytes_rate"}
	unwrapFuncs := []string{"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time", "stddev_over_time",
		"stdvar_over_time", "quantile_over_time", "rate", "rate_counter"}
	selectors := []string{`{app="a"}`, `{service_name="s"}`, `{app="a", env=~"p.*"}`, `{detected_level="error"}`}
	ops := []string{"sum", "count", "min", "max", "avg", "stddev", "stdvar"}
	groupings := []string{
		``, `by ()`, `by (a)`, `by (a, b)`, `by (a_b)`, `by (a, a_b)`, `by (level)`, `by (detected_level)`, `by (service_name)`, `by (app)`, `by (k8s_pod_name)`,
		`by (k8s.pod.name)`, `by (_stream)`, `by (__error__)`, `by (a_extracted)`, `without (a)`, `without (a_b)`, `without (app)`, `without ()`,
	}
	// every parser x label filter x row function, alone and under the aggregations.
	for _, p := range parsers {
		for _, a := range after {
			for _, f := range rowFuncs[:4] {
				add(call(f, `{app="a"}`+p+a+` `, `[5m]`, ``))
			}
			for _, g := range []string{``, `by (a)`} {
				wrap("sum", g, call("rate", `{app="a"}`+p+a+` `, `[5m]`, ``))
				wrap("count", g, call("count_over_time", `{app="a"}`+p+a+` `, `[5m]`, ``))
			}
		}
	}
	// every selector x row function x grouping x aggregation, on two parsers.
	for _, s := range selectors[:2] {
		for _, p := range []string{` | json`, ` | logfmt a, b`} {
			for _, f := range []string{"count_over_time", "absent_over_time", "rate", "bytes_rate"} {
				for _, op := range ops {
					for _, g := range groupings {
						wrap(op, g, call(f, s+p+` `, `[5m]`, ``))
					}
				}
			}
		}
	}
	// unwrap after the parser: every function x conversion x unwrapped label, alone and with a range clause.
	labels := []string{`n`, `amount`, `a_b`, `duration_ms`, `app`, `level`, `_x`, `service_name`, `detected_level`, `status`, `k8s_x`, `Dur`}
	convs := []string{`%s`, `bytes(%s)`, `duration(%s)`, `duration_seconds(%s)`}
	for _, p := range parsers {
		for _, f := range unwrapFuncs {
			for _, l := range labels[:4] {
				add(call(f, fmt.Sprintf(`{app="a"}%s | unwrap %s `, p, l), `[5m]`, ``))
			}
		}
	}
	for _, f := range unwrapFuncs {
		for _, l := range labels {
			for _, cv := range convs {
				add(call(f, fmt.Sprintf(`{app="a"} | json | unwrap `+cv+` `, l), `[5m]`, ``))
				add(call(f, fmt.Sprintf(`{app="a"} | logfmt | unwrap `+cv+` `, l), `[5m]`, ``))
			}
		}
		for _, a := range after {
			add(call(f, `{app="a"} | json`+a+` | unwrap n `, `[5m]`, ``))
			add(call(f, `{app="a"} | logfmt`+a+` | unwrap n | __error__="" `, `[5m]`, ``))
			add(call(f, `{app="a"} | logfmt | unwrap n`+a+` `, `[5m]`, ``))
		}
	}
	// range-level groupings over unwrap (the functions Loki takes one on), and the vector aggregations around.
	for _, f := range []string{"avg_over_time", "min_over_time", "max_over_time", "first_over_time", "last_over_time", "stddev_over_time", "stdvar_over_time", "quantile_over_time"} {
		for _, g := range []string{`by (a)`, `by ()`, `by (a, b)`, `by (a_b)`, `by (level)`, `by (service_name)`, `by (detected_level)`, `by (_stream)`, `by (n)`, `without (a)`, `by (app)`} {
			for _, p := range []string{` | json`, ` | logfmt`, ` | logfmt a, n`, ` | json | logfmt`, ` | logfmt --strict`} {
				add(call(f, `{app="a"}`+p+` | unwrap n `, `[5m]`, ` `+g))
				add(call(f, `{app="a"}`+p+` | unwrap amount | __error__="" `, `[5m]`, ` `+g))
			}
		}
	}
	for _, op := range ops {
		for _, g := range groupings {
			for _, f := range []string{"sum_over_time", "stddev_over_time", "stdvar_over_time", "last_over_time", "quantile_over_time", "rate", "rate_counter"} {
				wrap(op, g, call(f, `{app="a"} | json | unwrap n `, `[5m]`, ``))
				if g == `by (a)` || g == `` || g == `without (a)` || g == `by (a_b)` {
					wrap(op, g, call(f, `{app="a"} | logfmt | unwrap amount `, `[5m]`, ``))
				}
			}
		}
	}
	for _, op := range []string{"sum", "count", "avg"} {
		for _, p := range parsers {
			for _, f := range []string{"sum_over_time", "last_over_time", "quantile_over_time", "rate"} {
				for _, g := range []string{``, `by (a)`} {
					wrap(op, g, call(f, `{app="a"}`+p+` | unwrap n `, `[5m]`, ``))
				}
			}
		}
	}
	// ranges and offsets.
	for _, r := range []string{`[1m]`, `[1h] offset 1h`, `[30s] offset 0s`, `[0s]`, `[1.5h]`, `[2w]`} {
		for _, f := range rowFuncs {
			add(call(f, `{app="a"} | json `, r, ``))
			wrap("sum", `by (a)`, call(f, `{app="a"} | logfmt `, r, ``))
			wrap("count", ``, call(f, `{app="a"} | json | s="x" `, r, ``))
		}
		for _, f := range unwrapFuncs {
			add(call(f, `{app="a"} | json | unwrap n `, r, ``))
			wrap("avg", `by (a)`, call(f, `{app="a"} | logfmt | unwrap n `, r, ``))
		}
	}
	// text the translator reads as syntax, behind a parser.
	for _, v := range []string{`| unwrap x`, `| json`, `| copy x`, `| unpack_json`, `| stats by (x)`, `without (x)`, `a bool b`, `|||`, `| unpack_logfmt`, `as n`, `as __lvp_rate`} {
		for _, f := range []string{"count_over_time", "rate", "sum_over_time", "last_over_time"} {
			body := fmt.Sprintf(`{app="a"} | json |= %q `, v)
			if strings.HasSuffix(f, "sum_over_time") || f == "last_over_time" {
				body += `| unwrap n `
			}
			add(call(f, body, `[5m]`, ``))
			for _, g := range []string{``, `by (a)`, `without (a)`} {
				wrap("sum", g, call(f, body, `[5m]`, ``))
				wrap("count", g, call(f, body, `[5m]`, ``))
			}
			add(call(f, fmt.Sprintf(`{app="a"} | logfmt | s=%q `, v)+map[bool]string{true: `| unwrap n `, false: ``}[f != "count_over_time" && f != "rate"], `[5m]`, ``))
		}
	}
	// the regexp, pattern and unpack parsers and the stages around a json parser, which stay with the translator.
	for _, st := range []string{` | regexp "(?P<a>x)"`, ` | pattern "<a> <b>"`, ` | unpack`, ` | json | drop a`, ` | json | keep a`, ` | json | line_format "x"`, ` | json | label_format a=b`, ` | json | decolorize`} {
		for _, f := range []string{"count_over_time", "rate"} {
			add(call(f, `{app="a"}`+st+` `, `[5m]`, ``))
			wrap("sum", `by (a)`, call(f, `{app="a"}`+st+` `, `[5m]`, ``))
		}
		add(call("sum_over_time", `{app="a"}`+st+` | unwrap n `, `[5m]`, ``))
	}
	return out
}

// TestParserMetricGridShadow holds the lowering of range and vector aggregations over a log query with a json or
// logfmt parser to the string translator on the generated grid under every profile: 0 mismatches, the text byte
// for byte, and the rows each side answers are counted.
// conformance: architecture-syntax-package-layering
func TestParserMetricGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := parserMetricGrid()
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
