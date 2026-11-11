package querycorpus_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// detectedLevelGrid is the generated grid of label filters on detected_level, which the proxy derives on the read
// path: every operator against the level spellings Loki normalises (in any letter case), unknown, words no level is,
// the empty value in both quotings, escaped and raw strings, regexps (alternations, wildcards, case-insensitive,
// invalid), comparisons and ip(); the spacing and parentheses of a stage; several filters with line filters, label
// filters, pattern filters, service_name filters and presence filters between them; the filters before and after
// json, logfmt, regexp and pattern parsers, drops, keeps and formatting stages; and the same log queries under the
// range and vector aggregations, groupings by detected_level, level and service_name, a ranking, an unwrap, a binary
// operation and label_replace. Deterministic; the lowering and the string translator are held to each other on
// every row under every profile, byte for byte (TestDetectedLevelGridShadow).
func detectedLevelGrid() []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		if !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	values := []string{
		`"error"`, `"ERROR"`, `"Error"`, `"err"`, `"warn"`, `"WARN"`, `"warning"`, `"info"`, `"INFO"`, `"information"`,
		`"debug"`, `"Debug"`, `"trace"`, `"fatal"`, `"critical"`, `"unknown"`, `"UNKNOWN"`, `"Unknown"`, `"notice"`,
		`"x"`, `""`, "`error`", "``", "`UNKNOWN`", `"a\"b"`, `"\x65rror"`, `"error"`, `"error "`, `"é"`, `"5"`,
	}
	regexps := []string{
		`"error|warn"`, `"(?i)err.*"`, `".*"`, `".+"`, `"unknown|error"`, `"debu.*"`, `"("`, `"e.r"`, `"^error$"`,
		`"warn|"`, `"[ew].*"`, `"ERROR|warn"`, `"unknown"`, `"(?i)unknown"`, `""`, `"x"`, "`info|debug`", `"error\\|warn"`,
	}
	selectors := []string{`{app="a"}`, `{service_name="api"}`, `{app="a", env=~"prod|stg"}`, `{detected_level="info"}`, `{app!="a", env="x"}`}
	// every operator against every value.
	for _, s := range selectors[:2] {
		for _, op := range []string{"=", "==", "!="} {
			for _, v := range values {
				add(fmt.Sprintf(`%s | detected_level%s%s`, s, op, v))
			}
		}
		for _, op := range []string{"=~", "!~"} {
			for _, v := range append(append([]string{}, regexps...), values[:8]...) {
				add(fmt.Sprintf(`%s | detected_level%s%s`, s, op, v))
			}
		}
		for _, f := range []string{`>5`, `>=1`, `<"x"`, `<=1KB`, `=5`, `!=5s`, `=ip("10.0.0.0/8")`, `!=ip("10.0.0.1")`} {
			add(s + ` | detected_level` + f)
		}
	}
	// the spacing and parentheses of a stage, and names the translator reads as detected_level only once sanitized.
	for _, f := range []string{
		`detected_level = "error"`, `detected_level   !=   "info"`, `(detected_level="error")`, `((detected_level="error"))`,
		`( detected_level =~ "error|warn" )`, `detected_level=~"error|warn" `, `detected_level==""`, `(detected_level="")`,
		`detected.level="error"`, `detected-level="error"`, `Detected_level="error"`, `DETECTED_LEVEL="error"`,
		`detected_level="error" or detected_level="warn"`, `detected_level="error" and app="x"`, `detected_level="error", app="x"`,
		`app="x" or detected_level!="info"`, `(detected_level="error" or level="warn") and app="x"`, `detected_level="error" detected_level!="x"`,
	} {
		add(`{app="a"} |` + f)
		add(`{app="a"} | ` + f)
	}
	// the filters among other filters: line filters, a pattern filter that adds pipes, label filters, a service_name
	// filter, presence filters, the parser error drops, and several in a row.
	levels := []string{`detected_level="error"`, `detected_level!="info"`, `detected_level=~"error|warn"`, `detected_level!~"debug|trace"`, `detected_level="unknown"`, `detected_level=""`, `detected_level!=""`, `detected_level=~".*"`, `detected_level="x"`}
	others := []string{
		`|= "x"`, `!= "y"`, `|~ "z.+"`, `|> "<_> x <_> y"`, `!> "<_> a"`, `| app="b"`, `| status>=500`, `| level="error"`,
		`| service_name="api"`, `| drop __error__`, `| drop __error__, __error_details__`, `| drop __error__="x"`, `| a=~"x|y"`,
		`| k8s.pod.name="p"`, `| x=""`,
	}
	for _, s := range selectors {
		for _, l := range levels {
			add(s + ` | ` + l)
			for _, o := range others {
				add(s + ` ` + o + ` | ` + l)
				add(s + ` | ` + l + ` ` + o)
				add(s + ` | ` + l + ` ` + o + ` | ` + levels[0])
				add(s + ` ` + o + ` | ` + l + ` ` + o)
			}
			for _, m := range levels {
				add(s + ` | ` + l + ` | ` + m)
				add(s + ` | ` + l + ` |= "k" | ` + m + ` != "j"`)
			}
		}
	}
	// the filters before and after the stages that change labels or the line.
	stages := []string{
		`| json`, `| logfmt`, `| json | drop __error__`, `| logfmt | drop __error__, __error_details__`, `| json level="lvl"`,
		`| regexp "(?P<lvl>\\w+)"`, `| pattern "<lvl> <_>"`, `| drop level`, `| drop detected_level`, `| drop app`, `| keep level`,
		`| keep detected_level`, `| keep app`, `| line_format "{{.app}}"`, `| label_format lvl=level`, `| label_format detected_level=level`,
		`| decolorize`, `| json | logfmt | drop __error__, __error_details__`,
	}
	for _, l := range levels {
		for _, st := range stages {
			add(`{app="a"} | ` + l + ` ` + st)
			add(`{app="a"} ` + st + ` | ` + l)
			add(`{app="a"} | ` + l + ` ` + st + ` | ` + levels[1])
			add(`{app="a"} |= "x" | ` + l + ` ` + st + ` | app="b"`)
		}
	}
	// the log queries under the metric queries.
	inners := []string{
		`{app="a"} | detected_level="error"`, `{app="a"} | detected_level!="info"`, `{app="a"} | detected_level=~"error|warn"`,
		`{app="a"} | detected_level=~"unknown|error"`, `{app="a"} | detected_level=""`, `{app="a"} | detected_level!=""`,
		`{app="a"} | detected_level="unknown"`, `{app="a"} | detected_level=~".*"`, `{app="a"} | detected_level!~"debug|trace"`,
		`{service_name="api"} | detected_level="error"`, `{app="a"} |= "x" | detected_level="error"`,
		`{app="a"} | detected_level!="debug" | detected_level!="info"`, `{app="a"} | drop __error__ | detected_level="warn"`,
		`{app="a"} | detected_level="info" | json`, `{app="a"} | detected_level="info" | json | drop __error__`,
		`{app="a"} | detected_level="info" | logfmt | drop __error__, __error_details__`, `{app="a"} | json | detected_level="error"`,
		`{app="a"} | detected_level="error" | app="b"`, `{app="a"} | app="b" | detected_level="error"`,
		`{app="a"} | detected_level="error" | service_name="api"`, `{app="a"} | detected_level="error" |> "<_> x"`,
		`{app="a"} | detected_level=~"error|warn" | json | status>=500`, `{app="a"} | detected_level="error" | line_format "{{.app}}"`,
	}
	funcs := []string{`count_over_time(%s [5m])`, `rate(%s [5m])`, `bytes_over_time(%s [1m])`, `bytes_rate(%s [1m])`, `absent_over_time(%s [5m])`}
	aggs := []string{
		`%s`, `sum(%s)`, `sum by (detected_level) (%s)`, `sum by (level) (%s)`, `sum by (level, detected_level) (%s)`,
		`sum by (service_name) (%s)`, `sum by (app) (%s)`, `count(%s)`, `max by (detected_level) (%s)`, `sum without (pod) (%s)`,
		`topk(5, sum by (service_name) (%s))`, `sum by (status) (%s)`, `sum by (version) (%s)`,
	}
	for _, in := range inners {
		for _, f := range funcs {
			for _, a := range aggs {
				add(fmt.Sprintf(a, fmt.Sprintf(f, in)))
			}
		}
		add(`sum(count_over_time(` + in + ` [5m])) / sum(count_over_time({app="a"} [5m]))`)
		add(`sum by (detected_level) (rate(` + in + ` [5m])) > 0`)
		add(`label_replace(sum by (detected_level) (count_over_time(` + in + ` [5m])), "l", "$1", "detected_level", "(.*)")`)
		add(`sum by (app) (avg_over_time(` + in + ` | unwrap latency [5m]))`)
		add(`quantile_over_time(0.9, ` + in + ` | unwrap latency [5m])`)
		add(`sum(count_over_time(` + in + ` [5m] offset 1h))`)
	}
	return out
}

// TestDetectedLevelGridShadow holds the lowering of label filters on detected_level to the string translator on the
// generated grid under every profile, byte for byte: 0 mismatches, the rows each side answers counted, and every
// filter-only log query with one turned away by the flip's screen (gridShadow).
// conformance: architecture-syntax-package-layering
func TestDetectedLevelGridShadow(t *testing.T) {
	t.Cleanup(querycorpus.PauseRecording())
	grid := detectedLevelGrid()
	rows, native, delegated, problems := gridShadow(t, grid, true)
	var kinds []string
	for c, n := range delegated {
		kinds = append(kinds, fmt.Sprintf("%s=%d", c, n))
	}
	sort.Strings(kinds)
	t.Logf("%d queries x %d profiles: %d rows, %d lowered, %d delegated (%s)", len(grid), len(planProfiles()), rows, native, rows-native-len(problems), strings.Join(kinds, ", "))
	if len(problems) > 0 {
		t.Fatalf("%d rows lower unlike the string translator:\n%s", len(problems), joinFirst(problems, 40))
	}
	if native < rows/2 {
		t.Fatalf("only %d of %d rows lowered", native, rows)
	}
}
