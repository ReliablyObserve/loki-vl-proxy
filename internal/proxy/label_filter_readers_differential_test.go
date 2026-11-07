package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The readers of a label filter stage (the drilldown field-comparison strip,
// the profile's label filter conditions, the unwrap comparison and the
// line_format __error__ filter) read the text of the stage with regexps before
// the stage carried its parsed filter. These tests keep the old readers as
// reference implementations and compare them with the typed readers over every
// label filter shape and every label filter of the benchmark and semantics
// query sets.

var (
	oldLabelFilterCompareRE      = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=~|!~|!=|=|>=|<=|>|<)`)
	oldIPLabelFilterRE           = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*!?=\s*ip\(\s*"[^"]*"\s*\)\s*$`)
	oldNumericLabelFilterRE      = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:==|!=|>=|<=|>|<)\s*-?[0-9][0-9A-Za-z.]*\s*$`)
	oldUnwrapComparisonRE        = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(==|!=|>=|<=|>|<|=)\s*([^\s"'` + "`" + `(),]+)\s*$`)
	oldHasFieldComparisonRE      = hasFieldComparisonRE
	oldStripFieldComparisonStgRE = stripFieldComparisonStgRE
)

func oldHasFieldComparisonStages(query string) bool {
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		return oldHasFieldComparisonRE.MatchString(query)
	}
	for _, stage := range lq.Pipeline {
		lfs, ok := stage.(*logqlpkg.LabelFilterStage)
		if ok && oldLabelFilterCompareRE.MatchString(lfs.Raw) {
			return true
		}
	}
	return false
}

func oldStripFieldComparisonStages(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return query
	}
	lq, err := logqlpkg.ParseLogQuery(query)
	if err != nil {
		result := oldStripFieldComparisonStgRE.ReplaceAllString(query, " ")
		for strings.Contains(result, "  ") {
			result = strings.ReplaceAll(result, "  ", " ")
		}
		return strings.TrimSpace(result)
	}
	out := filterPipeline(lq, func(s logqlpkg.Stage) bool {
		lfs, ok := s.(*logqlpkg.LabelFilterStage)
		if ok && oldLabelFilterCompareRE.MatchString(lfs.Raw) {
			return false
		}
		return true
	})
	return out.String()
}

func oldLabelFilterConditions(raw string, before lineFieldExposure) []lineFieldFilter {
	if m := oldNumericLabelFilterRE.FindStringSubmatch(raw); m != nil {
		return []lineFieldFilter{{name: m[1], before: before}}
	}
	if m := oldIPLabelFilterRE.FindStringSubmatch(raw); m != nil {
		return []lineFieldFilter{{name: m[1], before: before}}
	}
	parsed, err := logqlpkg.ParseLogQuery("{" + raw + "}")
	if err != nil {
		return nil
	}
	conditions := make([]lineFieldFilter, 0, len(parsed.Selector.Matchers))
	for _, m := range parsed.Selector.Matchers {
		op := [...]string{"=", "!=", "=~", "!~"}[m.Op]
		condition, err := translator.NewLabelFilterCondition(m.Name, op, m.Value)
		if err != nil {
			return nil
		}
		conditions = append(conditions, lineFieldFilter{name: m.Name, matchesEmpty: condition.Matches(""), before: before})
	}
	return conditions
}

func oldUnwrapLabelFilterKeeps(stage *logqlpkg.LabelFilterStage, labels map[string]string) (keep, known bool) {
	if compiled, valid := compileOrderedJSONStage(stage); valid && compiled.filter != nil {
		return compiled.filter.Matches(labels[compiled.filter.Field]), true
	}
	m := oldUnwrapComparisonRE.FindStringSubmatch(stage.Raw)
	if m == nil {
		return false, false
	}
	conv, ok := unwrapComparisonConv(m[3])
	if !ok {
		return false, false
	}
	bound, _ := convertUnwrap(m[3], conv)
	raw, present := labels[m[1]]
	if !present {
		return false, true
	}
	value, err := convertUnwrap(raw, conv)
	if err != nil {
		return true, true
	}
	switch m[2] {
	case ">":
		return value > bound, true
	case ">=":
		return value >= bound, true
	case "<":
		return value < bound, true
	case "<=":
		return value <= bound, true
	case "!=":
		return value != bound, true
	default:
		return value == bound, true
	}
}

func oldIsErrorEmptyFilter(raw string) bool {
	return strings.Join(strings.Fields(raw), "") == errorLabel+`=""`
}

// labelFilterShapes is the text of label filter stages (what follows the `|`).
// Texts Loki rejects are in it too: a query that does not parse is skipped.
var labelFilterShapes = []string{
	// string comparisons, both quote styles, escapes, regex operators
	`level="error"`, `level!="debug"`, "level=`error`", `msg="a\"b"`, `msg="a\\b"`, `msg="é"`, `msg="\q"`,
	`msg=~"a.*"`, `msg!~"a.*"`, "msg=~`a\\d+`", `a=""`, `a!=""`, `a=~".*"`, `a!~".+"`, `a=~""`, `a!~""`, `a = "x"`, `a ="x"`, `a= "x"`,
	// the __error__ filters
	`__error__=""`, `__error__ = ""`, "__error__=``", `__error__!=""`, `__error__="x"`, `(__error__="")`, `__error__=~""`, `__error_details__=""`, `__error__= ""`,
	// names with dots, dashes, underscores, non-ASCII, colons
	`http.status>=500`, `http.status="200"`, `service.name="x"`, `a-b="x"`, `a-b>5`, `_x>5`, `_x="y"`, `A1_b="z"`, `日本="x"`, `日本>5`, `a:b="x"`, `a:b>5`, `a/b="y"`,
	`a.b-c=~"x"`, `detected_level="info"`, `detected_level=ip("1.2.3.4")`,
	// numbers, durations, bytes
	`status>=500`, `status > 500`, `status>500`, `status==500`, `status=500`, `status!=500`, `status<500`, `status<=500`, `status>-5`, `status>+5`, `status>.5`,
	`status>1.5`, `status>1e5`, `status>1e+5`, `status>0x10`, `status>5_0`, `latency>250ms`, `latency>1h30m`, `latency>=1.5s`, `latency=250ms`, `latency!=250ms`,
	`latency==250ms`, `latency<-1s`, `size>1KB`, `size>=1MiB`, `size!=10b`, `size<5GB`, `size=1kb`, `size==1kb`, `status=~"5.."`, `status>"5"`, `status>abc`, `status>5abc`,
	`status>5.5.5`, `status>1.`, `status<0`, `status>=-0`, `a.b>5`, `a.b>5ms`, `a-b>5`,
	// ip()
	`addr=ip("10.0.0.0/8")`, `addr!=ip("10.0.0.0/8")`, `addr = ip( "10.0.0.0/8" )`, `addr=IP("1.2.3.4")`, "addr=ip(`10.0.0.0/8`)", `addr=ip("a\"b")`, `addr=ip("")`,
	`addr!=ip(` + "`10.0.0.0/8`" + `)`, `addr!=IP("1.2.3.4")`, `addr!=ip ("1.2.3.4")`, `addr!=ip("zzz")`, `addr=IP("zzz")`, `addr=ip("zzz")`, `a="1", addr!=ip("1.1.1.1")`,
	`日本=ip("1.1.1.1")`, `a.b!=ip("1.1.1.1")`, `a.b=ip("zzz")`, `addr=ip("1.1.1.1"), b=~"x"`, `addr=ip("1.1.1.1-1.1.1.9")`, `addr!=ip("::1")`, `addr!=ip("a\"b")`,
	`(addr!=ip("1.1.1.1"))`, `a="1" and addr!=ip("1.1.1.1")`, `(a="1", addr!=ip("1.1.1.1"))`, `addr=ip("1.1.1.1") or a="1"`,
	`addr=~ip("x")`, `addr>ip("x")`, `a.b=ip("1.1.1.1")`, `addr==ip("1.1.1.1")`, `addr=ip ("1.1.1.1")`, `addr=ip("1.1.1.1") `,
	// compounds: comma, juxtaposition, and, or, parentheses
	`a="1", b="2"`, `a="1" b="2"`, `a="1" and b="2"`, `a="1" or b="2"`, `a="1", b="2", c="3"`, `a="1",b="2"`, `(a="1")`, `((a="1"))`, `(a="1", b="2")`,
	`(a="1" or b="2")`, `a="1", (b="2" or c="3")`, `(a="1" or b="2"), c="3"`, `a="1" or b="2", c="3"`, `a="1" and b=~"x" and c!="y"`, `a="1" AND b="2"`, `a="1" OR b="2"`,
	`a=~"x", b!~"y"`, `a="1", a="2"`, `a="1", b=""`, `a="", b=""`,
	`status>=500, level="error"`, `level="error", status>=500`, `status>=500 and latency>1s`, `status>=500, status<600`, `(status>=500)`, `((status>=500))`,
	`(status>=500, latency>1s)`, `status>=500 or status<100`, `addr=ip("1.1.1.1"), a="1"`, `a="1", addr=ip("1.1.1.1")`, `(addr=ip("1.1.1.1"))`,
	`(latency>250ms)`, `( latency>250ms )`, `(size>1KB)`, `(status=5)`, `a="1" status>5`, `status>5 a="1"`, `a.b="1", c.d="2"`, `a="1", b.c>5`,
	// not label filters Loki accepts
	`a=b`, `a=`, `="x"`, `a="1" and`, `(a="1"`, `a="unterminated`, `a=1abc`,
	// stages the parser keeps opaque
	`foo`, `foo "x" y=1`, `foo("x") y=1`, `foo.bar`, `foo (a=1)`, `foo "a=1"`, `foo 12`,
}

// labelFilterQueries is every query the label filter shapes make: the filter
// alone, before a parser, after a parser and after a line_format.
func labelFilterQueries() []string {
	var out []string
	for _, f := range labelFilterShapes {
		out = append(out,
			`{app="a"} | `+f,
			`{app="a"} | json | `+f,
			`{app="a"} | `+f+` | json`,
			`{app="a"} | line_format "x" | `+f,
			`{app="a"} | json | `+f+` | unwrap latency`,
			`sum(rate({app="a"} | json | `+f+` | unwrap latency [5m]))`,
			`sum(count_over_time({app="a"} | json | `+f+` [5m]))`,
		)
	}
	return out
}

// jsonStrings returns every string in the JSON document at path.
func jsonStrings(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch v := v.(type) {
		case string:
			out = append(out, v)
		case []any:
			for _, e := range v {
				walk(e)
			}
		case map[string]any:
			for _, e := range v {
				walk(e)
			}
		}
	}
	walk(doc)
	return out
}

// realLabelFilterQueries is every query of the benchmark shapes and the
// semantics matrix that has a pipe.
func realLabelFilterQueries(t *testing.T) []string {
	t.Helper()
	var out []string
	seen := map[string]bool{}
	for _, path := range []string{"../../bench/ab/shapes.json", "../../test/e2e-compat/query-semantics-matrix.json"} {
		for _, s := range jsonStrings(t, path) {
			if seen[s] || !strings.Contains(s, "{") || !strings.Contains(s, "|") {
				continue
			}
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// labelFilterStagesOf returns the label filter stages of a query's pipeline,
// metric wrapper stripped; nil when the query does not parse.
func labelFilterStagesOf(query string) []*logqlpkg.LabelFilterStage {
	lq, err := logqlpkg.ParseLogQuery(stripMetricWrapper(query))
	if err != nil {
		return nil
	}
	var out []*logqlpkg.LabelFilterStage
	for _, s := range lq.Pipeline {
		if lf, ok := s.(*logqlpkg.LabelFilterStage); ok {
			out = append(out, lf)
		}
	}
	return out
}

var labelFilterReadingLabels = []map[string]string{
	nil, {"status": "500"}, {"status": "abc"}, {"status": "5"}, {"status": ""}, {"latency": "250ms"}, {"latency": "2h"}, {"latency": "xyz"},
	{"size": "1KB"}, {"size": "2MiB"}, {"size": "10"}, {"a": "1", "status": "0"}, {"http.status": "500"},
}

// labelFilterReading is what every reader of a label filter stage makes of a
// query.
type labelFilterReading struct {
	Has      bool
	Strip    string
	Stages   []stageReading
	ErrEmpty []bool
}

type stageReading struct {
	Conds []lineFieldFilter
	Cmp   string
	Keeps [][2]bool
}

// readLabelFilters runs the readers of a label filter stage on a query: the
// typed ones, or the text ones kept above as the reference.
func readLabelFilters(query string, typed bool) (labelFilterReading, bool) {
	var r labelFilterReading
	stages := labelFilterStagesOf(query)
	if typed {
		r.Has, r.Strip = hasFieldComparisonStages(query), stripFieldComparisonStages(query)
	} else {
		r.Has, r.Strip = oldHasFieldComparisonStages(query), oldStripFieldComparisonStages(query)
	}
	for _, st := range stages {
		var sr stageReading
		var errEmpty bool
		if typed {
			sr.Conds = labelFilterConditions(st.Expr, lineFieldExposure{})
			label, cmp, literal, ok := unwrapComparison(st)
			sr.Cmp = fmt.Sprint(label, cmp, literal, ok)
			errEmpty = isErrorEmptyFilter(st.Expr)
		} else {
			sr.Conds = oldLabelFilterConditions(st.Raw, lineFieldExposure{})
			m := oldUnwrapComparisonRE.FindStringSubmatch(st.Raw)
			if m == nil {
				sr.Cmp = fmt.Sprint("", "", "", false)
			} else {
				sr.Cmp = fmt.Sprint(m[1], m[2], m[3], true)
			}
			errEmpty = oldIsErrorEmptyFilter(st.Raw)
		}
		if len(sr.Conds) == 0 {
			sr.Conds = nil
		}
		for _, labels := range labelFilterReadingLabels {
			var keep, known bool
			if typed {
				keep, known = unwrapLabelFilterKeeps(st, labels)
			} else {
				keep, known = oldUnwrapLabelFilterKeeps(st, labels)
			}
			sr.Keeps = append(sr.Keeps, [2]bool{keep, known})
		}
		r.Stages = append(r.Stages, sr)
		r.ErrEmpty = append(r.ErrEmpty, errEmpty)
	}
	return r, len(stages) > 0
}

// stripped is whether the strip reader dropped the stage of a one-stage query.
func (r labelFilterReading) stripped(query string) labelFilterReading {
	r.Strip = fmt.Sprint(r.Strip != query)
	return r
}

// canonicalLabelFilter writes the filter of expr plainly: every leaf as
// name, operator and double-quoted value, "and" chains joined by commas, an
// "or" under an "and" (and the reverse) in a group. ok is false for a leaf the
// scanner could not read.
func canonicalLabelFilter(e *logqlpkg.LabelFilterExpr, rename func(string) string) (string, bool) {
	if e.IsLeaf() {
		value := e.Value
		switch e.Kind {
		case logqlpkg.LabelFilterValueString:
			value = strconv.Quote(value)
		case logqlpkg.LabelFilterValueIP:
			value = "ip(" + strconv.Quote(value) + ")"
		case logqlpkg.LabelFilterValueNone:
			return "", false
		}
		return rename(e.Name) + e.Cmp + value, true
	}
	var parts []string
	for _, child := range []*logqlpkg.LabelFilterExpr{e.Left, e.Right} {
		text, ok := canonicalLabelFilter(child, rename)
		if !ok {
			return "", false
		}
		if !child.IsLeaf() && (child.Op != e.Op || e.Op == logqlpkg.LabelFilterOr && child == e.Right) {
			text = "(" + text + ")"
		}
		parts = append(parts, text)
	}
	if e.Op == logqlpkg.LabelFilterOr {
		return strings.Join(parts, " or "), true
	}
	return strings.Join(parts, ", "), true
}

func labelFilterLeaves(e *logqlpkg.LabelFilterExpr, out []*logqlpkg.LabelFilterExpr) []*logqlpkg.LabelFilterExpr {
	if e.IsLeaf() {
		return append(out, e)
	}
	return labelFilterLeaves(e.Right, labelFilterLeaves(e.Left, out))
}

func noSpaces(s string) string { return strings.Join(strings.Fields(s), "") }

// groupParens counts the parentheses of a label filter that are not the
// parentheses of an ip() value.
func groupParens(s string) int {
	s = strings.ToLower(noSpaces(s))
	return strings.Count(s, "(") - strings.Count(s, "ip(")
}

// plainNumericLiteral is a literal the old numeric reader accepted: -?[0-9][0-9A-Za-z.]*.
func plainNumericLiteral(v string) bool {
	v = strings.TrimPrefix(v, "-")
	if v == "" || v[0] < '0' || v[0] > '9' {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '.' {
			return false
		}
	}
	return true
}

// A stage's classes, read off its parsed filter and its text.
const (
	classGroup      = "group"      // a parenthesized group the tree does not keep
	classAnd        = "and"        // an "and" or a juxtaposition, the tree of a comma
	classBackquote  = "backquote"  // a back-quoted empty value, the tree of ""
	classNumeric    = "numeric"    // a literal the old reader misread (spaces the scanner added, a sign, a leading dot)
	classNonASCII   = "non-ascii"  // a label name the old reader could not read
	classOpaque     = "opaque"     // a stage the parser keeps opaque: no filter
	classUnreadable = "unreadable" // a string the scanner cannot unescape
	classPlainSpelt = "plain"      // none of the above
)

func labelFilterClasses(st *logqlpkg.LabelFilterStage) (classes []string, canonical string) {
	e := st.Expr
	if e == nil {
		return []string{classOpaque}, ""
	}
	canonical, ok := canonicalLabelFilter(e, func(n string) string { return n })
	if !ok {
		return []string{classUnreadable}, ""
	}
	leaves := labelFilterLeaves(e, nil)
	for _, l := range leaves {
		for _, r := range l.Name {
			if r > 127 {
				classes = append(classes, classNonASCII)
				break
			}
		}
		switch l.Kind {
		case logqlpkg.LabelFilterValueNumber, logqlpkg.LabelFilterValueDuration, logqlpkg.LabelFilterValueBytes:
			if !plainNumericLiteral(l.Value) || !strings.Contains(st.Raw, l.Value) {
				classes = append(classes, classNumeric)
			}
		case logqlpkg.LabelFilterValueString:
			if l.Value == "" && strings.Contains(st.Raw, "``") {
				classes = append(classes, classBackquote)
			}
		}
	}
	if groupParens(st.Raw) > groupParens(canonical) {
		classes = append(classes, classGroup)
	}
	if strings.Count(st.Raw, ",") < strings.Count(canonical, ",") {
		classes = append(classes, classAnd)
	}
	if len(classes) == 0 {
		classes = []string{classPlainSpelt}
	}
	return classes, canonical
}

// The typed readers of a label filter stage (the drilldown field-comparison
// strip, the profile's label filter conditions, the unwrap comparison and the
// line_format __error__ filter) read what the text readers read, except for
// the spellings Loki reads as the same filter:
//   - a stage the parser keeps opaque, and every other spelling, read the same;
//   - a group, an "and" or a juxtaposition, and a back-quoted empty value read
//     like the filter written plainly, and the plain filter reads as before;
//   - a literal the old reader misread is read as written (see
//     TestLabelFilterReadersNumericLiterals);
//   - a non-ASCII label name is detected like an ASCII one.
//
// conformance: profiles/stage-field-exposure, semantics/unwrap-conversion-error, semantics/detected-fields-empty-answer, semantics/line-format-template-error-keeps-entry
func TestLabelFilterReadersMatchTextReaders(t *testing.T) {
	classCount := map[string]int{}
	check := func(t *testing.T, query string) {
		t.Helper()
		stages := labelFilterStagesOf(query)
		if len(stages) == 0 {
			return
		}
		oldR, _ := readLabelFilters(query, false)
		newR, _ := readLabelFilters(query, true)
		skipped := false
		for _, st := range stages {
			classes, canonical := labelFilterClasses(st)
			for _, c := range classes {
				classCount[c]++
			}
			if len(classes) == 1 && (classes[0] == classPlainSpelt || classes[0] == classOpaque || classes[0] == classUnreadable) {
				continue
			}
			skipped = true
			if len(stages) > 1 {
				// Only the one-stage queries of the shapes carry these classes.
				t.Errorf("%q: stage %q is %v in a query of several stages", query, st.Raw, classes)
				continue
			}
			twin := `{app="a"} | json | ` + canonical
			if slices.Contains(classes, classNonASCII) {
				// (d) detected like an ASCII name: the same readers' results as the
				// stage with the name written in ASCII.
				ascii, _ := canonicalLabelFilter(st.Expr, func(string) string { return "zz" })
				asciiQuery := `{app="a"} | json | ` + ascii
				asciiOld, _ := readLabelFilters(asciiQuery, false)
				if newR.Has != asciiOld.Has || (newR.Strip != query) != (asciiOld.Strip != asciiQuery) {
					t.Errorf("%q: a non-ASCII name is not detected like an ASCII one: has=%v stripped=%v, ASCII twin has=%v stripped=%v", query, newR.Has, newR.Strip != query, asciiOld.Has, asciiOld.Strip != asciiQuery)
				}
				continue
			}
			if slices.Contains(classes, classNumeric) {
				// (c) is asserted by TestLabelFilterReadersNumericLiterals.
				continue
			}
			// (b) the stage reads like the filter written plainly, which reads as before.
			twinNew, ok1 := readLabelFilters(twin, true)
			twinOld, ok2 := readLabelFilters(twin, false)
			if !ok1 || !ok2 {
				t.Errorf("%q: canonical spelling %q does not parse", query, canonical)
				continue
			}
			if slices.Contains(classes, classGroup) && st.Expr.IsLeaf() && st.Expr.Kind != logqlpkg.LabelFilterValueNumber {
				// The unwrap evaluator compiles a string or ip() leaf unless it is
				// written in a group (compileOrderedJSONLabelFilter, unchanged).
				for i := range twinNew.Stages {
					twinNew.Stages[i].Keeps = nil
				}
				for i := range newR.Stages {
					newR.Stages[i].Keeps = nil
				}
				for i := range twinOld.Stages {
					twinOld.Stages[i].Keeps = nil
				}
			}
			if !reflect.DeepEqual(newR.stripped(query), twinNew.stripped(twin)) {
				t.Errorf("%q (%v): typed reading differs from the typed reading of %q:\n got %+v\nwant %+v", query, classes, canonical, newR.stripped(query), twinNew.stripped(twin))
			}
			if !reflect.DeepEqual(twinNew, twinOld) {
				t.Errorf("%q: typed reading of the canonical spelling differs from the text reading:\n got %+v\nwant %+v", twin, twinNew, twinOld)
			}
		}
		// (a) every stage outside the classes reads as before.
		if !skipped && !reflect.DeepEqual(oldR, newR) {
			t.Errorf("%q: typed reading differs from the text reading:\n got %+v\nwant %+v", query, newR, oldR)
		}
	}
	t.Run("shapes", func(t *testing.T) {
		for _, q := range labelFilterQueries() {
			check(t, q)
		}
	})
	real := realLabelFilterQueries(t)
	t.Run("real", func(t *testing.T) {
		before := map[string]int{}
		for k, v := range classCount {
			before[k] = v
		}
		n := 0
		for _, q := range real {
			if len(labelFilterStagesOf(q)) > 0 {
				n++
			}
			check(t, q)
		}
		// Every real label filter is outside the classes.
		for k, v := range classCount {
			if k != classPlainSpelt && v != before[k] {
				t.Errorf("real queries hold %d stages of class %q", v-before[k], k)
			}
		}
		t.Logf("%d real queries with a label filter, %d stages", n, classCount[classPlainSpelt]-before[classPlainSpelt])
	})
	t.Logf("stages by class: %v", classCount)
	for _, c := range []string{classGroup, classAnd, classBackquote, classNumeric, classNonASCII, classOpaque, classPlainSpelt} {
		if classCount[c] == 0 {
			t.Errorf("no stage of class %q: the table does not cover it", c)
		}
	}
}

// A numeric, duration or bytes literal is read as it is written, whatever the
// tokenizer did to the stage's text (`1h30m` became `1h 30m`), and the old
// reader declined a sign or a leading dot.
// conformance: semantics/unwrap-conversion-error, profiles/stage-field-exposure
func TestLabelFilterReadersNumericLiterals(t *testing.T) {
	for _, tc := range []struct {
		text, literal, conv string
		convOK              bool
	}{
		{`latency>1h30m`, "1h30m", "duration", true},
		{`latency>=1.5s`, "1.5s", "duration", true},
		{`status>1e5`, "1e5", "", true},
		{`status>0x10`, "0x10", "", false},
		{`status>+5`, "+5", "", true},
		{`status>.5`, ".5", "", true},
		{`status>1e+5`, "1e+5", "", true},
		{`size>1KB`, "1KB", "bytes", true},
		{`status>5_0`, "5_0", "", true},
	} {
		query := `{app="a"} | json | ` + tc.text + ` | unwrap latency`
		stages := labelFilterStagesOf(query)
		if len(stages) != 1 {
			t.Fatalf("%q: %d label filter stages", query, len(stages))
		}
		_, _, literal, ok := unwrapComparison(stages[0])
		if !ok || literal != tc.literal {
			t.Errorf("%q: literal %q ok=%v, want %q", tc.text, literal, ok, tc.literal)
			continue
		}
		if conv, convOK := unwrapComparisonConv(literal); conv != tc.conv || convOK != tc.convOK {
			t.Errorf("%q: conversion %q ok=%v, want %q ok=%v", tc.text, conv, convOK, tc.conv, tc.convOK)
		}
		conds := labelFilterConditions(stages[0].Expr, lineFieldExposure{})
		if len(conds) != 1 || conds[0].name != stages[0].Expr.Name {
			t.Errorf("%q: conditions %+v", tc.text, conds)
		}
	}
	// The kept value of a 1h30m bound.
	stages := labelFilterStagesOf(`{app="a"} | json | latency>1h30m | unwrap latency`)
	for value, want := range map[string]bool{"2h": true, "1h30m": false, "1h": false} {
		if keep, known := unwrapLabelFilterKeeps(stages[0], map[string]string{"latency": value}); !known || keep != want {
			t.Errorf("latency>1h30m on %q: keep=%v known=%v, want %v", value, keep, known, want)
		}
	}
}
