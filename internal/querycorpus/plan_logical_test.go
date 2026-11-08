package querycorpus_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// planPath holds the plan ratchets, one section each. This file reads and writes only its `logical` section and
// leaves the others as they are: LVP_QUERY_PLAN_UPDATE=1 rewrites it.
const planPath = "../../conformance/registry/query-plan.json"

const planDescription = "Ratchets of the query plan (internal/queryplan), one section per gate; a count may only fall, and LVP_QUERY_PLAN_UPDATE=1 rewrites the section of the test that ran. `logical` is the logical plan built from every query of the plan corpus (the curated sources and the capture sample): how many rows parse and build (all of them), and the spots the plan still holds as text, per construct (typing them lowers the count). `logical_key_sound` is, per profile, how the logical keys group the corpus and what the string translator wrote for the queries of a group: no group may differ in structure."

// readPlanFile is the sections of the ratchet file; a missing file has none.
func readPlanFile() (map[string]json.RawMessage, error) {
	file := map[string]json.RawMessage{}
	b, err := os.ReadFile(planPath)
	if err == nil {
		err = json.Unmarshal(b, &file)
	}
	return file, err
}

// writePlanSection rewrites one section of the ratchet file and leaves the others as they are.
func writePlanSection(t *testing.T, file map[string]json.RawMessage, name string, v any) {
	t.Helper()
	if _, ok := file["description"]; !ok {
		file["description"], _ = json.Marshal(planDescription)
	}
	file[name], _ = json.Marshal(v)
	out, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, append(out, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

type logicalSection struct {
	Rows    int            `json:"rows"`
	Parsed  int            `json:"parsed"`
	Built   int            `json:"built"`
	Untyped map[string]int `json:"untyped_by_construct"`
}

// planInputs is the logical input set a corpus row is built with: a metric query is a range query, the rest a log query.
func planInputs(e logql.Expr) queryplan.Inputs {
	if _, ok := e.(*logql.LogQuery); ok {
		return queryplan.Inputs{Kind: queryplan.KindLogs, StreamLabels: []string{"app", "namespace"}}
	}
	return queryplan.Inputs{Kind: queryplan.KindRange, StreamLabels: []string{"app", "namespace"}}
}

// walkPlan visits n and every node below it.
func walkPlan(n queryplan.Node, f func(queryplan.Node)) {
	f(n)
	switch x := n.(type) {
	case *queryplan.RangeAgg:
		f(x.In)
	case *queryplan.VectorAgg:
		walkPlan(x.In, f)
	case *queryplan.Rank:
		walkPlan(x.In, f)
	case *queryplan.BinOp:
		walkPlan(x.L, f)
		walkPlan(x.R, f)
	}
}

// countUntyped adds to c the spots of the plan that still hold text.
func countUntyped(root queryplan.Node, c map[string]int) {
	walkPlan(root, func(n queryplan.Node) {
		switch x := n.(type) {
		case *queryplan.Opaque:
			switch {
			case strings.HasPrefix(x.Raw, "label_replace"):
				c["opaque:label_replace"]++
			case strings.HasPrefix(x.Raw, "vector"):
				c["opaque:vector"]++
			case strings.HasPrefix(x.Raw, "approx_topk"):
				c["opaque:approx_topk"]++
			default:
				c["opaque:other"]++
			}
		case *queryplan.LogSelect:
			for _, s := range x.Stages {
				switch st := s.(type) {
				case *queryplan.LabelFilter:
					if st.Expr == nil {
						c["labelfilter_untyped"]++
					}
				case *queryplan.LineFormat:
					c["line_format_raw"]++
				case *queryplan.Parse:
					if (st.Type == logql.ParserJSON || st.Type == logql.ParserLogfmt) && st.Param != "" && len(st.Fields) == 0 {
						c["parser_param_raw"]++
					}
				}
			}
		}
	})
}

// TestLogicalBuildTotal: every corpus query logql.Parse accepts builds into a logical plan. The spots the plan holds
// as text are counted per construct in the `logical` section of query-plan.json; the counts may only fall.
func TestLogicalBuildTotal(t *testing.T) {
	got := logicalSection{Untyped: map[string]int{
		"opaque:label_replace": 0, "opaque:vector": 0, "opaque:approx_topk": 0, "opaque:other": 0,
		"labelfilter_untyped": 0, "line_format_raw": 0, "parser_param_raw": 0,
	}}
	for _, q := range planCorpus(t) {
		got.Rows++
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		got.Parsed++
		plan, err := queryplan.Build(expr, planInputs(expr))
		if err != nil {
			t.Errorf("Build(%q): %v", q, err)
			continue
		}
		got.Built++
		countUntyped(plan.Root, got.Untyped)
	}
	if got.Rows < 2000 {
		t.Fatalf("the corpus has %d rows, expected the curated sources and the capture sample", got.Rows)
	}
	if got.Built != got.Parsed {
		t.Errorf("built %d of %d parsed rows", got.Built, got.Parsed)
	}

	file, readErr := readPlanFile()
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		writePlanSection(t, file, "logical", got)
		t.Logf("wrote %s: %+v", planPath, got)
		return
	}
	if readErr != nil {
		t.Fatalf("read %s: %v", planPath, readErr)
	}
	var want logicalSection
	if err := json.Unmarshal(file["logical"], &want); err != nil {
		t.Fatalf("%s has no readable logical section: %v", planPath, err)
	}
	for name, have := range got.Untyped {
		switch limit := want.Untyped[name]; {
		case have > limit:
			t.Errorf("%s grew from %d to %d", name, limit, have)
		case have < limit:
			t.Errorf("%s fell from %d to %d: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestLogicalBuildTotal and commit %s",
				name, limit, have, strings.TrimPrefix(planPath, "../../"))
		}
	}
	t.Logf("rows %d, parsed %d, built %d, untyped %v", got.Rows, got.Parsed, got.Built, got.Untyped)
}

// TestLogicalKeyDistinct: queries Loki reads differently never share a key.
func TestLogicalKeyDistinct(t *testing.T) {
	r := `rate({app="a"}[5m])`
	pairs := [][2]string{
		{`sum by (a) (` + r + `)`, `sum without (a) (` + r + `)`},
		{`sum by () (` + r + `)`, `sum (` + r + `)`},
		{`sum without () (` + r + `)`, `sum (` + r + `)`},
		{`sum by () (` + r + `)`, `sum without () (` + r + `)`},
		{`sum by (a, b) (` + r + `)`, `sum by (b, a) (` + r + `)`},
		{`rate({app="a"}[5m] offset 0s)`, r},
		{`rate({app="a"}[5m] offset 1h)`, `rate({app="a"}[5m] offset 2h)`},
		{`rate({app="a"}[5m])`, `rate({app="a"}[10m])`},
		{`rate({app="a"}[5m])`, `count_over_time({app="a"}[5m])`},
		{`{app="a"} |= "x"`, `{app="a"} |~ "x"`},
		{`{app="a"} |= "x"`, `{app="a"} != "x"`},
		{`{app="a"} |= "x" or "y"`, `{app="a"} |= "y" or "x"`},
		{`{app="a"} |= "x" or "y"`, `{app="a"} |= "x" |= "y"`},
		{`{app="a"} |= ip("1.2.3.4")`, `{app="a"} |= "ip(1.2.3.4)"`},
		{`{app="a"} | json a, b`, `{app="a"} | json b, a`},
		{`{app="a"} | json a="x"`, `{app="a"} | json a="y"`},
		{`{app="a"} | json`, `{app="a"} | logfmt`},
		{`{app="a"} | json | logfmt`, `{app="a"} | logfmt | json`},
		{`{app="a"} | regexp "(?P<x>.)"`, `{app="a"} | pattern "(?P<x>.)"`},
		{`{app="a", b="c"}`, `{b="c", app="a"}`},
		{`{app="a"}`, `{app=~"a"}`},
		{`{app="a"} | drop x`, `{app="a"} | keep x`},
		{`{app="a"} | drop x, y`, `{app="a"} | drop y, x`},
		{`{app="a"} | drop x="y"`, `{app="a"} | drop x!="y"`},
		{`{app="a"} | x="1" or y="2"`, `{app="a"} | x="1" and y="2"`},
		{`{app="a"} | x>5`, `{app="a"} | x>=5`},
		{`{app="a"} | x=5`, `{app="a"} | x="5"`},
		{`{app="a"} | line_format "{{.a}}"`, `{app="a"} | line_format "{{.b}}"`},
		{`{app="a"} | label_format x=y`, `{app="a"} | label_format x="y"`},
		{`{app="a"} | decolorize`, `{app="a"}`},
		{`avg_over_time({app="a"} | unwrap n [5m])`, `avg_over_time({app="a"} | unwrap bytes(n) [5m])`},
		{`quantile_over_time(0.5, {app="a"} | unwrap n [5m])`, `quantile_over_time(0.9, {app="a"} | unwrap n [5m])`},
		{`sum by (x) (avg_over_time({app="a"} | unwrap n [5m]))`, `avg_over_time({app="a"} | unwrap n [5m]) by (x)`},
		{`topk(2, ` + r + `)`, `topk(3, ` + r + `)`},
		{`topk(2, ` + r + `)`, `bottomk(2, ` + r + `)`},
		{`topk(2, ` + r + `)`, `topk by (a) (2, ` + r + `)`},
		{`sort(` + r + `)`, `sort_desc(` + r + `)`},
		{r + ` > 1`, r + ` > bool 1`},
		{r + ` > 1`, r + ` < 1`},
		{r + ` / ` + r, r + ` / on (a) ` + r},
		{r + ` / on (a) ` + r, r + ` / ignoring (a) ` + r},
		{r + ` / on (a) group_left ` + r, r + ` / on (a) group_right ` + r},
		{r + ` / on (a) group_left (b) ` + r, r + ` / on (a) group_left (c) ` + r},
		{r + ` + 1`, `1 + ` + r},
		{r + ` + 1`, r + ` + 2`},
		{`label_replace(` + r + `, "a", "b", "c", "d")`, `label_replace(` + r + `, "a", "b", "c", "e")`},
		{`vector(1)`, `vector(2)`},
	}
	for _, p := range pairs {
		a, b := keyOf(t, p[0], queryplan.Inputs{}), keyOf(t, p[1], queryplan.Inputs{})
		if bytes.Equal(a, b) {
			t.Errorf("one key for %q and %q", p[0], p[1])
		}
	}

	// The same query under different logical inputs has different keys.
	base := queryplan.Inputs{Kind: queryplan.KindRange, StreamLabels: []string{"app"}}
	for _, in := range []queryplan.Inputs{
		{Kind: queryplan.KindInstant, StreamLabels: []string{"app"}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app", "env"}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app"}, Exposure: queryplan.Exposure{LokiProfile: true}},
		{Kind: queryplan.KindRange, StreamLabels: []string{"app"}, Exposure: queryplan.Exposure{DottedNames: true}},
	} {
		if bytes.Equal(keyOf(t, r, base), keyOf(t, r, in)) {
			t.Errorf("one key for inputs %+v and %+v", base, in)
		}
	}
}

func keyOf(t *testing.T, q string, in queryplan.Inputs) []byte {
	t.Helper()
	expr, err := logql.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	plan, err := queryplan.Build(expr, in)
	if err != nil {
		t.Fatalf("build %q: %v", q, err)
	}
	return plan.Key
}

// keySoundSection is, per profile, how the logical keys group the corpus.
type keySoundSection map[string]keySoundCounts

type keySoundCounts struct {
	Rows int `json:"rows"` // queries that parse
	Keys int `json:"keys"` // distinct plan keys
	// SharedTexts are the queries whose key an earlier query has too: the reuse the typed key already gives.
	SharedTexts int `json:"shared_texts"`
	// TextOnly are the queries whose translation differs from their group's first in text but not in structure.
	TextOnly int `json:"text_only"`
	// PhraseText are the queries whose translation and their group's first are both the query's own text as a
	// phrase filter (the translator's fallback for a query it leaves to the proxy), which differ as the texts do.
	PhraseText int `json:"phrase_text"`
	// SpellingDivergent are the queries one of whose spellings the translator translates and another it leaves
	// to the proxy as a phrase (parentheses, spacing): the translator reads the spelling, a logical key cannot.
	SpellingDivergent int `json:"spelling_divergent"`
	Unsound           int `json:"unsound"`
}

// translateOutcome is planProfile.translate with its error: the LogsQL (with markers) the proxy would hold for the
// query under the profile.
func (p planProfile) translateOutcome(q string) (string, error) {
	q = logql.CanonicalizeLogRanges(q)
	expr, err := logql.Parse(q)
	if err != nil {
		return "", err
	}
	if p.keepLine {
		if _, isLog := expr.(*logql.LogQuery); !isLog {
			return "", fmt.Errorf("not a log query")
		}
		return translator.TranslateLogQueryKeepingLine(q, p.labelFn, p.stream, p.caps)
	}
	return translator.TranslateLogQLWithCapabilities(q, p.labelFn, p.stream, p.caps)
}

// translation is what the string translator made of one query under one profile, and the structure it compares by.
type translation struct {
	text     string // the translator's output, or its error message
	canon    any    // the decoded operators with every query parsed; nil for an error
	unparsed bool   // a query of canon did not parse, so canon holds its text
	phrase   bool   // the translation is the query's own text as a phrase filter
}

func translateForKey(prof planProfile, q string) translation {
	out, err := prof.translateOutcome(q)
	if err != nil {
		return translation{text: err.Error()}
	}
	op, err := translator.DecodePhysical(out)
	if err != nil {
		return translation{text: "decode: " + err.Error()}
	}
	tr := translation{text: out, phrase: out == `"`+logql.CanonicalizeLogRanges(q)+`"`}
	tr.canon = canonOp(op, &tr.unparsed)
	return tr
}

// The structure of a decoded translation: the operators, with each VictoriaLogs query parsed. Only the operators
// the translator writes get a form; the text of a binary (Text, Tail) is not part of it, its parts are.
type (
	canonFold struct {
		Levels []physical.FoldLevel
		In     any
	}
	canonLabel struct {
		Replace []physical.LabelReplace
		In      any
	}
	canonBin struct {
		Op    string
		Bool  bool
		Match *physical.Matching
		L, R  any
	}
	canonScalar struct{ V float64 }
)

func canonOp(op physical.Op, unparsed *bool) any {
	switch v := op.(type) {
	case physical.Verbatim:
		return canonOp(v.In, unparsed)
	case physical.Fold:
		return canonFold{v.Levels, canonOp(v.In, unparsed)}
	case physical.LabelMap:
		return canonLabel{v.Replace, canonOp(v.In, unparsed)}
	case physical.Binary:
		return canonBin{v.Op, v.Bool, v.Match, canonOp(v.L, unparsed), canonOp(v.R, unparsed)}
	case physical.ScalarOp:
		return canonScalar{v.V}
	case physical.VLQuery:
		q, err := logsql.Parse(v.Text)
		if err != nil {
			*unparsed = true
			return strings.Join(strings.Fields(v.Text), " ")
		}
		foldDefaultFields(reflect.ValueOf(q))
		return q
	}
	return op
}

// foldDefaultFields sets the fields that default to _msg to their empty spelling, so `from _msg` reads like no from.
func foldDefaultFields(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			foldDefaultFields(v.Elem())
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			foldDefaultFields(v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			switch name := v.Type().Field(i).Name; {
			case f.Kind() == reflect.String && f.CanSet() && f.String() == "_msg" && (name == "From" || name == "Field" || name == "ResultField"):
				f.SetString("")
			default:
				foldDefaultFields(f)
			}
		}
	}
}

// firstDiff is the path of the first field in which a and b differ, "" when they are equal.
func firstDiff(a, b reflect.Value, path string) string {
	if !a.IsValid() || !b.IsValid() {
		if a.IsValid() == b.IsValid() {
			return ""
		}
		return path + " (missing)"
	}
	if a.Type() != b.Type() {
		return fmt.Sprintf("%s (%s vs %s)", path, a.Type(), b.Type())
	}
	if a.CanInterface() && b.CanInterface() && reflect.DeepEqual(a.Interface(), b.Interface()) {
		return ""
	}
	switch a.Kind() {
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return path + " (nil vs set)"
		}
		return firstDiff(a.Elem(), b.Elem(), path)
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if d := firstDiff(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name); d != "" {
				return d
			}
		}
	case reflect.Slice:
		if a.Len() != b.Len() {
			return fmt.Sprintf("%s (length %d vs %d)", path, a.Len(), b.Len())
		}
		for i := 0; i < a.Len(); i++ {
			if d := firstDiff(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i)); d != "" {
				return d
			}
		}
	}
	return path
}

// TestLogicalKeySound: queries with one logical key get one translation. For each of the 12 profiles, the queries
// are grouped by Plan.Key; the profile fixes the physical inputs (label style, stream fields, capabilities) and the
// logical inputs it has (the stream labels), so inside a group everything but the query text is equal. The string
// translator's output for the queries of a group must have the same structure: the decoded operators with each
// VictoriaLogs query parsed (the default spellings `from _msg` and `as _msg` read as omitted), compared field by
// field, or as whitespace-normalised text when a query does not parse. A group whose queries differ in structure
// has a key that merges what the translator keeps apart: the test fails with both texts and the first differing
// field. A group that differs only in text is counted as text_only. Two cases a key cannot tell apart are counted
// on their own: the translator's phrase fallback holds the query's own text (phrase_text), and it reads
// parentheses and spacing, so it translates one spelling of a query and leaves another to the proxy
// (spelling_divergent); both fall as the proxy evaluates the logical plan. The counts are the `logical_key_sound`
// section of query-plan.json; LVP_QUERY_PLAN_UPDATE=1 rewrites it.
func TestLogicalKeySound(t *testing.T) {
	type row struct {
		text string
		key  string
	}
	var rows []row
	for _, q := range planCorpus(t) {
		expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
		if err != nil {
			continue
		}
		plan, err := queryplan.Build(expr, planInputs(expr))
		if err != nil {
			t.Fatalf("Build(%q): %v", q, err)
		}
		rows = append(rows, row{q, string(plan.Key)})
	}
	if len(rows) < 2000 {
		t.Fatalf("%d parsed rows: the corpus did not load", len(rows))
	}
	groups := map[string][]int{}
	for i, r := range rows {
		groups[r.key] = append(groups[r.key], i)
	}
	profiles := planProfiles()
	if len(profiles) != 12 {
		t.Fatalf("%d profiles, want 12", len(profiles))
	}
	got := keySoundSection{}
	for _, prof := range profiles {
		c := keySoundCounts{Rows: len(rows), Keys: len(groups), SharedTexts: len(rows) - len(groups)}
		keys := make([]string, 0, len(groups))
		for k, members := range groups {
			if len(members) > 1 {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		reported := 0
		for _, k := range keys {
			members := groups[k]
			first := translateForKey(prof, rows[members[0]].text)
			for _, m := range members[1:] {
				other := translateForKey(prof, rows[m].text)
				diff := ""
				switch {
				case first.phrase && other.phrase:
					c.PhraseText++
					continue
				case first.phrase != other.phrase && first.canon != nil && other.canon != nil:
					c.SpellingDivergent++
					continue
				case first.canon == nil || other.canon == nil:
					// An error or an undecodable translation: the messages must read alike.
					if first.canon != nil || other.canon != nil {
						diff = "translated vs " + map[bool]string{true: "failed", false: "translated"}[first.canon != nil]
					} else if strings.Join(strings.Fields(first.text), " ") != strings.Join(strings.Fields(other.text), " ") {
						// Two messages: the same error about different text, not a different outcome.
						c.TextOnly++
						continue
					}
				case first.unparsed || other.unparsed:
					if d := firstDiff(reflect.ValueOf(first.canon), reflect.ValueOf(other.canon), "plan"); d != "" &&
						strings.Join(strings.Fields(first.text), " ") != strings.Join(strings.Fields(other.text), " ") {
						diff = d
					}
				default:
					diff = firstDiff(reflect.ValueOf(first.canon), reflect.ValueOf(other.canon), "plan")
				}
				switch {
				case diff != "":
					c.Unsound++
					if reported++; reported <= 5 {
						t.Errorf("%s: one logical key, different translations (%s)\n  %q\n  %q\n  -> %q\n  -> %q",
							prof.name, diff, rows[members[0]].text, rows[m].text, first.text, other.text)
					}
				case first.text != other.text:
					c.TextOnly++
				}
			}
		}
		got[prof.name] = c
		t.Logf("%-46s %+v", prof.name, c)
	}

	file, readErr := readPlanFile()
	if os.Getenv("LVP_QUERY_PLAN_UPDATE") == "1" {
		writePlanSection(t, file, "logical_key_sound", got)
		t.Logf("wrote %s", planPath)
		return
	}
	if readErr != nil {
		t.Fatalf("read %s: %v", planPath, readErr)
	}
	var want keySoundSection
	if err := json.Unmarshal(file["logical_key_sound"], &want); err != nil {
		t.Fatalf("%s has no readable logical_key_sound section: %v", planPath, err)
	}
	for name, c := range got {
		if w := want[name]; c != w {
			t.Errorf("%s: %+v, the committed section has %+v: run LVP_QUERY_PLAN_UPDATE=1 go test ./internal/querycorpus -run TestLogicalKeySound and commit %s",
				name, c, w, strings.TrimPrefix(planPath, "../../"))
		}
	}
}
