package proxy

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestMixedJSONRowClassRejectsUnprovedLabels(t *testing.T) {
	for _, label := range []string{"", "_hidden", "__error__", "service_name", "detected_level", "level", "k_extracted", "user.id", "k\" | limit 1", "1key", "k_", "user__id", "___", strings.Repeat("k", 129)} {
		if got, ok := newMixedJSONRowClass(label); ok || got != nil {
			t.Errorf("accepted unproved label %q", label)
		}
	}
	for _, label := range []string{"k", "user_id", "batch_id", "trace_id"} {
		if got, ok := newMixedJSONRowClass(label); !ok || got == nil {
			t.Errorf("rejected ordinary own-key label %q", label)
		}
	}
}

func TestMixedJSONRowClassPreservesOriginalBeforeScratchChanges(t *testing.T) {
	c, ok := newMixedJSONRowClass("user_id")
	if !ok || len(c.prepare) == 0 {
		t.Fatal("class missing preparation")
	}
	pack, ok := c.prepare[0].(logsql.PipePackJSON)
	if !ok || len(pack.Fields) != 0 || pack.ResultField != c.originalField {
		t.Fatalf("first pipe must preserve ALL original fields: %#v", c.prepare[0])
	}
	for _, pipe := range c.rawPipes() {
		if strings.HasPrefix(pipe.String(), "| delete ") || strings.HasPrefix(pipe.String(), "| fields ") {
			t.Fatalf("complement discarded original source: %s", pipe.String())
		}
	}
	// Actual pack/restore semantics, including original scratch collisions and
	// empty stream labels, require the real VL envelope regression. A Go JSON
	// marshal roundtrip alone would only test the standard library here.
}

func TestMixedJSONRowClassComplementAndSliceOwnership(t *testing.T) {
	c, _ := newMixedJSONRowClass("k")
	native, raw := c.nativePipes(), c.rawPipes()
	positive, ok := native[len(native)-1].(logsql.PipeFilter)
	if !ok || !reflect.DeepEqual(positive.Expr, c.safe) {
		t.Fatal("native path does not carry complete static predicate")
	}
	negative, ok := raw[len(raw)-1].(logsql.PipeFilter)
	if !ok {
		t.Fatal("raw complement missing filter")
	}
	not, ok := negative.Expr.(logsql.NotExpr)
	if !ok || !reflect.DeepEqual(not.Expr, c.safe) {
		t.Fatalf("raw predicate is not exact typed negation: %#v", negative.Expr)
	}
	want := native[len(native)-1].String()
	raw = append(raw, logsql.PipeLimit{N: 7})
	raw[0] = logsql.PipeLimit{N: 1}
	if c.prepare[0].String() == raw[0].String() || native[len(native)-1].String() != want {
		t.Fatal("returned pipeline slices share writable backing arrays")
	}
	if len(c.nativePipes()) != len(c.prepare)+1 {
		t.Fatal("caller append changed stored preparation")
	}
	for i, pipe := range native {
		switch node := pipe.(type) {
		case logsql.PipeUnpackJSON:
			node.Fields[0] = "mutated"
			if c.prepare[i].(logsql.PipeUnpackJSON).Fields[0] == "mutated" {
				t.Fatal("returned unpack Fields aliases stored preparation")
			}
		case logsql.PipeCopy:
			node.Pairs[0][1] = "mutated"
			if c.prepare[i].(logsql.PipeCopy).Pairs[0][1] == "mutated" {
				t.Fatal("returned copy Pairs aliases stored preparation")
			}
		}
	}
}

func TestMixedJSONRowClassRejectsNativeNumericStringNormalization(t *testing.T) {
	c, _ := newMixedJSONRowClass("k")
	filters := mixedClassFieldFilters(t, c.safe)
	for _, value := range []string{"-0", "1.00", "1e3", "+1", ".1", "NaN", "nan", "Inf", "Infinity", "-Inf", "true", "FALSE", "NULL", "InFiNiTy"} {
		accepted := true
		for _, filter := range filters {
			if filter.Field == mixedClassTargetPrefix+"k" && filter.Op == logsql.FieldOpRegexp {
				if regexp.MustCompile(filter.Value).MatchString(value) == filter.Negate {
					accepted = false
				}
			}
		}
		if accepted {
			t.Errorf("native normalization risk %q was admitted", value)
		}
	}
}

func TestMixedJSONRowClassFilterTreesDoNotAlias(t *testing.T) {
	for _, branch := range []string{"native", "raw"} {
		t.Run(branch, func(t *testing.T) {
			c, _ := newMixedJSONRowClass("k")
			before := c.safe.String()
			other := c.rawPipes()
			otherBefore := other[len(other)-1].String()
			pipes := c.nativePipes()
			if branch == "raw" {
				pipes = c.rawPipes()
			}
			expr := pipes[len(pipes)-1].(logsql.PipeFilter).Expr
			if not, ok := expr.(logsql.NotExpr); ok {
				expr = not.Expr
			}
			expr.(logsql.AllOf)[0] = logsql.FieldFilter{Field: "mutated", Op: logsql.FieldOpAny}
			if c.safe.String() != before || other[len(other)-1].String() != otherBefore {
				t.Fatal("caller mutation changed stored or independently returned predicate")
			}
			if c.nativePipes()[len(c.prepare)].String() != "| filter "+before {
				t.Fatal("caller mutation changed future native predicate")
			}
		})
	}
	// A NOT node is a value, but its child can own a mutable AllOf slice.
	c, _ := newMixedJSONRowClass("k")
	c.safe = logsql.AllOf{logsql.NotExpr{Expr: logsql.AllOf{logsql.FieldFilter{Field: "original", Op: logsql.FieldOpAny}}}}
	before := c.safe.String()
	pipes := c.nativePipes()
	outer := pipes[len(pipes)-1].(logsql.PipeFilter).Expr.(logsql.AllOf)
	inner := outer[0].(logsql.NotExpr).Expr.(logsql.AllOf)
	inner[0] = logsql.FieldFilter{Field: "mutated", Op: logsql.FieldOpAny}
	if c.safe.String() != before {
		t.Fatal("nested NOT child aliases stored predicate")
	}
}

// Indices describe the regression assertions only; production uses explicit
// calls at each guard and does not depend on positional mappings.
func mixedClassLiteralPrefilters(label string) []struct {
	index    int
	literals []string
} {
	return []struct {
		index    int
		literals []string
	}{
		{18, []string{"__", `_":`}},
		{19, []string{`"` + label + `":`}},
		{20, []string{"__vlmc_"}},
		{5, []string{"__", "_="}},
		{6, []string{label + "="}},
		{12, []string{"__vlmc_"}},
	}
}

// mixedClassGuardIndex maps a guard's index in the base list (the 21 fixed guards, envelope guards last at 16 to 20)
// to its place when the label adds nested-prefix guards before the envelope guards.
func mixedClassGuardIndex(guards logsql.AllOf, i int) int {
	if i >= 16 {
		return len(guards) - 21 + i
	}
	return i
}

func mixedClassPrefilteredGuard(guard logsql.FilterExpr, literals []string) logsql.FilterExpr {
	original := mixedClassOriginalGuard(guard)
	return mixedClassLiteralGuard(original.Field, original.Value, literals...)
}

func mixedClassOriginalGuard(guard logsql.FilterExpr) logsql.FieldFilter {
	if node, ok := guard.(logsql.NotExpr); ok {
		if conjunction, ok := node.Expr.(logsql.AllOf); ok && len(conjunction) == 2 {
			if _, ok := conjunction[0].(logsql.AnyOf); ok {
				original := conjunction[1].(logsql.FieldFilter)
				original.Negate = true
				return original
			}
		}
	}
	return guard.(logsql.FieldFilter)
}

func TestMixedJSONRowClassLiteralPrefilterImplications(t *testing.T) {
	for _, label := range []string{"k", "user_id", "batch_id"} {
		c, _ := newMixedJSONRowClass(label)
		guards := c.safe.(logsql.AllOf)
		for _, spec := range mixedClassLiteralPrefilters(label) {
			original := mixedClassOriginalGuard(guards[mixedClassGuardIndex(guards, spec.index)])
			if !original.Negate {
				t.Fatalf("guard %d is not negated", spec.index)
			}
			re := regexp.MustCompile(original.Value)
			matches := 0
			keys := []string{"k", label, "app", "_time", "__", "k_", "user__id", "_", "___", "__vlmc_original", "__vlmc_", "__vlmc_target_k"}
			values := []string{"", "ordinary", "__", `_":`, "_=", label + "=", "__vlmc_", `\\\"`}
			for _, key := range keys {
				for _, value := range values {
					for _, input := range []string{
						`{"` + key + `":"` + value + `"}`,
						`{"app":"` + value + `","` + key + `":"x"}`,
						`{` + key + `="` + value + `"}`,
						`{app="` + value + `",` + key + `="x"}`,
						`{"` + key + `" : "` + value + `"}`,
						value,
					} {
						positive := re.MatchString(input)
						prefilter := false
						for _, literal := range spec.literals {
							prefilter = prefilter || strings.Contains(input, literal)
						}
						if positive {
							matches++
						}
						if positive && !prefilter {
							t.Fatalf("label=%s guard=%d positive match lacks literal: %q", label, spec.index, input)
						}
						if !positive != (!prefilter || !positive) {
							t.Fatal("negated conjunction changes classification")
						}
					}
				}
			}
			if matches == 0 {
				t.Fatalf("label=%s guard=%d has no positive witness", label, spec.index)
			}
			candidate := mixedClassPrefilteredGuard(guards[mixedClassGuardIndex(guards, spec.index)], spec.literals).(logsql.NotExpr).Expr.(logsql.AllOf)
			want := original
			want.Negate = false
			if candidate[1] != want {
				t.Fatalf("guard %d original expression changed", spec.index)
			}
		}
	}
}

func TestMixedJSONRowClassEmitQueries(t *testing.T) {
	path := os.Getenv("LVP_MIXED_CLASS_QUERIES")
	if path == "" {
		t.Skip("set LVP_MIXED_CLASS_QUERIES to capture final production queries")
	}
	queries := make(map[string]map[string]string)
	for _, key := range []string{"k", "user_id"} {
		c, _ := newMixedJSONRowClass(key)
		query, agg := mixedJSONStatsQuery("*", c, key)
		queries[key] = map[string]string{"native": (logsql.Query{Pipes: c.nativePipes()}).String(), "raw": (logsql.Query{Pipes: c.rawPipes()}).String(), "statsQuery": query, "statsAgg": agg, "statsGroupField": mixedStatsGroupField}

	}
	body, err := json.MarshalIndent(queries, "", "  ")
	if err == nil {
		err = os.WriteFile(path, body, 0o600)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestMixedJSONRowClassRelevantAliasesRetainsSafety(t *testing.T) {
	for _, key := range []string{"k", "user_id"} {
		c, _ := newMixedJSONRowClass(key)
		got := c.safe.(logsql.AllOf)
		alias := got[14].(logsql.FieldFilter)
		if alias.Field != "_msg" || alias.Op != logsql.FieldOpRegexp || !alias.Negate {
			t.Fatal("missing production key-alias guard")
		}
		if pattern := orderedJSONKeyAliasPattern([]string{key}); pattern != "" && alias.Value != pattern {
			t.Fatal("production does not reuse existing alias guard")
		}
		for _, body := range []string{`{" ` + key + ` ":"alias","` + key + `":"blue"}`, `{"` + key + `":"blue"," ` + key + ` ":null}`, `{"":"empty","` + key + `":"blue"}`, `{"prefix":{" ":{}} ,"` + key + `":"blue"}`} {
			if mixedClassRegexMatch(t, got[14], body) {
				t.Fatalf("relevant trim/blank alias admitted:%s", body)
			}
		}
		for _, body := range []string{`{"other.key":"unrelated","` + key + `":"blue"}`, `{"` + key + `":"blue","1other":"unrelated"}`} {
			if !mixedClassRegexMatch(t, got[14], body) {
				t.Fatalf("irrelevant spelling still blocked:%s", body)
			}
		}
	}
}

func TestMixedJSONRowClassLongAliasPatternFallsBack(t *testing.T) {
	many := strings.Repeat("a_", 63) + "a"
	if len(many) > 128 || mixedClassBodyAliasPattern(many) != mixedClassInvalidBodyKey {
		t.Fatal("many-span name did not take the early conservative backstop")
	}
	key := strings.Repeat("abcdefghijklmnopqr_", 6) + "tail"
	if len(key) > 128 || len(orderedJSONKeyAliasPattern([]string{key})) <= vlDefaultMaxQueryLen/4 {
		t.Fatal("test does not exercise bounded alias fallback")
	}
	c, ok := newMixedJSONRowClass(key)
	if !ok {
		t.Fatal("ordinary bounded key rejected")
	}
	guard := c.safe.(logsql.AllOf)[14].(logsql.FieldFilter)
	if guard.Value != mixedClassInvalidBodyKey || !guard.Negate {
		t.Fatal("long alias expansion did not retain conservative key guard")
	}
	if !mixedClassRegexMatch(t, guard, `{"`+key+`":"blue"}`) {
		t.Fatal("fallback rejected an ordinary native body key")
	}
	if mixedClassRegexMatch(t, guard, `{"other.key":"x","`+key+`":"blue"}`) {
		t.Fatal("fallback widened admission for invalid unrelated keys")
	}
	query, agg := mixedJSONStatsQuery("*", c, key)
	if len(query)+len(agg) > vlDefaultMaxQueryLen {
		t.Fatal("bounded fallback still exceeds the backend query limit")
	}
}

func TestMixedJSONRowClassStatsProjectionKeepsClass(t *testing.T) {
	for _, key := range []string{"k", "user_id", "c", "g"} {
		c, _ := newMixedJSONRowClass(key)
		native := (logsql.Query{Pipes: c.nativePipes()}).String()
		raw := (logsql.Query{Pipes: c.rawPipes()}).String()
		query, _ := mixedJSONStatsQuery("*", c, key)
		suffix := appendMixedClassPipes("", []logsql.Pipe{logsql.PipeFields{Labels: []string{"_time", mixedClassTargetPrefix + key}}, logsql.PipeRename{Pairs: [][2]string{{mixedClassTargetPrefix + key, mixedStatsGroupField}}}})
		if !strings.HasSuffix(query, suffix) {
			t.Fatal("stats must project away original output-name collisions before renaming")
		}
		if (logsql.Query{Pipes: c.nativePipes()}).String() != native || (logsql.Query{Pipes: c.rawPipes()}).String() != raw {
			t.Fatal("output projection changed class or original row envelope")
		}
	}
}

func mixedClassRegexMatch(t *testing.T, expr logsql.FilterExpr, value string) bool {
	t.Helper()
	if not, ok := expr.(logsql.NotExpr); ok {
		return !mixedClassRegexMatch(t, not.Expr, value)
	}
	if any, ok := expr.(logsql.AnyOf); ok {
		for _, child := range any {
			if mixedClassRegexMatch(t, child, value) {
				return true
			}
		}
		return false
	}
	if all, ok := expr.(logsql.AllOf); ok {
		for _, child := range all {
			if !mixedClassRegexMatch(t, child, value) {
				return false
			}
		}
		return true
	}
	f := expr.(logsql.FieldFilter)
	return regexp.MustCompile(f.Value).MatchString(value) != f.Negate
}

func TestMixedJSONRowClassLocalPredicates(t *testing.T) {
	c, _ := newMixedJSONRowClass("user_id")
	filters := mixedClassFieldFilters(t, c.safe)
	for _, tc := range []struct {
		name, field, value string
		want               bool
	}{
		{"ordinary source", c.originalField, `{"_msg":"body","app":"api"}`, true},
		{"canonical internal source", c.originalField, `{"_time":"now","_stream_id":"stream","_stream":"{}","user_name":"value"}`, true},
		{"trimmed metadata alias", c.originalField, `{" user_id":"collision"}`, false},
		{"punctuation metadata alias", c.originalField, `{"user.id":"collision"}`, false},
		{"collapsed metadata alias", c.originalField, `{"user__id":"collision"}`, false},
		{"trailing metadata alias", c.originalField, `{"user_id_":"collision"}`, false},
		{"empty-sanitized metadata alias", c.originalField, `{"___":"collision"}`, false},
		{"stored own key", c.originalField, `{"user_id":"collision"}`, false},
		{"snapshot collision", c.originalField, `{"` + c.originalField + `":"collision"}`, false},
		{"ordinary stream", mixedClassStreamField, `{app="api",env="production"}`, true},
		{"canonical underscore stream", mixedClassStreamField, `{app="api",user_name="value",_private="value"}`, true},
		{"collapsed stream alias", mixedClassStreamField, `{app="api",user__id="collision"}`, false},
		{"trailing stream alias", mixedClassStreamField, `{app="api",user_id_="collision"}`, false},
		{"empty-sanitized stream alias", mixedClassStreamField, `{app="api",___="collision"}`, false},
		{"empty stream target", mixedClassStreamField, `{app="api",user_id=""}`, false},
		{"stream target", mixedClassStreamField, `{app="api",user_id="stored"}`, false},
		{"trimmed stream target", mixedClassStreamField, `{app="api", user_id=""}`, false},
		{"ordinary body", "_msg", `{"user_id":"u1","service":{"name":"api"}}`, true},
		{"nested prefix before direct", "_msg", `{"user":{"id":"nested"},"user_id":"outer"}`, false},
		{"duplicate direct", "_msg", `{"user_id":"first","user_id":"last"}`, false},
		{"raw replacement character", "_msg", `{"user_id":"before�after"}`, false},
		{"escaped value", "_msg", `{"user_id":"before\nafter"}`, false},
		{"assignment in string", "_msg", `{"user_id":"body","note":"user_id=shadow"}`, false},
		{"punctuation body alias", "_msg", `{"user.id":"nested","user_id":"outer"}`, false},
		{"oversized unrelated key before target", "_msg", `{"` + strings.Repeat("x", 129) + `":"other","user_id":"outer"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matched := true
			for _, filter := range filters {
				if filter.Field != tc.field || filter.Op != logsql.FieldOpRegexp {
					continue
				}
				re, err := regexp.Compile(filter.Value)
				if err != nil {
					t.Fatal(err)
				}
				if re.MatchString(tc.value) == filter.Negate {
					matched = false
				}
			}
			if matched != tc.want {
				t.Fatalf("local field predicates accepted=%v, want %v", matched, tc.want)
			}
		})
	}
	// This is a local regexp contract, not an interpreter for JSON/logfmt or
	// proof that VL executes the preparation. Actual backend tests cover those.
}

// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassLongKeyValidJSONDomain(t *testing.T) {
	c, _ := newMixedJSONRowClass("k")
	var guard logsql.FieldFilter
	validityRetained := false
	for _, filter := range mixedClassFieldFilters(t, c.safe) {
		if filter.Field == "_msg" && filter.Op == logsql.FieldOpRegexp && strings.Contains(filter.Value, "{129}") {
			guard = filter
		}
		if filter.Field == mixedClassValidField && filter.Op == logsql.FieldOpAny {
			validityRetained = true
		}
	}
	if guard.Value == "" || !guard.Negate || !validityRetained {
		t.Fatal("long-key rejection must retain the positive backend JSON validity predicate")
	}
	current := regexp.MustCompile(guard.Value)
	previous := regexp.MustCompile(`"[A-Za-z_][A-Za-z0-9_]{128}[A-Za-z0-9_]*"\s*:`)
	// The guard is a cheaper superset of the previous one: it rejects every key of 129 or more name
	// characters, and also any other quoted run that long (a long value), which then takes the exact
	// complement instead of the native count. The independent rule below is a scan, not a regexp.
	quotedRun := func(body []byte) bool {
		for i := 0; i < len(body); i++ {
			if body[i] != '"' {
				continue
			}
			n := 0
			for j := i + 1; j < len(body) && (body[j] == '_' || body[j] >= '0' && body[j] <= '9' || body[j] >= 'A' && body[j] <= 'Z' || body[j] >= 'a' && body[j] <= 'z'); j++ {
				n++
			}
			if n >= 129 {
				return true
			}
		}
		return false
	}
	for _, length := range []int{1, 128, 129} {
		name := strings.Repeat("x", length)
		for _, value := range []string{"plain", strings.Repeat("v", 128), strings.Repeat("v", 129), "braces{,}commas", "tail,"} {
			for _, fixture := range []struct {
				value any
				// key: a key of 129 or more characters, which the previous guard also rejected.
				key bool
			}{
				{map[string]any{"k": "blue", name: value}, length > 128},
				{map[string]any{"k": "blue", "other": map[string]any{name: value}}, length > 128},
				{map[string]any{"k": "blue", "other": []any{value, map[string]any{name: value}}}, length > 128},
				{map[string]any{"k": "blue", "other": []any{name, value}}, false},
			} {
				body, err := json.Marshal(fixture.value)
				if err != nil || !json.Valid(body) || strings.ContainsAny(string(body), "\\=") {
					t.Fatal("fixture is outside the positive validity/ASCII/no-backslash domain")
				}
				if current.Match(body) != quotedRun(body) {
					t.Fatalf("long-run rejection differs from the scan for valid JSON: %s", body)
				}
				if previous.Match(body) != fixture.key {
					t.Fatalf("fixture's long-key expectation is wrong for valid JSON: %s", body)
				}
				if previous.Match(body) && !current.Match(body) {
					t.Fatalf("the guard stopped rejecting a long key the previous guard rejected: %s", body)
				}
			}
		}
	}
	// A 128-character value stays native; a 129-character one takes the exact complement.
	if current.MatchString(`{"k":"blue","v":"`+strings.Repeat("v", 128)+`"}`) || !current.MatchString(`{"k":"blue","v":"`+strings.Repeat("v", 129)+`"}`) {
		t.Fatal("the run bound moved: 128 name characters must stay native, 129 must not")
	}
	// A malformed value followed by a colon is not a key. The broad
	// expression matches it; the positive backend validity predicate must
	// still reject the whole row. This local test is not a
	// replacement for the actual VL partition/complete-envelope oracle.
	malformed := `{"k":"blue","other":"` + strings.Repeat("v", 129) + `":0}`
	if json.Valid([]byte(malformed)) || !previous.MatchString(malformed) {
		t.Fatal("missing malformed-value counterexample to treating this predicate as a JSON validator")
	}
}

func mixedClassFieldFilters(t *testing.T, expr logsql.FilterExpr) []logsql.FieldFilter {
	t.Helper()
	var out []logsql.FieldFilter
	switch node := expr.(type) {
	case logsql.AllOf:
		for _, child := range node {
			out = append(out, mixedClassFieldFilters(t, child)...)
		}
	case logsql.FieldFilter:
		out = append(out, node)
	case logsql.NotExpr:
		// Only unwrap the exact literal-prefilter guard shape. Other NOT
		// subtrees (the JSON hazard) are covered by their dedicated tests.
		if conjunction, ok := node.Expr.(logsql.AllOf); ok && len(conjunction) == 2 {
			if _, ok := conjunction[0].(logsql.AnyOf); ok {
				out = append(out, mixedClassOriginalGuard(node))
			}
		}
	}
	return out
}

// Pin the pre-migration spelling from the recorded actual VL query. Legacy
// users keep their query text and cache identity while the new class uses nodes.
func TestMixedJSONRowClassTypedHazardPreservesLegacyBytes(t *testing.T) {
	const legacy = `((_msg:"\\" _msg:~"\\\\(?s:.)(?:[^\"A-Za-z]*[A-Za-z]*)*\"\\s*:") or ((_msg:nan or _msg:Nan or _msg:nAn or _msg:NAn or _msg:naN or _msg:NaN or _msg:nAN or _msg:NAN or _msg:inf or _msg:Inf or _msg:iNf or _msg:INf or _msg:inF or _msg:InF or _msg:iNF or _msg:INF) _msg:~":\\s*(?i:nan|inf)") or (_msg:"+" _msg:~":\\s*\\+") or _msg:~":\\s*[.eE]")`
	if got := jsonLokiRejectHazardFilter().String(); got != legacy || jsonLokiRejectHazard != legacy {
		t.Fatalf("typed hazard changed legacy bytes:\n got: %s\nwant: %s", got, legacy)
	}
	c, _ := newMixedJSONRowClass("k")
	before := c.safe.String()
	pipes := c.nativePipes()
	expr := pipes[len(pipes)-1].(logsql.PipeFilter).Expr
	if !mutateMixedClassOR(expr) {
		t.Fatal("typed hazard has no slice-backed OR to exercise")
	}
	if c.safe.String() != before || c.nativePipes()[len(c.prepare)].String() != "| filter "+before {
		t.Fatal("returned hazard OR aliases the stored predicate")
	}
	other := jsonLokiRejectHazardFilter()
	if !mutateMixedClassOR(other) || jsonLokiRejectHazardFilter().String() != legacy {
		t.Fatal("independent typed hazard instances share a mutable OR")
	}
	assertMixedClassTypedFilters(t, c.safe)
}

func mutateMixedClassOR(expr logsql.FilterExpr) bool {
	switch node := expr.(type) {
	case logsql.AnyOfLower:
		node[0] = logsql.FieldFilter{Field: "mutated", Op: logsql.FieldOpAny}
		return true
	case logsql.AnyOf:
		node[0] = logsql.FieldFilter{Field: "mutated", Op: logsql.FieldOpAny}
		return true
	case logsql.AllOf:
		for _, child := range node {
			if mutateMixedClassOR(child) {
				return true
			}
		}
	case logsql.NotExpr:
		return mutateMixedClassOR(node.Expr)
	case logsql.Group:
		return mutateMixedClassOR(node.Expr)
	}
	return false
}

func assertMixedClassTypedFilters(t *testing.T, expr logsql.FilterExpr) {
	t.Helper()
	switch node := expr.(type) {
	case logsql.AllOf:
		for _, child := range node {
			assertMixedClassTypedFilters(t, child)
		}
	case logsql.AnyOfLower:
		for _, child := range node {
			assertMixedClassTypedFilters(t, child)
		}
	case logsql.AnyOf:
		for _, child := range node {
			assertMixedClassTypedFilters(t, child)
		}
	case logsql.NotExpr:
		assertMixedClassTypedFilters(t, node.Expr)
	case logsql.Group:
		assertMixedClassTypedFilters(t, node.Expr)
	case logsql.FieldFilter, logsql.LenRange:
	default:
		t.Fatalf("new class contains an untyped filter node: %T", expr)
	}
}

// VictoriaLogs evaluates an AND in written order and stops at the first rejecting guard, so the class's conjuncts
// are in cost order. The order never changes the class (AND commutes); it decides how many rows reach the long
// regexps and the guards that force the packed envelope. This pins the cost order and that nothing was lost.
// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassGuardsAreInCostOrder(t *testing.T) {
	for _, label := range []string{"user_id", "tenant", "a_b_c"} {
		class, ok := newMixedJSONRowClass(label)
		if !ok {
			t.Fatalf("%s: class refused", label)
		}
		guards := class.safe.(logsql.AllOf)
		// The first guard is the cheap flat-printable-object check on the message alone.
		if got := guards[0].String(); !strings.HasPrefix(got, `_msg:~"^\\{[\\x20-\\x7e]*\\}$"`) {
			t.Fatalf("%s: the first guard is %s", label, got)
		}
		firstEnvelope, lastOther := -1, -1
		for i, g := range guards {
			if strings.Contains(g.String(), mixedClassOriginalField) {
				if firstEnvelope < 0 {
					firstEnvelope = i
				}
			} else {
				lastOther = i
			}
		}
		// Every guard that reads the packed envelope comes after every guard that does not.
		if firstEnvelope < 0 || firstEnvelope < lastOther {
			t.Fatalf("%s: an envelope guard (index %d) precedes a message, stream or target guard (last at %d)", label, firstEnvelope, lastOther)
		}
		// Nothing was dropped: the fixed guards plus one nested-prefix guard per underscore span.
		if want := 21 + strings.Count(label, "_"); len(guards) != want {
			t.Fatalf("%s: %d guards, want %d", label, len(guards), want)
		}
	}
}

// The certified class drops the packed envelope from the native count and keeps it for the exact complement, and
// its arithmetic keeps the packed envelope under the class's own bound for the widest field set it accepts.
// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassCertifiedStructureAndBound(t *testing.T) {
	class, _ := newMixedJSONRowClass("user_id")
	names := []string{"_msg", "_stream", "_stream_id", "_time", "app", "env"}
	cert, ok := class.certified(names)
	if !ok {
		t.Fatal("clean names refused")
	}
	for _, pipe := range cert.nativePipes() {
		if _, packed := pipe.(logsql.PipePackJSON); packed {
			t.Fatal("the certified native count packs the envelope")
		}
	}
	if _, packed := cert.rawPipes()[0].(logsql.PipePackJSON); !packed {
		t.Fatal("the exact complement lost the packed original envelope")
	}
	if strings.Contains(cert.safe.String(), mixedClassOriginalField) {
		t.Fatal("a certified guard reads the packed envelope")
	}
	// The full class is unchanged by deriving the certified one.
	if !strings.Contains(class.safe.String(), mixedClassOriginalField) || len(class.nativePrepare) != len(class.prepare) {
		t.Fatal("deriving the certified class changed the class")
	}
	// The widest accepted field set stays under the bound.
	wide := []string{"_msg", "_stream", "_stream_id", "_time"}
	for i := 0; i < mixedCertOtherFieldsMax; i++ {
		wide = append(wide, strings.Repeat("a", mixedCertOtherNameMax-1)+strconv.Itoa(i))
	}
	if _, ok := class.certified(wide); !ok {
		t.Fatal("the widest accepted set was refused")
	}
	if got := mixedClassCertifiedWorstPacked(wide[4:]); got > 4096 {
		t.Fatalf("worst packed envelope %d exceeds the class bound", got)
	}
	// Refusals: an unclean name, too many fields, a name too long for the bound, the label itself.
	for _, bad := range [][]string{
		append(append([]string(nil), names...), "a__b"), append(append([]string(nil), names...), "trail_"),
		append(append([]string(nil), names...), "user_id"), append(append([]string(nil), names...), "9lead"),
		append(append([]string(nil), names...), strings.Repeat("a", mixedCertOtherNameMax+1)),
		append(append([]string(nil), wide...), "extra1"),
	} {
		if _, ok := class.certified(bad); ok {
			t.Fatalf("names accepted: %v", bad)
		}
	}
	if !mixedClassNamesWithin([]string{"a"}, []string{"a", "b"}) || mixedClassNamesWithin([]string{"a", "c"}, []string{"a", "b"}) {
		t.Fatal("name subset check")
	}
}

// A window with many stored fields, a long message or a long stream descriptor is certified from the window's
// measured maximum lengths: each guard's bound is the measured maximum rounded up, and the worst packed envelope
// under those bounds stays within the class's own bound, so the certified count answers as the full class does.
// conformance: semantics/two-parsers-keep-one
func TestMixedClassMeasuredBoundsCertifyWideWindows(t *testing.T) {
	class, _ := newMixedJSONRowClass("user_id")
	// The shape of the real mixed workload: fourteen stored fields, messages near 860 bytes, streams near 400.
	others := []string{"app", "cluster", "container", "env", "generator_profile", "generator_run", "level", "namespace",
		"owner_team", "pod", "region", "service_name", "version", "workload_scope"}
	names := append([]string{"_msg", "_stream", "_stream_id", "_time"}, others...)
	lengths := mixedClassLengths{"_msg": 857, "_stream": 401}
	for _, name := range others {
		lengths[name] = 20
	}
	lengths["pod"] = 33
	if _, ok := class.certified(names); ok {
		t.Fatal("the fixed bounds were expected to refuse fourteen fields")
	}
	cert, ok := class.certifiedFor(names, lengths)
	if !ok {
		t.Fatal("the measured bounds refused a window that fits the budget")
	}
	safe := cert.safe.String()
	for _, want := range []string{
		"len_range(0, 864)", "len_range(0, 416)", // 857 and 401 rounded up to 16
		"len_range(0, 48)", // pod: 33 rounded up
		"len_range(0, 32)", // the rest: 20 rounded up
	} {
		if !strings.Contains(safe, want) {
			t.Fatalf("the certified guards lack %s:\n%s", want, safe)
		}
	}
	if strings.Contains(safe, mixedClassOriginalField) {
		t.Fatal("a certified guard reads the packed envelope")
	}
	for _, pipe := range cert.nativePipes() {
		if _, packed := pipe.(logsql.PipePackJSON); packed {
			t.Fatal("the certified native count packs the envelope")
		}
	}
	if _, packed := cert.rawPipes()[0].(logsql.PipePackJSON); !packed {
		t.Fatal("the exact complement lost the packed original envelope")
	}
	bounds, _ := mixedCertBoundsFor(others, lengths)
	if got := mixedClassCertifiedWorstPackedBounds(others, bounds); got > 4096 {
		t.Fatalf("worst packed envelope %d exceeds the class bound", got)
	}
	// Rounding: up to the step, a zero length still bounds at one step, and an exact multiple stays.
	for n, want := range map[int]int{0: 16, 1: 16, 16: 16, 17: 32, 857: 864, 401: 416} {
		if got := mixedCertRound(n); got != want {
			t.Fatalf("mixedCertRound(%d)=%d, want %d", n, got, want)
		}
	}
	// A row longer than its bound is not a refusal of the window: its guard sends it to the exact complement.
	if !strings.Contains(safe, `len_range(0, 864)`) {
		t.Fatal("the message bound is not enforced per row")
	}
	// Refusals: a length missing for the message, the stream or another field; a negative length; a budget that the
	// bounds exceed (a message of 2000 bytes escapes to 4000); more fields than the ceiling; an unclean name.
	without := func(name string) mixedClassLengths {
		out := mixedClassLengths{}
		for k, v := range lengths {
			if k != name {
				out[k] = v
			}
		}
		return out
	}
	for _, name := range []string{"_msg", "_stream", "pod"} {
		if _, ok := class.certifiedFor(names, without(name)); ok {
			t.Fatalf("a window with no measured length for %s was certified", name)
		}
	}
	negative := without("")
	negative["pod"] = -1
	if _, ok := class.certifiedFor(names, negative); ok {
		t.Fatal("a negative length was accepted")
	}
	long := without("")
	long["_msg"] = 2000
	if _, ok := class.certifiedFor(names, long); ok {
		t.Fatal("a window whose worst packed envelope exceeds the bound was certified")
	}
	many := append([]string(nil), names[:4]...)
	manyLengths := mixedClassLengths{"_msg": 100, "_stream": 100}
	for i := 0; i <= mixedCertMeasuredFieldsMax; i++ {
		name := "f" + strconv.Itoa(i)
		many = append(many, name)
		manyLengths[name] = 1
	}
	if _, ok := class.certifiedFor(many, manyLengths); ok {
		t.Fatal("more fields than the ceiling were certified")
	}
	if _, ok := class.certifiedFor(append(append([]string(nil), names...), "a__b"), lengths); ok {
		t.Fatal("an unclean name was certified from measured lengths")
	}
	// With no measurement the form is the fixed one, unchanged.
	fixed, ok := class.certified(names[:6])
	fromNil, okNil := class.certifiedFor(names[:6], nil)
	if !ok || !okNil || fixed.safe.String() != fromNil.safe.String() {
		t.Fatal("certified and certifiedFor(nil) differ")
	}
}

// The field-name check replaces the envelope key guards for a whole window, so it must imply them: for every name
// the check accepts, a packed row holding that name next to ordinary fields passes each envelope guard of the class.
// conformance: semantics/two-parsers-keep-one
func TestMixedClassFieldNamesCleanImpliesEnvelopeKeyGuards(t *testing.T) {
	alphabet := []string{"a", "Z", "_", "1", "-", ".", " ", `"`, `\`, "é"}
	var names []string
	var gen func(prefix string, depth int)
	gen = func(prefix string, depth int) {
		if prefix != "" {
			names = append(names, prefix)
		}
		if depth == 0 {
			return
		}
		for _, a := range alphabet {
			gen(prefix+a, depth-1)
		}
	}
	gen("", 4)
	names = append(names, "user_id", "user", "app", "_time", "_stream", "_stream_id", "_msg", "level", "__vlmc_valid",
		"__vlmc_", "a_", "_a", "a__b", "a_b", strings.Repeat("a", 127), strings.Repeat("a", 128), strings.Repeat("a", 129))
	for _, label := range []string{"user_id", "k", "batch_id"} {
		class, ok := newMixedJSONRowClass(label)
		if !ok {
			t.Fatal(label)
		}
		guards := class.safe.(logsql.AllOf)
		envelope := guards[len(guards)-4:] // grammar, alias, label, scratch (the length bound is not a regexp)
		accepted := 0
		for _, name := range names {
			if !mixedClassFieldNameClean(label, name) {
				continue
			}
			accepted++
			for _, row := range []string{
				`{"` + name + `":"v"}`,
				`{"_msg":"{\"a\":\"b\"}","_stream":"{app=\"x\"}","_stream_id":"0001","_time":"2026-11-23T00:00:00Z","app":"x","` + name + `":"v"}`,
				`{"app":"x","` + name + `":"a\"b\\c d"}`,
			} {
				for i, g := range envelope {
					if !mixedClassRegexMatch(t, g, row) {
						t.Fatalf("label=%s: the check accepts %q but envelope guard %d rejects %q", label, name, i, row)
					}
				}
			}
		}
		if accepted < 100 {
			t.Fatalf("label=%s: only %d names accepted; the test lost its coverage", label, accepted)
		}
	}
}

// conformance: semantics/two-parsers-keep-one
func TestMixedJSONRowClassExplicitOrderDropsOnlyTheKeyTextGuards(t *testing.T) {
	for _, label := range []string{"k", "user_id", "a_b_c"} {
		full, ok1 := newMixedJSONRowClassFor(label, false)
		explicit, ok2 := newMixedJSONRowClassFor(label, true)
		if !ok1 || !ok2 || full.explicit || !explicit.explicit {
			t.Fatalf("%s: classes missing or mislabeled", label)
		}
		if def, _ := newMixedJSONRowClass(label); !reflect.DeepEqual(def.safe, full.safe) {
			t.Fatalf("%s: newMixedJSONRowClass is not the full class", label)
		}
		// The full class has the 129-run guard, the alias guard and one nested-prefix guard per underscore span; the
		// explicit one has none of them and every other guard, in the same order.
		dropped := 2 + strings.Count(label, "_")
		if len(full.safe.(logsql.AllOf))-len(explicit.safe.(logsql.AllOf)) != dropped {
			t.Fatalf("%s: explicit class drops %d guards, want %d", label, len(full.safe.(logsql.AllOf))-len(explicit.safe.(logsql.AllOf)), dropped)
		}
		isKeyText := func(g logsql.FilterExpr) bool {
			f, ok := g.(logsql.FieldFilter)
			return ok && f.Field == "_msg" && f.Negate && (f.Value == `"[A-Za-z0-9_]{129}` || f.Value == mixedClassBodyAliasPattern(label) ||
				(strings.Contains(label, "_") && strings.HasSuffix(f.Value, `"\s*:\s*\{`)))
		}
		kept := func(c *mixedJSONRowClass) []string {
			var out []string
			for _, g := range c.safe.(logsql.AllOf) {
				if !isKeyText(g) {
					out = append(out, g.String())
				}
			}
			return out
		}
		if !reflect.DeepEqual(kept(full), kept(explicit)) {
			t.Fatalf("%s: the explicit class differs from the full one beyond the three key-text guards", label)
		}
		for _, g := range explicit.safe.(logsql.AllOf) {
			if isKeyText(g) {
				t.Fatalf("%s: explicit class still has a key-text guard: %s", label, g.String())
			}
		}
		dropCount := 0
		for _, g := range full.safe.(logsql.AllOf) {
			if isKeyText(g) {
				dropCount++
			}
		}
		if dropCount != dropped {
			t.Fatalf("%s: the full class has %d key-text guards, want %d", label, dropCount, dropped)
		}
	}
}

// A class derived for one parser order must never be served to the other: the certificate memo is keyed by the order.
func TestMixedCertMemoKeySeparatesTheParserOrders(t *testing.T) {
	p, err := New(Config{BackendURL: "http://127.0.0.1:1", LogLevel: "error", MetadataInventoryParallelism: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	from := time.Unix(1700000000, 0)
	a := p.mixedCertMemoKey(context.Background(), `{app="a"}`, "k", false, from, from.Add(time.Hour))
	b := p.mixedCertMemoKey(context.Background(), `{app="a"}`, "k", true, from, from.Add(time.Hour))
	if a == b || a != p.mixedCertMemoKey(context.Background(), `{app="a"}`, "k", false, from, from.Add(time.Hour)) {
		t.Fatalf("memo keys: full %q explicit %q", a, b)
	}
}
