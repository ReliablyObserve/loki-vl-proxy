package proxy

import (
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

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
		{1, []string{"__", `_":`}},
		{2, []string{`"` + label + `":`}},
		{3, []string{"__vlmc_"}},
		{5, []string{"__", "_="}},
		{6, []string{label + "="}},
		{17, []string{"__vlmc_"}},
	}
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
			original := mixedClassOriginalGuard(guards[spec.index])
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
			candidate := mixedClassPrefilteredGuard(guards[spec.index], spec.literals).(logsql.NotExpr).Expr.(logsql.AllOf)
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
		alias := got[19].(logsql.FieldFilter)
		if alias.Field != "_msg" || alias.Op != logsql.FieldOpRegexp || !alias.Negate {
			t.Fatal("missing production key-alias guard")
		}
		if pattern := orderedJSONKeyAliasPattern([]string{key}); pattern != "" && alias.Value != pattern {
			t.Fatal("production does not reuse existing alias guard")
		}
		for _, body := range []string{`{" ` + key + ` ":"alias","` + key + `":"blue"}`, `{"` + key + `":"blue"," ` + key + ` ":null}`, `{"":"empty","` + key + `":"blue"}`, `{"prefix":{" ":{}} ,"` + key + `":"blue"}`} {
			if mixedClassRegexMatch(t, got[19], body) {
				t.Fatalf("relevant trim/blank alias admitted:%s", body)
			}
		}
		for _, body := range []string{`{"other.key":"unrelated","` + key + `":"blue"}`, `{"` + key + `":"blue","1other":"unrelated"}`} {
			if !mixedClassRegexMatch(t, got[19], body) {
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
	guard := c.safe.(logsql.AllOf)[19].(logsql.FieldFilter)
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
