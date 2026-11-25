package casefixtures

import "strings"

// OrderedMixedParserWitness is one shared input and its observable grouping.
// Query shapes and oracle replies remain in the existing registry/matrix runners.
type OrderedMixedParserWitness struct {
	Name, Group string
	Stream      Stream
}

// OrderedMixedParserRawComplement supplies the raw-complement witnesses to the
// seeder, recorded Loki oracle and typed compatibility matrix from one place.
// Each call owns fresh maps so a runner can scope labels without changing peers.
func OrderedMixedParserRawComplement() []OrderedMixedParserWitness {
	var out []OrderedMixedParserWitness
	for _, witness := range []struct {
		name, line, group string
		collision         bool
	}{
		{"replacement-rune", "{\"k\":\"before\ufffdafter\"}", "k", false},
		{"replacement-rune-escaped", `{"k":"before\ufffdafter"}`, "k", false},
		{"replacement-rune-object", "{\"k\":{\"nested\":\"before\ufffdafter\"}}", "k", false},
		{"replacement-rune-array", "{\"k\":[\"before\ufffdafter\"]}", "k", false},
		{"numeric-looking-string", `{"k":"-0"}`, "k", false},
		{"replacement-rune-collision", "{\"k\":\"before\ufffdafter\"}", "k_extracted", true},
		{"unrelated-long-key", `{"` + strings.Repeat("x", 1025) + `":0,"k":"body"}`, "k", false},
	} {
		name := "ordered-mixed-raw-" + witness.name
		labels := m{"app": name, "env": "compat_registry"}
		if witness.collision {
			labels["k"] = "stored"
		}
		out = append(out, OrderedMixedParserWitness{Name: witness.name, Group: witness.group, Stream: Stream{
			Name: name, Cases: []string{"semantics/two-parsers-keep-one"}, Labels: labels,
			Entries: []Entry{{Line: witness.line}}, Route: RouteJSONLine,
		}})
	}
	return out
}
