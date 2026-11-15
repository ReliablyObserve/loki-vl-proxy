package translate

import (
	"reflect"
	"strings"
	"testing"
)

// StreamLevelPickFields: the level text fields get a stream-only pick ahead of the others once two or more are known
// (detected_level, never a stream field Loki reads, does not count); with one or none the chain keeps its order, which
// cannot change a value then.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation
func TestStreamLevelPickFields(t *testing.T) {
	for _, c := range []struct {
		texts, want []string
	}{
		{nil, nil},
		{[]string{"level"}, nil},
		{[]string{"detected_level"}, nil},
		{[]string{"detected_level", "level"}, nil},
		{[]string{"level", "severity"}, []string{"level", "severity"}},
		{[]string{"detected_level", "level", "lvl"}, []string{"level", "lvl"}},
		{levelTextFields(DetectedLevelFieldNames), levelTextFields(DetectedLevelFieldNames)[1:]},
	} {
		if got := StreamLevelPickFields(c.texts); !reflect.DeepEqual(got, c.want) {
			t.Errorf("StreamLevelPickFields(%v) = %v, want %v", c.texts, got, c.want)
		}
	}
}

// The chain reads a stream's level fields before the same names stored on the row, after a stored detected_level:
// the stream picks sit between them, one per known name, each limited to the rows whose stream holds the field.
// One known name, a presence of one, or no known fields leave the chain as it was.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation
func TestDetectedLevelChainReadsStreamFieldsFirst(t *testing.T) {
	pipes := strings.Split(strings.TrimPrefix(BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil), "| "), " | ")
	if want := `format if (detected_level:*) "<detected_level>" as __dl_s keep_original_fields`; pipes[0] != want {
		t.Fatalf("first pipe %q, want %q", pipes[0], want)
	}
	names := StreamLevelPickFields(levelTextFields(DetectedLevelFieldNames))
	for i, name := range names {
		q := LogsQLFieldName(name)
		if want := `format if (` + q + `:* {` + q + `!=""}) "<` + name + `>" as __dl_s keep_original_fields`; pipes[1+i] != want {
			t.Errorf("pipe %d %q, want %q", 1+i, pipes[1+i], want)
		}
	}
	if want := `format if (level:*) "<level>" as __dl_s keep_original_fields`; pipes[1+len(names)] != want {
		t.Errorf("after the stream picks %q, want %q", pipes[1+len(names)], want)
	}
	for _, c := range []struct {
		name     string
		existing []string
		present  *DetectedLevelPresence
	}{
		{"no known fields", nil, nil},
		{"one known field", []string{"level"}, nil},
		{"a presence of one", nil, &DetectedLevelPresence{Stored: []string{"severity"}}},
	} {
		if chain := BuildDetectedLevelChain(c.existing, DetectedLevelChainGrouping, c.present); strings.Contains(chain, `!=""}`) {
			t.Errorf("%s: stream picks in %s", c.name, chain)
		}
	}
	two := BuildDetectedLevelChain(nil, DetectedLevelChainGrouping, &DetectedLevelPresence{Stored: []string{"level", "severity"}})
	if !strings.Contains(two, `format if (level:* {level!=""}) "<level>" as __dl_s keep_original_fields | format if (severity:* {severity!=""}) "<severity>" as __dl_s keep_original_fields | format if (level:*) "<level>"`) {
		t.Errorf("a presence of level and severity: %s", two)
	}
}
