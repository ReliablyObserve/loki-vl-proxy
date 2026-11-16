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

// The chain reads a stream's level fields before the ones stored on its rows, after a stored detected_level: one
// pipe marks the rows whose stream holds any known level field, and on those rows every pick reads its field only
// from the stream. One known name, a presence of one, or no known fields leave the chain as it was.
//
// conformance: semantics/detected-level-one-value-per-stream, severity-detected-level-derivation
func TestDetectedLevelChainReadsStreamFieldsFirst(t *testing.T) {
	pipes := strings.Split(strings.TrimPrefix(BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil), "| "), " | ")
	if want := `format if (detected_level:*) "<detected_level>" as __dl_s keep_original_fields`; pipes[0] != want {
		t.Fatalf("first pipe %q, want %q", pipes[0], want)
	}
	names := StreamLevelPickFields(levelTextFields(DetectedLevelFieldNames))
	alts := make([]string, len(names))
	for i, n := range names {
		alts[i] = "{" + LogsQLFieldName(n) + `!=""}`
	}
	if want := `format if (` + strings.Join(alts, " OR ") + `) "1" as __dl_t`; pipes[1] != want {
		t.Errorf("mark %q, want %q", pipes[1], want)
	}
	for _, c := range []struct {
		i    int
		want string
	}{
		{2, `format if (level:* (-__dl_t:* OR {level!=""})) "<level>" as __dl_s keep_original_fields`},
		{6, "format if (severity:* !(severity:~`^(Unspecified|(Trace|Debug|Info|Warn|Error|Fatal)[234]?)$` -severity_number:*) (-__dl_t:* OR {severity!=\"\"})) \"<severity>\" as __dl_s keep_original_fields"},
		{15, `format if (SEVERITY_TEXT:* (-__dl_t:* OR {SEVERITY_TEXT!=""})) "<SEVERITY_TEXT>" as __dl_s keep_original_fields`},
	} {
		if pipes[c.i] != c.want {
			t.Errorf("pipe %d %q, want %q", c.i, pipes[c.i], c.want)
		}
	}
	marks, gated := 0, 0
	for _, p := range pipes {
		if strings.HasSuffix(p, `"1" as __dl_t`) {
			marks++
		}
		if strings.Contains(p, "(-__dl_t:* OR {") {
			gated++
		}
	}
	if marks != 1 || gated != len(names) {
		t.Errorf("%d marks and %d gated picks, want 1 and %d", marks, gated, len(names))
	}
	for _, c := range []struct {
		name     string
		existing []string
		present  *DetectedLevelPresence
	}{
		{"no known fields", nil, nil},
		{"one known field", []string{"level"}, nil},
		{"a presence of one", nil, &DetectedLevelPresence{Stored: []string{"severity"}}},
		{"detected_level and one field", []string{"detected_level", "lvl"}, nil},
	} {
		if chain := BuildDetectedLevelChain(c.existing, DetectedLevelChainGrouping, c.present); strings.Contains(chain, "__dl_t") || strings.Contains(chain, `!=""}`) {
			t.Errorf("%s: stream mark in %s", c.name, chain)
		}
	}
	two := BuildDetectedLevelChain(nil, DetectedLevelChainGrouping, &DetectedLevelPresence{Stored: []string{"level", "severity"}})
	if !strings.Contains(two, `| format if ({level!=""} OR {severity!=""}) "1" as __dl_t | format if (level:* (-__dl_t:* OR {level!=""})) "<level>" as __dl_s keep_original_fields`) {
		t.Errorf("a presence of level and severity: %s", two)
	}
}
