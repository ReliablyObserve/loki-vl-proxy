package translate

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The collision check of name_extracted with an alias (-field-mapping-label-mode alias and both): the stream label
// is in _stream under its own name or under the alias's field; with no alias the text is the one written before.
//
// conformance: field-mapping-label-names
func TestExtractedFilterAliased(t *testing.T) {
	parsed := logsql.FieldFilter{Field: ExtractedScratch + "_pod", Op: logsql.FieldOpExact, Value: "x"}
	literal := logsql.FieldFilter{Field: "pod_extracted", Op: logsql.FieldOpExact, Value: "x"}
	plain, ok := ExtractedFilter("pod", parsed, literal)
	if !ok {
		t.Fatal("not written")
	}
	none, _ := ExtractedFilterAliased("pod", nil, parsed, literal)
	if none.String() != plain.String() {
		t.Fatalf("no alias changed the text:\n%s\n%s", none, plain)
	}
	aliased, _ := ExtractedFilterAliased("pod", []string{"k8s.pod.name"}, parsed, literal)
	if want := `__lxp_stream:~"[{,](?:pod|k8s\\.pod\\.name)=\""`; !strings.Contains(aliased.String(), want) {
		t.Fatalf("%s lacks %s", aliased, want)
	}
	var b1, b2, b3 strings.Builder
	ExtractedStatsText(&b1, []bool{true}, []string{"pod"}, []string{"pod_extracted"})
	ExtractedStatsTextAliased(&b2, []bool{true}, []string{"pod"}, []string{"pod_extracted"}, func(string) []string { return nil })
	ExtractedStatsTextAliased(&b3, []bool{true}, []string{"pod"}, []string{"pod_extracted"}, func(base string) []string { return []string{"k8s.pod.name"} })
	if b1.String() != b2.String() || !strings.Contains(b3.String(), `(?:pod|k8s\\.pod\\.name)=\"`) {
		t.Fatalf("stats text:\n%s\n%s\n%s", b1.String(), b2.String(), b3.String())
	}
}

// The probes read only their own answers: any other answer of a label function names no field and no alias.
//
// conformance: field-mapping-label-names
func TestStreamAliasProbes(t *testing.T) {
	answers := map[string]string{
		StreamAliasProbe + "pod":              StreamAliasProbe + "=k8s.pod.name",
		StreamAliasNameProbe + "k8s.pod.name": StreamAliasNameProbe + "=pod",
		StreamAliasProbe + "odd":              "k8s.pod.name",
		StreamAliasNameProbe + "odd":          "pod",
		StreamAliasProbe + "with,comma":       StreamAliasProbe + "=a,b",
	}
	fn := func(label string) string { return answers[label] }
	if got := StreamAliasFields(fn, "pod"); len(got) != 1 || got[0] != "k8s.pod.name" {
		t.Fatalf("fields %v", got)
	}
	if got := StreamAliasName(fn, "k8s.pod.name"); got != "pod" {
		t.Fatalf("name %q", got)
	}
	if StreamAliasFields(fn, "odd") != nil || StreamAliasName(fn, "odd") != "" || StreamAliasFields(nil, "pod") != nil || StreamAliasName(nil, "x") != "" {
		t.Fatal("a foreign answer was read as a probe answer")
	}
	if got := StreamAliasFields(fn, "with,comma"); len(got) != 1 || got[0] != "a,b" {
		t.Fatalf("a field with a comma: %v", got)
	}
	if got := streamLabelPattern("k8s_pod_name", nil); got != `[{,]k8s[._]pod[._]name="` {
		t.Fatalf("pattern %s", got)
	}
}
