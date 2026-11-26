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

// The alias pattern escapes the regexp metacharacters of a stored field name.
//
// conformance: field-mapping-label-names
func TestStreamLabelPattern(t *testing.T) {
	for _, c := range []struct {
		base   string
		fields []string
		want   string
	}{
		{"k8s_pod_name", nil, `[{,]k8s[._]pod[._]name="`},
		{"pod", []string{"k8s.pod.name"}, `[{,](?:pod|k8s\.pod\.name)="`},
		{"x", []string{`a+b(c)|d[e]{f}^$?*\`}, `[{,](?:x|a\+b\(c\)\|d\[e\]\{f\}\^\$\?\*\\)="`},
	} {
		if got := StreamLabelPattern(c.base, c.fields); got != c.want {
			t.Errorf("%s %v: %s, want %s", c.base, c.fields, got, c.want)
		}
	}
}
