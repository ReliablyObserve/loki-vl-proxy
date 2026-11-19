package capability

import (
	"strings"
	"testing"
)

// The generated reference names every gate with its release and registry item, and the profiles, flavours, sources
// and header read by the exporters keep their closed labels.
//
// conformance: vl-capability-layer
func TestRenderGatesDocAndLabels(t *testing.T) {
	doc := RenderGatesDoc()
	for _, g := range Gates() {
		if !strings.Contains(doc, "`"+g.Name+"`") || !strings.Contains(doc, "vl/capabilities/"+g.Registry) || !strings.Contains(doc, g.FromString()) {
			t.Errorf("the reference lacks %s", g.Name)
		}
	}
	if strings.Contains(doc, "{") && !strings.Contains(doc, "&#123;") && strings.Count(doc, "{") > strings.Count(doc, "`{") {
		t.Error("braces outside code spans are read as JSX")
	}
	for in, want := range map[string]string{
		"v1.54.0": "vl-v1.50-plus", "v1.49.3": "vl-v1.49-plus", "v1.40.0": "vl-v1.30-plus", "v1.29.0": "legacy-pre-v1.30", "": "",
	} {
		if got := ForRelease(ParseRelease(in)).Profile(); got != want {
			t.Errorf("profile of %q = %q, want %q", in, got, want)
		}
	}
	probed := compute(Release{}, verdicts{StreamMetadataEndpoints: VerdictOff})
	if probed.Profile() != "legacy-probed-pre-v1.30" {
		t.Errorf("a refused stream endpoint probe: %q", probed.Profile())
	}
	s := ForRelease(ParseRelease("v1.53.0"))
	if h := s.HeaderValue(); !strings.Contains(h, "epoch=0") || !strings.Contains(h, "release=v1.53.0") || !strings.Contains(h, "vl.pipe_coalesce") || (Set{}).HeaderValue() != "" || (Set{}).Digest() != emptyDigest {
		t.Errorf("header %q", h)
	}
	if itoa(0) != "0" || itoa(1234567890123) != "1234567890123" {
		t.Error("itoa")
	}
	for _, f := range []Flavour{FlavourUnknown, FlavourOSS, FlavourEnterprise, FlavourEnterpriseFIPS} {
		if f.String() == "" {
			t.Error("flavour label")
		}
	}
	if SourceMetrics.String() != "metrics" || Source(99).String() != "none" || Capability.String() != "capability" || SideClient.String() != "client" || VerdictOff.String() != "off" || ReasonImplied.String() != "implied" || GateID(200).String() != "unknown" {
		t.Error("labels")
	}
	if ParseRelease("v1.52.2-enterprise").Compare(ParseRelease("v1.52.2-enterprise-fips")) >= 0 || (Set{}).Why(PipeCoalesce) != ReasonDefault || (Set{}).On(GateID(200)) {
		t.Error("compare and zero set")
	}
}
