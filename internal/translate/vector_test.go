package translate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

// The service_name derivation is a coalesce on VictoriaLogs v1.51+ and format pipes before it.
// conformance: architecture-syntax-package-layering
func TestServiceNameGroupingPipes(t *testing.T) {
	coalesce := ServiceNameGroupingPipes(logsql.CapabilitiesFor("v1.53.0"))
	if !strings.HasPrefix(coalesce, "| coalesce(service_name, \"service.name\", service,") || !strings.HasSuffix(coalesce, `default "unknown_service" as service_name`) {
		t.Errorf("coalesce form: %s", coalesce)
	}
	if _, err := logsql.Parse("* " + coalesce); err != nil {
		t.Errorf("coalesce form does not parse: %v", err)
	}
	formats := ServiceNameGroupingPipes(logsql.CapabilitiesFor("v1.49.0"))
	if strings.Contains(formats, "coalesce") || !strings.HasPrefix(formats, `| format "<service.name>" as service_name keep_original_fields | format`) || !strings.HasSuffix(formats, `| format "unknown_service" as service_name keep_original_fields`) {
		t.Errorf("format form: %s", formats)
	}
	if _, err := logsql.Parse("* " + formats); err != nil {
		t.Errorf("format form does not parse: %v", err)
	}
}

// conformance: architecture-syntax-package-layering
func TestNormalizeAndJoinByLabels(t *testing.T) {
	up := func(s string) string { return strings.ToUpper(s) }
	for _, c := range []struct {
		in   []string
		fn   LabelTranslateFunc
		want []string
	}{
		{[]string{"app", " env ", "app", ""}, nil, []string{"app", "env"}},
		{[]string{"app", "service_name", "detected_level"}, up, []string{"APP", "service_name", "detected_level"}},
		{[]string{"a", "A"}, up, []string{"A"}},
		{[]string{"a"}, func(string) string { return " " }, []string{}},
		{nil, nil, []string{}},
	} {
		if got := NormalizeByLabels(c.in, c.fn); !reflect.DeepEqual(got, c.want) {
			t.Errorf("NormalizeByLabels(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if got := JoinByLabels([]string{"b", "a", ""}, []string{"a", "_stream", " b", "level"}); !reflect.DeepEqual(got, []string{"b", "a", "_stream", "level"}) {
		t.Errorf("JoinByLabels = %q", got)
	}
}

// A vector aggregation over another one keeps its own grouping labels that the series below have: a by list is cut
// to them in its own order, a without list is subtracted, and no clause keeps none.
// conformance: architecture-syntax-package-layering
func TestLevelKeeps(t *testing.T) {
	for _, c := range []struct {
		kept, labels []string
		without      bool
		want         []string
	}{
		{[]string{"a", "b"}, []string{"b", "a", "c"}, false, []string{"b", "a"}},
		{[]string{"a", "b"}, []string{"a", "a"}, false, []string{"a"}},
		{[]string{"a", "b"}, nil, false, nil},
		{nil, []string{"a"}, false, nil},
		{[]string{"a", "b", "c"}, []string{"b", "x"}, true, []string{"a", "c"}},
		{[]string{"a", "b"}, nil, true, []string{"a", "b"}},
		{nil, []string{"a"}, true, nil},
	} {
		if got := LevelKeeps(c.kept, c.labels, c.without); !reflect.DeepEqual(got, c.want) {
			t.Errorf("LevelKeeps(%v, %v, %v) = %v, want %v", c.kept, c.labels, c.without, got, c.want)
		}
	}
}
