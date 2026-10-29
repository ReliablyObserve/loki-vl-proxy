package translator

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
)

func TestCoverage_ParseWithoutMarker(t *testing.T) {
	clean, op, labels := ParseWithoutMarker(`app:="api"` + WithoutMarkerSuffix + `pod,node`)
	if op != "" {
		t.Fatalf("a marker without an operator has op %q", op)
	}
	if clean != `app:="api"` {
		t.Fatalf("unexpected clean query: %q", clean)
	}
	if len(labels) != 2 || labels[0] != "pod" || labels[1] != "node" {
		t.Fatalf("unexpected labels: %#v", labels)
	}

	clean, op, labels = ParseWithoutMarker(`app:="api"` + WithoutMarkerSuffix + `max:pod,node`)
	if clean != `app:="api"` || op != "max" || len(labels) != 2 || labels[0] != "pod" || labels[1] != "node" {
		t.Fatalf("marker with an operator: clean=%q op=%q labels=%#v", clean, op, labels)
	}

	clean, op, labels = ParseWithoutMarker(`app:="api"`)
	if clean != `app:="api"` || op != "" || labels != nil {
		t.Fatalf("expected unchanged query without marker, got clean=%q labels=%#v", clean, labels)
	}
}

func TestCoverage_ParseBinaryMetricExprFull(t *testing.T) {
	expr := BinaryMetricPrefix + `+:left_query|||right_query@@@on:job, instance@@@group_left:pod`
	op, left, right, vm, ok := ParseBinaryMetricExprFull(expr)
	if !ok {
		t.Fatal("expected binary metric expr to parse")
	}
	if op != "+" || left != "left_query" || right != "right_query" {
		t.Fatalf("unexpected parse result: op=%q left=%q right=%q", op, left, right)
	}
	if len(vm.On) != 2 || vm.On[0] != "job" || vm.On[1] != "instance" {
		t.Fatalf("unexpected on labels: %#v", vm.On)
	}
	if len(vm.GroupLeft) != 1 || vm.GroupLeft[0] != "pod" {
		t.Fatalf("unexpected group_left labels: %#v", vm.GroupLeft)
	}

	if _, _, _, _, ok := ParseBinaryMetricExprFull("rate({app=\"api\"}[5m])"); ok {
		t.Fatal("expected non-prefixed expression to fail parsing")
	}
}

func TestCoverage_SplitLabels(t *testing.T) {
	got := splitLabels(" job, instance ,, pod ")
	if len(got) != 3 || got[0] != "job" || got[1] != "instance" || got[2] != "pod" {
		t.Fatalf("unexpected split labels: %#v", got)
	}
}

// Coverage gap: extractQuotedValue edge cases
func TestCoverage_ExtractQuotedValue(t *testing.T) {
	tests := []struct {
		input    string
		wantVal  string
		wantRest string
	}{
		{`"hello" world`, `"hello"`, "world"},
		{`unquoted rest`, `"unquoted"`, "rest"},
		{`single`, `"single"`, ""},
		{`word | pipe`, `"word"`, "| pipe"},
		{`  "spaced"  `, `"spaced"`, ""},
	}
	for _, tc := range tests {
		val, rest := extractQuotedValue(tc.input)
		if val != tc.wantVal {
			t.Errorf("extractQuotedValue(%q) val = %q, want %q", tc.input, val, tc.wantVal)
		}
		if rest != tc.wantRest {
			t.Errorf("extractQuotedValue(%q) rest = %q, want %q", tc.input, rest, tc.wantRest)
		}
	}
}

// Coverage gap: extractUnwrapField edge cases
func TestCoverage_ExtractUnwrapField(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"no unwrap here", ""},
		{"| unwrap duration", "duration"},
		{"| unwrap duration | other", "duration"},
		{"| unwrap bytes_total [5m]", "bytes_total"},
		{"| unwrap  spaced_field ", "spaced_field"},
	}
	for _, tc := range tests {
		got := extractUnwrapField(tc.input)
		if got != tc.want {
			t.Errorf("extractUnwrapField(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Coverage gap: translateBareFilter
func TestCoverage_TranslateBareFilter(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"hello", `"hello"`},
		{"  spaced  ", `"spaced"`},
	}
	for _, tc := range tests {
		got := translateBareFilter(tc.input)
		if got != tc.want {
			t.Errorf("translateBareFilter(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Coverage gap: addByClause when no stats pipe exists
func TestCoverage_AddByClause_NoStats(t *testing.T) {
	got := addByClause(`app:="nginx"`, "app", nil)
	if got != `app:="nginx" | stats by (app)` {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_AddByClause_WithStatsAndTranslator(t *testing.T) {
	labelFn := func(label string) string {
		if label == "k8s_cluster_name" {
			return "k8s.cluster.name"
		}
		return label
	}
	// Labels are translated to VictoriaLogs fields, except service_name, which
	// the grouping derives per row.
	got := addByClause(`app:="nginx" | stats count(*) as hits`, "service_name, k8s_cluster_name", labelFn)
	want := `app:="nginx" | stats by (service_name, k8s.cluster.name) count(*) as hits`
	if got != want {
		t.Fatalf("addByClause returned %q, want %q", got, want)
	}
}

// TestDecolorizePipelineStage verifies that | decolorize is translated to the
// typed logsql.PipeDecolorize node string ("| decolorize") and passes through
// in the full translator output so the proxy can apply post-processing.
func TestDecolorizePipelineStage(t *testing.T) {
	got, err := TranslateLogQL(`{app="nginx"} | decolorize`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "| decolorize") {
		t.Fatalf("expected '| decolorize' in translated output, got: %q", got)
	}
}

// TestIPFilterTranslation verifies that | ip("cidr") stages emit the VL-native
// ipv4_range() for IPv4, whatever capabilities are known (an unknown backend
// version has none), and the regexp approximation only for IPv6.
func TestIPFilterTranslation(t *testing.T) {
	for name, caps := range map[string]logsql.Capabilities{
		"unknown version": {},
		"v1.52.0":         logsql.CapabilitiesFor("v1.52.0"),
	} {
		got := translatePipelineStage(`ip("192.168.0.0/16")`, nil, caps)
		if got != `| filter _msg:ipv4_range(192.168.0.0, 192.168.255.255)` {
			t.Fatalf("%s: expected native ipv4_range() filter, got: %q", name, got)
		}
	}

	got := translatePipelineStage(`ip("2001:db8::/32")`, nil, logsql.Capabilities{})
	if !strings.HasPrefix(got, "| filter _msg:~") {
		t.Fatalf("expected regexp fallback for an IPv6 ip() filter, got: %q", got)
	}
}

func TestCoverage_AddByClause_DeduplicatesTranslatedLabels(t *testing.T) {
	labelFn := func(label string) string {
		switch label {
		case "detected_level":
			return "level"
		default:
			return label
		}
	}

	// Loki answers `by (level, detected_level)` with both labels: the raw
	// stored level and the value it derived, so neither is folded away.
	got := addByClause(`service.name:="otel-app" | stats count()`, "level, detected_level", labelFn)
	want := `service.name:="otel-app" | stats by (level, detected_level) count()`
	if got != want {
		t.Fatalf("addByClause returned %q, want %q", got, want)
	}
}

// A marker appended after the without marker ends its label list and stays on
// the query, so the label_replace marker of a
// `label_replace(sum without (pod) (...), ...)` still reaches its parser.
func TestParseWithoutMarkerBeforeOtherMarkers(t *testing.T) {
	for _, tail := range []string{"__lvp_lr:YQBiAGMAZA==__"} {
		clean, _, labels := ParseWithoutMarker(`app:="api"` + WithoutMarkerSuffix + `pod,node` + tail)
		if clean != `app:="api"`+tail || len(labels) != 2 || labels[0] != "pod" || labels[1] != "node" {
			t.Errorf("tail %q: clean=%q labels=%v, want the marker kept and the labels pod,node", tail, clean, labels)
		}
	}
}
