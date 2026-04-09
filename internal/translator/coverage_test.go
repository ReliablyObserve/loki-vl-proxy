package translator

import "testing"

func TestCoverage_ParseWithoutMarker(t *testing.T) {
	clean, labels := ParseWithoutMarker(`app:=api` + WithoutMarkerSuffix + `pod,node`)
	if clean != `app:=api` {
		t.Fatalf("unexpected clean query: %q", clean)
	}
	if len(labels) != 2 || labels[0] != "pod" || labels[1] != "node" {
		t.Fatalf("unexpected labels: %#v", labels)
	}

	clean, labels = ParseWithoutMarker(`app:=api`)
	if clean != `app:=api` || labels != nil {
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
	got := addByClause("app:=nginx", "app", nil)
	if got != "app:=nginx | stats by (app)" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_AddByClause_WithStatsAndTranslator(t *testing.T) {
	labelFn := func(label string) string {
		if label == "service_name" {
			return "service.name"
		}
		return label
	}
	got := addByClause(`app:=nginx | stats count(*) as hits`, "service_name, cluster", labelFn)
	want := `app:=nginx | stats by (service.name, cluster) count(*) as hits`
	if got != want {
		t.Fatalf("addByClause returned %q, want %q", got, want)
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

	got := addByClause(`service.name:=otel-app | stats count()`, "level, detected_level", labelFn)
	want := `service.name:=otel-app | stats by (level) count()`
	if got != want {
		t.Fatalf("addByClause returned %q, want %q", got, want)
	}
}
