package translator

import (
	"strings"
	"testing"
)

func TestDetectedLevelStoredFieldsKeepsPriorityOrder(t *testing.T) {
	got := DetectedLevelStoredFields([]string{"msg", "severity_number", "lvl", "level", "detected_level", "app"})
	want := []string{"detected_level", "level", "lvl", "severity_number"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stored fields = %v, want %v", got, want)
	}
	if len(DetectedLevelFieldNames) != 16 {
		t.Fatalf("field list has %d names, want detected_level + 14 Loki names + severity_number", len(DetectedLevelFieldNames))
	}
}

func TestBuildHasLevelFilter(t *testing.T) {
	cases := []struct {
		fields []string
		want   string
	}{
		{nil, ""},
		{[]string{"app", "msg"}, ""},
		{[]string{"x", "level"}, "level:*"},
		{[]string{"severity_number", "severity_text", "log.level"}, `(log.level:* OR severity_text:* OR severity_number:*) !(log.level:"" severity_text:="Unspecified" severity_number:="0")`},
	}
	for _, tc := range cases {
		if got := BuildHasLevelFilter(tc.fields); got != tc.want {
			t.Fatalf("BuildHasLevelFilter(%v) = %q, want %q", tc.fields, got, tc.want)
		}
	}
}

func TestBuildStoredLevelFilter(t *testing.T) {
	known := "`(?i)^(trace|trc|debug|dbg|info|inf|information|warn|wrn|warning|error|err|critical|fatal)$`"
	cases := []struct {
		op, value string
		fields    []string
		want      string
	}{
		{"=", "warn", []string{"level"}, "level:~`(?i)^(warn|wrn|warning)$`"},
		{"!=", "warn", []string{"level"}, "level:~`(?i)^(warn|wrn|warning)$`"},
		{"=", "notice", []string{"lvl", "level"}, `(level:="notice" OR level:"" lvl:="notice")`},
		{"=~", "warn|notice", []string{"level"}, "(level:~`(?i)^(warn|wrn|warning)$` OR (level:~`^(?:warn|notice)$` !level:~" + known + "))"},
		// A known spelling that is not canonical never survives normalisation.
		{"=", "WARN", []string{"level"}, ""},
		{"=", "info", []string{"severity_number"}, "(severity_number:~`^(9|1[0-2])$` OR (severity_number:* !severity_number:~`^-?[0-9]+$`))"},
		{"=", "unknown", []string{"severity_number", "detected_level"}, "(detected_level:=\"unknown\" OR detected_level:\"\" severity_number:~`^(-?0|2[5-9]|[3-9][0-9]|[1-9][0-9]{2,})$`)"},
		{"=", "warn", nil, ""},
	}
	for _, tc := range cases {
		got, err := BuildStoredLevelFilter(tc.op, tc.value, tc.fields)
		if err != nil {
			t.Fatalf("%s %q: %v", tc.op, tc.value, err)
		}
		if got != tc.want {
			t.Fatalf("BuildStoredLevelFilter(%s, %q, %v)\n got %s\nwant %s", tc.op, tc.value, tc.fields, got, tc.want)
		}
	}
}

func TestBuildStoredLevelFilterOTelSynthesizedText(t *testing.T) {
	got, err := BuildStoredLevelFilter("=", "warn", []string{"severity_text", "severity_number"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"severity_text:~`(?i)^(warn|wrn|warning)$`",
		`!((severity_text:="Trace" severity_number:="1") OR `,
		`(severity_text:="Warn" severity_number:="13")`,
		`!(severity_text:="Unspecified" severity_number:="0")`,
		"severity_number:~`^1[3-6]$`",
		`(severity_text:"" OR (((severity_text:="Trace" severity_number:="1")`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("filter lacks %q:\n%s", want, got)
		}
	}
}

func TestBuildStoredLevelFilterRejectsBadMatchers(t *testing.T) {
	if _, err := BuildStoredLevelFilter("=~", "(", nil); err == nil {
		t.Fatal("invalid regexp accepted")
	}
	if _, err := BuildDetectedLevelFilter(">", "1", nil); err == nil {
		t.Fatal("unsupported operator accepted")
	}
}

func TestBuildDetectedLevelFilterPlans(t *testing.T) {
	cases := []struct {
		name      string
		op, value string
		fields    []string
		never     bool
		prefilter string
		filter    string
	}{
		{name: "empty value matches nothing", op: "=", value: "", never: true},
		{name: "not empty matches everything", op: "!=", value: ""},
		{name: "non-canonical spelling matches nothing", op: "=", value: "WARN", never: true},
		{name: "non-canonical spelling negated matches everything", op: "!=", value: "WARN"},
		{
			name: "canonical value with a stored level column", op: "=", value: "error", fields: []string{"level"},
			prefilter: "((level:*) OR i(error) OR i(err))",
			filter:    "| filter (level:~`(?i)^(error|err)$` OR (!(level:*) __dl:in(\"error\")))",
		},
		{
			name: "canonical value without stored levels", op: "=", value: "warn",
			prefilter: "(i(warn) OR i(wrn) OR i(warning))",
			filter:    "| filter (__dl:in(\"warn\"))",
		},
		{
			name: "negated regexp", op: "!~", value: "warn|info", fields: []string{"level"},
			filter: "| filter !((level:~`(?i)^(info|inf|information|warn|wrn|warning)$` OR (level:~`^(?:warn|info)$` !level:~`(?i)^(trace|trc|debug|dbg|info|inf|information|warn|wrn|warning|error|err|critical|fatal)$`)) OR (!(level:*) __dl:in(\"info\", \"warn\")))",
		},
		{
			name: "unknown cannot be prefiltered", op: "=~", value: "unk.*",
			filter: "| filter (__dl:in(\"unknown\"))",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := BuildDetectedLevelFilter(tc.op, tc.value, tc.fields)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Never != tc.never || plan.Prefilter != tc.prefilter {
				t.Fatalf("plan = never %v prefilter %q, want never %v prefilter %q", plan.Never, plan.Prefilter, tc.never, tc.prefilter)
			}
			if tc.filter == "" {
				if plan.Pipes != "" {
					t.Fatalf("unexpected pipes %q", plan.Pipes)
				}
				return
			}
			chain := BuildDetectedLevelChain(tc.fields, DetectedLevelChainGroup)
			if plan.Pipes != chain+" "+tc.filter {
				t.Fatalf("pipes tail = %q, want chain + %q", strings.TrimPrefix(plan.Pipes, chain), tc.filter)
			}
		})
	}
}

func TestBuildDetectedLevelChain(t *testing.T) {
	chain := BuildDetectedLevelChain(nil, DetectedLevelChainGroup)
	pipes := strings.Split(strings.TrimPrefix(chain, "| "), " | ")
	// 2 unpack + 28 picks + 7 normalisers + 1 keyword regexp + 4 finals + delete.
	if len(pipes) != 43 {
		t.Fatalf("chain has %d pipes, want 43:\n%s", len(pipes), chain)
	}
	for i, want := range map[int]string{
		0:  `unpack_json from _msg fields (level, LEVEL, Level, log.level, severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT) result_prefix "__j_"`,
		1:  `unpack_logfmt from _msg fields (level, LEVEL, Level, log.level, severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT) result_prefix "__l_"`,
		2:  `format if (__j_level:*) "<__j_level>" as __dl_v keep_original_fields`,
		29: `format if (__l_SEVERITY_TEXT:*) "<__l_SEVERITY_TEXT>" as __dl_v keep_original_fields`,
		30: "format if (__dl_v:~`(?i)^(trace|trc)$`) \"trace\" as __dl keep_original_fields",
		36: "format if (__dl_v:~`(?i)^(fatal)$`) \"fatal\" as __dl keep_original_fields",
		37: "extract_regexp if (__dl:\"\") `(?i)(?:^|[ \\t\\n\\[({\"=])(?P<__dl_e>trace|debug|fatal|critical|error|err|warning|warn|info)(?:$|[ \\t\\n\\[\\](){}:,!\"=])` from _msg",
		41: `format "unknown" as __dl keep_original_fields`,
		42: `delete __dl_*, __j_*, __l_*`,
	} {
		if pipes[i] != want {
			t.Fatalf("pipe %d = %q, want %q", i, pipes[i], want)
		}
	}

	logs := BuildDetectedLevelChain(nil, DetectedLevelChainLogs)
	if !strings.HasSuffix(logs, `| format "unknown" as __dl keep_original_fields | rename __dl as detected_level | delete __dl, __dl_*, __j_*, __l_*`) {
		t.Fatalf("logs chain ending: %s", logs[len(logs)-160:])
	}

	stored := BuildDetectedLevelChain([]string{"level", "app"}, DetectedLevelChainLogs)
	for _, want := range []string{
		`| unpack_json if (!(level:*)) from _msg fields (`,
		`| unpack_logfmt if (!(level:*)) from _msg fields (`,
		"| extract_regexp if (!(level:*) __dl:\"\") `",
		`| format if (!(level:*)) "<__dl>" as detected_level | delete __dl, __dl_*, __j_*, __l_*`,
	} {
		if !strings.Contains(stored, want) {
			t.Fatalf("chain with stored level lacks %q", want)
		}
	}
}
