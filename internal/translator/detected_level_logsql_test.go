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
		// VictoriaLogs before v1.50 stores the OTLP severity as one field, with
		// no number: "Unspecified" carries no level. Every other level field,
		// not only the ones ranked before it, keeps a row in.
		{[]string{"severity", "level", "lvl"}, `(level:* OR severity:* OR lvl:*) !(level:"" lvl:"" severity:="Unspecified" -severity_number:*)`},
		{[]string{"severity_number", "severity_text", "Severity_Text"}, `(severity_text:* OR Severity_Text:* OR severity_number:*) !(Severity_Text:"" severity_text:="Unspecified" severity_number:="0")`},
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
		// The names of a number's family, whatever the number's place in it.
		"(severity_text:~`^Trace[234]?$` severity_number:~`^(1|2|3|4)$`)",
		"(severity_text:~`^Warn[234]?$` severity_number:~`^(13|14|15|16)$`)",
		"(severity_text:=\"Unspecified\" severity_number:~`^(-[1-9][0-9]*|2[5-9]|[3-9][0-9]|[1-9][0-9]{2,})$`)",
		`!(severity_text:="Unspecified" severity_number:="0")`,
		"severity_number:~`^1[3-6]$`",
		`(severity_text:"" OR (((severity_text:~`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("filter lacks %q:\n%s", want, got)
		}
	}
}

// VictoriaLogs before v1.50 stores an OTLP record's severity as one field named
// severity and no number: its finer names (Info2) mean the coarse level, and
// Unspecified means none, so neither is a raw level word.
func TestBuildStoredLevelFilterSynthesizedSeverityName(t *testing.T) {
	got, err := BuildStoredLevelFilter("=", "info", []string{"severity"})
	if err != nil {
		t.Fatal(err)
	}
	want := "severity:~`(?i)^(info|inf|information)$`"
	if !strings.Contains(got, want) || !strings.Contains(got, "(-severity_number:* severity:~`^(Info)[234]?$`)") {
		t.Fatalf("filter lacks the derived names:\n%s", got)
	}
	got, err = BuildStoredLevelFilter("=~", "Unspecified|Info2", []string{"severity"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "!(severity:~`^(Unspecified|(Trace|Debug|Info|Warn|Error|Fatal)[234]?)$` -severity_number:*)") {
		t.Fatalf("a derived name matched as a raw word:\n%s", got)
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

// conformance: severity-detected-level-derivation
func TestBuildDetectedLevelChain(t *testing.T) {
	chain := BuildDetectedLevelChain(nil, DetectedLevelChainGroup)
	pipes := strings.Split(strings.TrimPrefix(chain, "| "), " | ")
	// 16 stored picks + 1 stored copy + 2 unpack + 28 body picks + 7
	// normalisers + 1 keyword regexp + 3 keyword finals + 1 stored word +
	// 7 severity numbers + 1 severity fallback + unknown + delete.
	if len(pipes) != 68 {
		t.Fatalf("chain has %d pipes, want 68:\n%s", len(pipes), chain)
	}
	for i, want := range map[int]string{
		0:  `format if (detected_level:*) "<detected_level>" as __dl_s keep_original_fields`,
		1:  `format if (level:*) "<level>" as __dl_s keep_original_fields`,
		14: `format if (SEVERITY_TEXT:*) "<SEVERITY_TEXT>" as __dl_s keep_original_fields`,
		15: `format if (__dl_s:*) "<__dl_s>" as __dl_v keep_original_fields`,
		16: `unpack_json from _msg fields (level, LEVEL, Level, log.level, severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT) result_prefix "__j_"`,
		// Lines Loki reads as JSON (a leading brace) never go to logfmt.
		17: "unpack_logfmt if (-_msg:~`^[\\s\\v\\x{85}\\p{Z}]*\\{`) from _msg fields (level, LEVEL, Level, log.level, severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT) result_prefix \"__l_\"",
		// A severity text VictoriaLogs derived from the OTLP number is no level
		// field: Unspecified and the finer names (Info2) of v1.47 (severity,
		// no number) and the text matching its number of v1.50+ (severity_text).
		5:  "format if (severity:* !(severity:~`^(Unspecified|(Trace|Debug|Info|Warn|Error|Fatal)[234]?)$` -severity_number:*)) \"<severity>\" as __dl_s keep_original_fields",
		18: `format if (__j_level:*) "<__j_level>" as __dl_v keep_original_fields`,
		45: `format if (__l_SEVERITY_TEXT:*) "<__l_SEVERITY_TEXT>" as __dl_v keep_original_fields`,
		46: "format if (__dl_v:~`(?i)^(trace|trc)$`) \"trace\" as __dl keep_original_fields",
		52: "format if (__dl_v:~`(?i)^(fatal)$`) \"fatal\" as __dl keep_original_fields",
		53: "extract_regexp if (__dl:\"\") `(?i)(?:^|[ \\t\\n\\[({\"=])(?P<__dl_e>trace|debug|fatal|critical|error|err|warning|warn|info)(?:$|[ \\t\\n\\[\\](){}:,!\"=])` from _msg",
		57: `format if (__dl:"" __dl_s:*) "<__dl_s>" as __dl keep_original_fields`,
		66: `format "unknown" as __dl keep_original_fields`,
		67: `delete __dl_*, __j_*, __l_*`,
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
		// A row with a level field is marked once, and skipped by the pipes
		// that read the line.
		`| format if (__dl_s:*) "1" as __dl_h | format if (__dl_s:*) "<__dl_s>" as __dl_v`,
		`| unpack_json if (-__dl_h:*) from _msg fields (`,
		"| unpack_logfmt if (-__dl_h:* -_msg:~`^[\\s\\v\\x{85}\\p{Z}]*\\{`) from _msg fields (",
		"| extract_regexp if (-__dl_h:* __dl:\"\") `",
		`| format if (-__dl_h:*) "<__dl>" as detected_level | delete __dl, __dl_*, __j_*, __l_*`,
	} {
		if !strings.Contains(stored, want) {
			t.Fatalf("chain with stored level lacks %q", want)
		}
	}

	// The OTLP severity of VictoriaLogs v1.50+ (severity_text, severity_number)
	// and before (severity alone): a row decides by a level text that is not
	// synthesised, a number other than 0, or a finer severity name.
	otlp := BuildDetectedLevelChain([]string{"severity_text", "severity_number", "severity"}, DetectedLevelChainGroup)
	for _, want := range []string{
		`| format if (-__dl_h:* severity_number:* !(severity_number:="0" severity_text:="Unspecified")) "1" as __dl_h`,
		"| format if (-__dl_h:* (severity:~`^(Unspecified|(Trace|Debug|Info|Warn|Error|Fatal)[234]?)$` -severity_number:*) -severity:=\"Unspecified\") \"1\" as __dl_h",
	} {
		if !strings.Contains(otlp, want) {
			t.Fatalf("OTLP chain lacks %q", want)
		}
	}
}

// Loki groups metric queries by the detected_level it derived at ingest, not
// by a stored level field, so the translation carries the derivation chain and
// groups by the value it writes.
//
// conformance: severity-exposure-surfaces, severity-detected-level-derivation
func TestMetricGroupingDerivesDetectedLevel(t *testing.T) {
	got, err := TranslateLogQLWithLabels(`sum by (detected_level) (count_over_time({app="x"}[5m]))`, nil)
	if err != nil {
		t.Fatalf("TranslateLogQLWithLabels() error = %v", err)
	}
	if !strings.HasPrefix(got, `app:="x" | format if (detected_level:*) "<detected_level>" as __dl_s keep_original_fields`) {
		t.Fatalf("grouping query does not start with the derivation chain: %q", got)
	}
	if !strings.HasSuffix(got, `| format "<__dl>" as detected_level | delete __dl, __dl_*, __j_*, __l_* | stats by (detected_level) count()`) {
		t.Fatalf("grouping query does not group by the derived value: %q", got)
	}

	// A query that does not group by detected_level keeps its cost: no chain.
	plain, err := TranslateLogQLWithLabels(`sum by (app) (count_over_time({app="x"}[5m]))`, nil)
	if err != nil {
		t.Fatalf("TranslateLogQLWithLabels() error = %v", err)
	}
	if plain != `app:="x" | stats by (app) count()` {
		t.Fatalf("unrelated grouping changed: %q", plain)
	}
}
