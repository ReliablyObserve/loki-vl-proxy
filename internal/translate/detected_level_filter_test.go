package translate

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// levelCond is a drop condition on detected_level.
type levelCond struct{ op, operand string }

func (c levelCond) Matches(v string) bool {
	switch c.op {
	case "=":
		return v == c.operand
	case "!=":
		return v != c.operand
	}
	return false
}
func (c levelCond) Operator() string { return c.op }
func (c levelCond) Operand() string  { return c.operand }

// A stored level normalises to its canonical level in any case; any other word stays as stored.
// conformance: severity-detected-level-derivation
func TestNormalizeStoredLevel(t *testing.T) {
	for in, want := range map[string]string{
		"WARNING": "warn", "warn": "warn", "Wrn": "warn", "inf": "info", "Information": "info", "Err": "error", "ERROR": "error",
		"dbg": "debug", "TRC": "trace", "critical": "critical", "Fatal": "fatal",
		"verbose": "verbose", "": "", "unknown": "unknown", "emerg": "emerg",
	} {
		if got := NormalizeStoredLevel(in); got != want {
			t.Errorf("NormalizeStoredLevel(%q) = %q, want %q", in, got, want)
		}
	}
}

// The OTel severity numbers 1..24 map to their six families, 0 and everything above 24 to unknown.
// conformance: severity-detected-level-derivation
func TestSeverityNumberRegexps(t *testing.T) {
	levelOf := func(n int) string {
		var got []string
		for _, level := range DetectedLevelCanonical() {
			re, ok := SeverityNumberRegexp(level)
			if ok && regexp.MustCompile(re).MatchString(strconv.Itoa(n)) {
				got = append(got, level)
			}
		}
		if len(got) != 1 {
			t.Errorf("severity_number %d maps to %v, want exactly one level", n, got)
			return ""
		}
		return got[0]
	}
	for n := 0; n <= 120; n++ {
		want := "unknown"
		switch {
		case n >= 1 && n <= 4:
			want = "trace"
		case n >= 5 && n <= 8:
			want = "debug"
		case n >= 9 && n <= 12:
			want = "info"
		case n >= 13 && n <= 16:
			want = "warn"
		case n >= 17 && n <= 20:
			want = "error"
		case n >= 21 && n <= 24:
			want = "fatal"
		}
		if got := levelOf(n); got != want {
			t.Errorf("severity_number %d = %q, want %q", n, got, want)
		}
	}
	if _, ok := SeverityNumberRegexp("critical"); ok {
		t.Error("no severity number maps to critical")
	}
	// The families are the OTel names VictoriaLogs derives from the numbers.
	fams := SeverityFamilies()
	if len(fams) != 6 || fams[0] != (SeverityFamily{Stem: "Trace", Lo: 1, Hi: 4}) || fams[5] != (SeverityFamily{Stem: "Fatal", Lo: 21, Hi: 24}) {
		t.Errorf("SeverityFamilies = %+v", fams)
	}
}

// conformance: severity-detected-level-derivation
func TestDetectedLevelTables(t *testing.T) {
	spellings := DetectedLevelSpellings()
	if len(spellings) != 7 || spellings[0].Level != "trace" || !slices.Equal(spellings[3].Spellings, []string{"warn", "wrn", "warning"}) {
		t.Errorf("DetectedLevelSpellings = %+v", spellings)
	}
	if got := DetectedLevelCanonical(); !slices.Equal(got, []string{"trace", "debug", "info", "warn", "error", "critical", "fatal", "unknown"}) {
		t.Errorf("DetectedLevelCanonical = %v", got)
	}
	if got := LevelKeywords(); !slices.Equal(got, []string{"trace", "debug", "fatal", "critical", "error", "err", "warning", "warn", "info"}) {
		t.Errorf("LevelKeywords = %v", got)
	}
	re := regexp.MustCompile(LevelKeywordRegexp())
	for line, want := range map[string]string{
		"an error happened": "error", "level=WARN x": "WARN", `{"m":"info"}`: "info", "[debug] x": "debug", "x err": "err",
		"terror": "", "errors": "", "no keyword": "", "informational": "",
	} {
		m := re.FindStringSubmatch(line)
		got := ""
		if m != nil {
			got = m[re.SubexpIndex("__dl_e")]
		}
		if got != want {
			t.Errorf("keyword of %q = %q, want %q", line, got, want)
		}
	}
	if got, want := LevelKeywordGate(), `_msg:(i(trace) OR i(debug) OR i(fatal) OR i(critical) OR i(error) OR i(err) OR i(warning) OR i(warn) OR i(info))`; got != want {
		t.Errorf("LevelKeywordGate = %s", got)
	}
	if got, want := DetectedLevelBodyKeys(), DetectedLevelFieldNames[1:len(DetectedLevelFieldNames)-1]; !slices.Equal(got, want) {
		t.Errorf("DetectedLevelBodyKeys = %v", got)
	}
	if got := DetectedLevelStoredFields([]string{"foo", "severity_number", "level", "detected_level"}); !slices.Equal(got, []string{"detected_level", "level", "severity_number"}) {
		t.Errorf("DetectedLevelStoredFields keeps the priority order, got %v", got)
	}
}

// What a filter on detected_level accepts: the canonical values, and the raw words Loki keeps as they are.
// conformance: severity-detected-level-derivation
func TestMatchDetectedLevel(t *testing.T) {
	for _, c := range []struct {
		op, value string
		targets   []string
		negated   bool
		regexp    bool
		raws      []string
		anyRaw    bool
		err       string
	}{
		{op: "=", value: "error", targets: []string{"error"}},
		{op: "=", value: "ERROR"}, // a label comparison is case-sensitive
		{op: "!=", value: "info", targets: []string{"info"}, negated: true},
		{op: "=~", value: "err.*", targets: []string{"error"}, regexp: true, anyRaw: true},
		{op: "=~", value: "warn|error", targets: []string{"warn", "error"}, regexp: true},
		{op: "=~", value: ".*", targets: []string{"trace", "debug", "info", "warn", "error", "critical", "fatal", "unknown"}, regexp: true, anyRaw: true},
		{op: "!~", value: "debug", targets: []string{"debug"}, negated: true, regexp: true},
		{op: "=", value: ""},
		{op: "=", value: "unknown", targets: []string{"unknown"}},
		{op: "=", value: "weird", raws: []string{"weird"}},
		{op: "=~", value: "weird|odd", regexp: true, raws: []string{"weird", "odd"}},
		{op: "=~", value: "(", err: `invalid detected_level regexp "("`},
		{op: ">", value: "x", err: `unsupported detected_level matcher ">"`},
	} {
		m, err := MatchDetectedLevel(c.op, c.value)
		if c.err != "" {
			if err == nil || !strings.HasPrefix(err.Error(), c.err) {
				t.Errorf("%s %q: err = %v, want prefix %s", c.op, c.value, err, c.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %q: %v", c.op, c.value, err)
			continue
		}
		if !slices.Equal(m.Targets, c.targets) && (len(m.Targets) != 0 || len(c.targets) != 0) {
			t.Errorf("%s %q: targets %v, want %v", c.op, c.value, m.Targets, c.targets)
		}
		if m.Negated != c.negated || m.Regexp != c.regexp {
			t.Errorf("%s %q: negated %v regexp %v", c.op, c.value, m.Negated, m.Regexp)
		}
		raws, anyRaw := DetectedLevelRawValues(m)
		if anyRaw != c.anyRaw || (len(raws) != 0 || len(c.raws) != 0) && !slices.Equal(raws, c.raws) {
			t.Errorf("%s %q: raws %v any %v, want %v %v", c.op, c.value, raws, anyRaw, c.raws, c.anyRaw)
		}
	}
}

// conformance: severity-detected-level-derivation
func TestBuildHasLevelFilter(t *testing.T) {
	for _, c := range []struct {
		fields []string
		want   string
	}{
		{nil, ""},
		{[]string{"app"}, ""},
		{[]string{"level"}, "level:*"},
		{[]string{"level", "severity_number"}, "(level:* OR severity_number:*)"},
		{[]string{"detected_level", "lvl"}, "(detected_level:* OR lvl:*)"},
		// An OTLP record VictoriaLogs gave the text Unspecified for severity 0 carries no level for Loki.
		{[]string{"severity_text", "severity_number"}, `(severity_text:* OR severity_number:*) !(severity_text:="Unspecified" severity_number:="0")`},
		{[]string{"severity"}, `severity:* !(severity:="Unspecified" -severity_number:*)`},
	} {
		if got := BuildHasLevelFilter(c.fields); got != c.want {
			t.Errorf("BuildHasLevelFilter(%v) = %q, want %q", c.fields, got, c.want)
		}
	}
}

// The filter of the stored level fields that decide a level the matcher accepts.
// conformance: severity-detected-level-derivation
func TestBuildStoredLevelFilter(t *testing.T) {
	for _, c := range []struct {
		op, value string
		fields    []string
		want      string
		contains  []string
	}{
		{"=", "error", nil, "", nil},
		{"=", "error", []string{"level"}, "level:~`(?i)^(error|err)$`", nil},
		{"!=", "error", []string{"level"}, "level:~`(?i)^(error|err)$`", nil}, // the caller negates the whole expression
		{"=", "weird", []string{"level"}, `level:="weird"`, nil},
		{"=", "unknown", []string{"level"}, `level:="unknown"`, nil},
		{"=", "info", []string{"severity"}, "", []string{"severity:~`(?i)^(info|inf|information)$`", "(-severity_number:* severity:~`^(Info)[234]?$`)"}},
		{"=~", "err.*", []string{"level", "severity_number"}, "", []string{"level:~`(?i)^(error|err)$`", "severity_number:~`^(1[7-9]|20)$`"}},
		{"=", "error", []string{"severity_text", "severity_number"}, "", []string{"severity_text:~`(?i)^(error|err)$`", `!(severity_text:="Unspecified" severity_number:="0")`, "severity_number:~`^(1[7-9]|20)$`"}},
		{"=", "info", []string{"severity_number"}, "", []string{"severity_number:~`^(9|1[0-2])$`", "(severity_number:* !severity_number:~`^-?[0-9]+$`)"}},
	} {
		got, err := BuildStoredLevelFilter(c.op, c.value, c.fields)
		if err != nil {
			t.Errorf("%s %q %v: %v", c.op, c.value, c.fields, err)
			continue
		}
		if c.want != "" || len(c.contains) == 0 {
			if got != c.want {
				t.Errorf("%s %q %v = %q, want %q", c.op, c.value, c.fields, got, c.want)
			}
		}
		for _, s := range c.contains {
			if !strings.Contains(got, s) {
				t.Errorf("%s %q %v = %q lacks %q", c.op, c.value, c.fields, got, s)
			}
		}
		if got != "" {
			mustParseLogsQL(t, got)
		}
	}
	if _, err := BuildStoredLevelFilter("~", "x", nil); err == nil || err.Error() != `unsupported detected_level matcher "~"` {
		t.Errorf("err = %v", err)
	}
}

// The plan of `| detected_level <op> "<value>"` in each mode.
// conformance: severity-detected-level-derivation
func TestBuildDetectedLevelFilterPlans(t *testing.T) {
	type want struct {
		never      bool
		prefilter  string
		match      string
		prefixPipe string // the prefilter pipe of the metric form
		hasPipes   bool
	}
	for _, c := range []struct {
		name      string
		mode      DetectedLevelFilterMode
		op, value string
		fields    []string
		want      want
	}{
		// `=""` matches no row: every row has a value. `!=""` matches every row.
		{"logs = empty", DetectedLevelFilterLogs, "=", "", nil, want{never: true}},
		{"logs != empty", DetectedLevelFilterLogs, "!=", "", nil, want{}},
		{"metric = empty", DetectedLevelFilterMetric, "=", "", nil, want{never: true}},
		{"stored = empty", DetectedLevelFilterStored, "=", "", nil, want{never: true}},
		// A regexp no value matches: no row for =~, every row for !~.
		{"logs =~ nomatch", DetectedLevelFilterLogs, "=~", "nomatch", nil, want{never: true}},
		{"logs !~ nomatch", DetectedLevelFilterLogs, "!~", "nomatch", nil, want{}},
		{"stored !~ nomatch", DetectedLevelFilterStored, "!~", "nomatch", nil, want{}},
		{"logs = error, stored level", DetectedLevelFilterLogs, "=", "error", []string{"level"},
			want{prefilter: "(level:(i(error) OR i(err)) OR (!(level:*) _msg:(i(error) OR i(err))))", match: `__dl:="error"`, hasPipes: true}},
		{"logs = error, no stored field", DetectedLevelFilterLogs, "=", "error", nil,
			want{prefilter: "((_msg:(i(error) OR i(err))))", match: `__dl:="error"`, hasPipes: true}},
		{"logs != error has no prefilter", DetectedLevelFilterLogs, "!=", "error", []string{"level"}, want{match: `-__dl:="error"`, hasPipes: true}},
		{"logs = unknown reads every row without a level", DetectedLevelFilterLogs, "=", "unknown", []string{"level"},
			want{prefilter: "(level:(i(unknown)) OR (!(level:*)))", match: `__dl:="unknown"`, hasPipes: true}},
		{"logs = unknown, no stored field: every row", DetectedLevelFilterLogs, "=", "unknown", nil, want{match: `__dl:="unknown"`, hasPipes: true}},
		{"logs =~ .*", DetectedLevelFilterLogs, "=~", ".*", nil, want{match: `__dl:~".*"`, hasPipes: true}},
		{"logs = raw word", DetectedLevelFilterLogs, "=", "weird", []string{"level"}, want{prefilter: `(level:="weird")`, match: `__dl:="weird"`, hasPipes: true}},
		{"logs =~ err.*", DetectedLevelFilterLogs, "=~", "err.*", []string{"level", "severity_number"},
			want{prefilter: "(level:* OR severity_number:* OR (!((level:* OR severity_number:*)) _msg:(i(error) OR i(err))))", match: `__dl:~"err"`, hasPipes: true}},
		{"metric = error", DetectedLevelFilterMetric, "=", "error", []string{"level"},
			want{match: `detected_level:="error"`, prefixPipe: "| filter (level:(i(error) OR i(err)) OR (!(level:*) _msg:(i(error) OR i(err))))", hasPipes: true}},
		{"metric != error", DetectedLevelFilterMetric, "!=", "error", []string{"level"}, want{match: `-detected_level:="error"`, hasPipes: true}},
		{"metric = unknown, no stored field", DetectedLevelFilterMetric, "=", "unknown", nil, want{match: `detected_level:="unknown"`, hasPipes: true}},
		{"stored = error, no stored field", DetectedLevelFilterStored, "=", "error", nil, want{never: true}},
		{"stored = error", DetectedLevelFilterStored, "=", "error", []string{"level"}, want{prefilter: "(level:~`(?i)^(error|err)$`)"}},
		{"stored != error", DetectedLevelFilterStored, "!=", "error", []string{"level"}, want{prefilter: "!(level:~`(?i)^(error|err)$`)"}},
		{"stored = unknown, no stored field: every row", DetectedLevelFilterStored, "=", "unknown", nil, want{}},
		{"stored != unknown, no stored field: no row", DetectedLevelFilterStored, "!=", "unknown", nil, want{never: true}},
		{"stored = unknown", DetectedLevelFilterStored, "=", "unknown", []string{"level"}, want{prefilter: `((level:="unknown" OR !(level:*)))`}},
		{"stored != unknown", DetectedLevelFilterStored, "!=", "unknown", []string{"level"}, want{prefilter: `!((level:="unknown" OR !(level:*)))`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, err := BuildDetectedLevelFilter(c.op, c.value, c.fields, c.mode)
			if err != nil {
				t.Fatal(err)
			}
			got := want{never: p.Never, prefilter: p.Prefilter, match: p.Match, prefixPipe: p.PrefilterPipe, hasPipes: p.Pipes != ""}
			if c.mode == DetectedLevelFilterMetric && c.want.prefixPipe != "" {
				// The prefilter of the metric form is a pipe of its own, not a filter on the selector.
				c.want.prefilter = ""
			}
			if got != c.want {
				t.Errorf("plan = %+v, want %+v", got, c.want)
			}
			if p.Pipes != "" {
				if !strings.HasPrefix(p.Pipes, "| ") {
					t.Errorf("pipes must start with a pipe: %.80s", p.Pipes)
				}
				mustParseLogsQL(t, "* "+p.Pipes)
				switch c.mode {
				case DetectedLevelFilterLogs:
					if !strings.HasSuffix(p.Pipes, " | filter "+p.Match+LevelChainEnd) {
						t.Errorf("the log form ends with the filter and the delete of the scratch fields: ...%s", p.Pipes[max(0, len(p.Pipes)-120):])
					}
				case DetectedLevelFilterMetric:
					if !strings.HasSuffix(p.Pipes, " | filter ("+p.Match+") OR "+LevelWitnessField+":*") {
						t.Errorf("the metric form lets the witness rows through: ...%s", p.Pipes[max(0, len(p.Pipes)-120):])
					}
				}
			}
			if p.Prefilter != "" {
				mustParseLogsQL(t, p.Prefilter)
			}
		})
	}
	if _, err := BuildDetectedLevelFilter("=~", "(", nil, DetectedLevelFilterLogs); err == nil || !strings.HasPrefix(err.Error(), `invalid detected_level regexp "("`) {
		t.Errorf("err = %v", err)
	}
	if _, err := BuildDetectedLevelFilter("~", "x", nil, DetectedLevelFilterLogs); err == nil {
		t.Error("an unsupported operator must fail")
	}
}

// Consecutive detected_level filters share one chain and one set of matchers.
// conformance: severity-detected-level-derivation
func TestJoinDetectedLevelFilters(t *testing.T) {
	fields := []string{"level"}
	plan := func(mode DetectedLevelFilterMode, op, value string) DetectedLevelFilterPlan {
		p, err := BuildDetectedLevelFilter(op, value, fields, mode)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	logsA, logsB := plan(DetectedLevelFilterLogs, "=", "error"), plan(DetectedLevelFilterLogs, "!=", "info")
	never := plan(DetectedLevelFilterLogs, "=", "")
	if got := JoinDetectedLevelFilters(nil, fields, DetectedLevelFilterLogs); got != "" {
		t.Errorf("no plans: %q", got)
	}
	if got := JoinDetectedLevelFilters([]DetectedLevelFilterPlan{never}, fields, DetectedLevelFilterLogs); got != "" {
		t.Errorf("a plan without pipes adds nothing: %q", got)
	}
	if got := JoinDetectedLevelFilters([]DetectedLevelFilterPlan{logsA, never}, fields, DetectedLevelFilterLogs); got != logsA.Pipes {
		t.Errorf("one plan with pipes is its own pipes")
	}
	joined := JoinDetectedLevelFilters([]DetectedLevelFilterPlan{logsA, logsB}, fields, DetectedLevelFilterLogs)
	if !strings.HasSuffix(joined, ` | filter (__dl:="error") (-__dl:="info")`+LevelChainEnd) {
		t.Errorf("joined log filters: ...%s", joined[max(0, len(joined)-120):])
	}
	if strings.Count(joined, "format if (detected_level:*)") != 1 {
		t.Error("the chain must run once")
	}
	mustParseLogsQL(t, "* "+joined)

	metricA, metricB := plan(DetectedLevelFilterMetric, "=", "error"), plan(DetectedLevelFilterMetric, "!=", "info")
	joinedMetric := JoinDetectedLevelFilters([]DetectedLevelFilterPlan{metricA, metricB}, fields, DetectedLevelFilterMetric)
	if !strings.HasPrefix(joinedMetric, metricA.PrefilterPipe+" ") || !strings.HasSuffix(joinedMetric, ` | filter ((detected_level:="error") (-detected_level:="info")) OR `+LevelWitnessField+`:*`) {
		t.Errorf("joined metric filters: %.120s ... %s", joinedMetric, joinedMetric[max(0, len(joinedMetric)-120):])
	}
	mustParseLogsQL(t, "* "+joinedMetric)
}

// The probe of a window's level fields reads the rows before the trailing filter pipes.
// conformance: severity-detected-level-derivation
func TestDetectedLevelProbeBase(t *testing.T) {
	for in, want := range map[string]string{
		`{app="x"}`:                          `{app="x"}`,
		`{app="x"} | filter level:error`:     `{app="x"}`,
		`{app="x"} | filter a | filter b`:    `{app="x"}`,
		`{a="x"} | extract "<a>" | filter b`: `{a="x"} | extract "<a>"`,
		`* | filter "a|b"`:                   `*`,
		`x | filter "a|b" | stats`:           `x | filter "a|b" | stats`,
		"":                                   "",
	} {
		if got := DetectedLevelProbeBase(in); got != want {
			t.Errorf("DetectedLevelProbeBase(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]int{
		"a | b": 2, `"a|b" | c`: 6, "`a|b` | c": 6, `"a\"|b" | c`: 8, "abc": -1, "": -1,
	} {
		if got := FirstTopLevelPipe(in); got != want {
			t.Errorf("FirstTopLevelPipe(%q) = %d, want %d", in, got, want)
		}
	}
}

// A stats query that reads the stored level keeps the rows a filter on detected_level can match, and every row the
// stored fields do not decide exactly.
// conformance: severity-detected-level-derivation
func TestBuildStoredLevelCopyFilter(t *testing.T) {
	known := "(?i)^(?:trace|trc|debug|dbg|info|inf|information|warn|wrn|warning|error|err|critical|fatal)$"
	if got := BuildStoredLevelCopyFilter[levelCond]("__s", "__m", nil); got != "" {
		t.Errorf("no filter: %q", got)
	}
	got := BuildStoredLevelCopyFilter("__s", "__m", []levelCond{{"=", "error"}})
	want := " | filter (__m:* OR -__s:* OR __s:~`(?i)^(?:error|err)$` OR (-__s:~`" + known + "` __s:=\"error\"))"
	if got != want {
		t.Errorf("= error:\n got  %s\n want %s", got, want)
	}
	got = BuildStoredLevelCopyFilter("__s", "__m", []levelCond{{"=~", "err.*"}})
	if want := " | filter (__m:* OR -__s:* OR (-__s:~`" + known + "` __s:~\"^(?:err.*)$\"))"; got != want {
		t.Errorf("=~: %s", got)
	}
	got = BuildStoredLevelCopyFilter("__s", "__m", []levelCond{{"!~", "x"}, {"=", "warn"}})
	if strings.Count(got, " | filter ") != 2 || !strings.Contains(got, "-__s:~\"^(?:x)$\"") || !strings.Contains(got, "__s:~`(?i)^(?:warn|wrn|warning)$`") {
		t.Errorf("two filters: %s", got)
	}
	// != info keeps the stored words of every other level and any raw word that is not info.
	got = BuildStoredLevelCopyFilter("__s", "__m", []levelCond{{"!=", "info"}})
	if strings.Contains(got, "(?:info|inf|information)") || !strings.Contains(got, "(?:error|err)") || !strings.Contains(got, `-__s:="info"`) {
		t.Errorf("!= info: %s", got)
	}
	for _, f := range strings.Split(strings.TrimPrefix(got, " | filter "), " | filter ") {
		mustParseLogsQL(t, f)
	}
}
