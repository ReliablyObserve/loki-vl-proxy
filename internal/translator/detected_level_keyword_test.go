package translator

import (
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// vlAnyCasePhrase is VictoriaLogs' i(phrase) over one value
// (lib/logstorage/filter_any_case_phrase.go matchAnyCasePhrase): the value is
// lowercased rune by rune (unicode.ToLower) and the phrase must stand between
// non-token runes, a token rune being a Unicode letter or digit or '_'.
func vlAnyCasePhrase(s, phrase string) bool {
	var lower strings.Builder
	for _, r := range s {
		lower.WriteRune(unicode.ToLower(r))
	}
	v := lower.String()
	isToken := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
	for pos := 0; ; {
		n := strings.Index(v[pos:], phrase)
		if n < 0 {
			return false
		}
		pos += n
		before, after := true, true
		if pos > 0 {
			r, _ := utf8.DecodeLastRuneInString(v[:pos])
			before = !isToken(r)
		}
		if end := pos + len(phrase); end < len(v) {
			r, _ := utf8.DecodeRuneInString(v[end:])
			after = !isToken(r)
		}
		if before && after {
			return true
		}
		pos++
	}
}

// gateWordRE reads the phrases out of the text of the gate.
var gateWordRE = regexp.MustCompile(`i\(([^)]*)\)`)

// levelKeywordGateMatches evaluates the text LevelKeywordGate() returns over
// one line: any of the phrases it lists, case-insensitively. The phrases come
// from the text, not from the keyword list, so a gate that lists fewer words
// than the regexp is seen.
func levelKeywordGateMatches(line string) bool {
	for _, m := range gateWordRE.FindAllStringSubmatch(LevelKeywordGate(), -1) {
		if vlAnyCasePhrase(line, m[1]) {
			return true
		}
	}
	return false
}

// chainKeywordLevel is what the chain's keyword scan gives a line: the gate,
// then the regexp, then the finals (error|err is error, warning|warn is warn,
// any other word lowercased); unknown when the gate or the regexp finds none.
func chainKeywordLevel(re *regexp.Regexp, line string) string {
	if !levelKeywordGateMatches(line) {
		return "unknown"
	}
	m := re.FindStringSubmatch(line)
	if m == nil {
		return "unknown"
	}
	switch w := strings.ToLower(m[1]); w {
	case "err":
		return "error"
	case "warning":
		return "warn"
	default:
		return w
	}
}

// The answers of Loki v3.7.7's detectLevelFromLogLine
// (pkg/distributor/field_detection.go), written out by hand from its source:
// the line is lowercased, each of trace, debug, fatal, critical, error, err,
// warning, warn, info is searched for its first occurrence with a left
// boundary of ' ', '\t', '\n', '[', '(', '{', '"', '=' (or the line start) and a
// right boundary of ' ', '\t', '\n', '[', ']', '(', ')', '{', '}', ':', ',', '!',
// '"', '=' (or the line end), and the occurrence with the smallest position
// wins, so the order of the words only matters at one position. The table
// does not call the chain to build its expectations.
//
// conformance: severity-detected-level-derivation
var lokiKeywordAnswers = []struct{ line, want string }{
	// case
	{"ERROR something", "error"}, {"Error: x", "error"}, {"eRrOr", "error"}, {"WARNING", "warn"},
	{"INFO", "info"}, {"Debug", "debug"}, {"TRACE", "trace"}, {"FaTaL", "fatal"}, {"Critical", "critical"},
	// substrings are no words
	{"errors found", "unknown"}, {"terror", "unknown"}, {"warnings", "unknown"}, {"information", "unknown"},
	{"informational", "unknown"}, {"debugger", "unknown"}, {"fatalities", "unknown"}, {"errno 5", "unknown"},
	{"preinfo", "unknown"}, {"tracer", "unknown"}, {"critically", "unknown"}, {"errx", "unknown"},
	// the long and short spellings
	{"warning: disk", "warn"}, {"this is a warning", "warn"}, {"warn", "warn"}, {"err:", "error"},
	{"failed err!", "error"}, {"an err", "error"},
	// several keywords: the smallest position wins
	{"info then error", "info"}, {"error then info", "error"}, {"warning: error", "warn"},
	{"error warn", "error"}, {"warn error", "warn"}, {"critical error", "critical"}, {"error critical", "error"},
	{"fatal critical", "fatal"}, {"critical fatal", "critical"}, {"x info debug trace", "info"},
	{"debug info", "debug"}, {"x errors then warn", "warn"}, {"terror then info", "info"},
	// left boundaries
	{"x=error", "error"}, {`"error"`, "error"}, {"[error]", "error"}, {"(err)", "error"}, {"{debug}", "debug"},
	{"a\terror", "error"}, {"a\nerror", "error"},
	{"foo-error", "unknown"}, {"foo_error", "unknown"}, {"5error", "unknown"}, {":error", "unknown"},
	{",error", "unknown"}, {"!error", "unknown"}, {"]error", "unknown"}, {")error", "unknown"},
	{"}error", "unknown"}, {"/error", "unknown"}, {".error", "unknown"}, {"'error", "unknown"},
	{"\rerror", "unknown"}, {" error", "unknown"}, {"éerror", "unknown"}, {"€error", "unknown"},
	{"\xffeerror", "unknown"}, {"\xffinfo", "unknown"},
	// right boundaries
	{"error404", "unknown"}, {"error.", "unknown"}, {"error;", "unknown"}, {"error/x", "unknown"},
	{"error-x", "unknown"}, {"error_x", "unknown"}, {"error\r", "unknown"}, {"error x", "unknown"},
	{"errorж", "unknown"}, {"error\xff", "unknown"}, {"error'", "unknown"}, {"error>", "unknown"},
	{"error[", "error"}, {"error]", "error"}, {"error(", "error"}, {"error)", "error"}, {"error{", "error"},
	{"error}", "error"}, {"error,", "error"}, {"error!", "error"}, {`error"`, "error"}, {"error=1", "error"},
	{"error\tx", "error"}, {"error\nx", "error"},
	// a rejected occurrence does not hide a later one
	{"terror error", "error"}, {"errors, then err", "error"}, {"x-info y=info", "info"},
	// nothing
	{"", "unknown"}, {"hello world", "unknown"}, {"GET /api/v1/users 200 12ms", "unknown"},
}

func TestKeywordScanLikeLoki(t *testing.T) {
	re := regexp.MustCompile(levelKeywordRegexp)
	for _, c := range lokiKeywordAnswers {
		if got := chainKeywordLevel(re, c.line); got != c.want {
			t.Errorf("%q: chain %q, Loki %q", c.line, got, c.want)
		}
		// the regexp alone, without the gate, gives the same answer: the gate removes no match
		m := re.FindStringSubmatch(c.line)
		alone := "unknown"
		if m != nil {
			alone = map[string]string{"err": "error", "warning": "warn"}[strings.ToLower(m[1])]
			if alone == "" {
				alone = strings.ToLower(m[1])
			}
		}
		if alone != c.want {
			t.Errorf("%q: the regexp alone gives %q, Loki %q", c.line, alone, c.want)
		}
	}
}

// Known difference, as before the gate: Loki lowercases with strings.ToLower
// (Unicode), the regexp folds ASCII only, so a line that spells a keyword with
// U+0130 reads info in Loki and unknown here. The gate lowercases like Loki,
// lets the line through, and the regexp finds nothing.
func TestKeywordScanUnicodeFoldIsUnchangedByTheGate(t *testing.T) {
	re := regexp.MustCompile(levelKeywordRegexp)
	if got := chainKeywordLevel(re, "İNFO ready"); got != "unknown" {
		t.Fatalf("İNFO: %q", got)
	}
	if re.MatchString("İNFO ready") {
		t.Fatal("the regexp folds U+0130")
	}
}

// The gate is a superset of the regexp: any line the regexp matches holds a
// keyword as a whole word of the tokenizer. The lines are made of random
// pieces around the keywords: every ASCII byte, bytes of multi-byte runes,
// invalid UTF-8, the other case of the letters.
func TestKeywordGateIsASupersetOfTheRegexp(t *testing.T) {
	re := regexp.MustCompile(levelKeywordRegexp)
	rng := rand.New(rand.NewSource(7))
	var seps []string
	for b := 0; b < 128; b++ {
		seps = append(seps, string(rune(b)))
	}
	for _, c := range " \t\n[](){}:,!\"=" { // Loki's boundaries, drawn often so that most lines match
		for k := 0; k < 12; k++ {
			seps = append(seps, string(c))
		}
	}
	seps = append(seps, "\u00a0", "é", "ж", "€", "\xff", "\xc2", "\u0130", "\u017f", "\u212a", "\u0663", "\u2028")
	words := append(append([]string(nil), levelKeywords...), "errors", "terror", "warnings", "inf", "wrn", "ERROR", "Warning", "iNfO")
	matched := 0
	for i := 0; i < 400000; i++ {
		var sb strings.Builder
		for j, n := 0, 1+rng.Intn(4); j < n; j++ {
			sb.WriteString(seps[rng.Intn(len(seps))])
			if rng.Intn(3) == 0 {
				sb.WriteString(seps[rng.Intn(len(seps))])
			}
			w := words[rng.Intn(len(words))]
			if rng.Intn(2) == 0 {
				w = strings.ToUpper(w)
			}
			sb.WriteString(w)
		}
		sb.WriteString(seps[rng.Intn(len(seps))])
		line := sb.String()
		if re.MatchString(line) {
			matched++
			if !levelKeywordGateMatches(line) {
				t.Fatalf("the regexp matches %q and the gate does not", line)
			}
		}
	}
	if matched < 40000 {
		t.Fatalf("only %d of the lines matched; the generator is too tame", matched)
	}
}

func TestLevelKeywordGateText(t *testing.T) {
	want := "_msg:(i(trace) OR i(debug) OR i(fatal) OR i(critical) OR i(error) OR i(err) OR i(warning) OR i(warn) OR i(info))"
	if got := LevelKeywordGate(); got != want {
		t.Fatalf("gate = %s", got)
	}
	// every chain that scans the line gates the scan on it: the full chain, a chain with no key or field
	for _, present := range []*DetectedLevelPresence{nil, {}, {Stored: []string{"level"}, BodyKeys: []string{"level"}}} {
		chain := BuildDetectedLevelChain(nil, DetectedLevelChainGrouping, present)
		if !strings.Contains(chain, "| extract_regexp if (") || !strings.Contains(chain, `__dl:"" `+want+") `(?i)") {
			t.Fatalf("keyword scan of %v is not gated: %s", present, chain)
		}
	}
}

// A chain that also unpacks the labels of its plan parses each line once: the
// level keys it reads and the labels share one unpack_json, the level keys are
// read only for rows no stored level decides although the unpack parses more
// rows, and every label ends up with the value the plan's own `unpack_json
// fields (...) keep_original_fields` would give it.
func TestBuildDetectedLevelChainShared(t *testing.T) {
	shared := &SharedUnpack{Keys: []string{"level", "app_name"}}
	chain := BuildDetectedLevelChainShared(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil, shared)
	if n := strings.Count(chain, "unpack_json"); n != 1 {
		t.Fatalf("%d unpack_json pipes: %s", n, chain)
	}
	for _, want := range []string{
		// every row is parsed (the plan unpacks every row), the level keys and the labels together
		`| unpack_json from _msg fields (level, LEVEL, Level, log.level, severity, SEVERITY, Severity, SeverityText, lvl, LVL, Lvl, severity_text, Severity_Text, SEVERITY_TEXT, app_name) result_prefix "__j_"`,
		// the level keys are read only where no stored level decides, whichever rows were parsed
		`| format if (-__dl_h:* __j_level:*) "<__j_level>" as __dl_v keep_original_fields`,
		`| format if (-__dl_h:* __j_SEVERITY_TEXT:*) "<__j_SEVERITY_TEXT>" as __dl_v keep_original_fields`,
		// the labels: the body's value where the field is not stored, before the closing delete
		`| format "<__dl>" as detected_level | format if (-level:* __j_level:*) "<__j_level>" as level | format if (-app_name:* __j_app_name:*) "<__j_app_name>" as app_name | delete __dl, __dl_*, __j_*, __l_*`,
	} {
		if !strings.Contains(chain, want) {
			t.Errorf("shared chain lacks %q:\n%s", want, chain)
		}
	}
	// The logfmt reads are the chain's own and unchanged.
	if !strings.Contains(chain, "| unpack_logfmt if (-__dl_h:* -_msg:~") {
		t.Errorf("the logfmt unpack changed: %s", chain)
	}

	// Without a shared unpack, or in a mode that has no labels, nothing changes.
	for _, mode := range []DetectedLevelChainMode{DetectedLevelChainGroup, DetectedLevelChainLogs} {
		if got, want := BuildDetectedLevelChainShared(DetectedLevelFieldNames, mode, nil, shared), BuildDetectedLevelChain(DetectedLevelFieldNames, mode, nil); got != want {
			t.Errorf("mode %d changed by a shared unpack", mode)
		}
	}
	if got, want := BuildDetectedLevelChainShared(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil, &SharedUnpack{}), BuildDetectedLevelChain(DetectedLevelFieldNames, DetectedLevelChainGrouping, nil); got != want {
		t.Error("an empty shared unpack changed the chain")
	}

	// The plan's own condition: a row it parses is parsed whether or not a stored
	// level decides it, and the chain's rows are parsed too.
	conditional := BuildDetectedLevelChainShared([]string{"level", "app"}, DetectedLevelChainGrouping, nil, &SharedUnpack{Keys: []string{"app_name"}, Cond: "(-app_name:*)"})
	if !strings.Contains(conditional, "| unpack_json if (-__dl_h:* or (-app_name:*)) from _msg fields (") {
		t.Errorf("conditional shared unpack: %s", conditional)
	}

	// Pruned: a label is no body key of the chain, so the mask never drops it,
	// and a window with no body key still unpacks the labels (and reads no
	// level key from them).
	none := BuildDetectedLevelChainShared(nil, DetectedLevelChainGrouping, &DetectedLevelPresence{Stored: []string{"Level"}}, &SharedUnpack{Keys: []string{"level"}})
	for _, want := range []string{
		`| unpack_json from _msg fields (level) result_prefix "__j_"`,
		`| format if (-level:* __j_level:*) "<__j_level>" as level | delete`,
	} {
		if !strings.Contains(none, want) {
			t.Errorf("chain without body keys lacks %q:\n%s", want, none)
		}
	}
	if strings.Contains(none, "as __dl_v keep_original_fields | format if (-__dl_h:* __j_") || strings.Contains(none, "unpack_logfmt") {
		t.Errorf("a window without body keys reads a level key: %s", none)
	}
	// With a plan condition and no body key the chain asks nothing of its own.
	only := BuildDetectedLevelChainShared(nil, DetectedLevelChainGrouping, &DetectedLevelPresence{Stored: []string{"level"}}, &SharedUnpack{Keys: []string{"level"}, Cond: "(-level:*)"})
	if !strings.Contains(only, "| unpack_json if ((-level:*)) from _msg fields (level)") {
		t.Errorf("plan condition alone: %s", only)
	}
}

// FuzzKeywordGateIsASupersetOfTheRegexp lets the fuzzer look for a line the
// keyword regexp matches and the gate (VictoriaLogs' i() over the nine words)
// does not, and for a line where the chain's scan disagrees with a plain
// search for Loki's first bounded keyword.
func FuzzKeywordGateIsASupersetOfTheRegexp(f *testing.F) {
	for _, c := range lokiKeywordAnswers {
		f.Add(c.line)
	}
	f.Add("İNFO ready")
	f.Add("\xff\xfeerror\xc2")
	re := regexp.MustCompile(levelKeywordRegexp)
	f.Fuzz(func(t *testing.T, line string) {
		if re.MatchString(line) && !levelKeywordGateMatches(line) {
			t.Fatalf("the regexp matches %q and the gate does not", line)
		}
		// Loki's own search, written out: the smallest position of a keyword
		// between its boundaries in the lowercased line (ASCII fold, as the regexp).
		lower := strings.Map(func(r rune) rune {
			if r >= 'A' && r <= 'Z' {
				return r + 'a' - 'A'
			}
			return r
		}, line)
		best, bestAt := "unknown", len(lower)+1
		for _, w := range levelKeywords {
			for from := 0; ; {
				at := strings.Index(lower[from:], w)
				if at < 0 {
					break
				}
				at += from
				left := at == 0 || strings.ContainsRune(" \t\n[({\"=", rune(lower[at-1]))
				end := at + len(w)
				right := end == len(lower) || strings.ContainsRune(" \t\n[](){}:,!\"=", rune(lower[end]))
				if left && right {
					if at < bestAt {
						best, bestAt = w, at
					}
					break
				}
				from = at + 1
			}
		}
		switch best {
		case "err":
			best = "error"
		case "warning":
			best = "warn"
		}
		if got := chainKeywordLevel(re, line); got != best {
			t.Fatalf("%q: the chain's scan says %q, a search for the first bounded keyword says %q", line, got, best)
		}
	})
}
