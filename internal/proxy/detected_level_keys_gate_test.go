package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

// vlIsTokenRune is VictoriaLogs' isTokenRune (lib/logstorage/tokenizer.go): an
// ASCII letter, digit or underscore, or a Unicode letter or digit.
func vlIsTokenRune(c rune) bool {
	if c < utf8.RuneSelf {
		return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
	}
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_'
}

// vlPhrasePos is VictoriaLogs' getPhrasePos (lib/logstorage/filter_phrase.go,
// identical in v1.40, v1.47 and v1.52): the first position of the phrase that
// has no token rune beside it where the phrase starts or ends with one, an
// invalid UTF-8 byte counting as a token rune.
func vlPhrasePos(s, phrase string) int {
	if len(phrase) == 0 {
		return 0
	}
	if len(phrase) > len(s) {
		return -1
	}
	r := rune(phrase[0])
	if r >= utf8.RuneSelf {
		r, _ = utf8.DecodeRuneInString(phrase)
	}
	startsWithToken := vlIsTokenRune(r)
	r = rune(phrase[len(phrase)-1])
	if r >= utf8.RuneSelf {
		r, _ = utf8.DecodeLastRuneInString(phrase)
	}
	endsWithToken := vlIsTokenRune(r)
	pos := 0
	for {
		n := strings.Index(s[pos:], phrase)
		if n < 0 {
			return -1
		}
		pos += n
		if startsWithToken && pos > 0 {
			r := rune(s[pos-1])
			if r >= utf8.RuneSelf {
				r, _ = utf8.DecodeLastRuneInString(s[:pos])
			}
			if r == utf8.RuneError || vlIsTokenRune(r) {
				pos++
				continue
			}
		}
		if endsWithToken && pos+len(phrase) < len(s) {
			r := rune(s[pos+len(phrase)])
			if r >= utf8.RuneSelf {
				r, _ = utf8.DecodeRuneInString(s[pos+len(phrase):])
			}
			if r == utf8.RuneError || vlIsTokenRune(r) {
				pos++
				continue
			}
		}
		return pos
	}
}

// vlSequence is VictoriaLogs' matchSequence (filter_sequence.go): each phrase
// is searched after the end of the one before.
func vlSequence(s string, phrases []string) bool {
	for _, phrase := range phrases {
		n := vlPhrasePos(s, phrase)
		if n < 0 {
			return false
		}
		s = s[n+len(phrase):]
	}
	return true
}

// twiceGateMatches evaluates the text of a gate over one line, through the
// fake VictoriaLogs' filter evaluator: the phrases and sequences are read out
// of the text, not taken from the key list, so a gate that lists less than the
// patterns is seen.
func twiceGateMatches(t testing.TB, gate, line string) bool {
	return fakeLogsQLMatch(t, gate, map[string]string{"_msg": line})
}

var (
	// the patterns of detectedLevelKeysRisk that need two level keys
	twiceJSONKeys   = regexp.MustCompile(detectedLevelJSONKey + `(?s:.*)` + detectedLevelJSONKey)
	twiceLogfmtKeys = regexp.MustCompile(detectedLevelLogfmtKey + `(?s:.*)` + detectedLevelLogfmtKey)
)

func twicePatternMatches(line string) bool {
	return twiceJSONKeys.MatchString(line) || twiceLogfmtKeys.MatchString(line)
}

// twiceGateCorpus returns lines for the superset test: every ordered pair of
// level keys (the key list itself, so a name added later is covered) in the
// JSON and the logfmt shapes with each whitespace and separator that matters,
// then random lines made of the keys, their near spellings and every kind of
// byte around them.
func twiceGateCorpus(random int) []string {
	var lines []string
	jsonShapes := []string{
		`{"%s":"x","%s":"y"}`, `{"%s" : 1, "%s"	:2}`, `{"m":"q","%s":"x","k":{"%s":"y"}}`,
		`  {"%s":"x"} {"%s":"y"}`, "{\"%s\"\n:\"x\",\r\n\"%s\":\"y\"}", `{"%s":"%s"}`, `{"a":"%s","b":"%s"}`,
	}
	logfmtShapes := []string{
		"%s=1 %s=2", "x %s=1\t%s=2", "%s=1\n%s=2", "%s=1\r%s=2", "%s=1\f%s=2", "a=b %s=\"%s=x\"", "%s=%s",
		"%s=1 %s", "%s %s=1",
	}
	for _, a := range detectedLevelKeyNames {
		for _, b := range detectedLevelKeyNames {
			for _, shape := range append(append([]string(nil), jsonShapes...), logfmtShapes...) {
				lines = append(lines, strings.Replace(strings.Replace(shape, "%s", a, 1), "%s", b, 1))
			}
		}
	}
	rng := rand.New(rand.NewSource(11))
	var seps []string
	for b := 0; b < 128; b++ {
		seps = append(seps, string(rune(b)))
	}
	for _, c := range "\"=: \t\n{}" { // the characters around a key, drawn often so that most lines match
		for k := 0; k < 10; k++ {
			seps = append(seps, string(c))
		}
	}
	seps = append(seps, "\u00a0", "\u2028", "\u0085", "é", "ж", "€", "\xff", "\xc2", "\ufffd")
	words := append([]string(nil), detectedLevelKeyNames...)
	for _, name := range detectedLevelKeyNames {
		words = append(words, name+"s", "x"+name, name+"2", strings.ToUpper(name), name+"é", "é"+name, strings.ReplaceAll(name, "_", "-"))
	}
	// the shapes of a key, drawn often so that many lines hold two of them
	forms := []string{`"%s":`, `"%s"  :`, " %s=", "\t%s=", "%s=", `"%s"`}
	for i := 0; i < random; i++ {
		var sb strings.Builder
		for j, n := 0, 1+rng.Intn(5); j < n; j++ {
			sb.WriteString(seps[rng.Intn(len(seps))])
			if rng.Intn(3) == 0 {
				sb.WriteString(seps[rng.Intn(len(seps))])
			}
			if rng.Intn(2) == 0 {
				sb.WriteString(strings.Replace(forms[rng.Intn(len(forms))], "%s", words[rng.Intn(len(words))], 1))
			} else {
				sb.WriteString(words[rng.Intn(len(words))])
			}
		}
		sb.WriteString(seps[rng.Intn(len(seps))])
		lines = append(lines, sb.String())
	}
	return lines
}

// firstTwiceGateMiss returns a line the patterns match and the gate does not,
// and how many lines the patterns matched.
func firstTwiceGateMiss(t testing.TB, gate string, lines []string) (miss string, matched int) {
	for _, line := range lines {
		if !twicePatternMatches(line) {
			continue
		}
		matched++
		if miss == "" && !twiceGateMatches(t, gate, line) {
			miss = line
		}
	}
	return miss, matched
}

// The gate in front of the two-keys patterns is a superset of them: any line
// they match holds two level keys as phrases of VictoriaLogs' tokenizer, so
// the verdict of the probe is the patterns', never the gate's. Every ordered
// pair of the key names is covered by lines of both shapes, and random lines
// around them. The gate is evaluated from its text, with the phrase and
// sequence rules of VictoriaLogs written out from filter_phrase.go and
// filter_sequence.go.
// conformance: severity-detected-level-derivation
func TestTwiceGateIsASupersetOfTheKeyPatterns(t *testing.T) {
	gate := detectedLevelTwiceGate()
	lines := twiceGateCorpus(300000)
	miss, matched := firstTwiceGateMiss(t, gate, lines)
	if miss != "" {
		t.Fatalf("the patterns match %q and the gate does not", miss)
	}
	n := len(detectedLevelKeyNames)
	if want := n * n * 6; matched < want || matched < 5000 {
		t.Fatalf("only %d lines matched the patterns (at least %d expected); the corpus is too tame", matched, want)
	}
}

// The gate is no trivial one: a line with one level key, or with a key and no
// second one, is rejected (that is the cost it removes), and lines that the
// patterns reject but that hold two keys are let through (the patterns decide).
// conformance: severity-detected-level-derivation
func TestTwiceGateRejectsSingleKeys(t *testing.T) {
	gate := detectedLevelTwiceGate()
	for line, want := range map[string]bool{
		`{"level":"info","msg":"x"}`:                         false,
		`ts=1 level=info msg=hello`:                          false,
		`just a line`:                                        false,
		`{"level":"info","severity":"warn"}`:                 true,
		`{"level":"info","level":"warn"}`:                    true,
		`{"log.level":"info","SeverityText":"warn"}`:         true,
		"level=info\tlvl=warn":                               true,
		`{"a":"level","b":"level"}`:                          true, // two phrases: the patterns reject it
		`level levels`:                                       false,
		`{"log":{"level":"info"},"msg":"log.level"}`:         true,
		`{"severity_text":"info","Severity_Text":"warn"}`:    true,
		`{"SEVERITY_TEXT":"info","severity_text":"warn"}`:    true,
		`{"msg":"xlevel and level2 and severity_texts"}`:     false,
		`{"level":"info"} {"level":"warn"} \u006cevel=error`: true,
		`{"log.level":"info"}`:                               false, // one occurrence holds the word level once
		`{"log.level":"a","log.level":"b"}`:                  true,
		`{"log.level":"a","level":"b"}`:                      true,
		`{"level":"a","log.level":"b"}`:                      true,
		`{"log.level":"a","severity":"b"}`:                   true,
	} {
		if got := twiceGateMatches(t, gate, line); got != want {
			t.Errorf("gate over %q = %v, want %v", line, got, want)
		}
	}
}

// A gate that lists less than the patterns reads is caught by the corpus: each
// mutation of the gate text narrows it, and the superset test must see a line
// it drops.
// conformance: severity-detected-level-derivation
func TestTwiceGateNarrowedIsCaught(t *testing.T) {
	gate := detectedLevelTwiceGate()
	lines := twiceGateCorpus(20000)
	if miss, _ := firstTwiceGateMiss(t, gate, lines); miss != "" {
		t.Fatalf("the unmutated gate misses %q", miss)
	}
	selfPair := regexp.MustCompile(`_msg:seq\("[^"]*","[^"]*"\) or `)
	crossPair := regexp.MustCompile(`\(_msg:"[^"]*" \([^()]*\)\)( or )?`)
	mutations := map[string]string{
		"no same-key sequences":    selfPair.ReplaceAllString(gate, ""),
		"no pairs of two keys":     crossPair.ReplaceAllString(gate, ""),
		"sequences reversed":       strings.Replace(gate, `_msg:seq("level","level")`, `_msg:seq("level","LEVEL")`, 1),
		"last key dropped":         strings.ReplaceAll(strings.ReplaceAll(gate, `_msg:seq("SEVERITY_TEXT","SEVERITY_TEXT")`, `_msg:seq("lvl","lvl")`), ` or _msg:"SEVERITY_TEXT"`, ""),
		"a key misspelt":           strings.ReplaceAll(gate, `"Lvl"`, `"Lvls"`),
		"phrases lose their words": strings.ReplaceAll(gate, `"Severity_Text"`, `"Severity"`),
	}
	for name, mutated := range mutations {
		if mutated == gate {
			t.Errorf("%s: the mutation did not change the gate", name)
			continue
		}
		if miss, _ := firstTwiceGateMiss(t, mutated, lines); miss == "" {
			t.Errorf("%s: no line of the corpus is dropped by the narrowed gate", name)
		}
	}
}

// FuzzTwiceGateIsASupersetOfTheKeyPatterns lets the fuzzer look for a line
// the two-keys patterns match and the gate does not.
// conformance: severity-detected-level-derivation
func FuzzTwiceGateIsASupersetOfTheKeyPatterns(f *testing.F) {
	for _, line := range twiceGateCorpus(0)[:400] {
		f.Add(line)
	}
	f.Add("\xfflevel=1 level=2")
	f.Add("{\"log.level\":1,\"level\":2}")
	gate := detectedLevelTwiceGate()
	f.Fuzz(func(t *testing.T, line string) {
		if twicePatternMatches(line) && !twiceGateMatches(t, gate, line) {
			t.Fatalf("the patterns match %q and the gate does not", line)
		}
	})
}

// The probe puts the gate in front of both two-keys patterns and in front of
// nothing else, and the query still fits VictoriaLogs' default query length
// (-search.maxQueryLen, 16 KiB) after a long selector of 6.7 KB, the longest
// the detected_level chain itself fits (TestPrunedChainWithWitnessFitsTheQueryLimit).
// conformance: severity-detected-level-derivation
func TestKeysRiskProbeGatesTheTwoKeyPatterns(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		queries = append(queries, r.Form.Get("query"))
	}))
	defer srv.Close()
	p := newSlidingTestProxy(t, srv.URL)
	var names []string
	for i := 0; i < 7; i++ {
		names = append(names, strings.Repeat("n", 120)+string(rune('0'+i)))
	}
	base := `app:~"` + strings.Repeat(strings.Join(names, "|")+"|", 8) + `x"`
	if len(base) < 6000 {
		t.Fatalf("base of %d bytes", len(base))
	}
	if risky, err := p.detectedLevelKeysRisk(context.Background(), base, time.Unix(1700000000, 0), time.Unix(1700003600, 0)); err != nil || risky {
		t.Fatalf("risky=%v err=%v", risky, err)
	}
	if len(queries) != 1 {
		t.Fatalf("%d queries", len(queries))
	}
	q, gate := queries[0], detectedLevelTwiceGate()
	if len(q) > vlDefaultMaxQueryLen {
		t.Fatalf("the probe is %d bytes, VictoriaLogs accepts %d", len(q), vlDefaultMaxQueryLen)
	}
	if n := strings.Count(q, gate); n != 1 {
		t.Fatalf("the gate occurs %d times in %s", n, q)
	}
	// the gate stands in front of the two patterns that need two keys, and of
	// no other branch
	twiceJSON := `_msg:~` + `"` + `\"(?:`
	at := strings.Index(q, gate)
	after := q[at+len(gate):]
	if !strings.HasPrefix(after, ` ((_msg:~"^[`) || strings.Count(after, "(?s:.*)") != 2 {
		t.Fatalf("the gate is not in front of the two-key patterns: %s", after)
	}
	ungated := strings.Replace(q, gate+" ", "", 1)
	if strings.Contains(ungated, "seq(") || !strings.Contains(ungated, twiceJSON) {
		t.Fatalf("removing the gate leaves %s", ungated)
	}
	if plain := detectedLevelKeysQuery(base, false); strings.Contains(plain, "seq(") || strings.Count(plain, "(?s:.*)") != 2 || len(plain)+len(gate) > len(q)+2 {
		t.Fatalf("the probe without the gate: %s", plain)
	}

	// A selector that fits VictoriaLogs' query length only without the gate (the
	// gate adds 2.5 KB) gets the probe without it: the same patterns, so the same
	// verdict, where the gated text would be refused.
	queries = nil
	long := `app:~"` + strings.Repeat("n", 12500) + `"`
	if len(detectedLevelKeysQuery(long, true)) <= vlDefaultMaxQueryLen || len(detectedLevelKeysQuery(long, false)) > vlDefaultMaxQueryLen {
		t.Fatalf("the long base of %d bytes does not separate the two texts (%d, %d)", len(long), len(detectedLevelKeysQuery(long, true)), len(detectedLevelKeysQuery(long, false)))
	}
	if risky, err := p.detectedLevelKeysRisk(context.Background(), long, time.Unix(1700000000, 0), time.Unix(1700003600, 0)); err != nil || risky {
		t.Fatalf("risky=%v err=%v", risky, err)
	}
	if len(queries) != 1 || strings.Contains(queries[0], "seq(") || len(queries[0]) > vlDefaultMaxQueryLen {
		t.Fatalf("a long selector should send the probe without the gate within %d bytes: %d queries, %d bytes", vlDefaultMaxQueryLen, len(queries), len(queries[0]))
	}
}

// The gate leaves out the pair of a dotted name and its last word (the one
// occurrence of log.level holds the word level), and the lines with both still
// pass through seq("level","level").
// conformance: severity-detected-level-derivation
func TestTwiceGateOmitsTheDottedPair(t *testing.T) {
	gate := detectedLevelTwiceGate()
	if strings.Contains(gate, `(_msg:"level" (_msg:"LEVEL" or _msg:"Level" or _msg:"log.level"`) {
		t.Fatalf("the pair of level and log.level is in the gate: %s", gate)
	}
	for _, name := range detectedLevelKeyNames {
		if name != "log.level" && !strings.Contains(gate, `_msg:"`+name+`"`) && name != detectedLevelKeyNames[0] {
			t.Errorf("%s is missing from the gate", name)
		}
	}
}

// The longest base the gated probe fits is exactly vlDefaultMaxQueryLen minus the
// probe without a base; one byte more gets the probe without the gate.
// conformance: severity-detected-level-derivation
func TestKeysRiskProbeGateEdgeOfTheQueryLength(t *testing.T) {
	overhead := len(detectedLevelKeysQuery("", true))
	for _, tc := range []struct {
		extra int
		gated bool
	}{{0, true}, {1, false}, {-1, true}} {
		var got []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			got = append(got, r.Form.Get("query"))
		}))
		p := newSlidingTestProxy(t, srv.URL)
		base := strings.Repeat("a", vlDefaultMaxQueryLen-overhead+tc.extra)
		if _, err := p.detectedLevelKeysRisk(context.Background(), base, time.Unix(1700000000, 0), time.Unix(1700003600, 0)); err != nil {
			t.Fatal(err)
		}
		srv.Close()
		if len(got) != 1 || strings.Contains(got[0], "seq(") == !tc.gated || len(got[0]) > vlDefaultMaxQueryLen {
			t.Errorf("base of %d bytes (limit %d, gated probe %d without a base): %d queries, gated=%v, lengths %v", len(base), vlDefaultMaxQueryLen, overhead, len(got), tc.gated, queryLengths(got))
		}
		if tc.gated && len(got) == 1 && len(got[0]) != vlDefaultMaxQueryLen+tc.extra {
			t.Errorf("the gated probe is %d bytes, want %d", len(got[0]), vlDefaultMaxQueryLen+tc.extra)
		}
	}
}

func queryLengths(qs []string) []int {
	out := make([]int, len(qs))
	for i, q := range qs {
		out[i] = len(q)
	}
	return out
}

// An operator may have lowered VictoriaLogs' -search.maxQueryLen below the gated
// probe: it answers 400 for the longer text, and the probe is asked again
// without the gate, which main's probe is. Any other refusal is returned.
// conformance: severity-detected-level-derivation
func TestKeysRiskProbeRetriesWithoutTheGateWhenRefusedAsTooLong(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int
		refusal string
		queries int
		wantErr bool
	}{
		{"limit below the gated text", 3000, "the `query` arg length cannot exceed -search.maxQueryLen=%d bytes; the current query length is %d bytes", 2, false},
		{"limit below both", 1000, "the `query` arg length cannot exceed -search.maxQueryLen=%d bytes; the current query length is %d bytes", 2, true},
		{"another 400 is not retried", 0, "cannot parse `query` arg: unexpected token", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				q := r.Form.Get("query")
				got = append(got, q)
				if len(q) > tc.limit {
					http.Error(w, fmtRefusal(tc.refusal, tc.limit, len(q)), http.StatusBadRequest)
				}
			}))
			defer srv.Close()
			p := newSlidingTestProxy(t, srv.URL)
			risky, err := p.detectedLevelKeysRisk(context.Background(), `app:="a"`, time.Unix(1700000000, 0), time.Unix(1700003600, 0))
			if (err != nil) != tc.wantErr || risky || len(got) != tc.queries {
				t.Fatalf("risky=%v err=%v, %d queries %v", risky, err, len(got), queryLengths(got))
			}
			if tc.queries == 2 && (!strings.Contains(got[0], "seq(") || strings.Contains(got[1], "seq(")) {
				t.Errorf("the first ask should be gated and the retry not")
			}
		})
	}
}

func fmtRefusal(format string, args ...any) string {
	if !strings.Contains(format, "%d") {
		return format
	}
	return fmt.Sprintf(format, args...)
}
