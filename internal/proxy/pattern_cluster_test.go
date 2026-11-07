package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// lokiPatternLineMatches is Loki's pattern line filter (|>) as its documentation and answers define it, the filter
// Logs Drilldown adds when a pattern is included: each literal is found at its first occurrence after the text
// before it (a leading literal anywhere in the line), every capture holds at least one character, and a pattern
// that ends on a literal ends the line there.
func lokiPatternLineMatches(pattern, line string) bool {
	pos, capturing := 0, false
	for pattern != "" {
		if strings.HasPrefix(pattern, "<") {
			if end := strings.IndexByte(pattern, '>'); end > 0 {
				capturing = true
				pattern = pattern[end+1:]
				continue
			}
		}
		next := strings.IndexByte(pattern[1:], '<') + 1
		if next == 0 {
			next = len(pattern)
		}
		literal := pattern[:next]
		pattern = pattern[next:]
		i := strings.Index(line[pos:], literal)
		if i < 0 || capturing && i == 0 {
			return false
		}
		pos += i + len(literal)
		capturing = false
	}
	if capturing {
		return pos < len(line)
	}
	return pos == len(line)
}

// The reference matcher reads patterns as Loki's pattern line filter does.
//
// conformance: loki_api_v1_patterns, patterns-loki-mining
func TestLokiPatternLineMatches(t *testing.T) {
	for _, tc := range []struct {
		pattern, line string
		want          bool
	}{
		{"<_>", "x", true},
		{"<_>", "", false},
		{"GET <_> HTTP/1.1", "GET /a HTTP/1.1", true},
		{"GET <_> HTTP/1.1", "GET /a HTTP/1.1 extra", false},
		{"GET <_> HTTP/1.1", "GET  HTTP/1.1", false},
		{"a <_> b <_>", "x a 1 b 2", true},
		{"a <_> b <_>", "a 1 b ", false},
		{"<_>msg<_>", `{"m":"msg"}`, true},
		{"<_> - <_> [<_>]", "1.2.3.4 - admin [now]", true},
	} {
		if got := lokiPatternLineMatches(tc.pattern, tc.line); got != tc.want {
			t.Errorf("%q |> %q = %v, want %v", tc.line, tc.pattern, got, tc.want)
		}
	}
}

func patternRowsOf(level string, lines ...string) []byte {
	var b strings.Builder
	for i, line := range lines {
		row, _ := json.Marshal(map[string]string{"_time": fmt.Sprintf("2026-11-06T10:%02d:%02dZ", i/60%60, i%60), "_msg": line, "level": level})
		b.Write(row)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// minePatterns is the patterns of lines observed at one level in the order given, sorted.
func minePatterns(level string, lines ...string) []string {
	miner := newPatternMiner()
	for _, line := range lines {
		miner.Observe(level, line, 0)
	}
	var texts []string
	for _, e := range patternEntriesByLevelAndPattern(miner) {
		texts = append(texts, e.pattern)
	}
	sort.Strings(texts)
	return texts
}

func expectPatterns(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("patterns\n got  %q\n want %q", got, want)
	}
}

// A line only joins a cluster with as many tokens, whatever the tokens before them read.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersSplitByTokenCount(t *testing.T) {
	expectPatterns(t, minePatterns("info", "a b c d", "a b c d e", "a b c d f"), "a b c d", "a b c d <_>")
	expectPatterns(t, minePatterns("info", "a b c d", "a b c d e", "a b c d f", "a b c d g h"), "a b c d", "a b c d <_>", "a b c d g h")
}

// A token with a digit goes down the variable branch of the tree, so lines that differ in such a value meet in one
// cluster: a value that changes is a variable, a word that does not stays.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersRouteNumbersToTheVariableBranch(t *testing.T) {
	expectPatterns(t, minePatterns("info", "user 123 logged in", "user 456 logged in"), "user <_> logged in")
	expectPatterns(t, minePatterns("info", "user alice logged in", "user bob logged in"), "user alice logged in", "user bob logged in")
	// Not only ASCII digits: any numeric character.
	expectPatterns(t, minePatterns("info", "user ٣ logged in", "user ٤ logged in"), "user <_> logged in")
	expectPatterns(t, minePatterns("info", "user x1 logged in", "user y22 logged in"), "user <_> logged in")
}

// A line joins the best cluster at its leaf only if it holds at least 0.3 of its tokens as they are: of four
// tokens one is too few, two are enough.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersNeedAThirdOfTheTokens(t *testing.T) {
	// The numbers lead both lines to one leaf; only the tokens they share decide.
	expectPatterns(t, minePatterns("info", "a 1 2 x", "a 5 6 y"), "a 1 2 x", "a 5 6 y")
	expectPatterns(t, minePatterns("info", "a 1 2 x", "a 1 6 y"), "a 1 <_> <_>")
	// Ten tokens: three equal is 0.3 and joins, two do not.
	expectPatterns(t, minePatterns("info", "a 1 2 3 4 5 6 7 8 x", "a 1 2 33 44 55 66 77 88 y"), "a 1 2 <_> <_> <_> <_> <_> <_> <_>")
	expectPatterns(t, minePatterns("info", "a 1 2 3 4 5 6 7 8 x", "a 1 22 33 44 55 66 77 88 y"), "a 1 2 3 4 5 6 7 8 x", "a 1 22 33 44 55 66 77 88 y")
}

// Of two clusters that score the same, the one with more variables takes the line, whichever was made first.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersTieGoesToTheOneWithMoreVariables(t *testing.T) {
	// The numbers lead every line to one leaf, where the clusters tell the lines apart by the tokens they hold.
	text := func(m *patternMiner, c *patternBucket) string {
		return m.joinPatternTokens(c.tokens, c.spacesAfter, c.format)
	}
	miner := newPatternMiner()
	first := miner.observeLine("info", "a 1 2 3 4 5 p", 0, 1)
	second := miner.observeLine("info", "a 6 7 8 9 10 q", 0, 1)
	if first == second {
		t.Fatal("the lines share only one token of seven: two clusters expected")
	}
	// The second cluster takes variables at four places.
	if got := miner.observeLine("info", "a 61 71 8 9 101 r", 0, 1); got != second {
		t.Fatalf("a line like the second joined %q", text(miner, got))
	}
	if got := text(miner, second); got != "a <_> <_> 8 9 <_> <_>" {
		t.Errorf("second cluster reads %q", got)
	}
	// "a 1 2 8 9 s t" holds three tokens of each cluster as they are (a variable never counts): the second has
	// variables, the first none, though the first was made first.
	if got := miner.observeLine("info", "a 1 2 8 9 s t", 0, 1); got != second {
		t.Fatalf("a tie did not go to the cluster with the variables: %q", text(miner, got))
	}
	if got := text(miner, first); got != "a 1 2 3 4 5 p" {
		t.Errorf("first cluster reads %q", got)
	}
	// With no variables to tell them apart the cluster made first takes the line.
	miner = newPatternMiner()
	first = miner.observeLine("info", "a 1 2 3 4 5 p", 0, 1)
	miner.observeLine("info", "a 6 7 8 9 10 q", 0, 1)
	if got := miner.observeLine("info", "a 1 2 8 9 s t", 0, 1); got != first {
		t.Errorf("a tie without variables did not go to the first cluster: %q", text(miner, got))
	}
}

// A node of the tree has 15 children at most: the 15th token that differs takes the last place as the variable
// child, and every token after it goes there too.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersFifteenChildrenThenTheVariableChild(t *testing.T) {
	words := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"}
	var lines, want []string
	for _, w := range words {
		lines = append(lines, w+" x y z")
	}
	for _, w := range words[:14] {
		want = append(want, w+" x y z")
	}
	want = append(want, "<_> x y z")
	expectPatterns(t, minePatterns("info", lines...), want...)
	// A word after the room is gone, even one seen before, joins the variable branch's cluster.
	expectPatterns(t, minePatterns("info", append(lines, "quebec x y z")...), want...)
	// A node with a variable child from a number has room for 14 words more, each on a branch of its own while
	// the cluster in the variable branch is nothing like it; after that the words share the variable branch.
	lines = []string{"1 p q r"}
	want = []string{"1 p q r"}
	for _, w := range words[:14] {
		lines = append(lines, w+" x y z")
		want = append(want, w+" x y z")
	}
	lines = append(lines, "quebec x y z", "romeo x y z")
	want = append(want, "<_> x y z")
	expectPatterns(t, minePatterns("info", lines...), want...)
}

// The last token never leads the line down the tree, and not more than 28 tokens do.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersNeverUseTheLastTokenOrMoreThan28(t *testing.T) {
	expectPatterns(t, minePatterns("info", "a b c d", "a b c e"), "a b c <_>")
	expectPatterns(t, minePatterns("info", "a b c d", "a b x d"), "a b c d", "a b x d")
	words := func(n int, change ...int) string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("k%c%c", 'a'+i/26, 'a'+i%26)
		}
		for _, at := range change {
			out[at] = "changed" + out[at]
		}
		return strings.Join(out, " ")
	}
	// Tokens 0-27 lead the way: the 29th and 30th of 30 are free to differ, the 28th is not.
	expectPatterns(t, minePatterns("info", words(30), words(30, 28, 29)), strings.Join(strings.Fields(words(30))[:28], " ")+" <_> <_>")
	if got := minePatterns("info", words(30), words(30, 27)); len(got) != 2 {
		t.Errorf("a differing 28th token joined: %q", got)
	}
	if got := minePatterns("info", words(40), words(40, 33)); len(got) != 1 {
		t.Errorf("a differing token past the 28th split a cluster: %q", got)
	}
}

// Logfmt and JSON lines are clustered by their tokens like any other line, and never with lines of another
// format.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersByFormat(t *testing.T) {
	expectPatterns(t, minePatterns("info",
		"id=11 user=bob ok=true x=y",
		"id=12 user=bob ok=true x=z",
		"id=13 user=bob ok=true x=w"), "id=<_> user=bob ok=true x=<_>")
	// A logfmt timestamp is a variable whatever it reads.
	expectPatterns(t, minePatterns("info",
		"ts=2026-11-06T10:00:00Z level=info msg=started worker=a",
		"ts=2026-11-06T10:00:01Z level=info msg=started worker=b"), "ts=<_> level=info msg=started worker=<_>")
	expectPatterns(t, minePatterns("info",
		`{"msg":"user 1 logged in"}`,
		`{"msg":"user 2 logged in"}`), "<_>user <_> logged in<_>")
	// The same four tokens as plain text, as JSON and as logfmt do not meet.
	expectPatterns(t, minePatterns("info", "a b c d", `{"msg":"a b c d"}`, "a=b c=d"), "a b c d", "<_>a b c d<_>", "a=b c=d")
}

// Levels never share a cluster.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersKeepLevelsApart(t *testing.T) {
	miner := newPatternMiner()
	for _, level := range []string{"info", "warn", "error"} {
		miner.Observe(level, "user 1 logged in", 0)
		miner.Observe(level, "user 2 logged in", 0)
	}
	entries := patternEntriesByLevelAndPattern(miner)
	if len(entries) != 3 {
		t.Fatalf("%d entries for three levels", len(entries))
	}
	for _, e := range entries {
		if e.pattern != "user <_> logged in" || e.total != 2 {
			t.Errorf("%s: %q with %d lines", e.level, e.pattern, e.total)
		}
	}
}

// The same lines in the same order give the same answer every time: the miner reads no map in an order it was not
// given, and a line joins the cluster that was made first when clusters tie. (Clustering depends on the order of
// the lines, so shuffled lines are not expected to agree.)
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternClustersAreTheSameEveryRun(t *testing.T) {
	lines := patternMinerCorpusLines(5, 900)
	body := patternMinerCorpusNDJSON(lines, 0)
	var first string
	for run := 0; run < 12; run++ {
		got, _ := extractLogPatternsStreamWithStats(bytes.NewReader(body), "1m", 1000, defaultLogRowLevels())
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if run == 0 {
			first = string(encoded)
		} else if string(encoded) != first {
			t.Fatalf("run %d answered differently from the first", run)
		}
	}
	// Clusters come back in the order they were made, whatever level they have.
	miner := newPatternMiner()
	for _, l := range lines {
		miner.Observe("info", l, 0)
	}
	again := newPatternMiner()
	for _, l := range lines {
		again.Observe("info", l, 0)
	}
	a, b := miner.AllClusters(), again.AllClusters()
	if len(a) != len(b) {
		t.Fatalf("%d clusters, then %d", len(a), len(b))
	}
	for i := range a {
		if miner.joinPatternTokens(a[i].tokens, a[i].spacesAfter, a[i].format) != again.joinPatternTokens(b[i].tokens, b[i].spacesAfter, b[i].format) {
			t.Fatalf("cluster %d differs between runs", i)
		}
	}
}

// A group counted from VictoriaLogs is two lines, the smallest and the largest, trained in that order. Its count goes
// to the cluster the smallest ended in. When the largest joined that cluster there is one; when it made a cluster
// of its own, or joined another, that cluster gets nothing from the group, and a cluster no group counted is no
// pattern in the answer.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternGroupCountsGoToTheSmallestLinesCluster(t *testing.T) {
	count := func(m *patternMiner, c *patternBucket, n int) {
		c.buckets[60] += n
		c.total += n
	}
	// Both lines in one cluster.
	miner := newPatternMiner()
	c := miner.observeGroup("info", "job 1 done ok x", "job 9 done ok y")
	count(miner, c, 5)
	if got := patternEntriesByLevelAndPattern(miner); len(got) != 1 || got[0].pattern != "job <_> done ok <_>" || got[0].total != 5 {
		t.Errorf("one cluster expected, got %d entries", len(got))
	}
	// The largest line starts a cluster of its own: it gets no count and is no pattern.
	miner = newPatternMiner()
	c = miner.observeGroup("info", "a 1 2 x", "a 5 6 y")
	count(miner, c, 7)
	if len(miner.AllClusters()) != 2 {
		t.Fatalf("%d clusters trained, want 2", len(miner.AllClusters()))
	}
	got := patternEntriesByLevelAndPattern(miner)
	if len(got) != 1 || got[0].pattern != "a 1 2 x" || got[0].total != 7 {
		t.Errorf("entries %+v", got)
	}
	// The largest line joins a cluster that is counted already: that cluster keeps its own count only.
	miner = newPatternMiner()
	other := miner.observeLine("info", "a 5 6 y", 60, 3)
	c = miner.observeGroup("info", "a 1 2 x", "a 5 6 y")
	if c == other {
		t.Fatal("the smallest line joined the largest's cluster")
	}
	count(miner, c, 7)
	if other.total != 3 || c.total != 7 {
		t.Errorf("counts %d and %d, want 3 and 7", other.total, c.total)
	}
	// A smallest line the miner does not read is no cluster.
	if miner.observeGroup("info", "too short", "a 5 6 y") != nil {
		t.Error("an unreadable smallest line gave a cluster")
	}
	// A largest line the miner does not read changes nothing.
	if miner.observeGroup("info", "a 1 2 x", "short") == nil {
		t.Error("a readable smallest line gave no cluster")
	}
}

// Access, worker and logfmt-like lines mined together: values that change from line to line, a number or an
// address, are variables; a word that stays, or a value of a few kinds, stays in the pattern, as the clusters of
// lines of one kind keep it. Every line still matches a pattern under the pattern line filter Logs Drilldown
// adds when a pattern is included.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsOfMixedLinesMatchTheirLines(t *testing.T) {
	var lines []string
	paths := []string{"/api/v1/cart", "/api/v1/users", "/healthz", "/api/v1/orders/17", "/static/app.js", "/login"}
	agents := []string{"kube-probe/1.28", "sqlmap/1.7.8#stable (https://sqlmap.org)", "Mozilla/5.0 (X11; Linux x86_64)", "curl/8.4.0", "Go-http-client/1.1", "python-requests/2.31"}
	users := []string{"-", "admin"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	for i := 0; i < 240; i++ {
		lines = append(lines,
			fmt.Sprintf(`10.0.%d.%d - %s [06/Nov/2026:10:%02d:%02d +0000] "%s %s HTTP/1.1" %d %d "-" "%s" %d`,
				i%7, i%250, users[i%2], i%60, i%59, methods[i%4], paths[i%6], []int{200, 201, 404, 500}[i%4], 100+i*13, agents[i%6], i%9),
			fmt.Sprintf("worker %d processed %d items in %dms", i%5, i*3, i%70),
			fmt.Sprintf("component=collector status=200 took %dms", 5+i%40),
		)
	}
	patterns := extractLogPatterns(patternRowsOf("info", lines...), "1m", 1000, defaultLogRowLevels())
	var texts []string
	for _, p := range patterns {
		texts = append(texts, p["pattern"].(string))
	}
	for _, want := range []string{"worker <_> processed <_> items in <_>", "component=collector status=200 took <_>"} {
		if !slices.Contains(texts, want) {
			t.Errorf("pattern %q missing from %q", want, texts)
		}
	}
	for _, text := range texts {
		for _, raw := range []string{"10.0.", "06/Nov", "worker 1", "worker 2", "took 5"} {
			if strings.Contains(text, raw) {
				t.Errorf("pattern %q keeps the changing value %q", text, raw)
			}
		}
	}
	for _, line := range lines {
		matched := false
		for _, text := range texts {
			if lokiPatternLineMatches(text, line) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("line %q matches none of its patterns %q", line, texts)
		}
	}
}

// Keys stay: logfmt lines with different keys in a place are different kinds of line, never key <_>=<_>, and the
// level of a line is its own cluster's: level=info lines and level=warn lines are never one pattern.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsKeepKeysAndLevels(t *testing.T) {
	var body []byte
	for _, level := range []string{"info", "warn", "error"} {
		var lines []string
		for i := 0; i < 30; i++ {
			lines = append(lines,
				fmt.Sprintf(`level=%s msg="cache hit" key=k%d ttl=%ds size_bytes=%d op=get`, level, i, i%9+1, 100+i*7),
				fmt.Sprintf(`level=%s msg="cache write" key=k%d op=set ttl=%ds size_bytes=%d`, level, i, i%9+1, 100+i*7),
			)
		}
		body = append(body, patternRowsOf(level, lines...)...)
	}
	patterns := extractLogPatterns(body, "1m", 1000, defaultLogRowLevels())
	got := map[string]bool{}
	for _, p := range patterns {
		text, level := p["pattern"].(string), p["level"].(string)
		got[level+" "+text] = true
		if strings.Contains(text, "<_>=") {
			t.Errorf("%s pattern %q wildcards a key", level, text)
		}
		if !strings.HasPrefix(text, "level="+level+" ") {
			t.Errorf("%s pattern %q does not keep its level", level, text)
		}
	}
	if len(got) != 6 {
		t.Errorf("%d patterns for two kinds of line at three levels: %v", len(got), got)
	}
	for _, level := range []string{"info", "warn", "error"} {
		for _, want := range []string{
			level + ` level=` + level + ` msg="cache hit" key=<_> ttl=<_> size_bytes=<_> op=get`,
			level + ` level=` + level + ` msg="cache write" key=<_> op=set ttl=<_> size_bytes=<_>`,
		} {
			if !got[want] {
				t.Errorf("pattern %q missing from %v", want, got)
			}
		}
	}
}

// patternReadsLine reports whether a line reads as a pattern when its variables are placed wherever they fit:
// every literal of the pattern in order, every variable at least one character, the whole line.
func patternReadsLine(pattern, line string) bool {
	var re strings.Builder
	re.WriteString("(?s)^")
	for pattern != "" {
		if strings.HasPrefix(pattern, patternVarPlaceholder) {
			re.WriteString(".+")
			pattern = pattern[len(patternVarPlaceholder):]
			continue
		}
		next := strings.Index(pattern, patternVarPlaceholder)
		if next < 0 {
			next = len(pattern)
		}
		re.WriteString(regexp.QuoteMeta(pattern[:next]))
		pattern = pattern[next:]
	}
	re.WriteString("$")
	return regexp.MustCompile(re.String()).MatchString(line)
}

// Every line the miner accepts reads as one of the patterns answered for its level, whether it is mined alone or
// with others. Lines the property cannot judge are skipped:
//   - a '<' or '>' reads as a capture in the pattern line filter, and a newline, a carriage return or an invalid
//     UTF-8 byte is not text its lines hold;
//   - spaces at the ends of a line or in runs are written by the tokenizer once between tokens;
//   - lines mined together that space their tokens differently ("a b 0" and "a b!0"), as a cluster is written
//     with the spaces of the first line it was made of;
//   - JSON and logfmt lines are written from their parsed values, so an escape or a quote the line holds is not
//     written as the line has it: they are judged only when their pattern alone reads as them;
//   - a line that reads as a pattern but that the filter's first-occurrence rule for literals (shared with Loki's
//     filter) cannot place, a variable's value holding the literal after it: {""1 against {<_>"<_>.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func FuzzPatternsMatchTheirLines(f *testing.F) {
	for _, seed := range [][3]string{
		{`1.2.3.4 - admin [x +0000] "GET /a HTTP/1.1" 200 1 "-" "curl/8" 3`, `5.6.7.8 - - [y +0000] "GET /b/c HTTP/1.1" 404 22 "-" "kube-probe/1.28" 0`, `9.9.9.9 - - [z +0000] "GET /d HTTP/1.1" 500 3 "-" "sqlmap/1.7.8#stable (https://sqlmap.org)" 1`},
		{"level=info msg=\"cache hit\" key=k1 ttl=5s", "level=info msg=\"cache miss\" key=k2 ttl=9s", "level=info msg=done key=k3 ttl=1s"},
		{`{"message":"user login failed for alice"}`, `{"message":"user login failed for bob"}`, `{"msg":"user login ok for carol now"}`},
		{"worker 1 processed 3 items", "worker 2 processed 30 items", "worker 3 processed 300 items"},
		{"a b c d", "a b c e", "a b x d"},
		{`k="a b" c d e`, `k="a c" c d f`, `k="x" c d g`},
	} {
		f.Add(seed[0], seed[1], seed[2])
	}
	f.Fuzz(func(t *testing.T, a, b, c string) {
		lines := []string{a, b, c}
		for _, line := range lines {
			if strings.ContainsAny(line, "\n\r<>") || !utf8.ValidString(line) ||
				strings.HasPrefix(line, " ") || strings.HasSuffix(line, " ") || strings.Contains(line, "  ") {
				return
			}
		}
		reads := func(entries []*patternBucket, line string, read func(pattern, line string) bool) bool {
			for _, e := range entries {
				if e.level == "info" && read(e.pattern, line) {
					return true
				}
			}
			return false
		}
		check := func(mined ...string) {
			miner := newPatternMiner()
			for _, line := range mined {
				miner.Observe("info", line, 0)
			}
			entries := patternEntriesByLevelAndPattern(miner)
			var spaces []int
			spaced := false
			for _, line := range mined {
				if _, s, _, ok := miner.patternLineTokens(line); ok {
					if spaced && !slices.Equal(spaces, s) {
						return
					}
					spaces, spaced = s, true
				}
			}
			for _, line := range mined {
				_, _, format, ok := miner.patternLineTokens(line)
				if !ok {
					continue
				}
				if format != patternFormatPlain {
					alone := newPatternMiner()
					alone.Observe("info", line, 0)
					if !reads(patternEntriesByLevelAndPattern(alone), line, patternReadsLine) {
						continue
					}
				}
				if reads(entries, line, lokiPatternLineMatches) {
					continue
				}
				if reads(entries, line, patternReadsLine) {
					continue // the filter's rule for literals cannot place it
				}
				var patterns []string
				for _, e := range entries {
					patterns = append(patterns, e.pattern)
				}
				t.Fatalf("line %q of %q reads as none of %q", line, mined, patterns)
			}
		}
		check(lines...)
		for _, line := range lines {
			check(line)
		}
	})
}

// The miner's answer for 20k varied access-log lines.
func BenchmarkExtractLogPatternsVariedLines(b *testing.B) {
	var lines []string
	for i := 0; i < 20000; i++ {
		lines = append(lines, fmt.Sprintf(`10.%d.%d.%d - u%d [06/Nov/2026:10:%02d:%02d +0000] "%s /api/v%d/item/%d HTTP/1.1" %d %d "-" "agent-%d/%d.%d" %d`,
			i%200, i%97, i%13, i%31, i%60, i%59, []string{"GET", "POST", "PUT", "DELETE"}[i%4], i%3, i, 200+i%5, i*7, i%50, i%9, i%7, i%11))
	}
	body := patternRowsOf("info", lines...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractLogPatterns(body, "1m", 1000, defaultLogRowLevels())
	}
}

// uniquePatternLine is the k-th of lines that share no token with each other, so each starts a cluster of its own.
func uniquePatternLine(k int) string {
	words := make([]string, 8)
	for i := range words {
		n := k*8 + i
		words[i] = fmt.Sprintf("w%c%c%c", 'a'+n/676%26, 'a'+n/26%26, 'a'+n%26)
	}
	return strings.Join(words, " ")
}

func clusterTexts(m *patternMiner) []string {
	var texts []string
	for _, c := range m.AllClusters() {
		texts = append(texts, m.joinPatternTokens(c.tokens, c.spacesAfter, c.format))
	}
	return texts
}

// A miner keeps patternMaxClusters clusters: the 301st replaces the one used least recently, and a cluster a line
// joined again counts as used.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternMinerKeepsThreeHundredClustersEvictingTheLeastRecentlyUsed(t *testing.T) {
	miner := newPatternMiner()
	for k := 0; k < patternMaxClusters; k++ {
		miner.Observe("info", uniquePatternLine(k), 0)
	}
	if got := len(miner.AllClusters()); got != patternMaxClusters {
		t.Fatalf("%d clusters after %d lines, want all kept", got, patternMaxClusters)
	}
	// The first line again: its cluster is the most recently used now, so the second line's is the one to go.
	miner.Observe("info", uniquePatternLine(0), 0)
	miner.Observe("info", uniquePatternLine(patternMaxClusters), 0)
	texts := clusterTexts(miner)
	if len(texts) != patternMaxClusters {
		t.Fatalf("%d clusters, want the cap of %d", len(texts), patternMaxClusters)
	}
	if !slices.Contains(texts, uniquePatternLine(0)) || slices.Contains(texts, uniquePatternLine(1)) || !slices.Contains(texts, uniquePatternLine(2)) || !slices.Contains(texts, uniquePatternLine(patternMaxClusters)) {
		t.Errorf("the wrong cluster was evicted: first %v, second %v", slices.Contains(texts, uniquePatternLine(0)), slices.Contains(texts, uniquePatternLine(1)))
	}
}

// An evicted cluster takes its samples with it: they are in no answer.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternMinerEvictedClustersLoseTheirSamples(t *testing.T) {
	miner := newPatternMiner()
	for k := 0; k < patternMaxClusters+1; k++ {
		miner.Observe("info", uniquePatternLine(k), 60)
	}
	for _, e := range patternEntriesByLevelAndPattern(miner) {
		if e.pattern == uniquePatternLine(0) {
			t.Fatalf("the evicted cluster's pattern is answered with %d lines", e.total)
		}
	}
	total := 0
	for _, e := range patternEntriesByLevelAndPattern(miner) {
		total += e.total
	}
	if total != patternMaxClusters {
		t.Errorf("%d lines answered, want the %d of the clusters kept", total, patternMaxClusters)
	}
}

// Once evictions exceed a quarter of the lines admitted, the miner admits no more lines: of lines that each start a
// cluster, the 401st is the last (101 evictions against 401 lines), whatever comes after, even a line of a cluster
// kept. Other streams and levels have miners of their own.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternMinerStopsAdmittingLinesAfterTooManyEvictions(t *testing.T) {
	miner := newPatternMiner()
	admitted := 0
	for k := 0; k < 500; k++ {
		if miner.observeStreamLine("a", "info", uniquePatternLine(k), 60, 1) != nil {
			admitted++
		}
	}
	if want := 401; admitted != want {
		t.Errorf("%d lines admitted, want %d", admitted, want)
	}
	if got := miner.observeStreamLine("a", "info", uniquePatternLine(499), 60, 1); got != nil {
		t.Error("a line of a cluster the miner kept was admitted after it stopped")
	}
	if got := len(miner.AllClusters()); got != patternMaxClusters {
		t.Errorf("%d clusters kept, want %d", got, patternMaxClusters)
	}
	if miner.observeStreamLine("b", "info", uniquePatternLine(0), 60, 1) == nil {
		t.Error("another stream's miner stopped with it")
	}
	if miner.observeStreamLine("a", "warn", uniquePatternLine(0), 60, 1) == nil {
		t.Error("another level's miner stopped with the stream's")
	}
	// Lines that join clusters evict nothing: any number of them is admitted.
	miner = newPatternMiner()
	for k := 0; k < 1000; k++ {
		miner.Observe("info", "same line again and again", 60)
	}
	if got := patternEntriesByLevelAndPattern(miner)[0].total; got != 1000 {
		t.Errorf("%d of 1000 repeated lines admitted", got)
	}
}

// Lines of different streams are mined apart, as Loki's pattern ingester does: two pods whose lines differ only in
// their last token are two patterns, one for each, where together they are one. A stream's lines are mined oldest
// first, whatever order the source gave them in.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsAreMinedPerStreamOldestFirst(t *testing.T) {
	row := func(stream, msg string, second int) string {
		encoded, _ := json.Marshal(map[string]string{"_time": fmt.Sprintf("2026-11-06T10:00:%02dZ", second), "_msg": msg, "level": "info", "_stream_id": stream})
		return string(encoded) + "\n"
	}
	mine := func(rows ...string) []string {
		var texts []string
		for _, p := range extractLogPatterns([]byte(strings.Join(rows, "")), "10s", maxPatternResponseLimit, defaultLogRowLevels()) {
			texts = append(texts, p["pattern"].(string))
		}
		sort.Strings(texts)
		return texts
	}
	if got := mine(row("pod-a", "paid order for eur", 1), row("pod-b", "paid order for usd", 2)); !slices.Equal(got, []string{"paid order for eur", "paid order for usd"}) {
		t.Errorf("two streams gave %q", got)
	}
	if got := mine(row("pod-a", "paid order for eur", 1), row("pod-a", "paid order for usd", 2)); !slices.Equal(got, []string{"paid order for <_>"}) {
		t.Errorf("one stream gave %q", got)
	}
	// Distinct times, newest first or not, give one answer.
	var asc, desc []string
	for i := 0; i < 50; i++ {
		asc = append(asc, row("s", fmt.Sprintf("job %d finished with status %s", i, []string{"ok", "failed", "retry"}[i%3]), i))
	}
	for i := len(asc) - 1; i >= 0; i-- {
		desc = append(desc, asc[i])
	}
	if a, b := mine(asc...), mine(desc...); !slices.Equal(a, b) {
		t.Errorf("newest first %q, oldest first %q", b, a)
	}
}

// Loki answers a pattern only when it holds lokiPatternMinLines lines inside the requested range: 29 lines are
// left out, 30 answered, and a pattern of 40 lines of which 25 are in the range is left out. A sample counts when
// the step it starts is inside [start, end).
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsNeedThirtyLinesInTheRange(t *testing.T) {
	entry := func(pattern string, samples ...[2]int) patternResultEntry {
		e := patternResultEntry{Pattern: pattern}
		for _, s := range samples {
			e.Samples = append(e.Samples, []interface{}{int64(s[0]), s[1]})
		}
		return e
	}
	const start, end = int64(1000), int64(2000)
	entries := []patternResultEntry{
		entry("twenty nine lines here", [2]int{1000, 29}),
		entry("thirty lines here ok", [2]int{1000, 10}, [2]int{1500, 20}),
		entry("forty lines of which twenty five in", [2]int{900, 15}, [2]int{1100, 25}),
		entry("forty lines of which thirty in", [2]int{900, 10}, [2]int{1100, 30}),
		entry("starts at the end", [2]int{1990, 10}, [2]int{2000, 30}),
	}
	var kept []string
	for _, e := range dropPatternsBelowMinLines(entries, start*int64(time.Second), end*int64(time.Second)) {
		kept = append(kept, e.Pattern)
	}
	if want := []string{"thirty lines here ok", "forty lines of which thirty in"}; !slices.Equal(kept, want) {
		t.Errorf("kept %q, want %q", kept, want)
	}
	// Without a range every sample counts.
	if got := dropPatternsBelowMinLines(entries, 0, 0); len(got) != 4 {
		t.Errorf("%d kept without a range, want the four with 30 lines or more", len(got))
	}
	if lokiPatternMinLines != 30 {
		t.Errorf("the threshold is %d", lokiPatternMinLines)
	}
}

// A JSON line is mined from its message as the line writes it, escapes and all: a "\n" in the source stays the two
// characters, as in Loki's pattern.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternJSONMessageKeepsItsEscapes(t *testing.T) {
	for _, tc := range []struct{ line, want string }{
		{`{"msg":"GET /a returned 200 in 5ms\nnext line"}`, `GET /a returned 200 in 5ms\nnext line`},
		{`{"level":"info","n":{"msg":"nested"},"msg":"say \"hi\" to all of you","x":1}`, `say \"hi\" to all of you`},
		{`{"a":[1,"msg"],"message":"café is open now, come in"}`, `café is open now, come in`},
		{`{"_msg":"plain text of four","msg":"first by order of fields wins"}`, `first by order of fields wins`},
	} {
		got, ok := patternJSONMessage(tc.line)
		if !ok || got != tc.want {
			t.Errorf("%s\n got %q (%v)\nwant %q", tc.line, got, ok, tc.want)
		}
	}
}
