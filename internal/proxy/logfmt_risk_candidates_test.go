package proxy

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// logfmtCandidateProbeRE matches the candidate probe of logfmtParseRisk
// (logfmtParseRiskQueries): clauses of absent stored fields, each with the
// key's expression or none, and no line-shape expression.
var logfmtCandidateProbeRE = regexp.MustCompile(`^.* \| filter (\(-\S+:\*(?: -\S+:\*)*(?: _msg:~"(?:[^"\\]|\\.)*")?\)(?: or \(-\S+:\*(?: -\S+:\*)*(?: _msg:~"(?:[^"\\]|\\.)*")?\))*) \| limit 1$`)

// logfmtCandidateClauseRE reads one clause of a candidate probe.
var logfmtCandidateClauseRE = regexp.MustCompile(`\(((?:-\S+:\*(?: |\)))+)(?:_msg:~("(?:[^"\\]|\\.)*")\))?`)

func isLogfmtCandidateProbe(query string) bool {
	return logfmtCandidateProbeRE.MatchString(query)
}

// logfmtCandidateClause is one clause of a candidate probe: the fields that
// must be absent and the key expression (nil: none).
type logfmtCandidateClause struct {
	absent []string
	key    *regexp.Regexp
}

// logfmtCandidateClauses caches the parsed clauses of each candidate probe.
var logfmtCandidateClauses sync.Map

func parseLogfmtCandidates(query string, keep bool) []logfmtCandidateClause {
	if cached, ok := logfmtCandidateClauses.Load(query); ok {
		return cached.([]logfmtCandidateClause)
	}
	m := logfmtCandidateProbeRE.FindStringSubmatch(query)
	if m == nil {
		return nil
	}
	var clauses []logfmtCandidateClause
	for _, raw := range logfmtCandidateClauseRE.FindAllStringSubmatch(m[1], -1) {
		var clause logfmtCandidateClause
		for _, field := range strings.Fields(strings.ReplaceAll(raw[1], ")", " ")) {
			clause.absent = append(clause.absent, strings.Trim(strings.TrimSuffix(strings.TrimPrefix(field, "-"), ":*"), "`\""))
		}
		if raw[2] != "" {
			pattern, err := strconv.Unquote(raw[2])
			if err != nil {
				panic(err)
			}
			clause.key = regexp.MustCompile(pattern)
		}
		clauses = append(clauses, clause)
	}
	if keep {
		logfmtCandidateClauses.Store(query, clauses)
	}
	return clauses
}

// logfmtCandidateMatches evaluates a candidate probe for a line: a clause
// matches when every field it names is absent (empty) in stored and the line
// matches its key expression.
func logfmtCandidateMatches(query string, stored map[string]string, msg string) bool {
	return logfmtClausesMatch(parseLogfmtCandidates(query, true), stored, msg)
}

func logfmtClausesMatch(clauses []logfmtCandidateClause, stored map[string]string, msg string) bool {
	for _, clause := range clauses {
		absent := true
		for _, name := range clause.absent {
			if stored[name] != "" {
				absent = false
			}
		}
		if absent && (clause.key == nil || clause.key.MatchString(msg)) {
			return true
		}
	}
	return false
}

// logfmtRiskReference is Loki's side of logfmtParseRisk for one line, with the
// probe's own expressions: a line lacking every stored spelling of a field is
// risky when its key is in a shape the two parsers read differently, or when a
// label may come from a key spelled differently.
func logfmtRiskReference(fields []string, stored map[string]string, values map[string]string, msg string) bool {
	match := func(pattern string) bool { return regexp.MustCompile(pattern).MatchString(msg) }
	absent := func(field string) bool {
		if values[field] != "" {
			return false
		}
		if vl, ok := stored[field]; ok && values[vl] != "" {
			return false
		}
		return true
	}
	anyAbsent := false
	for _, field := range fields {
		if !absent(field) {
			continue
		}
		anyAbsent = true
		re := newLogfmtRiskPatterns(field)
		if match(re.key) && (!match(re.wellFormed) || match(re.repeated) || (match(re.keyAssignment) && !match(re.safeValue))) {
			return true
		}
	}
	if alias := logfmtKeyAliasPattern(fields); alias != "" && anyAbsent && match(alias) {
		return true
	}
	return false
}

// The candidate probe answers the windows without a candidate line, where the
// probe is not sent, and leaves every other verdict to the probe; with an
// earlier candidate seen the probe runs alone. The verdict is the probe's
// alone, over stored and body-only levels, keys spelled alike and
// differently, risky and safe shapes and an empty window.
// conformance: quality/logfmt-parse-risk-candidates, parser-logfmt
func TestLogfmtParseRiskCandidatesEdgeTable(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	stream := func(level string) map[string]string {
		labels := map[string]string{"app": "api"}
		if level != "" {
			labels["level"] = level
		}
		return labels
	}
	versioned := map[string]string{"service_version": "service.version"}
	for _, tc := range []struct {
		name       string
		fields     []string
		stored     map[string]string
		rows       []pushdownRow
		candidates bool // the candidate probe finds a line
		risky      bool
	}{
		{"stored level, body level", []string{"level"}, nil, []pushdownRow{{stream: stream("info"), msg: "level=error msg=collision"}}, false, false},
		{"stored level, tab before the key", []string{"level"}, nil, []pushdownRow{{stream: stream("info"), msg: "n=1\tlevel=warn"}}, false, false},
		{"no level anywhere", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "op=select n=1"}}, false, false},
		{"body level, well formed", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: `level=warn op=update msg="slow query"`}}, true, false},
		{"body level after a tab", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "n=1\tlevel=warn"}}, true, true},
		{"body level repeated", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "level=info level=error"}}, true, true},
		{"longer key holding the name", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "loglevel=debug msg=ok"}}, true, false},
		{"quoted value with an escape", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: `level="a \"b\""`}}, true, true},
		{"quoted value with a space", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: `level="a b" x=1`}}, true, false},
		{"non-ASCII value", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "level=错误"}}, true, true},
		{"empty value", []string{"level"}, nil, []pushdownRow{{stream: stream(""), msg: "level="}}, true, false},
		{"mixed window, one risky line", []string{"level"}, nil, []pushdownRow{
			{stream: stream("info"), msg: "n=1\tlevel=warn"},
			{stream: stream(""), msg: "level=info msg=ok"},
			{stream: stream(""), msg: "msg='a b' level=warn"},
		}, true, true},
		{"empty window", []string{"level"}, nil, nil, false, false},
		{"underscore label, stored spelling present", []string{"service_version"}, versioned, []pushdownRow{{stream: stream(""), vl: map[string]string{"service.version": "1"}, msg: "service-version=2"}}, false, false},
		{"underscore label, key spelled differently", []string{"service_version"}, versioned, []pushdownRow{{stream: stream(""), msg: "service-version=2 op=x"}}, true, true},
		{"underscore label, plain line", []string{"service_version"}, versioned, []pushdownRow{{stream: stream(""), msg: "op=x n=1"}}, true, false},
		{"two labels, one stored", []string{"level", "op"}, nil, []pushdownRow{{stream: stream("info"), msg: "op=a b=1"}}, true, false},
		{"two labels, the other risky", []string{"level", "op"}, nil, []pushdownRow{{stream: stream("info"), msg: "b=1\top=a"}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]pushdownRow, len(tc.rows))
			for i, row := range tc.rows {
				row.ts = s0.Add(time.Duration(i+1) * time.Second)
				rows[i] = row
			}
			srv, fake := newPushdownFakeVL(t, rows, nil)
			p := newSlidingTestProxy(t, srv.URL)
			start, end := s0, s0.Add(time.Minute)
			base := `app:="api"`
			full, candidateQuery := logfmtParseRiskQueries(base, tc.fields, tc.stored)
			if !isLogfmtCandidateProbe(candidateQuery) || isLogfmtCandidateProbe(full) {
				t.Fatalf("probe shapes: candidates %q full %q", candidateQuery, full)
			}
			found, err := p.statsPushdownRiskExists(context.Background(), candidateQuery, start, end)
			if err != nil || found != tc.candidates {
				t.Fatalf("candidate probe found=%v err=%v, want %v", found, err, tc.candidates)
			}
			ungated, err := p.statsPushdownRiskExists(context.Background(), full, start, end)
			if err != nil || ungated != tc.risky {
				t.Fatalf("probe alone risky=%v err=%v, want %v", ungated, err, tc.risky)
			}
			calls := func() (candidates, probes int) {
				fake.mu.Lock()
				defer fake.mu.Unlock()
				return len(fake.candidates), len(fake.guards)
			}
			heldWant := logfmtCandidatesClean
			if tc.candidates {
				heldWant = logfmtCandidatesSeen
			}
			// Nothing checked before: both probes, the probe's verdict.
			c0, g0 := calls()
			risky, held, err := p.logfmtParseRisk(context.Background(), base, tc.fields, tc.stored, start, end, "")
			if err != nil || risky != tc.risky || (!risky && held != heldWant) {
				t.Fatalf("first: risky=%v held=%q err=%v, want %v %q", risky, held, err, tc.risky, heldWant)
			}
			// A risky verdict does not wait for the candidate probe; let it land.
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
				if c1, _ := calls(); c1-c0 == 1 {
					break
				}
			}
			if c1, g1 := calls(); c1-c0 != 1 || g1-g0 != 1 {
				t.Fatalf("first: candidate probes %d and probes %d, want 1 and 1", c1-c0, g1-g0)
			}
			// Clean parts before: the candidate probe, then the probe only for a candidate line.
			c0, g0 = calls()
			risky, held, err = p.logfmtParseRisk(context.Background(), base, tc.fields, tc.stored, start, end, logfmtCandidatesClean)
			if err != nil || risky != tc.risky || held != heldWant {
				t.Fatalf("clean: risky=%v held=%q err=%v, want %v %q", risky, held, err, tc.risky, heldWant)
			}
			wantProbes := 0
			if tc.candidates {
				wantProbes = 1
			}
			if c1, g1 := calls(); c1-c0 != 1 || g1-g0 != wantProbes {
				t.Fatalf("clean: candidate probes %d and probes %d, want 1 and %d", c1-c0, g1-g0, wantProbes)
			}
			// A candidate line before: the probe alone.
			c0, g0 = calls()
			risky, held, err = p.logfmtParseRisk(context.Background(), base, tc.fields, tc.stored, start, end, logfmtCandidatesSeen)
			if err != nil || risky != tc.risky || held != logfmtCandidatesSeen {
				t.Fatalf("seen: risky=%v held=%q err=%v, want %v", risky, held, err, tc.risky)
			}
			if c1, g1 := calls(); c1 != c0 || g1-g0 != 1 {
				t.Fatalf("seen: candidate probes %d and probes %d, want 0 and 1", c1-c0, g1-g0)
			}
		})
	}
}

// candidateGateFakeVL answers the candidate probe from candidates (or fails
// it) and the probe with a risky line, counting both.
type candidateGateFakeVL struct {
	mu             sync.Mutex
	candidates     bool
	candidateFail  bool
	candidateCalls int
	probeCalls     int
}

func newCandidateGateFakeVL(t *testing.T, fake *candidateGateFakeVL) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		query := r.Form.Get("query")
		if r.URL.Path != "/select/logsql/query" || !strings.HasSuffix(query, " | limit 1") {
			http.NotFound(w, r)
			return
		}
		row, _ := json.Marshal(map[string]string{"_time": "2023-11-14T22:13:21Z", "_msg": "n=1\tlevel=warn", "_stream": `{app="api"}`})
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if isLogfmtCandidateProbe(query) {
			fake.candidateCalls++
			if fake.candidateFail {
				http.Error(w, "backend failure", http.StatusInternalServerError)
				return
			}
			if fake.candidates {
				_, _ = w.Write(append(row, '\n'))
			}
			return
		}
		fake.probeCalls++
		_, _ = w.Write(append(row, '\n'))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// After clean parts, a window without a candidate line is answered without the
// probe, whose regular expressions VictoriaLogs takes a fixed time to
// simplify; a window with one, a failed candidate probe or an earlier
// candidate seen sends the probe, whose verdict is the answer.
// conformance: quality/logfmt-parse-risk-candidates
func TestLogfmtParseRiskCandidatesSkipTheProbe(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	for _, tc := range []struct {
		name             string
		candidates, fail bool
		seen             string
		risky            bool
		candidateCalls   int
		probeCalls       int
	}{
		{"clean before, no candidate", false, false, logfmtCandidatesClean, false, 1, 0},
		{"clean before, candidate line", true, false, logfmtCandidatesClean, true, 1, 1},
		{"clean before, candidate probe fails", false, true, logfmtCandidatesClean, true, 1, 1},
		{"candidate seen before", false, false, logfmtCandidatesSeen, true, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &candidateGateFakeVL{candidates: tc.candidates, candidateFail: tc.fail}
			p := newSlidingTestProxy(t, newCandidateGateFakeVL(t, fake).URL)
			risky, _, err := p.logfmtParseRisk(context.Background(), `app:="api"`, []string{"level"}, nil, s0, s0.Add(time.Minute), tc.seen)
			if err != nil || risky != tc.risky {
				t.Fatalf("risky=%v err=%v, want %v", risky, err, tc.risky)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.candidateCalls != tc.candidateCalls || fake.probeCalls != tc.probeCalls {
				t.Fatalf("candidate probes %d and probes %d, want %d and %d", fake.candidateCalls, fake.probeCalls, tc.candidateCalls, tc.probeCalls)
			}
		})
	}
}

// logfmtRiskCorpus holds lines of every shape the probe tells apart.
var logfmtRiskCorpus = []string{
	"level=warn ts=2026-09-14T21:14:23+02:00 op=UPDATE duration_ms=3194",
	`msg="disk full" level=error retry`,
	`msg="a level=warn b" user=x`,
	"loglevel=debug msg=ok",
	"no level here",
	`{"level":"error"}`,
	"ts=1\tlevel=error",
	"level=info level=error",
	"level level=error",
	`msg="x"level=error`,
	`level="a \"b\""`,
	"a=b=c level=error",
	"level=错误",
	"  level=error",
	"level=",
	"level='error' msg=x",
	"service-version=1 level=info",
	"service_version=2 op=read",
	"service.version=3 op=write",
	"op=a\tb=1",
	"op=x op=y",
	"",
}

// Over random windows of random lines, stored levels and label sets, the
// probe behind its candidates answers what the probe alone answers, which is
// Loki's side of the probe (logfmtRiskReference).
// conformance: quality/logfmt-parse-risk-candidates, parser-logfmt
func TestLogfmtParseRiskCandidatesDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20261104))
	s0 := time.Unix(1700000400, 0).UTC()
	labelSets := [][]string{{"level"}, {"level", "op"}, {"service_version"}, {"op", "service_version"}}
	versioned := map[string]string{"service_version": "service.version"}
	for i := 0; i < 120; i++ {
		var rows []pushdownRow
		for j := 0; j < 1+rng.Intn(8); j++ {
			labels := map[string]string{"app": "api"}
			if rng.Intn(2) == 0 {
				labels["level"] = []string{"info", "warn"}[rng.Intn(2)]
			}
			row := pushdownRow{ts: s0.Add(time.Duration(rng.Intn(600)) * time.Second), stream: labels, msg: logfmtRiskCorpus[rng.Intn(len(logfmtRiskCorpus))]}
			if rng.Intn(4) == 0 {
				row.vl = map[string]string{"service.version": "1"}
			}
			rows = append(rows, row)
		}
		fields := labelSets[rng.Intn(len(labelSets))]
		from := s0.Add(time.Duration(rng.Intn(300)) * time.Second)
		to := from.Add(time.Duration(1+rng.Intn(400)) * time.Second)
		want := false
		for _, row := range rows {
			if row.ts.Before(from) || row.ts.After(to) {
				continue
			}
			values := map[string]string{}
			for k, v := range row.stream {
				values[k] = v
			}
			for k, v := range row.vl {
				values[k] = v
			}
			want = want || logfmtRiskReference(fields, versioned, values, row.msg)
		}
		srv, _ := newPushdownFakeVL(t, rows, nil)
		p := newSlidingTestProxy(t, srv.URL)
		full, _ := logfmtParseRiskQueries(`app:="api"`, fields, versioned)
		ungated, err := p.statsPushdownRiskExists(context.Background(), full, from, to)
		if err != nil {
			t.Fatal(err)
		}
		seen := []string{"", logfmtCandidatesClean, logfmtCandidatesSeen}[rng.Intn(3)]
		gated, _, err := p.logfmtParseRisk(context.Background(), `app:="api"`, fields, versioned, from, to, seen)
		if err != nil {
			t.Fatal(err)
		}
		if gated != ungated || gated != want {
			t.Fatalf("case %d fields %v window %v..%v: gated=%v probe alone=%v reference=%v rows=%+v", i, fields, from, to, gated, ungated, want, rows)
		}
		srv.Close()
	}
}

// Every line the probe finds is a candidate line, whatever the label name and
// the line, so a window without a candidate line holds no risky one.
func FuzzLogfmtParseRiskCandidatesCoverTheProbe(f *testing.F) {
	for _, line := range logfmtRiskCorpus {
		f.Add(line, "level", "")
		f.Add(line, "service_version", "op")
	}
	f.Add("_1x=1", "_1x", "")
	f.Fuzz(func(t *testing.T, line, first, second string) {
		fields := []string{first}
		if second != "" && second != first {
			fields = append(fields, second)
		}
		// Parsed keys reach a label as Loki label names.
		for _, field := range fields {
			if !lokiLabelNameRE.MatchString(field) || !utf8ValidString(line) {
				return
			}
		}
		full, candidates := logfmtParseRiskQueries(`app:="api"`, fields, nil)
		if !strings.HasSuffix(full, " | limit 1") || !strings.HasSuffix(candidates, " | limit 1") {
			t.Fatalf("probe shapes: %q %q", full, candidates)
		}
		if logfmtRiskReference(fields, nil, map[string]string{}, line) && !logfmtClausesMatch(parseLogfmtCandidates(candidates, false), map[string]string{}, line) {
			t.Fatalf("risky line %q for %q is not a candidate of %q", line, fields, candidates)
		}
	})
}

var lokiLabelNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func utf8ValidString(s string) bool {
	return strings.ToValidUTF8(s, "�") == s
}

// Grafana's logs volume for a `| logfmt` selector, asked again for windows
// moved by refreshes, answers what Loki answers. The first request sends the
// line-shape probe beside its candidate probe. Over lines carrying their level
// as a stored field no window holds a candidate line, so the moved parts are
// checked by the candidate probe alone; over lines holding it only in the
// body the first request finds a candidate line and every moved part is
// checked by the probe alone.
// conformance: quality/logfmt-parse-risk-candidates, parser-logfmt, severity-detected-level-derivation, loki_api_v1_query_range
func TestLogfmtVolumeRefreshSkipsTheLineShapeProbe(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	for _, tc := range []struct {
		name               string
		stored             bool
		candidates, probes int
	}{
		{"stored levels", true, 3, 1},
		{"body levels", false, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lines []jsonVolumeLine
			for i := 0; i < 180; i++ {
				level := []string{"info", "warn", "error"}[i%3]
				stored := ""
				if tc.stored {
					stored = level
				}
				lines = append(lines, jsonVolumeLine{s0.Add(time.Duration(i) * 5 * time.Second), stored, "level=" + level + " op=update n=" + strconv.Itoa(i)})
			}
			srv, fake := newJSONVolumeFakeVL(t, lines)
			p := newSlidingTestProxy(t, srv.URL)
			query := `sum by (level, detected_level) (count_over_time({app="api"} | logfmt | drop __error__[1m]))`
			for i, end := range []time.Time{s0.Add(10 * time.Minute), s0.Add(11 * time.Minute), s0.Add(12 * time.Minute)} {
				start := end.Add(-8 * time.Minute)
				want := lokiLevelVolumeReference(lines, "logfmt", "count_over_time", []string{"level", "detected_level"}, start, end, time.Minute, time.Minute)
				got := runJSONVolumeQueryRange(t, p, query, start, end, time.Minute)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("request %d: result differs from the Loki reference\n got: %v\nwant: %v", i, got, want)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.candidateCalls) != tc.candidates || fake.lineShapeCalls != tc.probes || fake.rawCalls != 0 {
				t.Fatalf("candidate probes %d, line-shape probes %d, raw reads %d; want %d, %d, 0", len(fake.candidateCalls), fake.lineShapeCalls, fake.rawCalls, tc.candidates, tc.probes)
			}
		})
	}
}
