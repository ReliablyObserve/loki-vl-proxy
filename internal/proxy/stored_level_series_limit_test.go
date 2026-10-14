package proxy

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// Loki counts the series of the answer: the spellings of one level are one
// series (the stream level INFO, info and Information all give
// detected_level=info), and a query is refused only when the answer holds more
// than the limit. The stored-level plan reads those spellings as separate
// VictoriaLogs groups, so the limit applies to the merged answer, and a
// filter on detected_level runs in VictoriaLogs, keeping the groups of the
// rows that can match.
//
// conformance: semantics/json-metric-detected-level-derivation
func TestStoredLevelPlanCountsTheSeriesOfTheAnswer(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	// Loki v3.7.7's detected_level for each stream level (normalizeLogLevel).
	loki := map[string]string{"INFO": "info", "warn": "warn", "Warning": "warn", "ERR": "error", "notice": "notice", "dbg": "debug", "info": "info", "Information": "info", "unknown": "unknown"}
	levels := []string{"INFO", "warn", "Warning", "ERR", "notice", "dbg", "info", "Information", "unknown"}
	var lines []jsonVolumeLine
	for i := 0; i < 108; i++ {
		lines = append(lines, jsonVolumeLine{ts: s0.Add(time.Duration(i) * 5 * time.Second), level: levels[i%len(levels)], msg: `{"n":` + strconv.Itoa(i) + `}`})
	}
	start, end := s0.Add(time.Minute), s0.Add(8*time.Minute)
	// matrix counts the lines of (t-1m, t] by the labels of groupBy.
	matrix := func(groupBy string, keep func(detected string) bool) map[string]map[int64]string {
		out := map[string]map[int64]string{}
		for at := start; !at.After(end); at = at.Add(time.Minute) {
			counts := map[string]int{}
			for _, line := range lines {
				if !line.ts.After(at.Add(-time.Minute)) || line.ts.After(at) || !keep(loki[line.level]) {
					continue
				}
				label := map[string]string{"level": line.level, "detected_level": loki[line.level]}[groupBy]
				counts[canonicalLabelsKey(map[string]string{groupBy: label})]++
			}
			for key, n := range counts {
				if out[key] == nil {
					out[key] = map[int64]string{}
				}
				out[key][at.Unix()] = strconv.Itoa(n)
			}
		}
		return out
	}
	any := func(string) bool { return true }
	for _, tc := range []struct {
		name, query, groupBy string
		limit                int
		keep                 func(string) bool
		wantSeries           int // 0: Loki's series limit error
		wantChain            bool
	}{
		// three spellings of info: three series at the limit of three
		{"filter on a level keeps its spellings only", `sum by (level) (count_over_time({app="api"} | json | drop __error__ | detected_level="info" [1m]))`, "level", 3,
			func(d string) bool { return d == "info" }, 3, false},
		{"regexp filter", `sum by (level) (count_over_time({app="api"} | json | drop __error__ | detected_level=~"warn|notice" [1m]))`, "level", 3,
			func(d string) bool { return d == "warn" || d == "notice" }, 3, false},
		{"negated filter", `sum by (level) (count_over_time({app="api"} | json | drop __error__ | detected_level!="info" [1m]))`, "level", 6,
			func(d string) bool { return d != "info" }, 6, false},
		{"a word Loki does not know compares as stored", `sum by (level) (count_over_time({app="api"} | json | drop __error__ | detected_level="notice" [1m]))`, "level", 1,
			func(d string) bool { return d == "notice" }, 1, false},
		// nine spellings, six levels: exactly the limit passes, one more is refused
		{"merged answer at the limit", `sum by (detected_level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, "detected_level", 6, any, 6, false},
		{"merged answer over the limit", `sum by (detected_level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, "detected_level", 5, any, 0, false},
		// 9 groups are past twice the limit of 4: the chain's groups are the series
		{"past the group guard the chain counts", `sum by (detected_level) (count_over_time({app="api"} | json | drop __error__ [1m]))`, "detected_level", 4, any, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, fake := newJSONVolumeFakeVL(t, lines)
			fake.presence = true
			p := newSlidingTestProxy(t, srv.URL)
			p.maxStatsQuerySeries = tc.limit
			code, got, body := detectedLevelQuery(t, p, tc.query, start, end, time.Minute)
			fake.mu.Lock()
			calls := append([]string(nil), fake.statsCalls...)
			raw := fake.rawCalls
			fake.mu.Unlock()
			if tc.wantSeries == 0 {
				if code != http.StatusBadRequest || !strings.Contains(body, fmt.Sprintf("maximum number of series (%d) reached", tc.limit)) {
					t.Fatalf("expected Loki's series limit error, got %d %.200s", code, body)
				}
			} else {
				want := matrix(tc.groupBy, tc.keep)
				if code != http.StatusOK || len(got) != tc.wantSeries || !reflect.DeepEqual(got, want) {
					t.Fatalf("status %d %.200s\n got: %v\nwant: %v\nstats: %q", code, body, got, want, calls)
				}
			}
			if chain := len(chainStatsCalls(calls)) > 0; chain != tc.wantChain || raw != 0 {
				t.Fatalf("chain stats query ran: %v (want %v), raw reads %d, stats %q", chain, tc.wantChain, raw, calls)
			}
		})
	}
}

// 30 pods with a line of each of three levels: the filter keeps a third of the
// groups in VictoriaLogs, so a limit that only the answer's series (30) fit
// under is not tripped by the 90 groups an unfiltered stored-level read holds.
// Without a filter the 90 groups pass the guard (twice the limit) of 40 and
// the chain counts the 30 series instead; the verdict is remembered for that
// query, so the next request goes to the chain at once.
// conformance: semantics/json-metric-detected-level-derivation
func TestStoredLevelPlanManyPodsUnderTheSeriesLimit(t *testing.T) {
	s0 := time.Unix(1700000400, 0).UTC()
	end := s0.Add(5 * time.Minute)
	var rows []pushdownRow
	for pod := 0; pod < 30; pod++ {
		for l, level := range []string{"error", "info", "warn"} {
			rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(10*pod+l+1) * time.Second), stream: map[string]string{"app": "api", "env": "prod", "level": level}, msg: `{"pod":"p` + strconv.Itoa(pod) + `"}`})
		}
	}
	want := func(count string) map[string]map[int64]string {
		out := map[string]map[int64]string{}
		for pod := 0; pod < 30; pod++ {
			out[canonicalLabelsKey(map[string]string{"pod": "p" + strconv.Itoa(pod)})] = map[int64]string{end.Unix(): count}
		}
		return out
	}
	run := func(t *testing.T, query string, limit int) (*pushdownFakeVL, *Proxy) {
		t.Helper()
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.presence = true
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		p.maxStatsQuerySeries = limit
		code, got, body := detectedLevelQuery(t, p, query, end, end, time.Minute)
		if code != http.StatusOK || !reflect.DeepEqual(got, want("1")) {
			t.Fatalf("status %d %.200s\n got: %v\n stats: %q", code, body, got, fake.stats)
		}
		return fake, p
	}
	t.Run("filtered", func(t *testing.T) {
		fake, _ := run(t, `sum by (pod) (count_over_time({env="prod"} | json | drop __error__ | detected_level="error" [5m]))`, 50)
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.stats) != 1 || fake.raw != 0 || !strings.Contains(fake.stats[0], "copy detected_level as __dl_d, level as __dl_l") {
			t.Fatalf("expected one stored-level query: %q (raw %d)", fake.stats, fake.raw)
		}
	})
	t.Run("past the guard", func(t *testing.T) {
		// three spellings of one level per pod: 90 groups, 30 series
		rows = rows[:0]
		for pod := 0; pod < 30; pod++ {
			for l, level := range []string{"error", "ERR", "Err"} {
				rows = append(rows, pushdownRow{ts: s0.Add(time.Duration(10*pod+l+1) * time.Second), stream: map[string]string{"app": "api", "env": "prod", "level": level}, msg: `{"pod":"p` + strconv.Itoa(pod) + `"}`})
			}
		}
		query := `sum by (pod, detected_level) (count_over_time({env="prod"} | json | drop __error__ [10m]))`
		srv, fake := newPushdownFakeVL(t, rows, nil)
		fake.presence = true
		p := newFilterPushdownProxy(t, srv.URL)
		fake.stored = p.labelTranslator.ToVL
		p.maxStatsQuerySeries = 40 // the guard keeps 80 groups: the stored-level read has 90
		for ask := 0; ask < 2; ask++ {
			at := end.Add(time.Duration(ask) * time.Second)
			code, got, body := detectedLevelQuery(t, p, query, at, at, time.Minute)
			if code != http.StatusOK || len(got) != 30 {
				t.Fatalf("ask %d: status %d %.200s, %d series", ask, code, body, len(got))
			}
			for key, points := range got {
				if !strings.Contains(key, `detected_level="error"`) || points[at.Unix()] != "3" {
					t.Fatalf("ask %d: %s: %v", ask, key, points)
				}
			}
			fake.mu.Lock()
			stored, chain := len(fake.stats)-len(chainStatsCalls(fake.stats)), len(chainStatsCalls(fake.stats))
			fake.mu.Unlock()
			// the first ask tries the stored fields and then the chain; the verdict is
			// remembered, so the second goes to the chain alone
			if stored != 1 || chain != ask+1 {
				t.Fatalf("ask %d: %d stored-level and %d chain stats queries so far", ask, stored, chain)
			}
		}
	})
}

// The filter on detected_level that the stored-level plan sends to
// VictoriaLogs keeps exactly the rows whose stored level normalises to a value
// the filter accepts, and every row the stored fields do not decide (no stored
// level, or a stored detected_level), which the plan sends to the chain.
// conformance: semantics/json-metric-detected-level-derivation
func TestStoredLevelCopyFilterIsExact(t *testing.T) {
	words := []string{"info", "INFO", "Information", "inf", "warn", "WARNING", "wrn", "error", "ERR", "Err", "trace", "TRC", "debug", "dbg", "critical", "CRITICAL", "fatal",
		"notice", "unknown", "30", " info", "info ", "infoo", "errors", "Zulu"}
	for _, tc := range []struct{ op, value string }{
		{"=", "info"}, {"=", "error"}, {"=", "unknown"}, {"=", "notice"}, {"=", "INFO"}, {"=", " info"}, {"=", "30"},
		{"!=", "info"}, {"!=", "warn"}, {"!=", "notice"}, {"!=", "unknown"},
		{"=~", "warn|error"}, {"=~", "err.*"}, {"=~", "info|notice"}, {"=~", ".+"}, {"=~", "(?i)zulu"}, {"=~", "unk.*"},
		{"!~", "warn|error"}, {"!~", "info"}, {"!~", "n.*"},
	} {
		filter, err := translator.NewDropCondition("detected_level", tc.op, tc.value)
		if err != nil {
			t.Fatal(err)
		}
		pipes := splitLogsQLPipes("x" + translator.BuildStoredLevelCopyFilter("__dl_l", "__dl_d", []translator.DropCondition{filter}))
		if len(pipes) != 2 || !strings.HasPrefix(pipes[1], "filter ") {
			t.Fatalf("%s %q: %q", tc.op, tc.value, pipes)
		}
		expr := strings.TrimPrefix(pipes[1], "filter ")
		for _, word := range words {
			want := filter.Matches(translator.NormalizeStoredLevel(word))
			if got := fakeLogsQLMatch(t, expr, map[string]string{"__dl_l": word}); got != want {
				t.Errorf("detected_level%s%q on a stored level %q: VictoriaLogs filter keeps it: %v, Loki's answer %v\n%s", tc.op, tc.value, word, got, want, expr)
			}
		}
		for name, values := range map[string]map[string]string{"no stored level": {}, "stored detected_level": {"__dl_d": "x", "__dl_l": "info"}} {
			if !fakeLogsQLMatch(t, expr, values) {
				t.Errorf("detected_level%s%q drops a row with %s, which only the chain can read", tc.op, tc.value, name)
			}
		}
	}
}
