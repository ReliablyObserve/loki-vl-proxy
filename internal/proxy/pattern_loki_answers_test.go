package proxy

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// lokiPatternAnswers holds fixture streams and Loki's /patterns answers for requests over them, recorded from
// Loki's pattern ingester by test/e2e-compat TestDrilldown_Patterns_TextsMatchLokisOnTheSameLines
// (LVP_RECORD_LOKI_PATTERNS=1 records it again).
const lokiPatternAnswers = "testdata/loki_pattern_answers.json.gz"

type lokiPatternAnswerFile struct {
	EndMs   int64 `json:"end_ms"`
	Streams []struct {
		Kind      string   `json:"kind"`
		Level     string   `json:"level"`
		Lines     []string `json:"lines"`
		OffsetsMs []int64  `json:"offsets_ms"`
	} `json:"streams"`
	Queries []struct {
		Name    string `json:"name"`
		Streams []int  `json:"streams"`
		FromMs  int64  `json:"from_ms"`
		Loki    []struct {
			Pattern string `json:"pattern"`
			Total   int64  `json:"total"`
		} `json:"loki"`
	} `json:"queries"`
}

// patternMinerLokiFloor is, for each recorded request, the share of the lines in Loki's patterns that are in a
// pattern the miner answers with the same text: a ratchet, raised when the miner matches Loki better and never
// lowered.
var patternMinerLokiFloor = map[string]float64{}

// patternMinerLokiNotExact names the recorded requests whose answer is not Loki's exactly, with the reason: every
// other request must match, pattern for pattern with the same totals and no pattern more.
var patternMinerLokiNotExact = map[string]string{
	"nginx_last_quarter": "Loki's template there is trained on lines outside the range",
}

// The miner gives Loki's patterns for the lines Loki's pattern ingester mined, read in the order Loki read them:
// for kinds of lines services write, lines at the edges of Loki's mining and the request shapes clients send, the
// share of the lines in Loki's patterns that are in a pattern the miner answers with Loki's text never falls
// below its floor.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns
func TestPatternMinerMatchesLokisRecordedPatterns(t *testing.T) {
	f, err := os.Open(lokiPatternAnswers)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var answers lokiPatternAnswerFile
	if err := json.NewDecoder(zr).Decode(&answers); err != nil {
		t.Fatal(err)
	}
	end := time.UnixMilli(answers.EndMs).UTC()
	var sum float64
	for _, q := range answers.Queries {
		type row struct {
			stream      int
			offset      int64
			line, level string
		}
		var rows []row
		for _, i := range q.Streams {
			s := answers.Streams[i]
			for j, line := range s.Lines {
				if s.OffsetsMs[j] >= q.FromMs {
					rows = append(rows, row{i, s.OffsetsMs[j], line, s.Level})
				}
			}
		}
		// VictoriaLogs answers newest first.
		sort.SliceStable(rows, func(a, b int) bool { return rows[a].offset > rows[b].offset })
		start := end.Add(time.Duration(q.FromMs) * time.Millisecond)
		var body strings.Builder
		for _, r := range rows {
			encoded, _ := json.Marshal(map[string]string{"_time": end.Add(time.Duration(r.offset) * time.Millisecond).Format(time.RFC3339Nano), "_msg": r.line, "level": r.level, "_stream_id": fmt.Sprintf("stream-%d", r.stream)})
			body.Write(encoded)
			body.WriteByte('\n')
		}
		mined := extractLogPatterns([]byte(body.String()), "10s", maxPatternResponseLimit, defaultLogRowLevels())
		have := map[string]bool{}
		minedTotal := map[string]int64{}
		for _, e := range dropPatternsBelowMinLines(patternResultEntriesFromMaps(mined), start.UnixNano(), end.UnixNano()) {
			have[e.Pattern] = true
			for _, s := range e.Samples {
				// Loki answers the samples of the steps that start inside the range.
				if ts := patternSampleCount(s[0]); ts >= start.Unix() && ts < end.Unix() {
					minedTotal[e.Pattern] += patternSampleCount(s[1])
				}
			}
		}
		answered := 0
		lokiHas := map[string]bool{}
		sameTotals := 0
		for _, l := range q.Loki {
			lokiHas[l.Pattern] = true
			if minedTotal[l.Pattern] == l.Total {
				sameTotals++
			}
		}
		extra := 0
		for text := range have {
			if !lokiHas[text] {
				extra++
			}
		}
		share := 0.0
		var missing []string
		if len(q.Loki) == 0 {
			if len(mined) == 0 {
				share = 1
			}
		} else {
			var matched, lines int64
			for _, l := range q.Loki {
				lines += l.Total
				if have[l.Pattern] {
					matched += l.Total
				} else {
					missing = append(missing, l.Pattern)
				}
			}
			share = float64(matched) / float64(lines)
		}
		sum += share
		answered = len(have)
		t.Logf("%-18s Loki %3d patterns, miner %3d: %5.1f%% of Loki's lines, %3d not Loki's, %3d of Loki's with Loki's total", q.Name, len(q.Loki), answered, 100*share, extra, sameTotals)
		if testing.Verbose() && (extra > 0 || sameTotals < len(q.Loki)) && os.Getenv("LVP_PATTERNS_DIFF") == q.Name {
			for _, l := range q.Loki {
				t.Logf("    loki  %5d / miner %5d  %s", l.Total, minedTotal[l.Pattern], l.Pattern)
			}
			for text := range have {
				if !lokiHas[text] {
					t.Logf("    miner only %5d  %s", minedTotal[text], text)
				}
			}
		}
		if _, deferred := patternMinerLokiNotExact[q.Name]; !deferred && (extra > 0 || sameTotals < len(q.Loki) || answered != len(q.Loki)) {
			t.Errorf("%s: not Loki's answer: %d patterns answered against %d, %d not Loki's, %d of Loki's with Loki's total", q.Name, answered, len(q.Loki), extra, sameTotals)
		}
		if share+1e-9 < patternMinerLokiFloor[q.Name] {
			var got []string
			for _, p := range mined {
				got = append(got, fmt.Sprint(p["pattern"]))
			}
			sort.Strings(got)
			sort.Strings(missing)
			t.Errorf("%s: %.1f%% of Loki's lines in patterns with Loki's text, floor %.1f%%\n missing %q\n answered %q", q.Name, 100*share, 100*patternMinerLokiFloor[q.Name], missing, got)
		}
	}
	t.Logf("mean over %d requests: %.1f%% of Loki's lines in patterns with Loki's text", len(answers.Queries), 100*sum/float64(len(answers.Queries)))
}

func patternSampleCount(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}
