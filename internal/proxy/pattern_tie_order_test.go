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

// lokiPatternRecordedRow is a line of a recorded request: its stream, its offset from the recording's end, its text
// and level.
type lokiPatternRecordedRow struct {
	stream      int
	offsetMs    int64
	line, level string
}

// patternTiedPushLines is how many lines of a stream a push carries at one timestamp in the tests below: the
// compose stack's log generator pushes 15 lines per stream at one time.
const patternTiedPushLines = 15

// Loki mines a stream's lines in the order its pattern ingester received them; VictoriaLogs keeps no order for lines
// of a stream that share a timestamp (it sorts rows by stream and time, unstably), so the proxy reads such ties in
// their text's order (patternExactLinesQuery), the same on every read, rebuild and peer. On Loki's recorded answers:
// the lines mined in the order Loki received them give Loki's answer; the same lines with each push's lines given
// one timestamp and mined in their text's order (what the proxy reads back from VictoriaLogs) do not, for some
// requests. A pattern's samples and totals can then differ from Loki's by the lines that joined another cluster
// (often one under the answer's 30-line rule), at the same sample: a documented difference, not a bucketing one.
//
// conformance: drilldown/patterns-mined-differently, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsLokiAnswerDependsOnTheOrderOfTiedLines(t *testing.T) {
	answers := readLokiPatternAnswers(t)
	end := time.UnixMilli(answers.EndMs).UTC()
	tiedDiffers := 0
	for _, q := range answers.Queries {
		rows := lokiPatternRecordedRows(answers, q.Streams, q.FromMs)
		want := map[string]int64{}
		for _, l := range q.Loki {
			want[l.Pattern] = l.Total
		}
		arrival := lokiPatternMinedTotals(t, end, q.FromMs, rows)
		tied := lokiPatternMinedTotals(t, end, q.FromMs, tiePatternPushes(end, rows))
		arrivalDiff, tiedDiff := patternTotalsDiff(want, arrival), patternTotalsDiff(want, tied)
		t.Logf("%-18s Loki %5d lines: %4d differ mined as received, %4d with each push's lines tied", q.Name, patternLines(want), arrivalDiff, tiedDiff)
		if _, deferred := patternMinerLokiNotExact[q.Name]; deferred {
			continue
		}
		if arrivalDiff != 0 {
			t.Errorf("%s: mined in the order Loki received them, %d lines are not in Loki's patterns and totals", q.Name, arrivalDiff)
		}
		if tiedDiff != 0 {
			tiedDiffers++
		}
	}
	if tiedDiffers == 0 {
		t.Errorf("no recorded request differs from Loki once each push's lines are tied and read in text order: the documented difference in drilldown/patterns-mined-differently no longer holds")
	}
}

func readLokiPatternAnswers(t *testing.T) lokiPatternAnswerFile {
	t.Helper()
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
	return answers
}

// lokiPatternRecordedRows is a request's lines, a stream after another (as VictoriaLogs sorts them) and each
// stream's in the order Loki received them.
func lokiPatternRecordedRows(answers lokiPatternAnswerFile, streams []int, fromMs int64) []lokiPatternRecordedRow {
	var rows []lokiPatternRecordedRow
	for _, i := range streams {
		s := answers.Streams[i]
		for j, line := range s.Lines {
			if s.OffsetsMs[j] >= fromMs {
				rows = append(rows, lokiPatternRecordedRow{i, s.OffsetsMs[j], line, s.Level})
			}
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		if rows[a].stream != rows[b].stream {
			return fmt.Sprintf("stream-%d", rows[a].stream) < fmt.Sprintf("stream-%d", rows[b].stream)
		}
		return rows[a].offsetMs < rows[b].offsetMs
	})
	return rows
}

// tiePatternPushes gives each run of up to patternTiedPushLines lines of a stream inside one 10 s sample the time
// of the run's first line, and orders the run's lines by text: the lines a push carries at one timestamp, as the
// proxy reads them back from VictoriaLogs. Every line stays in its sample.
func tiePatternPushes(end time.Time, rows []lokiPatternRecordedRow) []lokiPatternRecordedRow {
	out := append([]lokiPatternRecordedRow(nil), rows...)
	sampleOf := func(r lokiPatternRecordedRow) int64 {
		return patternSampleStart(end.Add(time.Duration(r.offsetMs) * time.Millisecond).UnixNano())
	}
	for from := 0; from < len(out); {
		to := from + 1
		for to < len(out) && to-from < patternTiedPushLines && out[to].stream == out[from].stream && sampleOf(out[to]) == sampleOf(out[from]) {
			to++
		}
		push := out[from:to]
		for i := range push {
			push[i].offsetMs = push[0].offsetMs
		}
		sort.SliceStable(push, func(a, b int) bool { return push[a].line < push[b].line })
		from = to
	}
	return out
}

// lokiPatternMinedTotals mines rows in their order, as the exact tier reads lines, and returns each answered
// pattern's lines inside the request's range.
func lokiPatternMinedTotals(t *testing.T, end time.Time, fromMs int64, rows []lokiPatternRecordedRow) map[string]int64 {
	t.Helper()
	start := end.Add(time.Duration(fromMs) * time.Millisecond)
	var body strings.Builder
	for _, r := range rows {
		encoded, _ := json.Marshal(map[string]string{"_time": end.Add(time.Duration(r.offsetMs) * time.Millisecond).Format(time.RFC3339Nano), "_msg": r.line, "level": r.level, "_stream_id": fmt.Sprintf("stream-%d", r.stream)})
		body.Write(encoded)
		body.WriteByte('\n')
	}
	miner := newPatternMiner()
	miner.fillStep = 10
	miner.sampleFromNs, miner.sampleToNs = start.UnixNano(), end.UnixNano()
	if err := readPatternLines(strings.NewReader(body.String()), miner, defaultLogRowLevels(), len(rows)); err != nil {
		t.Fatal(err)
	}
	totals := map[string]int64{}
	for _, e := range dropPatternsBelowMinLines(patternResultEntriesFromMaps(buildPatternResponse(miner, maxPatternResponseLimit)), 0, 0) {
		for _, s := range e.Samples {
			if ts := patternSampleCount(s[0]); ts >= start.Unix() && ts < end.Unix() {
				totals[e.Pattern] += patternSampleCount(s[1])
			}
		}
	}
	return totals
}

// patternTotalsDiff is how many lines got differ from want: per pattern, the difference of the totals, a pattern
// one side lacks counting all of its lines.
func patternTotalsDiff(want, got map[string]int64) int64 {
	var diff int64
	for pattern, n := range got {
		if _, ok := want[pattern]; !ok {
			diff += n
		}
	}
	for pattern, n := range want {
		d := got[pattern] - n
		if d < 0 {
			d = -d
		}
		diff += d
	}
	return diff
}

func patternLines(totals map[string]int64) int64 {
	var n int64
	for _, v := range totals {
		n += v
	}
	return n
}
