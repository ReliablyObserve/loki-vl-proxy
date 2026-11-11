package proxy

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

// lokiSplitsRecording is testdata/loki_query_stats_splits.json: Loki 3.7.9's
// answers on the test stack, recorded to pin the emulation rules of
// query_stats_loki.go (the recorder's request and Loki's figures; for log
// queries also VictoriaLogs' line count of each one-hour piece of the
// window).
type lokiSplitsRecording struct {
	MetricRange []struct {
		Start, End, Step int64
		MissSplits       int64 `json:"miss_splits"`
		MissShards       int64 `json:"miss_shards"`
		HitSplits        int64 `json:"hit_splits"`
	} `json:"metric_range"`
	Instant []struct {
		RangeSeconds int64 `json:"range_seconds"`
		Splits       int64 `json:"splits"`
		Shards       int64 `json:"shards"`
	} `json:"instant"`
	VolumeRange []struct {
		Start, End, Step int64
		Splits           int64 `json:"splits"`
	} `json:"volume_range"`
	Log []struct {
		Start, End, Limit int64
		Direction         string
		Entries           int64
		TMin              int64 `json:"t_min"`
		TMax              int64 `json:"t_max"`
		Splits, Shards    int64
		Lines             int64
		Pieces            []struct {
			Start, End int64
			VLLines    int64 `json:"vl_lines"`
		}
	} `json:"log"`
}

func readLokiSplitsRecording(t *testing.T) lokiSplitsRecording {
	t.Helper()
	raw, err := os.ReadFile("testdata/loki_query_stats_splits.json")
	if err != nil {
		t.Fatal(err)
	}
	var rec lokiSplitsRecording
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// TestLokiStatsEmulation_MatchesRecordedLokiAnswers: the splits and shards of
// metric, instant and volume_range queries, and the pieces a log query
// reads, follow Loki's recorded answers: every recorded row.
//
// conformance: query-stats-envelope
func TestLokiStatsEmulation_MatchesRecordedLokiAnswers(t *testing.T) {
	rec := readLokiSplitsRecording(t)
	sec := int64(time.Second)
	for _, r := range rec.MetricRange {
		n := lokiMetricRangePieces(r.Start*sec, r.End*sec, r.Step*sec)
		if splitsOf(n) != r.MissSplits || n != r.MissShards || max(n, 1) != r.HitSplits {
			t.Errorf("metric [%d,%d] step %d: pieces %d; Loki miss splits %d shards %d, hit splits %d", r.Start, r.End, r.Step, n, r.MissSplits, r.MissShards, r.HitSplits)
		}
	}
	for _, r := range rec.Instant {
		n := lokiInstantPieces(time.Duration(r.RangeSeconds) * time.Second)
		if splitsOf(n) != r.Splits || n != r.Shards {
			t.Errorf("instant [%ds]: pieces %d; Loki splits %d shards %d", r.RangeSeconds, n, r.Splits, r.Shards)
		}
	}
	for _, r := range rec.VolumeRange {
		if n := lokiPieces(r.Start*sec, r.End*sec, r.Step*sec); n != r.Splits {
			t.Errorf("volume_range [%d,%d] step %d: %d buckets; Loki splits %d", r.Start, r.End, r.Step, n, r.Splits)
		}
	}
	var whole, partial int
	for _, r := range rec.Log {
		name := fmt.Sprintf("log [%d,%d] limit %d %s", r.Start, r.End, r.Limit, r.Direction)
		run := lokiLogQueryRun(r.Start, r.End, r.Limit, r.Direction != "forward", r.Entries, r.TMin, r.TMax, r.Entries > 0)
		splits := int64(0)
		if run.pieces > 1 {
			splits = run.ran
		}
		if splits != r.Splits || run.ran != r.Shards {
			t.Errorf("%s: ran %d of %d pieces; Loki splits %d shards %d", name, run.ran, run.pieces, r.Splits, r.Shards)
		}
		var lines int64
		for _, p := range r.Pieces {
			if p.End > run.from && p.Start < run.to {
				lines += p.VLLines
			}
		}
		switch {
		case lines == r.Lines:
			whole++
		case r.Direction == "forward" && lines > r.Lines:
			// Loki read part of a piece it holds in chunks already flushed
			// to its store (forward queries start at the oldest piece): a
			// documented difference, the emulation reads pieces whole.
			partial++
		default:
			t.Errorf("%s: the pieces run hold %d lines; Loki processed %d", name, lines, r.Lines)
		}
	}
	t.Logf("log rows: %d read whole pieces as emulated, %d forward rows partly read by Loki (store chunks)", whole, partial)
	if whole == 0 {
		t.Fatal("no recorded log row reads whole pieces")
	}
}

// TestLokiLogQueryRun_Table is the edge-case table of the pieces a log query
// reads: unsplit windows, a limit not filled, the last entry in the newest
// piece, in an older piece, forward, and a window ending on a boundary.
//
// conformance: query-stats-envelope
func TestLokiLogQueryRun_Table(t *testing.T) {
	h := lokiSplitInterval
	base := int64(1_800_000_000) * int64(time.Second) / h * h // an hour boundary
	m := int64(time.Minute)
	for _, tc := range []struct {
		name                  string
		start, end, limit     int64
		backward              bool
		entries, tMin, tMax   int64
		wantFrom, wantTo, ran int64
		pieces                int64
	}{
		{"unsplit window", base + 5*m, base + 20*m, 10, true, 10, base + 19*m, base + 20*m, base + 5*m, base + 20*m, 1, 1},
		{"limit not filled", base - 30*m, base + 20*m, 100, true, 40, base - 29*m, base + 19*m, base - 30*m, base + 20*m, 2, 2},
		{"filled in the newest piece", base - 30*m, base + 20*m, 100, true, 100, base + 10*m, base + 20*m, base, base + 20*m, 1, 2},
		{"filled in an older piece", base - 90*m, base + 20*m, 100, true, 100, base - 40*m, base + 20*m, base - h, base + 20*m, 2, 3},
		{"forward, filled in the oldest piece", base - 30*m, base + 20*m, 100, false, 100, base - 30*m, base - 10*m, base - 30*m, base, 1, 2},
		{"end on a boundary", base - 2*h, base, 100, true, 100, base - 10*m, base - m, base - h, base, 1, 2},
	} {
		run := lokiLogQueryRun(tc.start, tc.end, tc.limit, tc.backward, tc.entries, tc.tMin, tc.tMax, true)
		if run.from != tc.wantFrom || run.to != tc.wantTo || run.ran != tc.ran || run.pieces != tc.pieces {
			t.Errorf("%s: %+v, want from %d to %d ran %d of %d", tc.name, run, tc.wantFrom-base, tc.wantTo-base, tc.ran, tc.pieces)
		}
	}
}

// TestLokiStats_MetricBytesFromTheSelectorsMeanLine: in Loki's accounting a
// metric query's bytes processed are its lines times the mean line size the
// selector's log answers showed (Loki counts the bytes of the lines a metric
// query reads; VictoriaLogs' aggregated answer does not tell them); with no
// log answer of the selector or its tenant there is no estimate (0), and the
// victorialogs mode never estimates.
//
// conformance: query-stats-envelope
func TestLokiStats_MetricBytesFromTheSelectorsMeanLine(t *testing.T) {
	stub := &statsStubVL{rows: 20}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	now := time.Now().Truncate(time.Minute)
	window := url.Values{"start": {fmt.Sprint(now.Add(-10 * time.Minute).Unix())}, "end": {fmt.Sprint(now.Unix())}, "step": {"60"}}
	with := func(q string) url.Values {
		v := url.Values{"query": {q}}
		for k, x := range window {
			v[k] = x
		}
		return v
	}
	for _, mode := range []string{QueryStatsModeLoki, QueryStatsModeVictoriaLogs} {
		h := newQueryStatsTestProxyMode(t, vl.URL, "", mode)
		before := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", with(`sum(count_over_time({app="api"}[1m]))`), nil))
		logs := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", with(`{app="api"} != "x"`), nil))
		after := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", with(`sum(count_over_time({app="api"} |= "line" [1m]))`), nil))
		if before.Summary.Bytes != 0 {
			t.Fatalf("%s: bytes %d before any log answer of the selector", mode, before.Summary.Bytes)
		}
		mean := float64(logs.Summary.Bytes) / float64(logs.Summary.Lines)
		want := int64(0)
		if mode == QueryStatsModeLoki {
			want = int64(mean*float64(after.Summary.Lines) + 0.5)
		}
		if after.Summary.Lines <= 0 || after.Summary.Bytes != want {
			t.Fatalf("%s: metric %+v after the log answer %+v: bytes %d, want %d", mode, after.Summary, logs.Summary, after.Summary.Bytes, want)
		}
	}
}

type statsSplitsView struct {
	Summary struct {
		Splits int64 `json:"splits"`
		Shards int64 `json:"shards"`
		Lines  int64 `json:"totalLinesProcessed"`
		Bytes  int64 `json:"totalBytesProcessed"`
	} `json:"summary"`
	Querier struct {
		ExecTime float64 `json:"querierExecTime"`
	} `json:"querier"`
}

// TestLokiStats_SplitsShardsAndQuerierTimePerRoute: through the handlers, a
// window of three one-hour pieces reports Loki's splits and shards per route
// on a miss and a repeat in Loki's accounting, the VictoriaLogs calls in the
// victorialogs mode, and querier.querierExecTime the time spent in
// VictoriaLogs calls (none on a hit, none for volume in Loki's accounting).
//
// conformance: query-stats-envelope
func TestLokiStats_SplitsShardsAndQuerierTimePerRoute(t *testing.T) {
	stub := &statsStubVL{rows: 5}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	h := lokiSplitInterval / int64(time.Second)
	end := time.Now().Unix()/h*h - 600 // ten minutes before an hour boundary
	start := end - 2*h                 // touches three one-hour pieces
	rangeParams := func(q string, extra ...string) url.Values {
		v := url.Values{"query": {q}, "start": {fmt.Sprint(start)}, "end": {fmt.Sprint(end)}, "step": {"60"}}
		for i := 0; i < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		return v
	}
	type want struct{ splits, shards, hitSplits, hitShards int64 }
	for _, tc := range []struct {
		name, path string
		params     url.Values
		loki       want
	}{
		{"metric", "/loki/api/v1/query_range", rangeParams(`sum(count_over_time({app="api"}[1m]))`), want{3, 3, 3, 0}},
		{"log, limit not filled", "/loki/api/v1/query_range", rangeParams(`{app="api"}`, "limit", "100"), want{3, 3, 3, 3}},
		{"instant 2h", "/loki/api/v1/query", url.Values{"query": {`sum(count_over_time({app="api"}[2h]))`}, "time": {fmt.Sprint(end)}}, want{2, 2, 2, 2}},
		{"volume_range", "/loki/api/v1/index/volume_range", rangeParams(`{app="api"}`), want{120, 0, 120, 0}},
	} {
		for _, mode := range []string{QueryStatsModeLoki, QueryStatsModeVictoriaLogs} {
			hd := newQueryStatsTestProxyMode(t, vl.URL, "", mode)
			var miss, hit statsSplitsView
			_ = json.Unmarshal(statsOf(t, serveQueryStats(t, hd, tc.path, tc.params, nil)), &miss)
			_ = json.Unmarshal(statsOf(t, serveQueryStats(t, hd, tc.path, tc.params, nil)), &hit)
			if mode == QueryStatsModeLoki {
				got := want{miss.Summary.Splits, miss.Summary.Shards, hit.Summary.Splits, hit.Summary.Shards}
				if got != tc.loki {
					t.Errorf("%s (loki): splits/shards miss %d/%d hit %d/%d, want %+v", tc.name, got.splits, got.shards, got.hitSplits, got.hitShards, tc.loki)
				}
			} else if miss.Summary.Shards != 0 || hit.Summary.Splits != 0 || miss.Summary.Splits <= 0 {
				t.Errorf("%s (victorialogs): miss %+v hit %+v, want VictoriaLogs calls as splits, no shards", tc.name, miss.Summary, hit.Summary)
			}
			volumeInLoki := tc.path == "/loki/api/v1/index/volume_range" && mode == QueryStatsModeLoki
			if (miss.Querier.ExecTime > 0) == volumeInLoki {
				t.Errorf("%s (%s): querierExecTime %v on a miss", tc.name, mode, miss.Querier.ExecTime)
			}
		}
	}
}

// TestLokiLogSummary_EstimatesTheRunFromTheAnswer: without a volume answer of
// the panel, a log answer that filled its limit reports the lines of the run
// as its entries spread over the time they cover (exact when the limit was
// not filled), never fewer than it returned, and bytes as those lines times
// the returned lines' mean size; the panel's count answer, when held, gives
// the run's lines exactly.
//
// conformance: query-stats-envelope
func TestLokiLogSummary_EstimatesTheRunFromTheAnswer(t *testing.T) {
	h := lokiSplitInterval
	base := int64(1_800_000_000) * int64(time.Second) / h * h
	m := int64(time.Minute)
	ts19 := func(v int64) (b [19]byte) {
		copy(b[:], fmt.Sprintf("%019d", v))
		return b
	}
	for _, tc := range []struct {
		name                string
		start, end, limit   int64
		entries, tMin, tMax int64
		totals              statsTotals
		wantLines, wantPost int64
		wantBytesPerLine    int64
	}{
		{"limit filled in the last fifth", base + 5*m, base + 55*m, 1000, 1000, base + 45*m, base + 55*m, statsTotals{}, 5000, 5000, 30},
		{"limit not filled", base + 5*m, base + 55*m, 1000, 400, base + 6*m, base + 54*m, statsTotals{}, 400, 400, 30},
		{"newest piece only", base - 30*m, base + 20*m, 1000, 1000, base + 15*m, base + 20*m, statsTotals{}, 4000, 4000, 30},
		{"the panel's count answer", base - 30*m, base + 20*m, 1000, 1000, base + 15*m, base + 20*m,
			statsTotals{postFilter: 3700, hasPostFilter: true, lines: 3700, hasLines: true}, 3700, 3700, 30},
	} {
		w := &queryStatsWriter{route: statsRouteRange, lokiMode: true,
			sreq: statsRequest{start: tc.start, end: tc.end, limit: tc.limit, backward: true, ok: true}}
		w.entries = entryCounter{n: tc.entries, lineBytes: 30 * tc.entries, hasTS: true, tsMin: ts19(tc.tMin), tsMax: ts19(tc.tMax)}
		s, _ := w.lokiLogSummary(&tc.totals, false)
		if s.lines != tc.wantLines || s.postFilter != tc.wantPost || s.bytes != tc.wantBytesPerLine*tc.wantLines {
			t.Errorf("%s: %+v, want lines %d post %d bytes %d", tc.name, s, tc.wantLines, tc.wantPost, tc.wantBytesPerLine*tc.wantLines)
		}
	}
}
