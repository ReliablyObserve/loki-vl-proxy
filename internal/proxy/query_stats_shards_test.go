package proxy

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// shardsAnswer is a log answer's body: the entries (nanosecond timestamps,
// ascending) dealt round-robin over `streams` streams, each stream sorted in
// the query's direction, as VictoriaLogs rows reach the proxy's encoder.
func shardsAnswer(entries []int64, streams int, backward bool) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"streams","result":[`)
	first := true
	for s := 0; s < streams; s++ {
		var own []int64
		for i := s; i < len(entries); i += streams {
			own = append(own, entries[i])
		}
		if len(own) == 0 {
			continue
		}
		if backward {
			slices.Reverse(own)
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&b, `{"stream":{"app":"s%d"},"values":[`, s)
		for i, ts := range own {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `["%d","line %d"]`, ts, i)
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`],"stats":{}}}`)
	return []byte(b.String())
}

// shardsOfAnswer is the shards the stats layer replays for a repeat of the
// answer: the answer read in writes of `chunk` bytes (0: one write).
func shardsOfAnswer(body []byte, start, end int64, chunk int) int64 {
	w := &queryStatsWriter{}
	w.entries.trackTS = true
	w.sreq, w.sreqRead = statsRequest{start: start, end: end, ok: true}, true
	if chunk <= 0 {
		chunk = len(body)
	}
	for i := 0; i < len(body); i += chunk {
		w.entries.add(body[i:min(i+chunk, len(body))])
	}
	return w.lokiLogHitShards()
}

// TestLokiLogHitShards_Table: Loki answers a repeat of a log query from its
// results cache for every piece that held no returned entry (a piece its
// filters emptied too), so the repeat's shards are the pieces that hold an
// entry, whichever stream the entry is in and however many pieces lie
// between them. The table covers empty pieces at the edges and between
// entries, entries on a piece boundary, one stream or several, both
// directions, an answer cut into writes (which falls back to the span
// between the oldest and newest entry) and one beyond the map's width.
//
// conformance: semantics/query-stats-shards-empty-pieces, query-stats-envelope
func TestLokiLogHitShards_Table(t *testing.T) {
	h := lokiSplitInterval
	base := int64(1_800_000_000) * int64(time.Second) / h * h // an hour boundary
	sec := int64(time.Second)
	for _, tc := range []struct {
		name       string
		start, end int64
		entries    []int64
		streams    int
		want       int64
	}{
		{"one piece", base + 10*sec, base + 20*sec, []int64{base + 12*sec, base + 13*sec}, 1, 1},
		{"lines in the second of two pieces", base - 40*60*sec, base + 20*60*sec, []int64{base + 5*60*sec}, 1, 1},
		{"lines in the first of two pieces", base - 40*60*sec, base + 20*60*sec, []int64{base - 5*60*sec}, 1, 1},
		{"lines in both of two pieces", base - 40*60*sec, base + 20*60*sec, []int64{base - 5*60*sec, base + 5*60*sec}, 1, 2},
		{"lines in the middle of three pieces", base - 30*60*sec, base + 90*60*sec, []int64{base + 3600*sec + 3*60*sec}, 1, 1},
		{"empty pieces between entries", base, base + 5*3600*sec - 1, []int64{base + 20*60*sec, base + 4*3600*sec + 9*60*sec, base + 4*3600*sec + 10*60*sec}, 1, 2},
		{"entries in three of five pieces, two streams", base, base + 5*3600*sec - 1,
			[]int64{base + 20*60*sec, base + 3*3600*sec + 3*sec, base + 3*3600*sec + 4*sec, base + 4*3600*sec + 1*sec}, 2, 3},
		{"entry on the piece boundary", base - 2*3600*sec, base + 3600*sec, []int64{base - 1, base}, 1, 2},
		{"last nanosecond of a piece", base - 3600*sec, base + 3600*sec, []int64{base - 1}, 1, 1},
		{"entries beyond the map width fall back to the span", base, base + 700*3600*sec, []int64{base + sec, base + 600*3600*sec}, 1, 601},
	} {
		for _, backward := range []bool{true, false} {
			body := shardsAnswer(tc.entries, tc.streams, backward)
			if got := shardsOfAnswer(body, tc.start, tc.end, 0); got != tc.want {
				t.Errorf("%s backward=%v: %d shards, want %d", tc.name, backward, got, tc.want)
			}
			// A timestamp split across writes is read all the same: writes of
			// 7 bytes cut every timestamp.
			if cut := shardsOfAnswer(body, tc.start, tc.end, 7); cut != tc.want {
				t.Errorf("%s backward=%v in writes of 7 bytes: %d shards, want %d", tc.name, backward, cut, tc.want)
			}
		}
	}
}

// TestLokiLogHitShards_AnswerWrittenInChunks: a real log answer reaches the
// stats layer in several writes (the streams writer flushes in 32 KiB
// pieces), which cut about one timestamp in ten entries of 300 bytes. A
// sparse answer of 1000 entries in the pieces 0, 2 and 4 of a window of five
// reports the three pieces however it is cut, from 1 byte writes to one
// write, and every entry's timestamp is read.
//
// conformance: semantics/query-stats-shards-empty-pieces
func TestLokiLogHitShards_AnswerWrittenInChunks(t *testing.T) {
	h := lokiSplitInterval
	base := int64(1_800_000_000) * int64(time.Second) / h * h
	var entries []int64
	for i := 0; i < 1000; i++ {
		piece := []int64{0, 2, 4}[i%3]
		entries = append(entries, base+piece*h+int64(i)*int64(time.Millisecond))
	}
	slices.Sort(entries)
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"streams","result":[`)
	for s := 0; s < 4; s++ {
		if s > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"stream":{"app":"s%d"},"values":[`, s)
		first := true
		for i := len(entries) - 1 - s; i >= 0; i -= 4 {
			if !first {
				b.WriteByte(',')
			}
			first = false
			fmt.Fprintf(&b, `["%d","%s"]`, entries[i], strings.Repeat("GET /api/v1/items \"x\" ", 12))
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`],"stats":{}}}`)
	body := []byte(b.String())
	if len(body) < 250_000 {
		t.Fatalf("answer of %d bytes: want entries of about 300 bytes", len(body))
	}
	start, end := base, base+5*h-1
	sizes := []int{1, 7, 61, 1000, 4096, 32 << 10, 33333, 64 << 10, len(body)}
	for _, size := range sizes {
		w := &queryStatsWriter{}
		w.entries.trackTS = true
		w.sreq, w.sreqRead = statsRequest{start: start, end: end, ok: true}, true
		for i := 0; i < len(body); i += size {
			w.entries.add(body[i:min(i+size, len(body))])
		}
		if w.entries.logs != 1000 || w.entries.tsKept != 1000 || w.entries.pieces.lost {
			t.Fatalf("writes of %d bytes: %d entries, %d timestamps read, lost=%v", size, w.entries.logs, w.entries.tsKept, w.entries.pieces.lost)
		}
		if got := w.lokiLogHitShards(); got != 3 {
			t.Errorf("writes of %d bytes: %d shards, want 3", size, got)
		}
		if w.entries.tsMin != entries[0] || w.entries.tsMax != entries[len(entries)-1] {
			t.Errorf("writes of %d bytes: oldest %d newest %d, want %d %d", size, w.entries.tsMin, w.entries.tsMax, entries[0], entries[len(entries)-1])
		}
	}
}

// TestEntryPieces_Table: the piece map marks the epoch-aligned 1h piece of
// each timestamp, once, keeps working across the anchor (backward answers
// start at the newest entry), and reports a timestamp beyond its width lost.
//
// conformance: semantics/query-stats-shards-empty-pieces
func TestEntryPieces_Table(t *testing.T) {
	h := lokiSplitInterval
	base := int64(1_800_000_000) * int64(time.Second) / h * h
	for _, tc := range []struct {
		name  string
		ts    []int64
		count int64
		lost  bool
	}{
		{"one", []int64{base + 5}, 1, false},
		{"same piece twice", []int64{base + 5, base + h - 1, base}, 1, false},
		{"adjacent pieces", []int64{base + h - 1, base + h}, 2, false},
		{"older than the first", []int64{base, base - h, base - 255*h}, 3, false},
		{"newer than the first", []int64{base, base + h, base + 255*h}, 3, false},
		{"just in range below", []int64{base, base - 256*h}, 2, false},
		{"just out of range below", []int64{base, base - 257*h}, 1, true},
		{"just in range above", []int64{base, base + 255*h}, 2, false},
		{"just out of range above", []int64{base, base + 256*h}, 1, true},
		{"alternating pieces", []int64{base, base + h, base, base + h, base + 2*h}, 3, false},
	} {
		var p entryPieces
		for _, ts := range tc.ts {
			p.mark(ts)
		}
		if got := p.count(); got != tc.count || p.lost != tc.lost {
			t.Errorf("%s: %d pieces lost=%v, want %d lost=%v", tc.name, got, p.lost, tc.count, tc.lost)
		}
	}
}

// FuzzEntryPieces: for any timestamps the map equals a set of piece indexes
// anchored at the first one, and flags exactly the timestamps outside its
// 512-piece window; the 19-digit text reads back as the number.
func FuzzEntryPieces(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})
	f.Add(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 0), 1<<62))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var p entryPieces
		want := map[int64]bool{}
		var anchor int64
		lost := false
		for i := 0; i+8 <= len(raw) && i < 8*200; i += 8 {
			// 19-digit timestamps: 1e18 plus a value below 8e18.
			ts := 1_000_000_000_000_000_000 + int64(binary.LittleEndian.Uint64(raw[i:])%7_000_000_000_000_000_000)
			idx := ts / lokiSplitInterval
			if i == 0 {
				anchor = idx - entryPiecesWords*32
			}
			if idx < anchor || idx >= anchor+entryPiecesWords*64 {
				lost = true
			} else {
				want[idx] = true
			}
			p.mark(ts)
			if got := parseTS19([]byte(fmt.Sprintf("%019d", ts))); got != ts {
				t.Fatalf("parseTS19(%019d) = %d", ts, got)
			}
		}
		if int(p.count()) != len(want) || p.lost != lost {
			t.Fatalf("map %d lost=%v, want %d lost=%v", p.count(), p.lost, len(want), lost)
		}
	})
}

// shardsRow is one row of testdata/loki_stats_shards.jsonl (the recorder's
// request and Loki 3.7.9's figures; for log rows the returned entries'
// timestamps and VictoriaLogs' line count of each 1h piece of the window).
type shardsRow struct {
	Kind         string  `json:"kind"`
	Case         string  `json:"case"`
	Seq          string  `json:"seq"`
	Query        string  `json:"query"`
	Start        int64   `json:"start"` // seconds
	End          int64   `json:"end"`
	Time         int64   `json:"time"`
	Step         int64   `json:"step"`
	RangeSeconds int64   `json:"range_seconds"`
	Limit        int64   `json:"limit"`
	Direction    string  `json:"direction"`
	Splits       int64   `json:"splits"`
	Shards       int64   `json:"shards"`
	Lines        int64   `json:"lines"`
	Entries      int64   `json:"entries"`
	EntryTS      []int64 `json:"entry_ts"`
	Pieces       []struct {
		Start, End int64
		VLLines    int64 `json:"vl_lines"`
	} `json:"pieces"`
}

func readShardsRecording(t *testing.T) []shardsRow {
	t.Helper()
	f, err := os.Open("testdata/loki_stats_shards.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []shardsRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r shardsRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Kind != "source" {
			rows = append(rows, r)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestLokiStatsShards_MatchesRecordedLokiAnswers: Loki's splits and shards
// for the sparse stream of testdata/loki_stats_shards.jsonl (15 lines in
// three separate hours, windows of 1 to 5 pieces with the lines in the
// first, middle, last or several pieces, limits that stop early, forward and
// backward, a filter that empties every piece, metric and instant queries):
//   - the first answer of a text runs every piece it reads (log), every
//     piece (metric, instant): the emulation's shards, whatever the lines'
//     placement;
//   - the identical repeat of a log query runs only the pieces that hold an
//     entry (none, for an empty answer: the results-cache hit), a metric
//     query_range repeat is the results-cache hit (no shards), an instant
//     query runs again;
//   - the same text over another limit, direction or window after a first
//     run: Loki never reports more shards than the first-answer rule (the
//     proxy's figure is an upper bound), and the rows where it reports fewer
//     are the documented remaining difference.
//
// conformance: semantics/query-stats-shards-empty-pieces, query-stats-envelope
func TestLokiStatsShards_MatchesRecordedLokiAnswers(t *testing.T) {
	sec := int64(time.Second)
	var logRepeats, fewer, other int
	for _, r := range readShardsRecording(t) {
		name := fmt.Sprintf("%s %s %s [%d,%d]", r.Kind, r.Case, r.Seq, r.Start, r.End)
		switch r.Kind {
		case "log":
			backward := r.Direction != "forward"
			var tMin, tMax int64
			if len(r.EntryTS) > 0 {
				tMin, tMax = r.EntryTS[0], r.EntryTS[len(r.EntryTS)-1]
			}
			run := lokiLogQueryRun(r.Start*sec, r.End*sec, r.Limit, backward, int64(len(r.EntryTS)), tMin, tMax, len(r.EntryTS) > 0)
			firstRule := run.ran
			switch r.Seq {
			case "first":
				if (run.pieces > 1 && r.Splits != run.ran) || (run.pieces <= 1 && r.Splits != 0) {
					t.Errorf("%s: splits %d, the rule runs %d of %d pieces", name, r.Splits, run.ran, run.pieces)
				}
				if r.Shards != firstRule {
					t.Errorf("%s: Loki shards %d, the first-answer rule %d (%d entries)", name, r.Shards, firstRule, len(r.EntryTS))
				}
			case "repeat":
				logRepeats++
				if len(r.EntryTS) == 0 {
					// An empty answer repeats as the results-cache hit.
					if r.Shards != 0 || r.Lines != 0 {
						t.Errorf("%s: empty answer repeat shards %d lines %d, want the results-cache hit (0, 0)", name, r.Shards, r.Lines)
					}
					continue
				}
				body := shardsAnswer(r.EntryTS, 2, backward)
				if got := shardsOfAnswer(body, r.Start*sec, r.End*sec, 0); got != r.Shards {
					t.Errorf("%s: the repeat's shards %d, Loki %d (entries in %d pieces)", name, got, r.Shards, len(r.EntryTS))
				}
			default:
				other++
				if r.Shards > firstRule {
					t.Errorf("%s: Loki reports %d shards, more than the %d pieces the first-answer rule runs", name, r.Shards, firstRule)
				}
				if r.Shards < firstRule {
					fewer++
				}
			}
		case "metric_range":
			n := lokiMetricRangePieces(r.Start*sec, r.End*sec, r.Step*sec)
			switch r.Seq {
			case "first":
				if r.Shards != n || r.Splits != splitsOf(n) {
					t.Errorf("%s: Loki splits %d shards %d, the pieces %d", name, r.Splits, r.Shards, n)
				}
			case "repeat":
				if r.Shards != 0 || r.Splits != max(n, 1) {
					t.Errorf("%s: Loki repeat splits %d shards %d, want the results-cache hit (%d, 0)", name, r.Splits, r.Shards, max(n, 1))
				}
			}
		case "instant":
			n := lokiInstantPieces(time.Duration(r.RangeSeconds) * time.Second)
			if r.Shards != n || r.Splits != splitsOf(n) {
				t.Errorf("%s: Loki splits %d shards %d, the pieces %d (the instant query runs again on a repeat)", name, r.Splits, r.Shards, n)
			}
		}
	}
	if logRepeats < 20 || other == 0 || fewer == 0 {
		t.Fatalf("recording too small: %d log repeats, %d other-window rows, %d with fewer shards", logRepeats, other, fewer)
	}
}

// sparseStubVL answers a log query with rows at fixed times (a sparse
// stream), the rest as a stats stub; it counts the requests it receives.
type sparseStubVL struct {
	mu       sync.Mutex
	times    []time.Time
	pad      string // appended to every line
	requests int
}

func (s *sparseStubVL) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/select/logsql/query" {
		_, _ = w.Write([]byte(`{"values":[]}`))
		return
	}
	_ = r.ParseForm()
	times := slices.Clone(s.times) // ascending
	if strings.Contains(r.Form.Get("query"), "sort by (_time desc)") {
		slices.Reverse(times)
	}
	if n, err := strconv.Atoi(r.Form.Get("limit")); err == nil && n < len(times) {
		times = times[:n]
	}
	var b strings.Builder
	for i, tm := range times {
		fmt.Fprintf(&b, `{"_time":%q,"_stream":"{app=\"api\"}","_msg":"line %d %s","app":"api"}`+"\n", tm.UTC().Format(time.RFC3339Nano), i, s.pad)
	}
	_, _ = w.Write([]byte(b.String()))
}

// TestQueryStats_LogRepeatShardsAreThePiecesWithEntries: through the whole
// handler chain, the first answer of a log query reports every piece it
// ran as shards, and a response-cache hit of it (and every later hit) the
// pieces that hold an entry, as Loki's repeat does; both directions, with
// the limit filled early, with empty pieces on the edges and between
// entries, and in the victorialogs mode (which never emulates Loki).
//
// conformance: semantics/query-stats-shards-empty-pieces, query-stats-envelope
func TestQueryStats_LogRepeatShardsAreThePiecesWithEntries(t *testing.T) {
	// Five pieces two days ago; entries in the first, third and fifth.
	h0 := time.Now().Add(-48 * time.Hour).Truncate(time.Hour)
	at := func(piece int, d time.Duration) time.Time { return h0.Add(time.Duration(piece)*time.Hour + d) }
	times := []time.Time{at(0, 10*time.Minute), at(0, 11*time.Minute), at(2, 20*time.Minute), at(4, 5*time.Minute), at(4, 6*time.Minute)}
	start, end := at(0, 5*time.Minute), at(4, 50*time.Minute)
	for _, tc := range []struct {
		name, direction, limit, mode string
		first, repeat, splits        int64
	}{
		{"backward", "backward", "1000", "", 5, 3, 5},
		{"forward", "forward", "1000", "", 5, 3, 5},
		{"backward filled in the third piece", "backward", "3", "", 3, 2, 3},
		{"forward filled in the third piece", "forward", "3", "", 3, 2, 3},
		{"backward filled in the first piece", "backward", "4", "", 5, 3, 5},
		{"forward filled in the first piece", "forward", "2", "", 1, 1, 1},
		{"victorialogs mode", "backward", "1000", QueryStatsModeVictoriaLogs, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &sparseStubVL{times: times}
			vl := httptest.NewServer(stub)
			defer vl.Close()
			h := newQueryStatsTestProxyMode(t, vl.URL, "", tc.mode)
			params := url.Values{"query": {`{app="api"}`}, "start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(end.Unix())},
				"limit": {tc.limit}, "direction": {tc.direction}}
			miss := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, nil))
			if miss.Summary.Entries == 0 {
				t.Fatalf("miss %+v: want entries", miss.Summary)
			}
			before := func() int { stub.mu.Lock(); defer stub.mu.Unlock(); return stub.requests }
			n := before()
			for i := 0; i < 2; i++ {
				hit := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, nil))
				if before() != n {
					t.Fatalf("hit %d reached VictoriaLogs", i)
				}
				if tc.mode != "" {
					continue
				}
				if hit.Summary.Shards != tc.repeat || hit.Summary.Splits != miss.Summary.Splits {
					t.Errorf("hit %d: splits %d shards %d, want Loki's repeat: splits %d, the %d pieces holding an entry", i, hit.Summary.Splits, hit.Summary.Shards, miss.Summary.Splits, tc.repeat)
				}
			}
			if tc.mode == "" && (miss.Summary.Shards != tc.first || miss.Summary.Splits != tc.splits) {
				t.Errorf("miss: splits %d shards %d, want splits %d and the %d pieces run as shards", miss.Summary.Splits, miss.Summary.Shards, tc.splits, tc.first)
			}
		})
	}
}

// TestParseTS19_Table: the eight-digit groups read back the number, at the
// ends of the 19-digit range and across the groups' carries.
//
// conformance: semantics/query-stats-shards-empty-pieces
func TestParseTS19_Table(t *testing.T) {
	for _, v := range []int64{1_000_000_000_000_000_000, 1_000_000_099_999_999_999, 1_794_490_755_123_456_789, 1_999_999_999_999_999_999,
		4_000_000_000_000_000_000, 9_223_372_036_854_775_807} {
		if got := parseTS19([]byte(fmt.Sprintf("%019d", v))); got != v {
			t.Errorf("parseTS19(%019d) = %d", v, got)
		}
	}
}

// TestQueryStats_LogRepeatShardsOfALargeAnswer: an answer of 1000 entries of
// about 300 bytes (cut into several writes on its way out, gzip-compressed
// for the client) reports the pieces that hold an entry on its repeat, the
// same as a small one.
//
// conformance: semantics/query-stats-shards-empty-pieces
func TestQueryStats_LogRepeatShardsOfALargeAnswer(t *testing.T) {
	h0 := time.Now().Add(-72 * time.Hour).Truncate(time.Hour)
	var times []time.Time
	for i := 0; i < 1000; i++ {
		times = append(times, h0.Add(time.Duration([]int{0, 2, 4}[i%3])*time.Hour+time.Duration(i)*time.Millisecond))
	}
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	stub := &sparseStubVL{times: times, pad: strings.Repeat("GET /api/v1/items 200 12ms ", 10)}
	vl := httptest.NewServer(stub)
	defer vl.Close()
	for _, compression := range []string{"", "gzip"} {
		t.Run("compression="+compression, func(t *testing.T) {
			h := newQueryStatsTestProxy(t, vl.URL, compression)
			params := url.Values{"query": {`{app="api"}`}, "start": {fmt.Sprint(h0.Unix())}, "end": {fmt.Sprint(h0.Add(5*time.Hour - time.Second).Unix())},
				"limit": {"5000"}, "direction": {"backward"}, "nonce": {compression}}
			headers := map[string]string{"Accept-Encoding": "gzip"}
			miss := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, headers))
			hit := statsView(t, serveQueryStats(t, h, "/loki/api/v1/query_range", params, headers))
			if miss.Summary.Entries != 1000 || miss.Summary.Shards != 5 || hit.Summary.Shards != 3 || hit.Summary.Splits != 5 {
				t.Errorf("miss %+v, hit %+v: want 1000 entries, 5 shards on the first answer and 3 on the repeat", miss.Summary, hit.Summary)
			}
		})
	}
}
