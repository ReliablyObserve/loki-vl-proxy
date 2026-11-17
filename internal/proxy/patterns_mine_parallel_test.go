package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// patternMineRowJSON is one row of the exact tier's lines query as VictoriaLogs writes it.
func patternMineRowJSON(buf *bytes.Buffer, at, msg, stream, level string) {
	row := map[string]string{"_time": at, "_msg": msg, "_stream_id": stream}
	if level != "" {
		row["level"] = level
	}
	encoded, _ := json.Marshal(row)
	buf.Write(encoded)
	buf.WriteByte('\n')
}

// randomPatternMineBody is a lines-query answer drawn from rng: a few to many streams of skewed sizes, levels that
// change inside a stream, plain, logfmt and JSON lines, lines too short or too long to mine, empty lines, streams
// varied enough to evict clusters and to stop being mined, sorted by stream and time as VictoriaLogs sorts them or
// not, and now and then a row the reader rejects (a bad time, a row without a message) part way.
func randomPatternMineBody(rng *rand.Rand) (body []byte, rows int) {
	streams := 1 + rng.Intn(80)
	n := 50 + rng.Intn(3000)
	type row struct {
		stream     int
		at         time.Time
		msg, level string
	}
	base := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	levels := []string{"info", "warn", "error", "debug", ""}
	words := []string{"order", "user", "cache", "shipped", "failed", "retry", "berlin", "oslo", "GET", "POST", "/api/v1/items", "payment", "db", "timeout"}
	varied := rng.Intn(3) == 0 // some streams draw random words: many clusters, evictions, the miner giving up
	out := make([]row, 0, n)
	for i := 0; i < n; i++ {
		// Skewed: a few streams hold most lines.
		s := int(float64(streams) * rng.Float64() * rng.Float64() * rng.Float64())
		level := levels[(s+i/(1+rng.Intn(40)))%len(levels)]
		var msg string
		switch k := rng.Intn(10); {
		case k == 0:
			msg = fmt.Sprintf(`level=%s msg="request done" id=%d took=%dms`, level, rng.Intn(1000), rng.Intn(90))
		case k == 1:
			msg = fmt.Sprintf(`{"level":"%s","msg":"charge %d for customer %d done","attempt":%d}`, level, rng.Intn(500), rng.Intn(50), rng.Intn(3))
		case k == 2:
			msg = "short line"
		case k == 3 && rng.Intn(20) == 0:
			msg = strings.Repeat("x ", 1600) // longer than a line Loki mines
		case k == 4 && rng.Intn(10) == 0:
			msg = ""
		case varied && s%2 == 0:
			parts := make([]string, 4+rng.Intn(5))
			for j := range parts {
				parts[j] = fmt.Sprintf("w%d", rng.Intn(5000))
			}
			msg = strings.Join(parts, " ")
		default:
			msg = fmt.Sprintf("%s %d %s to %s in %d ms", words[rng.Intn(len(words))], rng.Intn(100), words[rng.Intn(len(words))], words[rng.Intn(len(words))], rng.Intn(500))
		}
		out = append(out, row{stream: s, at: base.Add(time.Duration(rng.Intn(3600*1000)) * time.Millisecond), msg: msg, level: level})
	}
	if rng.Intn(4) != 0 {
		sort.SliceStable(out, func(i, j int) bool {
			if out[i].stream != out[j].stream {
				return out[i].stream < out[j].stream
			}
			return out[i].at.Before(out[j].at)
		})
	}
	var buf bytes.Buffer
	bad := -1
	if rng.Intn(5) == 0 {
		bad = rng.Intn(len(out))
	}
	for i, r := range out {
		at := r.at.Format(time.RFC3339Nano)
		msg := r.msg
		if i == bad {
			if rng.Intn(2) == 0 {
				at = "not a time"
			} else {
				msg = vlDefaultMsgValue
			}
		}
		patternMineRowJSON(&buf, at, msg, fmt.Sprintf("%032x", r.stream*2654435761), r.level)
	}
	return buf.Bytes(), len(out)
}

// minePatternBody mines a lines-query answer as the exact tier does, on workers workers (serially under 2).
func minePatternBody(body []byte, workers, maxRows int, setup func(*patternMiner)) (*patternMiner, error) {
	miner := newPatternMiner()
	setup(miner)
	var pm *patternParallelMiner
	if workers > 1 {
		pm = newPatternParallelMiner(miner, workers)
	}
	err := scanPatternLineRows(bytes.NewReader(body), maxRows, defaultLogRowLevels(), workers, patternLineConsumer(miner, patternLineSink(miner, pm)))
	if pm != nil {
		pm.finish()
	}
	return miner, err
}

// patternMinerDigest writes down everything a miner holds that an answer, the state or its snapshot reads: its
// miners in order with their keys and bookkeeping, and each one's clusters in order with their templates, stamps
// and samples; then the answer built from it.
func patternMinerDigest(t *testing.T, m *patternMiner) string {
	t.Helper()
	keyOf := make(map[*patternClusterer]patternMinerKey, len(m.miners))
	for k, c := range m.miners {
		keyOf[c] = k
	}
	if len(keyOf) != len(m.order) {
		t.Fatalf("%d miners, %d in order", len(keyOf), len(m.order))
	}
	var b strings.Builder
	for _, c := range m.order {
		k, ok := keyOf[c]
		if !ok {
			t.Fatal("a miner in order is not in the map")
		}
		fmt.Fprintf(&b, "miner %q %q clock=%d admitted=%d evicted=%d trees=%d\n", k.stream, k.level, c.clock, c.admitted, c.evicted, len(c.trees))
		for _, cl := range c.live {
			samples, _ := json.Marshal(cl.buckets)
			fmt.Fprintf(&b, "  %q %v %d used=%d total=%d %s\n", cl.tokens, cl.spacesAfter, cl.format, cl.used, cl.total, samples)
		}
	}
	answer, err := json.Marshal(buildPatternResponse(m, maxPatternResponseLimit))
	if err != nil {
		t.Fatal(err)
	}
	b.Write(answer)
	return b.String()
}

// Mining a read on workers gives the miner, and so the answer and the state, that mining it on one goroutine
// gives, whatever the streams, their sizes, the order of the rows and where a read fails.
//
// conformance: patterns-loki-mining, loki_api_v1_patterns
func TestPatternParallelMiningMinesAsSerial(t *testing.T) {
	cases := 120
	if testing.Short() {
		cases = 40
	}
	for seed := int64(1); seed <= int64(cases); seed++ {
		rng := rand.New(rand.NewSource(seed))
		body, rows := randomPatternMineBody(rng)
		maxRows := rows
		if rng.Intn(6) == 0 {
			maxRows = rng.Intn(rows) // the read stops at -patterns-max-counts-rows
		}
		fill := int64(rng.Intn(3) * 30)
		from := time.Date(2026, 11, 16, 9, 10, 0, 0, time.UTC).UnixNano() * int64(rng.Intn(2))
		setup := func(m *patternMiner) {
			m.fillStep = fill
			m.sampleFromNs = from
			m.shortWholeLine = seed%5 == 0
		}
		serial, serialErr := minePatternBody(body, 1, maxRows, setup)
		want := patternMinerDigest(t, serial)
		for _, workers := range []int{2, 3, 5} {
			parallel, err := minePatternBody(body, workers, maxRows, setup)
			if fmt.Sprint(err) != fmt.Sprint(serialErr) {
				t.Fatalf("seed %d, %d workers: error %v, serial %v", seed, workers, err, serialErr)
			}
			if got := patternMinerDigest(t, parallel); got != want {
				t.Fatalf("seed %d, %d workers: the miner differs from serial mining\n--- parallel\n%s\n--- serial\n%s", seed, workers, firstLines(got, 40), firstLines(want, 40))
			}
		}
	}
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

// FuzzPatternParallelMiningMinesAsSerial mines any lines, in any streams, on workers and on one goroutine, and
// wants the same miner.
func FuzzPatternParallelMiningMinesAsSerial(f *testing.F) {
	f.Add([]byte("GET /api/v1/users 200 15ms\nGET /api/v1/users 404 2ms\nlevel=info msg=done id=7 t=1\n{\"msg\":\"charge 5 for customer 9 done\"}\n"), uint8(3), uint8(2))
	f.Add([]byte("a b c d\na b c e\nz y x w\n\n\xff\xfe bad utf8 line here\n"), uint8(1), uint8(4))
	f.Add([]byte(strings.Repeat("w1 w2 w3 w4 w5\nw6 w7 w8 w9 w0\n", 50)), uint8(7), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, streams, workers uint8) {
		lines := strings.Split(string(data), "\n")
		if len(lines) > 3000 {
			lines = lines[:3000]
		}
		var buf bytes.Buffer
		base := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
		levels := []string{"info", "warn", "", "error"}
		for i, line := range lines {
			s := 0
			if streams > 0 {
				s = (i * 7 / 3) % int(streams)
			}
			level := ""
			if line != "" {
				level = levels[int(line[0])%len(levels)]
			}
			patternMineRowJSON(&buf, base.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), line, fmt.Sprintf("s%d", s), level)
		}
		setup := func(m *patternMiner) { m.fillStep = 10 }
		serial, serialErr := minePatternBody(buf.Bytes(), 1, len(lines), setup)
		parallel, err := minePatternBody(buf.Bytes(), 2+int(workers%5), len(lines), setup)
		if fmt.Sprint(err) != fmt.Sprint(serialErr) {
			t.Fatalf("error %v, serial %v", err, serialErr)
		}
		if got, want := patternMinerDigest(t, parallel), patternMinerDigest(t, serial); got != want {
			t.Fatalf("the miner differs from serial mining\n--- parallel\n%s\n--- serial\n%s", firstLines(got, 30), firstLines(want, 30))
		}
	})
}

// withPatternMineSizing mines reads of more than linesPerWorker lines on up to cores workers for the test.
func withPatternMineSizing(t *testing.T, linesPerWorker, cores int) {
	t.Helper()
	oldLines, oldCores := patternMineLinesPerWorker, patternMineCores
	patternMineLinesPerWorker = linesPerWorker
	patternMineCores = func() int { return cores }
	t.Cleanup(func() { patternMineLinesPerWorker, patternMineCores = oldLines, oldCores })
}

// A read is mined on workers only when it holds lines enough for two, workers are free, and the miner starts
// empty in one miner per stream; the workers it took are given back.
//
// conformance: patterns-loki-mining
func TestPatternParallelMiningIsSizedByTheRead(t *testing.T) {
	withPatternMineSizing(t, 100, 4)
	for _, tc := range []struct {
		name  string
		lines int
		setup func(*patternMiner)
		want  int
	}{
		{"too few lines", 150, func(*patternMiner) {}, 0},
		{"lines not counted", -1, func(*patternMiner) {}, 0},
		{"two workers", 250, func(*patternMiner) {}, 2},
		{"capped by the cores", 100_000, func(*patternMiner) {}, 4},
		{"one miner per level", 100_000, func(m *patternMiner) { m.oneStream = true }, 0},
		{"a miner that holds miners", 100_000, func(m *patternMiner) { m.Observe("info", "GET /api/v1/users 200 15ms", 0) }, 0},
	} {
		miner := newPatternMiner()
		tc.setup(miner)
		pm := startPatternMining(miner, tc.lines)
		got := 0
		if pm != nil {
			got = len(pm.workers)
			if running := patternMineWorkersRunning.Load(); running != int64(got) {
				t.Errorf("%s: %d workers running, %d started", tc.name, running, got)
			}
			pm.finish()
		}
		if got != tc.want {
			t.Errorf("%s: %d workers, want %d", tc.name, got, tc.want)
		}
		if running := patternMineWorkersRunning.Load(); running != 0 {
			t.Fatalf("%s: %d workers still counted after finish", tc.name, running)
		}
	}
	// Concurrent reads share the cores: one that finds fewer than two free mines on its own goroutine.
	first := startPatternMining(newPatternMiner(), 100_000)
	second := startPatternMining(newPatternMiner(), 100_000)
	if first == nil || len(first.workers) != 4 || second != nil {
		t.Fatalf("first read %v, second %v: want 4 workers, then none", first, second)
	}
	first.finish()
	if third := startPatternMining(newPatternMiner(), 300); third == nil || len(third.workers) != 3 {
		t.Fatalf("after the first read finished: %v, want 3 workers", third)
	} else {
		third.finish()
	}
}

// The workers of a read stop with it, whether it ended or failed part way, and a panic of a worker is raised on
// the request's goroutine, as one of serial mining would be.
//
// conformance: patterns-loki-mining
func TestPatternParallelMiningLeavesNoWorkers(t *testing.T) {
	before := runtime.NumGoroutine()
	var buf bytes.Buffer
	for i := 0; i < 2000; i++ {
		at := time.Date(2026, 11, 16, 9, 0, i%60, 0, time.UTC).Format(time.RFC3339Nano)
		if i == 1500 {
			at = "broken"
		}
		patternMineRowJSON(&buf, at, fmt.Sprintf("job %d of export finished in %dms", i, i%90), fmt.Sprintf("s%d", i%17), "info")
	}
	serial, serialErr := minePatternBody(buf.Bytes(), 1, 5000, func(*patternMiner) {})
	parallel, err := minePatternBody(buf.Bytes(), 4, 5000, func(*patternMiner) {})
	if err == nil || fmt.Sprint(err) != fmt.Sprint(serialErr) {
		t.Fatalf("error %v, serial %v: want the rejected row's", err, serialErr)
	}
	if got, want := patternMinerDigest(t, parallel), patternMinerDigest(t, serial); got != want {
		t.Fatal("a read that failed part way kept other lines than serial mining keeps")
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines after the reads, %d before", n, before)
	}

	pm := newPatternParallelMiner(newPatternMiner(), 2)
	pm.workers[0].miner = nil // the worker's next line panics
	for i := 0; i < 2*patternMineBatchRows; i++ {
		pm.observe(fmt.Sprintf("s%d", i), "info", "GET /api/v1/users 200 15ms", 0, 1)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a worker's panic was not raised by finish")
		}
	}()
	pm.finish()
}

// patternsAnswerBytes is the proxy's /patterns answer over [start, end), as sent.
func patternsAnswerBytes(t *testing.T, p *Proxy, query string, start, end time.Time) string {
	t.Helper()
	q := url.Values{"query": {query}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	w := httptest.NewRecorder()
	p.handlePatterns(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	return w.Body.String()
}

// patternMineRows is lines of many streams of a few lines each, as the stack's services write them.
func patternMineRows(vl *fakeStateVL, from time.Time, n int) {
	for i := 0; i < n; i++ {
		vl.add(stateRow{
			stream: fmt.Sprintf("api/p%03d", i%97),
			level:  []string{"info", "warn", "error"}[i%3],
			msg:    fmt.Sprintf("job %d of %s finished in %dms on %s", i, []string{"export", "import", "reindex"}[i%3], i%90, []string{"node-a", "node-b"}[i/7%2]),
			at:     from.Add(time.Duration(i) * 3 * time.Second),
		})
	}
}

// A cold /patterns request mined on workers answers byte for byte what one mined on one goroutine answers: from
// the exact tier's read of the range, and from the state built from the retention, whose snapshot is the same too.
//
// conformance: patterns-loki-mining, loki_api_v1_patterns, drilldown/patterns-full-range-counts
func TestPatternParallelMiningAnswersAsSerial(t *testing.T) {
	t.Run("services", func(t *testing.T) {
		vl := &fakeStateVL{}
		patternMineRows(vl, stateNow.Add(-110*time.Minute), 2000)
		patternParallelAnswersAsSerial(t, vl)
	})
	for seed := int64(1); seed <= 4; seed++ {
		t.Run(fmt.Sprintf("random-%d", seed), func(t *testing.T) {
			vl := &fakeStateVL{}
			randomPatternStateRows(vl, rand.New(rand.NewSource(seed)), stateNow.Add(-100*time.Minute))
			patternParallelAnswersAsSerial(t, vl)
		})
	}
}

// randomPatternStateRows is lines of streams of skewed sizes over most of the retention, several of a stream at
// one time now and then (the state reads such lines once), with levels changing inside a stream.
func randomPatternStateRows(vl *fakeStateVL, rng *rand.Rand, from time.Time) {
	streams := 2 + rng.Intn(60)
	at := from
	for i := 0; i < 1500+rng.Intn(1500); i++ {
		if rng.Intn(4) != 0 {
			at = at.Add(time.Duration(rng.Intn(6000)) * time.Millisecond)
		}
		s := int(float64(streams) * rng.Float64() * rng.Float64())
		vl.add(stateRow{
			stream: fmt.Sprintf("api/p%02d", s),
			level:  []string{"info", "warn", "error"}[(s+i/50)%3],
			msg:    fmt.Sprintf("%s %d for %s took %dms", []string{"order", "refund", "lookup"}[rng.Intn(3)], rng.Intn(9), []string{"berlin", "oslo", "rome", "w" + strconv.Itoa(rng.Intn(400))}[rng.Intn(4)], rng.Intn(50)),
			at:     at,
		})
	}
}

// patternParallelAnswersAsSerial compares the cold answers of proxies that mine on one goroutine and on workers.
func patternParallelAnswersAsSerial(t *testing.T, vl *fakeStateVL) {
	t.Helper()
	oldLines, oldCores := patternMineLinesPerWorker, patternMineCores
	t.Cleanup(func() { patternMineLinesPerWorker, patternMineCores = oldLines, oldCores })
	patternMineLinesPerWorker = 1 << 30
	now := stateNow
	query := `{app="api"}`

	exact := func() *Proxy {
		p := newPatternCountsProxy(t, vl.server(t).URL, 0)
		p.patternsExactMaxLines = 100_000
		return p
	}
	serialExact := patternsAnswerBytes(t, exact(), query, now.Add(-time.Hour), now)
	serialState := newStateProxy(t, vl, &now, 1<<30)
	serialStateAnswer := patternsAnswerBytes(t, serialState, query, now.Add(-time.Hour), now)

	patternMineLinesPerWorker, patternMineCores = 100, func() int { return 4 }
	reads := patternMineParallelReads.Load()
	if got := patternsAnswerBytes(t, exact(), query, now.Add(-time.Hour), now); got != serialExact {
		t.Fatalf("exact tier on workers answered\n%s\nserially\n%s", got, serialExact)
	}
	parallelState := newStateProxy(t, vl, &now, 1<<30)
	if got := patternsAnswerBytes(t, parallelState, query, now.Add(-time.Hour), now); got != serialStateAnswer {
		t.Fatalf("state built on workers answers\n%s\nserially\n%s", got, serialStateAnswer)
	}
	if n := patternMineParallelReads.Load() - reads; n != 2 {
		t.Fatalf("%d reads mined on workers, want the exact tier's and the state build's", n)
	}
	snapshot := func(p *Proxy) string {
		for _, tenant := range p.patternState.tenants {
			for _, s := range tenant.selectors {
				data, err := encodePatternStateSnapshot(tenant.snapshot(s))
				if err != nil {
					t.Fatal(err)
				}
				snap, _ := decodePatternStateSnapshot(data, 1<<30)
				// The folded clusters of a stream are listed in the order aging met its miners, a map's (so two
				// serial builds list them differently too): compared as a set.
				for _, folded := range snap.Folded {
					sort.Slice(folded, func(i, j int) bool {
						if folded[i].Level != folded[j].Level {
							return folded[i].Level < folded[j].Level
						}
						return folded[i].Pattern < folded[j].Pattern
					})
				}
				out, _ := json.Marshal(snap)
				return string(out)
			}
		}
		t.Fatal("no state")
		return ""
	}
	if got, want := snapshot(parallelState), snapshot(serialState); got != want {
		t.Fatal("the state built on workers snapshots differently from the one built serially")
	}
	// A refresh trains the state on: it is mined on the request's goroutine.
	patternMineRows(vl, stateNow, 300)
	now = stateNow.Add(16 * time.Minute)
	reads = patternMineParallelReads.Load()
	refreshed := patternsAnswerBytes(t, parallelState, query, now.Add(-time.Hour), now)
	if n := patternMineParallelReads.Load() - reads; n != 0 {
		t.Fatalf("a refresh was mined on workers (%d reads)", n)
	}
	if want := patternsAnswerBytes(t, serialState, query, now.Add(-time.Hour), now); refreshed != want {
		t.Fatalf("refreshed state built on workers answers\n%s\nserially\n%s", refreshed, want)
	}
}

// A row's time is read from its bytes as parseLokiTimeToUnixNano reads its text, whatever the text.
//
// conformance: patterns-loki-mining
func TestPatternRowTimeReadsAsLokiTimes(t *testing.T) {
	for _, raw := range []string{
		"2026-11-16T09:00:00Z", "2026-11-16T09:00:00.1Z", "2026-11-16T09:00:00.123456789Z", "2026-11-16T09:00:00.000000001Z",
		"2024-02-29T23:59:59.5Z", "2023-02-29T00:00:00Z", "2026-13-01T00:00:00Z", "2026-11-31T00:00:00Z", "2026-11-16T24:00:00Z",
		"2026-11-16T09:60:00Z", "2026-11-16T09:00:60Z", "2026-11-16T09:00:00.Z", "2026-11-16T09:00:00.1234567890Z",
		"2026-11-16T09:00:00+01:00", "2026-11-16T09:00:00z", "1677-01-01T00:00:00Z", "2263-01-01T00:00:00Z", "0000-01-01T00:00:00Z",
		"1700000000000000000", "1700000000", "", "now", " 2026-11-16T09:00:00Z", "2026-11-16 09:00:00Z", "2026-11-16T09:00:0aZ",
	} {
		gotNs, gotOK := patternRowTimeNs([]byte(raw))
		wantNs, wantOK := parseLokiTimeToUnixNano(raw)
		if raw == "now" {
			continue // the clock moves between the two reads
		}
		if gotNs != wantNs || gotOK != wantOK {
			t.Errorf("%q: %d %v, parseLokiTimeToUnixNano %d %v", raw, gotNs, gotOK, wantNs, wantOK)
		}
	}
}

// FuzzPatternRowTimeReadsAsLokiTimes checks the row time reader against parseLokiTimeToUnixNano on any text.
func FuzzPatternRowTimeReadsAsLokiTimes(f *testing.F) {
	for _, seed := range []string{"2026-11-16T09:00:00Z", "2026-11-16T09:00:00.123456789Z", "2024-02-29T23:59:59.5Z", "2026-02-30T00:00:00Z", "1700000000000000000"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if strings.Contains(raw, "now") {
			return // the clock moves between the two reads
		}
		gotNs, gotOK := patternRowTimeNs([]byte(raw))
		wantNs, wantOK := parseLokiTimeToUnixNano(raw)
		if gotNs != wantNs || gotOK != wantOK {
			t.Fatalf("%q: %d %v, parseLokiTimeToUnixNano %d %v", raw, gotNs, gotOK, wantNs, wantOK)
		}
	})
}

// A line's hash is hash/fnv's 64-bit FNV-1a, which snapshots written before hold.
//
// conformance: patterns-loki-mining, peer-cache-patterns
func TestPatternLineHashIsFNV1a(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		b := make([]byte, rng.Intn(400))
		rng.Read(b)
		h := fnv.New64a()
		_, _ = h.Write(b)
		if got, want := patternLineHash(b), h.Sum64(); got != want || patternLineHash(string(b)) != want {
			t.Fatalf("%q: %x, fnv %x", b, got, want)
		}
	}
}
