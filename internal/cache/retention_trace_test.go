package cache

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// Trace replays for the retention policy (retention.go): deterministic request
// traces driven through a real memory cache with a real disk cache below it,
// with the policy off and on. A request is a lookup (GetWithTier); a miss
// stores the answer (SetWithTTL), as the proxy's handlers do. The clock is
// simulated (clockOffsetNs pinned to the trace's time before every request),
// deferred LRU promotions are drained after every hit and the disk queue is
// flushed every simulated five seconds (its default interval), so a replay
// gives the same numbers on every run and machine.

// traceRequest is one lookup at a simulated offset from the trace start.
type traceRequest struct {
	at   time.Duration
	key  string
	size int
	ttl  time.Duration
}

// traceBudget sizes the tiers of a replay.
type traceBudget struct {
	l1Entries, l1Bytes int
	l2Entries          int
	l2Bytes            int64
}

// traceResult counts what a replay's lookups found, overall and from a
// window of the trace (to judge recovery after a flood).
type traceResult struct {
	requests, l1Hits, l2Hits                 int64
	requestBytes, l1Bytes, l2Bytes           int64
	windowRequests, windowHits, windowL1Hits int64
	windowRequestBytes, windowHitBytes       int64
	l2Writes                                 int64
	demoted                                  [DemotionCount]int64
}

func (r traceResult) hitRatio() float64 { return ratio(r.l1Hits+r.l2Hits, r.requests) }
func (r traceResult) l1Ratio() float64  { return ratio(r.l1Hits, r.requests) }
func (r traceResult) byteHitRatio() float64 {
	return ratio(r.l1Bytes+r.l2Bytes, r.requestBytes)
}
func (r traceResult) windowL1Ratio() float64   { return ratio(r.windowL1Hits, r.windowRequests) }
func (r traceResult) windowRatio() float64     { return ratio(r.windowHits, r.windowRequests) }
func (r traceResult) windowByteRatio() float64 { return ratio(r.windowHitBytes, r.windowRequestBytes) }

func ratio(n, d int64) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}

// traceValues holds one value buffer per size so a replay does not allocate
// per request; only its length matters to the cache.
var traceValues = map[int][]byte{}

func traceValue(size int) []byte {
	v, ok := traceValues[size]
	if !ok {
		v = make([]byte, size)
		traceValues[size] = v
	}
	return v
}

// replayTrace runs reqs through a fresh memory and disk cache with policy and
// counts, for requests at or after window, the window figures as well.
func replayTrace(t testing.TB, policy RetentionPolicy, budget traceBudget, reqs []traceRequest, window time.Duration) traceResult {
	t.Helper()
	saved := clockOffsetNs.Load()
	defer clockOffsetNs.Store(saved)
	base := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	setClock := func(at time.Duration) { clockOffsetNs.Store(int64(time.Until(base.Add(at)))) }
	setClock(0)

	dc, err := NewDiskCache(DiskCacheConfig{
		Path: filepath.Join(t.TempDir(), "trace.db"), MaxEntries: budget.l2Entries, MaxBytes: budget.l2Bytes,
		// Flushed by the replay only: no size, byte or timer flush in between.
		FlushInterval: 24 * time.Hour, FlushSize: 1 << 30, FlushBytes: 1 << 40, MaxBufferBytes: 1 << 40,
		MinTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := NewWithMaxBytes(time.Minute, budget.l1Entries, budget.l1Bytes)
	if err := c.SetRetention(policy); err != nil {
		t.Fatal(err)
	}
	c.SetL2(dc)
	defer func() {
		c.Close()
		_ = dc.Close()
	}()

	var res traceResult
	nextFlush := 5 * time.Second
	for _, r := range reqs {
		for r.at >= nextFlush {
			setClock(nextFlush)
			dc.Flush()
			nextFlush += 5 * time.Second
		}
		setClock(r.at)
		v, tier, ok := c.GetWithTier(r.key)
		res.requests++
		res.requestBytes += int64(r.size)
		inWindow := r.at >= window
		if inWindow {
			res.windowRequests++
			res.windowRequestBytes += int64(r.size)
		}
		if ok {
			if len(v) != r.size {
				t.Fatalf("%s: got %d bytes, stored %d", r.key, len(v), r.size)
			}
			switch tier {
			case "l1_memory":
				res.l1Hits++
				res.l1Bytes += int64(r.size)
				if inWindow {
					res.windowL1Hits++
				}
			case "l2_disk":
				res.l2Hits++
				res.l2Bytes += int64(r.size)
			}
			if inWindow {
				res.windowHits++
				res.windowHitBytes += int64(r.size)
			}
			c.drainPromotions()
			continue
		}
		c.SetWithTTL(r.key, traceValue(r.size), r.ttl)
	}
	for _, s := range dc.LayerStats() {
		res.l2Writes += s.L2.Admissions
	}
	for _, s := range c.SignalStats() {
		for d := range s.Demotions {
			res.demoted[d] += s.Demotions[d]
		}
	}
	return res
}

// traceTTL ends a TTL 37ms before the request grid (multiples of 100ms), so
// no lookup lands on an expiry instant, where the microseconds between pinning
// the clock and reading it would decide.
func traceTTL(d time.Duration) time.Duration { return d - 37*time.Millisecond }

// traceSize is a key's answer size: log-normal around median bytes, from a
// hash of the key so every policy sees the same sizes.
func traceSize(key string, median int) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(key); i++ {
		h = (h ^ uint64(key[i])) * 1099511628211
	}
	u1 := (float64(h>>11) + 0.5) / float64(1<<53)
	u2 := (float64((h*6364136223846793005+1442695040888963407)>>11) + 0.5) / float64(1<<53)
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	size := int(float64(median) * math.Exp(0.8*z))
	return min(max(size, 256), 24*median)
}

// drilldownTrace: users open Logs Drilldown service pages, picked by a Zipf
// law over services; each visit asks the page's panels (volume, logs, labels,
// fields, patterns, ...). A visit every two seconds for 2h20m.
func drilldownTrace() []traceRequest {
	rng := rand.New(rand.NewSource(1767))
	services := rand.NewZipf(rng, 1.07, 1, 399)
	const panels = 8
	var out []traceRequest
	for i := 0; i < 4200; i++ {
		at := time.Duration(i) * 2 * time.Second
		svc := services.Uint64()
		for p := 0; p < panels; p++ {
			key := fmt.Sprintf("query_range:dd:svc%d:p%d", svc, p)
			out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 6<<10), ttl: traceTTL(10 * time.Minute)})
		}
	}
	return out
}

// dashboardTrace: twelve dashboards of ten panels refreshed every minute,
// each panel asking its last hour as twelve 5m buckets (settled buckets live
// 2h, the open bucket 15s, below the disk's minimum TTL), beside Explore
// queries (a Zipf law over 1500 queries, one every two seconds, 5m TTL).
func dashboardTrace() []traceRequest {
	rng := rand.New(rand.NewSource(1768))
	explore := rand.NewZipf(rng, 1.1, 1, 1499)
	var out []traceRequest
	const duration = 2 * time.Hour
	for at := time.Duration(0); at < duration; at += time.Second {
		if at%(2*time.Second) == 0 {
			key := fmt.Sprintf("query_range:explore:q%d", explore.Uint64())
			out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 8<<10), ttl: traceTTL(5 * time.Minute)})
		}
		for d := 0; d < 12; d++ {
			if (at/time.Second)%60 != time.Duration(d*5) {
				continue
			}
			open := at.Truncate(5 * time.Minute)
			for p := 0; p < 10; p++ {
				for b := 0; b < 12; b++ {
					bucket := open - time.Duration(b)*5*time.Minute
					key := fmt.Sprintf("query-steps:v1:d%d:p%d:%d", d, p, bucket/time.Second)
					ttl := traceTTL(2 * time.Hour)
					if b == 0 {
						ttl = traceTTL(15 * time.Second)
					}
					out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 2<<10), ttl: ttl})
				}
			}
		}
	}
	return out
}

// floodTrace: steady traffic over a hot set (a Zipf law over 600 queries, two
// a second) and, from 30m to 40m, a one-time scan of 18000 distinct answers
// (thirty a second: a crawler or a sweep over every service and range). The
// window figures are the 20 minutes after the flood.
func floodTrace() ([]traceRequest, time.Duration) {
	rng := rand.New(rand.NewSource(1769))
	hot := rand.NewZipf(rng, 1.05, 1, 599)
	var out []traceRequest
	const duration = 60 * time.Minute
	scan := 0
	for at := time.Duration(0); at < duration; at += 100 * time.Millisecond {
		if at%(500*time.Millisecond) == 0 {
			key := fmt.Sprintf("query_range:hot:q%d", hot.Uint64())
			out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 8<<10), ttl: traceTTL(30 * time.Minute)})
		}
		if at >= 30*time.Minute && at < 40*time.Minute {
			for j := 0; j < 3; j++ {
				key := fmt.Sprintf("query_range:scan:%d", scan)
				scan++
				out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 8<<10), ttl: traceTTL(30 * time.Minute)})
			}
		}
	}
	return out, 40 * time.Minute
}

// shiftTrace: the working set moves: a Zipf law over 800 queries for 40
// minutes, then over 800 other queries for 40 minutes, four a second, 2h TTL
// (no expiry clears the first set). Protection can only hurt here: the window
// figures are the 40 minutes after the shift.
func shiftTrace() ([]traceRequest, time.Duration) {
	rng := rand.New(rand.NewSource(1770))
	zipf := rand.NewZipf(rng, 1.05, 1, 799)
	var out []traceRequest
	for at := time.Duration(0); at < 80*time.Minute; at += 250 * time.Millisecond {
		set := "a"
		if at >= 40*time.Minute {
			set = "b"
		}
		key := fmt.Sprintf("query_range:%s:q%d", set, zipf.Uint64())
		out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 8<<10), ttl: traceTTL(2 * time.Hour)})
	}
	return out, 40 * time.Minute
}

// ttlTrace: the production shape of #1767: budgets far above the working set,
// so every eviction is an expiry; a Zipf law over 3000 queries, one a second,
// 60s TTL.
func ttlTrace() []traceRequest {
	rng := rand.New(rand.NewSource(1771))
	zipf := rand.NewZipf(rng, 1.1, 1, 2999)
	var out []traceRequest
	for at := time.Duration(0); at < time.Hour; at += time.Second {
		key := fmt.Sprintf("query_range:ttl:q%d", zipf.Uint64())
		out = append(out, traceRequest{at: at, key: key, size: traceSize(key, 8<<10), ttl: traceTTL(time.Minute)})
	}
	return out
}

// distinctBytes is the bytes of the trace's distinct keys.
func distinctBytes(reqs []traceRequest) (int, int) {
	seen := map[string]int{}
	total := 0
	for _, r := range reqs {
		if _, ok := seen[r.key]; !ok {
			seen[r.key] = r.size
			total += r.size + len(r.key)
		}
	}
	return len(seen), total
}

type retentionTrace struct {
	name   string
	reqs   []traceRequest
	window time.Duration
	budget traceBudget
}

// retentionTraces sizes each trace's memory tier to a fraction of its distinct
// bytes and the disk to a larger one, so capacity binds on both tiers (the
// policy only acts on capacity evictions), except ttl-bound.
func retentionTraces() []retentionTrace {
	sized := func(reqs []traceRequest, l1, l2 float64) traceBudget {
		keys, bytes := distinctBytes(reqs)
		entries := max(1, int(math.Ceil(l2))) * keys
		return traceBudget{l1Entries: entries, l1Bytes: int(float64(bytes) * l1), l2Entries: entries, l2Bytes: int64(float64(bytes) * l2)}
	}
	dd := drilldownTrace()
	dash := dashboardTrace()
	flood, floodWindow := floodTrace()
	shift, shiftWindow := shiftTrace()
	ttl := ttlTrace()
	return []retentionTrace{
		{name: "drilldown-zipf", reqs: dd, window: time.Hour, budget: sized(dd, 0.10, 0.25)},
		{name: "dashboard-refresh", reqs: dash, window: time.Hour, budget: sized(dash, 0.15, 0.35)},
		{name: "one-scan-flood", reqs: flood, window: floodWindow, budget: sized(flood, 0.03, 0.06)},
		{name: "working-set-shift", reqs: shift, window: shiftWindow, budget: sized(shift, 0.25, 0.45)},
		{name: "ttl-bound", reqs: ttl, window: 30 * time.Minute, budget: sized(ttl, 4, 8)},
	}
}

// retentionTraceDefault is the policy the flags' defaults select.
const retentionTraceDefault = "protect20"

var retentionPolicies = []struct {
	name   string
	policy RetentionPolicy
}{
	{"lru", RetentionPolicy{}},
	{"protect10", RetentionPolicy{ProtectPercent: 10}},
	{"protect20", RetentionPolicy{ProtectPercent: 20}},
	{"protect35", RetentionPolicy{ProtectPercent: 35}},
	{"protect50", RetentionPolicy{ProtectPercent: 50}},
	{"protect80", RetentionPolicy{ProtectPercent: 80}},
	{"demote", RetentionPolicy{DemoteOnEvict: true}},
	{"protect20+demote", RetentionPolicy{ProtectPercent: 20, DemoteOnEvict: true}},
}

// TestRetentionTraceReplay prints the hit ratio, byte hit ratio, memory hit
// ratio and the window figures of every trace under every policy, and holds
// the proposed default to: no trace worse than plain LRU on any ratio, the
// ttl-bound trace exactly equal (expiry evictions are unaffected), and the
// flood recovered from better than LRU.
//
// conformance: cache-reuse-retention
func TestRetentionTraceReplay(t *testing.T) {
	if raceDetectorEnabled {
		// About 400k lookups; the replay is single-threaded, so the race
		// detector adds nothing but time. The coverage run (no -race) runs it,
		// and TestRetentionTraceReplayDeterministic runs under -race.
		t.Skip("trace replay runs without the race detector")
	}
	var rows []string
	results := map[string]map[string]traceResult{}
	for _, tr := range retentionTraces() {
		results[tr.name] = map[string]traceResult{}
		for _, p := range retentionPolicies {
			r := replayTrace(t, p.policy, tr.budget, tr.reqs, tr.window)
			results[tr.name][p.name] = r
			rows = append(rows, fmt.Sprintf("| %s | %s | %d | %.4f | %.4f | %.4f | %.4f | %.4f | %.4f | %d | %d/%d |",
				tr.name, p.name, r.requests, r.hitRatio(), r.byteHitRatio(), r.l1Ratio(), r.windowRatio(), r.windowByteRatio(), r.windowL1Ratio(),
				r.l2Writes, r.demoted[DemotionRetained], r.demoted[DemotionWritten]))
		}
	}
	t.Logf("\n| trace | policy | requests | hit ratio | byte hit ratio | L1 hit ratio | window hit ratio | window byte hit ratio | window L1 hit ratio | L2 writes | demoted retained/written |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n%s", strings.Join(rows, "\n"))

	def := retentionTraceDefault
	for name, byPolicy := range results {
		base, got := byPolicy["lru"], byPolicy[def]
		for _, m := range []struct {
			what      string
			base, got float64
		}{
			{"hit ratio", base.hitRatio(), got.hitRatio()},
			{"byte hit ratio", base.byteHitRatio(), got.byteHitRatio()},
			{"window hit ratio", base.windowRatio(), got.windowRatio()},
			{"window byte hit ratio", base.windowByteRatio(), got.windowByteRatio()},
			{"memory hit ratio", base.l1Ratio(), got.l1Ratio()},
			{"window memory hit ratio", base.windowL1Ratio(), got.windowL1Ratio()},
		} {
			if m.got < m.base {
				t.Errorf("%s: %s %s %.4f below lru %.4f", name, def, m.what, m.got, m.base)
			}
		}
	}
	ttl := results["ttl-bound"]
	keys := make([]string, 0, len(ttl))
	for k := range ttl {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if r := ttl[k]; r.l1Hits != ttl["lru"].l1Hits || r.l2Hits != ttl["lru"].l2Hits {
			t.Errorf("ttl-bound: %s hits %d/%d differ from lru %d/%d: expiry evictions must be unaffected", k, r.l1Hits, r.l2Hits, ttl["lru"].l1Hits, ttl["lru"].l2Hits)
		}
	}
	if f := results["one-scan-flood"]; f[def].windowRatio() <= f["lru"].windowRatio() {
		t.Errorf("one-scan-flood: %s window hit ratio %.4f not above lru %.4f", def, f[def].windowRatio(), f["lru"].windowRatio())
	}
	checkRetentionDocTable(t, results)
}

// retentionDocPolicies are the policies the published table shows.
var retentionDocPolicies = []string{"lru", "protect20", "protect50", "protect80", "demote", "protect20+demote"}

// retentionDocFile holds the published table, between the markers.
const (
	retentionDocFile  = "../../docs/cache-troubleshooting.md"
	retentionDocBegin = "<!-- retention-trace-table:begin (TestRetentionTraceReplay; LVP_RETENTION_TABLE_UPDATE=1 rewrites it) -->"
	retentionDocEnd   = "<!-- retention-trace-table:end -->"
)

// checkRetentionDocTable holds the published table to the replay: the docs
// show these numbers, so a change to the policy or the traces must update
// them (LVP_RETENTION_TABLE_UPDATE=1).
func checkRetentionDocTable(t *testing.T, results map[string]map[string]traceResult) {
	t.Helper()
	pct := func(v float64) string { return fmt.Sprintf("%.2f%%", 100*v) }
	var b strings.Builder
	b.WriteString("| Trace | Policy | Hit ratio | Byte hit ratio | L1 hit ratio | After the event: hit ratio | After the event: L1 hit ratio |\n|---|---|---:|---:|---:|---:|---:|\n")
	for _, tr := range []string{"drilldown-zipf", "dashboard-refresh", "one-scan-flood", "working-set-shift", "ttl-bound"} {
		for _, p := range retentionDocPolicies {
			r := results[tr][p]
			fmt.Fprintf(&b, "| `%s` | `%s` | %s | %s | %s | %s | %s |\n", tr, p, pct(r.hitRatio()), pct(r.byteHitRatio()), pct(r.l1Ratio()), pct(r.windowRatio()), pct(r.windowL1Ratio()))
		}
	}
	want := retentionDocBegin + "\n" + b.String() + retentionDocEnd
	raw, err := os.ReadFile(retentionDocFile)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	i, j := strings.Index(doc, retentionDocBegin), strings.Index(doc, retentionDocEnd)
	if i < 0 || j < i {
		t.Fatalf("%s: no retention table markers", retentionDocFile)
	}
	if got := doc[i : j+len(retentionDocEnd)]; got != want {
		if os.Getenv("LVP_RETENTION_TABLE_UPDATE") == "1" {
			if err := os.WriteFile(retentionDocFile, []byte(doc[:i]+want+doc[j+len(retentionDocEnd):]), 0o644); err != nil {
				t.Fatal(err)
			}
			return
		}
		t.Errorf("%s: the retention table differs from the replay; rerun with LVP_RETENTION_TABLE_UPDATE=1\n%s", retentionDocFile, want)
	}
}

// TestRetentionTraceReplayDeterministic replays one trace twice per policy:
// the numbers the replay test holds must not depend on scheduling.
//
// conformance: cache-reuse-retention
func TestRetentionTraceReplayDeterministic(t *testing.T) {
	reqs, window := floodTrace()
	budget := retentionTraces()[2].budget
	for _, p := range retentionPolicies {
		if p.name != "lru" && p.name != retentionTraceDefault && p.name != "protect20+demote" {
			continue
		}
		a := replayTrace(t, p.policy, budget, reqs, window)
		b := replayTrace(t, p.policy, budget, reqs, window)
		if a != b {
			t.Fatalf("%s: replays differ:\n%+v\n%+v", p.name, a, b)
		}
	}
}
