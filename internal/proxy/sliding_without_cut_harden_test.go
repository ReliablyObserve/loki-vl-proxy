package proxy

import (
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// swSetVersion makes both proxies of the runner read the backend as version
// ("": unknown). A proxy keeps the first version it learns, so it is cleared
// first.
func (r *swRunner) swSetVersion(version string) {
	r.emu.version = version
	for _, p := range r.proxies {
		p.backendVersionSemver = ""
		p.storeBackendVersion(version, version)
	}
}

// swVersions: unknown, the oldest and the newest supported VictoriaLogs.
var swVersions = []string{"", "v1.49.0", "v1.53.0"}

// The cut route answers Loki's windows on every supported VictoriaLogs, and
// where there is no line at all (no series, an empty matrix on both routes).
//
// conformance: semantics/without-sliding-sum-folded-buckets, metric-series-identity
func TestSlidingSumWithoutCutVersions(t *testing.T) {
	runner := newSWRunner(t)
	cut := 0
	for _, version := range swVersions[1:] {
		runner.swSetVersion(version)
		for _, fn := range []string{"count_over_time", "bytes_over_time", "rate"} {
			rnd := rand.New(rand.NewSource(int64(len(version))*31 + int64(len(fn))))
			for i := 0; i < 40; i++ {
				s, streams := swGenerateScenario(rnd, fn)
				if i%10 == 9 {
					s.rows = nil // no line in any window
				}
				if swCheck(t, runner, fmt.Sprintf("%s %s #%d", version, fn, i), s, streams, fn) {
					cut++
				}
			}
		}
	}
	if cut < 40 {
		t.Fatalf("scenarios folded by VictoriaLogs: %d", cut)
	}
}

// With the extent cache on, one tenant's cut read is never another's: the
// second tenant reads every bucket again and gets the first one's answer, and
// the first reads next to nothing the second time.
//
// conformance: semantics/without-sliding-sum-folded-buckets, tenant-numeric-routing
func TestSlidingSumWithoutCutTenants(t *testing.T) {
	s0 := time.Now().Add(-3 * time.Hour).Truncate(time.Hour)
	var rows []emuRow
	for i := 0; i < 600; i++ {
		stream := fmt.Sprintf(`{app="uw",pod="p%d",zone="%s"}`, i%37, []string{"x", "y"}[i%2])
		rows = append(rows, emuRow{ts: s0.UnixNano() + int64(i)*int64(11*time.Second) + int64(i%7)*1e6, stream: stream, msg: "x=1"})
	}
	emu := &vlEmulator{t: t, rows: rows, field: "amount"}
	backend, shim := newExtentShim(t, emu.handler())
	p := newExtentTestProxy(t, backend.URL, true, 64<<20)
	p.labelTranslator = NewLabelTranslator(LabelStyleUnderscores, nil)
	start, end := s0.Add(10*time.Minute), s0.Add(70*time.Minute)
	params := url.Values{"query": {`sum without (pod) (count_over_time({app="uw"}[1m]))`}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "step": {"20"}}
	read := func(tenant string) (string, int64) {
		shim.reset()
		res := doCompatProxyRequest(p, "/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": tenant})
		if res.Code != 200 {
			t.Fatalf("tenant %s: %d %s", tenant, res.Code, res.Body)
		}
		p.statsExtentStores.wait()
		_, buckets := shim.bucketsRead()
		return res.Body.String(), buckets
	}
	a1, first := read("11")
	b1, second := read("22")
	a2, third := read("11")
	t.Logf("buckets read: tenant a %d, tenant b %d, tenant a again %d", first, second, third)
	if first == 0 || second < first || third >= first/2 {
		t.Fatalf("the buckets of one tenant were reused by another, or not kept: %d, %d, %d", first, second, third)
	}
	if a1 != b1 || a1 != a2 {
		t.Fatalf("the answers differ:\na: %.400s\nb: %.400s\na again: %.400s", a1, b1, a2)
	}
}

// FuzzSlidingWithoutCut: whatever streams, lines, window, step and excluded
// labels the seed draws, the cut route and the per-stream route answer the same
// bytes, both are Loki's windows (swReference), and VictoriaLogs folds the streams
// exactly where the route is meant to (swCheck), on an unknown, the oldest and
// the newest supported VictoriaLogs.
//
// conformance: semantics/without-sliding-sum-folded-buckets, metric-series-identity
func FuzzSlidingWithoutCut(f *testing.F) {
	for seed := int64(1); seed <= 6; seed++ {
		f.Add(seed*7919, uint8(seed), uint8(seed))
	}
	runner := newSWRunner(f)
	fns := []string{"count_over_time", "bytes_over_time", "rate"}
	f.Fuzz(func(t *testing.T, seed int64, fn, version uint8) {
		runner.emu.t = t
		runner.swSetVersion(swVersions[int(version)%len(swVersions)])
		name := fns[int(fn)%len(fns)]
		s, streams := swGenerateScenario(rand.New(rand.NewSource(seed)), name)
		swCheck(t, runner, fmt.Sprintf("seed %d", seed), s, streams, name)
	})
}
