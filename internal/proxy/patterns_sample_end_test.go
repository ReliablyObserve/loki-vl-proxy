package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// The lines of a range end with the 10 s sample its end falls in: Loki answers every sample that starts inside the
// range with all of its lines, including those after the range's end.
//
// conformance: drilldown/patterns-range-end-inside-sample, drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternSamplesEndNs(t *testing.T) {
	const s = int64(time.Second)
	b := int64(1794684010) * s // starts a sample
	for _, c := range []struct {
		name      string
		end, want int64
	}{
		{"boundary", b, b},
		{"boundary+1ns", b + 1, b + 10*s},
		{"mid-sample", b + 5*s, b + 10*s},
		{"boundary+9s (1 s before the next)", b + 9*s, b + 10*s},
		{"boundary+10s-1ns", b + 10*s - 1, b + 10*s},
		{"next boundary", b + 10*s, b + 10*s},
		{"zero", 0, 0},
		{"near the largest time", math.MaxInt64 - 1, math.MaxInt64 - 1},
	} {
		if got := patternSamplesEndNs(c.end); got != c.want {
			t.Errorf("%s: patternSamplesEndNs(%d) = %d, want %d", c.name, c.end, got, c.want)
		}
	}
}

// samplesEndRows is a line every 250 ms of one stream from `from` to `to`, numbered.
func samplesEndRows(from, to time.Time) []stateRow {
	var rows []stateRow
	for at, i := from, 0; at.Before(to); at, i = at.Add(250*time.Millisecond), i+1 {
		rows = append(rows, stateRow{stream: "api/a", level: "info", msg: fmt.Sprintf("order %d shipped to berlin", i), at: at})
	}
	return rows
}

// lokiPatternSamples is Loki's answer for rows over [start, end) at step: each line in its 10 s sample, each
// sample starting inside the range counted whole into the step it falls in, epoch-aligned.
func lokiPatternSamples(rows []stateRow, start, end time.Time, step int64) map[int64]int {
	want := map[int64]int{}
	for _, row := range rows {
		sample := patternSampleStart(row.at.UnixNano())
		if ns := sample * int64(time.Second); ns < start.UnixNano() || ns >= end.UnixNano() {
			continue
		}
		want[sample/step*step]++
	}
	return want
}

// samplesEndAnswer is the proxy's samples for the one pattern of the rows, and how many lines they hold.
func samplesEndAnswer(t *testing.T, p *Proxy, start, end time.Time) map[int64]int {
	t.Helper()
	return samplesEndAnswerAt(t, p, start, end, "")
}

// samplesEndAnswerAt is samplesEndAnswer with the request's step ("" lets the proxy derive Loki's).
func samplesEndAnswerAt(t *testing.T, p *Proxy, start, end time.Time, step string) map[int64]int {
	t.Helper()
	q := url.Values{"query": {`{app="api"}`}, "start": {strconv.FormatInt(start.UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}}
	if step != "" {
		q.Set("step", step)
	}
	w := httptest.NewRecorder()
	p.handlePatterns(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp patternsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	got := map[int64]int{}
	for _, e := range resp.Data {
		for _, s := range e.Samples {
			ts, _ := numberToInt(s[0])
			n, _ := numberToInt(s[1])
			if n != 0 {
				got[int64(ts)] += n
			}
		}
	}
	return got
}

// A range that ends inside a 10 s sample is answered with that sample whole, as Loki answers it, from the pattern
// state, the exact tier and the counts tier alike: ends one second and one nanosecond before a sample's end, on
// it, just after it and inside it, over 5 minutes (10 s steps) and an hour (14 s steps, and 60 s asked), each range
// starting as far inside a sample as it ends (a sample starting before the range is left out).
//
// conformance: drilldown/patterns-range-end-inside-sample, drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsRangeEndingInsideASampleCountsItWhole(t *testing.T) {
	b := stateNow.Add(-30 * time.Minute) // starts a sample (and an hour's 14 s step grid is epoch-aligned)
	rows := samplesEndRows(b.Add(-70*time.Minute), b.Add(40*time.Second))
	ends := []struct {
		name string
		end  time.Time
	}{
		{"sample end -1s", b.Add(9 * time.Second)},
		{"sample end -1ns", b.Add(10*time.Second - time.Nanosecond)},
		{"on a sample start", b},
		{"sample start +1ns", b.Add(time.Nanosecond)},
		{"mid-sample", b.Add(5 * time.Second)},
		{"sample start +1s", b.Add(time.Second)},
	}
	ranges := []struct {
		name      string
		span      time.Duration
		stepParam string
		step      int64
	}{
		{"5m", 5 * time.Minute, "", 10},
		{"1h", time.Hour, "", 14},
		{"1h step 60s", time.Hour, "60s", 60},
	}
	for _, tier := range []string{"state", "exact", "counts"} {
		for _, rg := range ranges {
			if tier == "counts" && rg.step%10 != 0 {
				// The counts tier buckets lines by their own time: exact for a step of whole samples only
				// (drilldown/patterns-counts-tier-step-buckets).
				continue
			}
			for _, c := range ends {
				t.Run(tier+"/"+rg.name+"/"+c.name, func(t *testing.T) {
					vl := &fakeStateVL{}
					vl.add(rows...)
					now := stateNow
					p := newStateProxy(t, vl, &now, 1<<30)
					switch tier {
					case "exact":
						p.patternState = nil
					case "counts": // a selector over the exact tier's bound and the state's, counted by VictoriaLogs
						p.patternState = nil
						p.patternsExactMaxLines = 10
					}
					start := c.end.Add(-rg.span)
					want := lokiPatternSamples(rows, start, c.end, rg.step)
					got := samplesEndAnswerAt(t, p, start, c.end, rg.stepParam)
					if tier == "counts" && vl.countsCalls == 0 {
						t.Fatal("the counts tier did not answer")
					}
					if fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("samples\n got  %v\n want %v", got, want)
					}
				})
			}
		}
	}
}

// The whole last sample is read once: the same request again reads nothing from VictoriaLogs; a range ending
// earlier inside the sample the state already read reads nothing either and answers the sample whole; a later
// range reads only the lines after it (from the overlap on). A sample still being written (now inside it) is read
// to now and completed by the next refresh.
//
// conformance: drilldown/patterns-range-end-inside-sample, drilldown/patterns-full-range-counts, loki_api_v1_patterns, patterns-loki-mining
func TestPatternsRangeEndingInsideASampleReusesItsRead(t *testing.T) {
	b := stateNow.Add(-30 * time.Minute)
	rows := samplesEndRows(b.Add(-20*time.Minute), b.Add(40*time.Second))
	vl := &fakeStateVL{}
	vl.add(rows...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	calls := func() int {
		vl.mu.Lock()
		defer vl.mu.Unlock()
		return vl.counts + len(vl.lineRanges) + vl.countsCalls
	}
	ask := func(end time.Time) {
		t.Helper()
		start := end.Add(-5 * time.Minute)
		if got, want := samplesEndAnswer(t, p, start, end), lokiPatternSamples(rows, start, end, 10); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("end %s: samples\n got  %v\n want %v", end, got, want)
		}
	}

	ask(b.Add(9 * time.Second))
	if n := len(vl.lineRanges); n != 1 || !vl.lineRanges[0][1].Equal(b.Add(10*time.Second)) {
		t.Fatalf("first read %v, want one read ending with the sample at %s", vl.lineRanges, b.Add(10*time.Second))
	}
	cold := calls()
	ask(b.Add(9 * time.Second))
	if extra := calls() - cold; extra != 0 {
		t.Fatalf("repeat: %d VictoriaLogs calls, want 0", extra)
	}
	ask(b.Add(5 * time.Second))
	if extra := calls() - cold; extra != 0 {
		t.Fatalf("a range ending earlier inside the sample read: %d VictoriaLogs calls, want 0", extra)
	}
	ask(b.Add(25 * time.Second))
	if n := len(vl.lineRanges); n != 2 || !vl.lineRanges[1][0].Equal(b.Add(10*time.Second-patternStateOverlap)) || !vl.lineRanges[1][1].Equal(b.Add(30*time.Second)) {
		t.Fatalf("refresh reads %v, want one more read from the overlap to the sample at %s", vl.lineRanges, b.Add(30*time.Second))
	}

	// now inside the sample being written: read to now, the rest on the next refresh.
	live := &fakeStateVL{}
	written := samplesEndRows(b.Add(-20*time.Minute), b.Add(7*time.Second))
	live.add(written...)
	now = b.Add(7 * time.Second)
	q := newStateProxy(t, live, &now, 1<<30)
	end := b.Add(9 * time.Second) // Drilldown's window a little after now
	samplesEndAnswer(t, q, end.Add(-5*time.Minute), end)
	if n := len(live.lineRanges); n != 1 || !live.lineRanges[0][1].Equal(now) {
		t.Fatalf("read %v, want the lines up to now", live.lineRanges)
	}
	live.add(samplesEndRows(b.Add(7*time.Second), b.Add(10*time.Second))...)
	now = b.Add(12 * time.Second)
	end = b.Add(11 * time.Second)
	got := samplesEndAnswer(t, q, end.Add(-5*time.Minute), end)
	live.mu.Lock()
	all := append([]stateRow(nil), live.rows...)
	live.mu.Unlock()
	if want := lokiPatternSamples(all, end.Add(-5*time.Minute), end, 10); got[b.Unix()] != want[b.Unix()] || want[b.Unix()] != 40 {
		t.Fatalf("the sample finished after the first read: %d lines, want %d (all 40)", got[b.Unix()], want[b.Unix()])
	}
}

// BenchmarkPatternsRangeEndingInsideASample is a 5-minute /patterns request ending inside a sample: cold (the state
// built from 20 minutes of lines, one every 250 ms) and warm (the state read already).
func BenchmarkPatternsRangeEndingInsideASample(b *testing.B) {
	t := &testing.T{}
	at := stateNow.Add(-30 * time.Minute)
	rows := samplesEndRows(at.Add(-20*time.Minute), at.Add(40*time.Second))
	end := at.Add(9 * time.Second)
	q := url.Values{"query": {`{app="api"}`}, "start": {strconv.FormatInt(end.Add(-5*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(end.UnixNano(), 10)}, "step": {"10s"}}
	vl := &fakeStateVL{}
	vl.add(rows...)
	srv := vl.server(t)
	defer srv.Close()
	newProxy := func() *Proxy {
		now := stateNow
		p, err := New(Config{BackendURL: srv.URL, LogLevel: "error", PatternsCounts: true})
		if err != nil {
			b.Fatal(err)
		}
		p.patternsExactMaxLines = 100_000
		p.patternState = newPatternStateStore(1<<30, DefaultPatternsStateStreamIdle)
		p.patternState.now = func() time.Time { return now }
		return p
	}
	ask := func(p *Proxy) {
		w := httptest.NewRecorder()
		p.handlePatterns(w, httptest.NewRequest(http.MethodGet, "/loki/api/v1/patterns?"+q.Encode(), nil))
		if w.Code != http.StatusOK {
			b.Fatalf("status %d", w.Code)
		}
	}
	b.Run("cold", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ask(newProxy())
		}
	})
	b.Run("warm", func(b *testing.B) {
		p := newProxy()
		ask(p)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ask(p)
		}
	})
}

// The counts tier reads a range's samples whole and reuses what it read: the same request again makes no
// VictoriaLogs call, and a later end over two hours reads the windows at the range's ends again but takes the
// settled window between them from the cache, its counts unchanged by the samples' bounds.
//
// conformance: drilldown/patterns-range-end-inside-sample, loki_api_v1_patterns
func TestPatternsCountsTierRangeEndingInsideASampleReusesItsReads(t *testing.T) {
	b := stateNow.Add(-30 * time.Minute)
	rows := samplesEndRows(b.Add(-150*time.Minute), b.Add(40*time.Second))
	vl := &fakeStateVL{}
	vl.add(rows...)
	now := stateNow
	p := newStateProxy(t, vl, &now, 1<<30)
	p.patternState = nil
	p.patternsExactMaxLines = 10
	p.queryRangeHistoryCacheTTL = time.Hour // settled windows are cached, as with the deployed default
	calls := func() int {
		vl.mu.Lock()
		defer vl.mu.Unlock()
		return vl.countsCalls
	}
	ask := func(start, end time.Time) {
		t.Helper()
		if got, want := samplesEndAnswerAt(t, p, start, end, "60s"), lokiPatternSamples(rows, start, end, 60); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("[%s, %s): samples\n got  %v\n want %v", start, end, got, want)
		}
	}
	end := b.Add(9 * time.Second)
	ask(end.Add(-2*time.Hour), end)
	cold := calls()
	if cold == 0 {
		t.Fatal("the counts tier did not answer")
	}
	ask(end.Add(-2*time.Hour), end)
	if extra := calls() - cold; extra != 0 {
		t.Fatalf("repeat: %d VictoriaLogs calls, want 0", extra)
	}
	later := end.Add(20 * time.Second)
	ask(later.Add(-2*time.Hour), later)
	if extra := calls() - cold; extra <= 0 || extra >= cold {
		t.Fatalf("a later end: %d VictoriaLogs calls after %d cold, want fewer (the settled window from the cache)", extra, cold)
	}
}
