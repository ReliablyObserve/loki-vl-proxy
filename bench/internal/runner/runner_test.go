package runner

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

func mustNs(t *testing.T, v string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", v, err)
	}
	return n
}

var (
	dataStart = time.Date(2026, 9, 11, 0, 0, 30, 0, time.UTC)
	dataEnd   = time.Date(2026, 9, 14, 19, 47, 0, 0, time.UTC)
)

func rangeURL(window, step time.Duration) string {
	v := url.Values{
		"query": {`sum(rate({app="a"} |~ "[0-9]" [1h]))`},
		"start": {strconv.FormatInt(dataEnd.Add(-window).UnixNano(), 10)},
		"end":   {strconv.FormatInt(dataEnd.UnixNano(), 10)},
	}
	if step > 0 {
		v.Set("step", strconv.FormatInt(int64(step/time.Second), 10))
	}
	return "http://x/loki/api/v1/query_range?" + v.Encode()
}

func TestJitterNeverPassesReferenceOrDataStart(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	cases := []struct {
		name   string
		window time.Duration
		step   time.Duration
		jitter time.Duration
	}{
		{"short log window", 5 * time.Minute, 0, 2 * time.Hour},
		{"1h metric", time.Hour, time.Minute, 2 * time.Hour},
		{"window starting before data", 95 * time.Hour, time.Hour, 24 * time.Hour},
		{"window near data start", 83 * time.Hour, 5 * time.Minute, 48 * time.Hour},
		{"lookback limits room", 90 * time.Hour, time.Minute, 2 * time.Hour},
		{"lookback reaches before data start", 90*time.Hour + 47*time.Minute, time.Minute, 2 * time.Hour},
	}
	for _, tc := range cases {
		raw := rangeURL(tc.window, tc.step)
		// The query reads 1h before start; the regex class "[0-9]" is not a range.
		origStart := mustNs(t, mustQuery(t, raw).Get("start")) - int64(time.Hour)
		for i := 0; i < 2000; i++ {
			q := mustQuery(t, applyJitter(raw, tc.jitter, dataStart, rng))
			start, end := mustNs(t, q.Get("start")), mustNs(t, q.Get("end"))
			if end > dataEnd.UnixNano() {
				t.Fatalf("%s: end %d past reference time", tc.name, end)
			}
			if end-start != int64(tc.window) {
				t.Fatalf("%s: window size changed", tc.name)
			}
			shift := dataEnd.UnixNano() - end
			if shift < 0 || shift >= int64(tc.jitter) {
				t.Fatalf("%s: shift %s outside [0, jitter)", tc.name, time.Duration(shift))
			}
			if tc.step > 0 && shift%int64(tc.step) != 0 {
				t.Fatalf("%s: shift %s is not a whole number of steps", tc.name, time.Duration(shift))
			}
			// A window that already starts before the data cannot move further back;
			// otherwise its start stays inside the data.
			if origStart >= dataStart.UnixNano() && start-int64(time.Hour) < dataStart.UnixNano() {
				t.Fatalf("%s: evaluated start %s shifted before data start", tc.name, time.Unix(0, start-int64(time.Hour)).UTC())
			}
			if origStart < dataStart.UnixNano() && shift != 0 {
				t.Fatalf("%s: window starting before the data was shifted further back", tc.name)
			}
		}
	}
}

func TestJitterWithoutDataStartIsBackwardOnly(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	raw := "http://x/loki/api/v1/query?" + url.Values{"query": {"q"}, "time": {strconv.FormatInt(dataEnd.UnixNano(), 10)}}.Encode()
	for i := 0; i < 500; i++ {
		ts := mustNs(t, mustQuery(t, applyJitter(raw, time.Hour, time.Time{}, rng)).Get("time"))
		if ts > dataEnd.UnixNano() || dataEnd.UnixNano()-ts >= int64(time.Hour) {
			t.Fatalf("instant time %d outside [ref-jitter, ref]", ts)
		}
	}
}

func TestJitterShiftQuantizesToStep(t *testing.T) {
	params := url.Values{"start": {"0"}, "end": {"3600000000000"}, "step": {"60"}}
	if got := jitterShift(params, 150*time.Second, time.Time{}); got != 2*time.Minute {
		t.Fatalf("shift = %s, want 2m", got)
	}
	if got := jitterShift(params, 59*time.Second, time.Time{}); got != 0 {
		t.Fatalf("shift below one step = %s, want 0", got)
	}
}

func mustQuery(t *testing.T, raw string) url.Values {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query()
}

func TestUniqueShiftMovesInWholeStepsInsideData(t *testing.T) {
	window, step := 15*time.Minute, time.Minute
	q := mustQuery(t, rangeURL(window, step))
	seen := map[time.Duration]bool{}
	const workers = 10
	for seq := 0; seq < 50; seq++ {
		for w := 0; w < workers; w++ {
			shift := uniqueShift(q, w, workers, seq, dataStart)
			if shift%step != 0 {
				t.Fatalf("worker %d seq %d: shift %s is not whole steps", w, seq, shift)
			}
			if seen[shift] {
				t.Fatalf("worker %d seq %d: shift %s repeated while windows remain", w, seq, shift)
			}
			seen[shift] = true
		}
	}

	// Instant and log queries without a step move in whole seconds.
	instant := url.Values{"query": {`count_over_time({app="a"}[5m])`}, "time": {strconv.FormatInt(dataEnd.UnixNano(), 10)}}
	if got := uniqueShift(instant, 3, 4, 2, dataStart); got != 11*time.Second {
		t.Fatalf("instant shift = %s, want 11s", got)
	}

	// A long window with little room wraps and never reads before the data.
	long := mustQuery(t, rangeURL(90*time.Hour, time.Hour))
	earliest, _ := workload.EarliestEvaluated(long)
	for seq := 0; seq < 200; seq++ {
		shift := uniqueShift(long, seq%7, 7, seq, dataStart)
		if shift%time.Hour != 0 || earliest-int64(shift) < dataStart.UnixNano() {
			t.Fatalf("seq %d: shift %s leaves the data or the step grid", seq, shift)
		}
	}
}

func TestRunCountsErrorsAndDegradedResponses(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) % 5 {
		case 1:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"status":"error"}`))
		case 2:
			w.Header().Set("X-Proxy-Stale-Response", "true")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`))
		case 3:
			_, _ = w.Write([]byte(`{"status":"success","warnings":["partial"],"data":{"resultType":"matrix","result":[]}}`))
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		}
	}))
	defer srv.Close()
	res := Run(context.Background(), Config{
		TargetURL:   srv.URL,
		Concurrency: 2,
		Duration:    150 * time.Millisecond,
		Queries:     []workload.Query{{Name: "q", Path: "/loki/api/v1/query_range", Params: url.Values{"query": {"x"}}}},
	})
	s := res.Overall
	if s.Count < 5 || s.Errors == 0 || s.Status5xx == 0 || s.Degraded == 0 {
		t.Fatalf("errors and degraded answers not counted: %+v", s)
	}
	if s.Degraded < s.Count/5 || s.Errors+s.Degraded > s.Count {
		t.Fatalf("unexpected split: count=%d errors=%d degraded=%d", s.Count, s.Errors, s.Degraded)
	}
	if q := res.ByQuery["q"]; q == nil || q.Degraded != s.Degraded {
		t.Fatalf("per-query degraded count missing: %+v", q)
	}
}

func TestDistinctWindows(t *testing.T) {
	long := mustQuery(t, rangeURL(90*time.Hour, time.Hour))
	n, bounded := DistinctWindows(long, dataStart)
	// The window reads 91h back (1h lookback) from a data end 91h47m after the
	// data start: the unshifted window and none further back fit.
	if !bounded || n != 1 {
		t.Fatalf("distinct windows %d bounded=%v", n, bounded)
	}
	for seq := 0; seq < 5; seq++ {
		if got := uniqueShift(long, seq, 4, seq, dataStart); got != 0 {
			t.Fatalf("a query without room must not shift, got %s", got)
		}
	}
	short := mustQuery(t, rangeURL(15*time.Minute, time.Minute))
	if n, _ := DistinctWindows(short, dataStart); n < 5000 {
		t.Fatalf("short window distinct windows %d", n)
	}
	before := mustQuery(t, rangeURL(95*time.Hour, time.Hour))
	if n, _ := DistinctWindows(before, dataStart); n != 1 || uniqueShift(before, 1, 2, 3, dataStart) != 0 {
		t.Fatalf("a window already before the data must not shift: %d", n)
	}
	if _, bounded := DistinctWindows(short, time.Time{}); bounded {
		t.Fatal("no data start means unbounded")
	}
}
