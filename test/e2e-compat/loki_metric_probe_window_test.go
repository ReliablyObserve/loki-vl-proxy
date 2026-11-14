//go:build e2e

package e2e_compat

import (
	"net/url"
	"strconv"
	"testing"
	"time"
)

// lokiProbeCovers reports whether Loki's answer to the probe counts a line at
// ts: Loki (align_queries_with_step) floors start and end to the step and a
// count_over_time(...[5m]) point at P counts the lines in (P-5m, P].
func lokiProbeCovers(t *testing.T, probe url.Values, ts time.Time) bool {
	t.Helper()
	parse := func(k string) int64 {
		v, err := strconv.ParseInt(probe.Get(k), 10, 64)
		if err != nil {
			t.Fatalf("probe %s=%q: %v", k, probe.Get(k), err)
		}
		return v
	}
	step := parse("step")
	start := parse("start") / int64(time.Second) / step * step
	end := parse("end") / int64(time.Second) / step * step
	line := ts.UnixNano()
	for p := start; p <= end; p += step {
		if line > (p-300)*int64(time.Second) && line <= p*int64(time.Second) {
			return true
		}
	}
	return false
}

// The probe of Loki's range metrics must count lines written just before it
// starts at any second of the minute and on every attempt. A probe ending at a
// raw now (Loki floors it to the step) missed lines written in the same minute
// whenever now was past :30, so the 90 s wait stayed empty and timed out.
//
// conformance: semantics/query-range-step-alignment
func TestCompat_LokiMetricProbeWindowCoversLines(t *testing.T) {
	minute := time.Date(2026, 11, 14, 19, 30, 0, 0, time.UTC)
	for _, sec := range []int{0, 1, 29, 30, 31, 45, 59} {
		for _, frac := range []time.Duration{0, 500 * time.Millisecond, 999 * time.Millisecond} {
			now := minute.Add(time.Duration(sec)*time.Second + frac)
			// pushStream(t, time.Now().Add(-30*time.Second), ...) writes one line a second.
			lines := []time.Time{now.Add(-30 * time.Second), now.Add(-29 * time.Second), now.Add(-28 * time.Second), now.Add(-time.Millisecond), now.Add(-14 * time.Minute)}
			for _, attempt := range []int{0, 1, 2, 59, 90, 179, 180, 400} {
				probe := lokiMetricProbeParams("q", now.Add(-10*time.Minute), now, attempt)
				for _, ts := range lines {
					if !lokiProbeCovers(t, probe, ts) {
						t.Errorf("now %s attempt %d: probe %v does not count the line at %s", now.Format(time.StampMilli), attempt, probe, ts.Format(time.StampMilli))
					}
				}
			}
		}
	}

	// The probe before the fix: end at the raw now, step 60.
	now := minute.Add(31 * time.Second)
	old := url.Values{"start": {strconv.FormatInt(now.Add(-10*time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(now.UnixNano(), 10)}, "step": {"60"}}
	if lokiProbeCovers(t, old, now.Add(-30*time.Second)) {
		t.Fatal("the old probe was expected to miss a line written 30 s before :31")
	}
}

// The reserved-name fixture (reservedCaseFixture) writes its three streams one
// line a second from base+1s to base+7s and waits on [base-1m, base+1m]. With
// a fixed step of 59 and base 1794684296 (CI run 43631666675) Loki's last point
// was 1794684302, before the kind=s line at base+7s, so the wait could never
// see three series.
//
// conformance: semantics/reserved-field-name-case-outbound, semantics/query-range-step-alignment
func TestCompat_ReservedCaseProbeWindowCoversFixture(t *testing.T) {
	failed := time.Unix(1794684296, 0)
	old := url.Values{"start": {strconv.FormatInt(failed.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(failed.Add(time.Minute).UnixNano(), 10)}, "step": {"59"}}
	if lokiProbeCovers(t, old, failed.Add(7*time.Second)) {
		t.Fatal("the old probe was expected to miss the kind=s line of the failed run")
	}
	for offset := 0; offset < 240; offset++ {
		base := failed.Add(time.Duration(offset) * time.Second)
		for attempt := 0; attempt <= 200; attempt++ {
			probe := lokiMetricProbeParams("q", base.Add(-time.Minute), base.Add(time.Minute), attempt)
			for n := 1; n <= 7; n++ {
				if ts := base.Add(time.Duration(n) * time.Second); !lokiProbeCovers(t, probe, ts) {
					t.Fatalf("base %d attempt %d: probe %v does not count the line at base+%ds", base.Unix(), attempt, probe, n)
				}
			}
		}
	}
}
