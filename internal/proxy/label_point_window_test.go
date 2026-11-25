package proxy

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// conformance: semantics/labels-point-window-is-the-index-day
func TestWidenLabelPointWindow(t *testing.T) {
	now := time.Date(2026, 11, 24, 21, 30, 0, 0, time.UTC)
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
	recentWindow := func() (string, string) { return ns(now.Add(-labelIngesterWindow)), ns(now) }
	dayWindow := func(anchor time.Time) (string, string) {
		day := anchor.UTC().Truncate(24 * time.Hour)
		return ns(day), ns(day.Add(24 * time.Hour))
	}
	row := now.Add(-time.Hour)
	old := now.Add(-30 * time.Hour)
	for _, tc := range []struct {
		name       string
		start, end string
		ok, recent bool
		anchor     time.Time // the instant whose UTC day is the answer, for an older point
	}{
		{"start equals end, the clicked row", ns(row), ns(row), true, true, time.Time{}},
		{"one nanosecond is a window", ns(row), ns(row.Add(time.Nanosecond)), false, false, time.Time{}},
		{"one millisecond is a window", ns(row), ns(row.Add(time.Millisecond)), false, false, time.Time{}},
		{"59 seconds is a window", ns(row), ns(row.Add(59 * time.Second)), false, false, time.Time{}},
		{"seconds spelling", fmt.Sprint(row.Unix()), fmt.Sprint(row.Unix()), true, true, time.Time{}},
		{"RFC 3339 spelling", row.Format(time.RFC3339Nano), row.Format(time.RFC3339Nano), true, true, time.Time{}},
		{"exactly three hours back is recent", ns(now.Add(-3 * time.Hour)), ns(now.Add(-3 * time.Hour)), true, true, time.Time{}},
		{"after now is read as now", ns(now.Add(time.Hour)), ns(now.Add(time.Hour)), true, true, time.Time{}},
		{"just past three hours is its day", ns(now.Add(-3*time.Hour - time.Second)), ns(now.Add(-3*time.Hour - time.Second)), true, false, now.Add(-3*time.Hour - time.Second)},
		{"an older day", ns(old), ns(old), true, false, old},
		{"exactly one minute is a window", ns(row), ns(row.Add(time.Minute)), false, false, time.Time{}},
		{"an hour is a window", ns(now.Add(-time.Hour)), ns(now), false, false, time.Time{}},
		{"no start", "", ns(row), false, false, time.Time{}},
		{"no end", ns(row), "", false, false, time.Time{}},
		{"unparseable", "yesterday", "today", false, false, time.Time{}},
		{"reversed bounds", ns(row.Add(time.Second)), ns(row), false, false, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end, ok := widenLabelPointWindow(tc.start, tc.end, now)
			if ok != tc.ok {
				t.Fatalf("widened = %v, want %v (%q %q)", ok, tc.ok, start, end)
			}
			if !ok {
				return
			}
			wantStart, wantEnd := recentWindow()
			if !tc.recent {
				wantStart, wantEnd = dayWindow(tc.anchor)
			}
			if start != wantStart || end != wantEnd {
				t.Fatalf("window [%s %s], want [%s %s]", start, end, wantStart, wantEnd)
			}
		})
	}
	// Every point of one older day shares one window, so one cached answer.
	a1, b1, _ := widenLabelPointWindow(ns(old), ns(old), now)
	a2, b2, _ := widenLabelPointWindow(ns(old.Add(40*time.Minute)), ns(old.Add(40*time.Minute)), now)
	if a1 != a2 || b1 != b2 {
		t.Fatalf("two points of one older day got different windows: [%s %s] [%s %s]", a1, b1, a2, b2)
	}
}

func labelsPoint(at time.Time) string {
	v := url.Values{"start": {strconv.FormatInt(at.UnixNano(), 10)}, "end": {strconv.FormatInt(at.UnixNano(), 10)}}
	return "/loki/api/v1/labels?" + v.Encode()
}

// A recent point lists the streams written in the last hours, as Loki's ingesters do: a label on a stream written
// well before the clicked row is in the answer, so the row's context query carries its structured metadata.
//
// conformance: semantics/labels-point-window-is-the-index-day
func TestLabelsRecentPointWindowListsTheIngesterWindowLikeLoki(t *testing.T) {
	// The label sits 2.5 hours back, on a stream the clicked row (one hour back) was not written on.
	fake := &fullRangeVL{earlyBefore: time.Now().Add(-150 * time.Minute).UnixNano()}
	_, mux := newBehaviorProxy(t, fake.server(t).URL, 0)
	row := time.Now().Add(-time.Hour)
	got := serveLokiStrings(t, mux, labelsPoint(row))
	for _, want := range []string{"app", "early_label"} {
		if !contains(got, want) {
			t.Fatalf("a recent point window's labels miss %q: %v (backend calls %v)", want, got, fake.calls)
		}
	}
	if again := serveLokiStrings(t, mux, labelsPoint(row.Add(5*time.Minute))); strings.Join(again, ",") != strings.Join(got, ",") {
		t.Fatalf("a second recent point answered %v, want %v", again, got)
	}
	// A window of 30 minutes is still the window asked for (a fresh proxy: the inventory caches above hold hours).
	fresh := &fullRangeVL{earlyBefore: perfBaseTimeNs - int64(3*time.Hour)} // the helper's windows end at perfBaseTimeNs
	_, freshMux := newBehaviorProxy(t, fresh.server(t).URL, 0)
	if narrow := serveLokiStrings(t, freshMux, fullRangeWindowPath("/loki/api/v1/labels", 30*time.Minute)); contains(narrow, "early_label") {
		t.Fatalf("a 30 minute window was widened: %v", narrow)
	}
}

// An older point lists its index period (a day) and the next point of that day reads the cache.
//
// conformance: semantics/labels-point-window-is-the-index-day
func TestLabelsOlderPointWindowListsItsDayAndIsCached(t *testing.T) {
	old := time.Now().Add(-30 * time.Hour)
	day := old.UTC().Truncate(24 * time.Hour)
	fake := &fullRangeVL{earlyBefore: day.Add(time.Hour).UnixNano()} // a stream written in the day's first hour
	_, mux := newBehaviorProxy(t, fake.server(t).URL, 0)
	got := serveLokiStrings(t, mux, labelsPoint(old))
	if !contains(got, "early_label") {
		t.Fatalf("an older point's labels miss the day's stream: %v (backend calls %v)", got, fake.calls)
	}
	calls := len(fake.calls)
	if again := serveLokiStrings(t, mux, labelsPoint(old.Add(25*time.Minute))); strings.Join(again, ",") != strings.Join(got, ",") {
		t.Fatalf("a second point of the same day answered %v, want %v", again, got)
	}
	if len(fake.calls) != calls {
		t.Fatalf("a second point of the same day read the backend again: %v", fake.calls[calls:])
	}
}

func TestWithWidenedLabelPointRewritesOnlyTheBounds(t *testing.T) {
	now := time.Now()
	at := strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10)
	wantStart, wantEnd, _ := widenLabelPointWindow(at, at, now)
	for _, tc := range []struct{ name, query string }{
		{"bounds only", "start=" + at + "&end=" + at},
		{"with a selector", "query=%7Bapp%3D%22a%22%7D&start=" + at + "&end=" + at + "&since=1h"},
		{"an encoded spelling of a bound", "st%61rt=" + at + "&end=" + at},
		{"a repeated bound", "start=" + at + "&start=1&end=" + at},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/labels?"+tc.query, nil)
			r.Header.Set("X-Scope-OrgID", "tenant")
			got := withWidenedLabelPoint(r, now)
			if got == r {
				t.Fatal("a point was not widened")
			}
			if got.FormValue("start") != wantStart || got.FormValue("end") != wantEnd {
				t.Fatalf("window [%s %s], want [%s %s] (%s)", got.FormValue("start"), got.FormValue("end"), wantStart, wantEnd, got.URL.RawQuery)
			}
			if got.Header.Get("X-Scope-OrgID") != "tenant" || got.URL.Path != r.URL.Path {
				t.Fatal("the copy lost the headers or the path")
			}
			if strings.Contains(tc.name, "selector") && got.FormValue("query") != `{app="a"}` || strings.Contains(tc.name, "selector") && got.FormValue("since") != "1h" {
				t.Fatalf("another parameter changed: %s", got.URL.RawQuery)
			}
			if r.FormValue("start") != at && tc.name == "bounds only" {
				t.Fatal("the original request was changed")
			}
		})
	}
	post := httptest.NewRequest("POST", "/loki/api/v1/labels?start="+at+"&end="+at, nil)
	if withWidenedLabelPoint(post, now) != post {
		t.Fatal("a POST was widened")
	}
}

// A request that is not a point costs nothing here: the handler parses the form anyway.
func TestWithWidenedLabelPointAllocatesNothingForAnOrdinaryWindow(t *testing.T) {
	now := time.Now()
	for _, target := range []string{"/loki/api/v1/labels", "/loki/api/v1/labels?start=" + strconv.FormatInt(now.Add(-time.Hour).UnixNano(), 10) + "&end=" + strconv.FormatInt(now.UnixNano(), 10)} {
		r := httptest.NewRequest("GET", target, nil)
		_ = r.FormValue("start") // the handler's own first read
		if allocs := testing.AllocsPerRun(100, func() { _ = withWidenedLabelPoint(r, now) }); allocs != 0 {
			t.Fatalf("%s: %v allocations, want 0", target, allocs)
		}
	}
}
