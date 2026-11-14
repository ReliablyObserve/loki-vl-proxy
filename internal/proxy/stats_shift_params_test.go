package proxy

import (
	"context"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
)

// The tumbling fetch's form is built once (tumblingStatsRangeParams), the shifted request copies only its form
// (shiftedStatsRequest), and the phase filter is written into the query in one allocation (withWindowPhaseFilter):
// each is held to the text the proxy built before, so VictoriaLogs sees the same request.

// tumblingParamsBefore is the form the tumbling fetch built before: the request's own form, overwritten by the
// sliding grid, with the first window read from one nanosecond after the start.
func tumblingParamsBefore(query string, startNs, endNs int64, window time.Duration) url.Values {
	params := buildStatsQueryRangeParams(query, strconv.FormatInt(startNs, 10), strconv.FormatInt(endNs, 10), "60")
	setSlidingStatsRangeParams(params, time.Unix(0, startNs), time.Unix(0, endNs), window)
	params.Set("start", time.Unix(0, startNs+1).UTC().Format(time.RFC3339Nano))
	return params
}

// conformance: window-bounds-and-step-alignment
func TestTumblingStatsRangeParamsAreTheFormBefore(t *testing.T) {
	for _, c := range []struct {
		startNs, endNs int64
		window         time.Duration
	}{
		{1_800_000_000_000_000_000, 1_800_003_600_000_000_000, 5 * time.Minute},
		{1_800_000_000_000_000_000, 1_800_000_000_000_000_000, time.Minute},
		{1_800_000_017_123_456_789, 1_800_003_600_987_654_321, 90 * time.Second},
		{1_800_000_000_000_000_001, 1_800_000_600_000_000_000, 1500 * time.Millisecond},
		{1_767_225_000_000_000_000, 1_767_225_600_000_000_000, 7 * time.Nanosecond},
		{-1_000_000_000, 1_000_000_000, time.Second},
	} {
		got := tumblingStatsRangeParams("app:=x | stats count()", c.startNs, c.endNs, c.window)
		want := tumblingParamsBefore("app:=x | stats count()", c.startNs, c.endNs, c.window)
		if !reflect.DeepEqual(got, want) || got.Encode() != want.Encode() {
			t.Errorf("%+v:\n got %s\nwant %s", c, got.Encode(), want.Encode())
		}
	}
}

// FuzzTumblingStatsRangeParams: any bounds and window give the form built before.
//
// conformance: window-bounds-and-step-alignment
func FuzzTumblingStatsRangeParams(f *testing.F) {
	f.Add(int64(1_800_000_000_000_000_000), int64(3_600_000_000_000), int64(300_000_000_000))
	f.Add(int64(1_800_000_017_123_456_789), int64(1), int64(1_500_000_000))
	f.Add(int64(0), int64(0), int64(7))
	f.Fuzz(func(t *testing.T, startNs, span, window int64) {
		if window <= 0 || span < 0 || startNs < -1<<60 || startNs > 1<<61 || span > 1<<60 {
			return
		}
		got := tumblingStatsRangeParams("q", startNs, startNs+span, time.Duration(window))
		want := tumblingParamsBefore("q", startNs, startNs+span, time.Duration(window))
		if got.Encode() != want.Encode() {
			t.Fatalf("got %s, want %s", got.Encode(), want.Encode())
		}
	})
}

// conformance: window-bounds-and-step-alignment
func TestShiftedStatsRequestCopiesTheFormOnly(t *testing.T) {
	r := httptest.NewRequest("GET", "/loki/api/v1/query_range?query=q&start=1800000000&end=1800000650&step=300", nil)
	_ = r.ParseForm()
	before := r.Form.Encode()
	type key struct{}
	ctx := context.WithValue(r.Context(), key{}, 1)
	const startNs, windowNs = int64(1_800_000_000_000_000_000), int64(300_000_000_000)
	shifted := shiftedStatsRequest(r, ctx, startNs, 1_800_000_650_000_000_000, windowNs)
	if r.Form.Encode() != before {
		t.Fatalf("the request's own form changed: %s", r.Form.Encode())
	}
	if got := shifted.FormValue("start"); got != strconv.FormatInt(startNs-windowNs, 10) {
		t.Fatalf("start %s", got)
	}
	if got := shifted.FormValue("end"); got != strconv.FormatInt(1_800_000_600_000_000_000, 10) {
		t.Fatalf("end %s, want the last evaluation time", got)
	}
	if shifted.FormValue("query") != "q" || shifted.FormValue("step") != "300" || shifted.Context().Value(key{}) != 1 {
		t.Fatalf("form or context lost: %v", shifted.Form)
	}
	// An end at or before the start keeps the request's own.
	if got := shiftedStatsRequest(r, ctx, startNs, startNs, windowNs).FormValue("end"); got != "1800000650" {
		t.Fatalf("end %s", got)
	}
}

// conformance: window-bounds-and-step-alignment
func TestWithWindowPhaseFilterIsTheTextBefore(t *testing.T) {
	start := time.Unix(1_800_000_017, 123)
	for _, q := range []string{
		`app:="x" | stats count()`,
		`app:="x"`,
		`app:="x" `,
		`app:="x" | unpack_json | stats by (app) stddev(__lvp_v)`,
		strings.Repeat(`a:="b" `, 400) + `| stats count()`,
	} {
		for _, sw := range [][2]time.Duration{{5 * time.Minute, time.Minute}, {15 * time.Minute, 5 * time.Minute}, {time.Hour, 59 * time.Minute}} {
			filter := windowPhaseFilter(start, sw[0], sw[1])
			at := strings.Index(q, " | ")
			want := strings.TrimRight(q, " ") + filter
			if at >= 0 && strings.TrimSpace(q[at:]) != "" {
				want = strings.TrimRight(q[:at], " ") + filter + " " + strings.TrimLeft(q[at:], " ")
			}
			if got := withWindowPhaseFilter(context.Background(), q, start, sw[0], sw[1]); got != want {
				t.Fatalf("%q:\n got %q\nwant %q", q, got, want)
			}
			if len(filter) > windowPhaseFilterMaxLen {
				t.Fatalf("filter of %d bytes, bound %d", len(filter), windowPhaseFilterMaxLen)
			}
		}
	}
}

// queryUsesParserStages reads unpack_json and extract before the unwrap gate is split off (no gate holds them):
// the answer is the one of the gate split first, for every gate.
//
// conformance: window-bounds-and-step-alignment
func TestQueryUsesParserStagesAroundTheGate(t *testing.T) {
	for _, conv := range []string{"", "duration", "bytes", "duration_seconds"} {
		gate := translator.UnwrapGateFor("n", conv)
		for base, want := range map[string]bool{
			`app:="x"`:                                       false,
			`app:="x" | unpack_json`:                         true,
			`app:="x" | unpack_logfmt`:                       false,
			`app:="x" | extract "<a> <b>"`:                   true,
			`app:="x" | extract_regexp "(?P<a>[0-9]+)"`:      true,
			`app:="x" | unpack_json | extract_regexp "(.+)"`: true,
		} {
			if got := queryUsesParserStages(base + gate); got != want {
				t.Errorf("%q with the %q gate: %v, want %v", base, conv, got, want)
			}
		}
	}
	if !queryUsesParserStages(`app:="x" | unpack_json`) || queryUsesParserStages(`app:="x" | stats count()`) {
		t.Fatal("without a gate")
	}
}
