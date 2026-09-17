package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/cache"
)

func TestAlignRangeRequestToStepGrid(t *testing.T) {
	const step = 137 * time.Second

	tests := []struct {
		name      string
		query     string
		start     int64
		end       int64
		wantStart int64
		wantEnd   int64
	}{
		{
			name:  "both bounds truncate down to a multiple of the step",
			query: `sum(rate({app="x"}[5m]))`,
			// 1700000000 = 12408759*137 + 17 and 1700003600 = 12408785*137 + 55,
			// so both bounds move DOWN to the grid point below them.
			start: 1700000000, end: 1700003600,
			wantStart: 1700000000 - 17, wantEnd: 1700003600 - 55,
		},
		{
			name:  "already aligned bounds are untouched",
			query: `sum(rate({app="x"}[5m]))`,
			start: 12408759 * 137, end: 12408785 * 137,
			wantStart: 12408759 * 137, wantEnd: 12408785 * 137,
		},
		{
			name:  "a log query has no evaluation grid",
			query: `{app="x"} |~ "boom"`,
			start: 1700000000, end: 1700003600,
			wantStart: 1700000000, wantEnd: 1700003600,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{}
			form.Set("start", strconv.FormatInt(tc.start*int64(time.Second), 10))
			form.Set("end", strconv.FormatInt(tc.end*int64(time.Second), 10))
			form.Set("step", strconv.Itoa(int(step.Seconds())))
			r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil)

			alignRangeRequestToStepGrid(r, tc.query)

			gotStart, _ := parseLokiTimeToUnixNano(r.FormValue("start"))
			gotEnd, _ := parseLokiTimeToUnixNano(r.FormValue("end"))
			if gotStart != tc.wantStart*int64(time.Second) {
				t.Errorf("start = %d, want %d", gotStart/int64(time.Second), tc.wantStart)
			}
			if gotEnd != tc.wantEnd*int64(time.Second) {
				t.Errorf("end = %d, want %d", gotEnd/int64(time.Second), tc.wantEnd)
			}
			// The rewritten bound has to reach the URL too: downstream paths
			// rebuild the backend request from r.URL.
			if q := r.URL.Query().Get("start"); q != r.FormValue("start") {
				t.Errorf("URL start = %q, form start = %q", q, r.FormValue("start"))
			}
		})
	}
}

func TestAlignRangeRequestToStepGrid_NoStepIsANoOp(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?start=1700000000000000000&end=1700003600000000000", nil)
	alignRangeRequestToStepGrid(r, `sum(rate({app="x"}[5m]))`)
	if got := r.FormValue("start"); got != "1700000000000000000" {
		t.Errorf("start = %q, want it untouched without a step", got)
	}
}

// The wiring half: with align-queries-with-step on, the bounds the backend sees
// are Loki's grid; with it off they are the client's own, which is Loki's
// binary default.
func TestQueryRangeAlignsBoundsBeforeQueryingTheBackend(t *testing.T) {
	const (
		step     = 137
		start    = int64(1700000000) // 12408759*137 + 17
		lookback = 300               // the [5m] range vector of the query below
	)

	for _, tc := range []struct {
		name      string
		align     bool
		wantStart int64
	}{
		{"aligned", true, start - 17 - lookback},
		{"not aligned", false, start - lookback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen url.Values
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				seen = r.Form
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
			}))
			defer backend.Close()

			p, err := New(Config{
				BackendURL:           backend.URL,
				Cache:                cache.New(60*time.Second, 100),
				LogLevel:             "error",
				AlignQueriesWithStep: tc.align,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Shutdown(t.Context()) })

			form := url.Values{}
			form.Set("query", `sum(count_over_time({app="x"}[5m]))`)
			form.Set("start", strconv.FormatInt(start*int64(time.Second), 10))
			form.Set("end", strconv.FormatInt((start+3600)*int64(time.Second), 10))
			form.Set("step", strconv.Itoa(step))

			rec := httptest.NewRecorder()
			p.handleQueryRange(rec, httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+form.Encode(), nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
			}

			got := seen.Get("start")
			if got == "" {
				t.Fatal("backend was never asked for a range")
			}
			ts, ok := backendStartUnix(got)
			if !ok {
				t.Fatalf("backend start %q is neither RFC3339 nor a Loki timestamp", got)
			}
			// The backend read starts one range-vector window before the first
			// evaluation point, so the two cases differ by exactly the 17s the
			// alignment removes.
			if ts != tc.wantStart {
				t.Errorf("backend start = %s (%d), want %d", got, ts, tc.wantStart)
			}
		})
	}
}

func backendStartUnix(raw string) (int64, bool) {
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts.Unix(), true
	}
	if ns, ok := parseLokiTimeToUnixNano(raw); ok {
		return ns / int64(time.Second), true
	}
	return 0, false
}

// Grafana sometimes sends the step as a raw nanosecond integer. Aligning only
// the duration spellings would leave those panels on the unaligned grid while
// their neighbours moved, which is the disagreement this flag exists to stop.
func TestAlignRangeRequestAcceptsNanosecondStep(t *testing.T) {
	for _, step := range []string{"137s", "137", "137000000000"} {
		r := httptest.NewRequest(http.MethodGet,
			"/loki/api/v1/query_range?query="+url.QueryEscape(`sum(rate({app="a"}[5m]))`)+
				"&start=1700000000&end=1700003600&step="+step, nil)
		alignRangeRequestToStepGrid(r, `sum(rate({app="a"}[5m]))`)
		if got := r.FormValue("start"); got != "1699999983000000000" {
			t.Errorf("step=%s: start = %s, want 1699999983000000000", step, got)
		}
		if got := r.FormValue("end"); got != "1700003545000000000" {
			t.Errorf("step=%s: end = %s, want 1700003545000000000", step, got)
		}
	}
}
