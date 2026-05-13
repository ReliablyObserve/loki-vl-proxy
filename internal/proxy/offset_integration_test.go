package proxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestQueryRange_OffsetShiftsTimeWindow(t *testing.T) {
	// rate({app="nginx"}[60s] offset 1h) with start=T end=T+30m step=60
	// range==step (tumbling window) → routes to stats_query_range.
	// The proxy must query VL with start=T-1h end=T+30m-1h (both shifted back 1h).
	base := time.Unix(1700000000, 0).UTC()
	offset := time.Hour

	var gotStart, gotEnd string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query_range":
			gotStart = r.FormValue("start")
			gotEnd = r.FormValue("end")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		default:
			if r.URL.Path != "/metrics" {
				t.Logf("unhandled: %s", r.URL.Path)
			}
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	// [60s] offset 1h: range==step (tumbling) → stats_query_range path, not manual log-fetch.
	params.Set("query", `rate({app="nginx"}[60s] offset 1h)`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	wantStart := strconv.FormatInt(base.Add(-offset).Unix(), 10)
	wantEnd := strconv.FormatInt(base.Add(30*time.Minute).Add(-offset).Unix(), 10)

	// The proxy applies a start-shift of -1×step for tumbling rate windows
	// (statsRateRangeEqualsStepShift) to correct VL's first-bucket drift.
	// Account for that: allow start to be [wantStart-step, wantStart].
	gotStartNs, _ := strconv.ParseInt(gotStart, 10, 64)
	wantStartNs, _ := strconv.ParseInt(wantStart, 10, 64)
	if gotStartNs < wantStartNs-60 || gotStartNs > wantStartNs {
		t.Errorf("start: got %s want in [%d, %d]", gotStart, wantStartNs-60, wantStartNs)
	}
	// VL extends end by one step; allow up to +step tolerance.
	gotEndNs, _ := strconv.ParseInt(gotEnd, 10, 64)
	wantEndNs, _ := strconv.ParseInt(wantEnd, 10, 64)
	if gotEndNs < wantEndNs || gotEndNs > wantEndNs+60 {
		t.Errorf("end: got %s want ~%s", gotEnd, wantEnd)
	}
}

func TestQueryRange_NoOffsetUnchanged(t *testing.T) {
	// Verify that queries without offset leave start/end untouched.
	// Use [60s] with step=60 so range==step (tumbling) → stats_query_range path.
	base := time.Unix(1700000000, 0).UTC()

	var gotStart string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query_range" {
			_ = r.ParseForm()
			gotStart = r.FormValue("start")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		} else {
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `rate({app="nginx"}[60s])`)
	params.Set("start", strconv.FormatInt(base.Unix(), 10))
	params.Set("end", strconv.FormatInt(base.Add(30*time.Minute).Unix(), 10))
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	// The proxy applies a start-shift of -1×step for tumbling rate windows
	// (statsRateRangeEqualsStepShift). Allow [wantStart-step, wantStart].
	wantStart := strconv.FormatInt(base.Unix(), 10)
	gotStartNs, _ := strconv.ParseInt(gotStart, 10, 64)
	wantStartNs, _ := strconv.ParseInt(wantStart, 10, 64)
	if gotStartNs < wantStartNs-60 || gotStartNs > wantStartNs {
		t.Errorf("start should be ~unmodified: got %s want in [%d, %d]", gotStart, wantStartNs-60, wantStartNs)
	}
}

func TestQueryRange_MultipleOffsetReturns400(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", fmt.Sprintf(`rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 2h)`))
	params.Set("start", "1700000000")
	params.Set("end", "1700001800")
	params.Set("step", "60")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query_range?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQueryRange(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for multiple offsets, got %d: %s", rec.Code, rec.Body.String())
	}
}
