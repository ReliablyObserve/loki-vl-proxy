package proxy

import (
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
	params.Set("query", `rate({app="a"}[5m] offset 1h) + rate({app="b"}[5m] offset 2h)`)
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

func TestQuery_OffsetShiftsTime(t *testing.T) {
	// Instant query: sum(count_over_time({app="nginx"}[5m] offset 1h))
	// eval time T → VL must receive time=T-1h.
	base := time.Unix(1700000000, 0).UTC()
	offset := time.Hour

	var gotTime string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		switch r.URL.Path {
		case "/select/logsql/stats_query":
			gotTime = r.FormValue("time")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42"]}]}}`))
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
	params.Set("query", `sum(count_over_time({app="nginx"}[5m] offset 1h))`)
	params.Set("time", strconv.FormatInt(base.UnixNano(), 10))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if gotTime == "" {
		t.Fatal("VL backend never received a stats_query request")
	}

	// nanosToVLTimestamp converts to Unix seconds; allow ±1s tolerance.
	gotTimeSec, err := strconv.ParseInt(gotTime, 10, 64)
	if err != nil {
		t.Fatalf("could not parse captured time %q: %v", gotTime, err)
	}
	wantTimeSec := base.Add(-offset).Unix()
	diff := gotTimeSec - wantTimeSec
	if diff < -1 || diff > 1 {
		t.Errorf("time: got %d want ~%d (diff %d); expected time shifted back by 1h", gotTimeSec, wantTimeSec, diff)
	}
}

func TestQuery_NoOffsetUnchanged(t *testing.T) {
	// Verify that instant queries without offset leave the time param unmodified.
	base := time.Unix(1700000000, 0).UTC()

	var gotTime string
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/select/logsql/stats_query" {
			_ = r.ParseForm()
			gotTime = r.FormValue("time")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42"]}]}}`))
		} else {
			http.NotFound(w, r)
		}
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `sum(count_over_time({app="nginx"}[5m]))`)
	params.Set("time", strconv.FormatInt(base.UnixNano(), 10))
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if gotTime == "" {
		t.Fatal("VL backend never received a stats_query request")
	}

	gotTimeSec, err := strconv.ParseInt(gotTime, 10, 64)
	if err != nil {
		t.Fatalf("could not parse captured time %q: %v", gotTime, err)
	}
	wantTimeSec := base.Unix()
	diff := gotTimeSec - wantTimeSec
	if diff < -1 || diff > 1 {
		t.Errorf("time should be ~unmodified: got %d want %d (diff %d)", gotTimeSec, wantTimeSec, diff)
	}
}

func TestQuery_MultipleOffsetReturns400(t *testing.T) {
	vlBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer vlBackend.Close()

	p := newGapTestProxy(t, vlBackend.URL)
	params := url.Values{}
	params.Set("query", `count_over_time({app="a"}[5m] offset 1h) + count_over_time({app="b"}[5m] offset 2h)`)
	params.Set("time", "1700000000")
	req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	p.handleQuery(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for multiple offsets, got %d: %s", rec.Code, rec.Body.String())
	}
}
