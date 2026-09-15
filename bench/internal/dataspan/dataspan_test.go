package dataspan

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	want := time.Date(2026, 9, 14, 19, 47, 54, 0, time.UTC)
	for _, in := range []string{"2026-09-14T19:47:54Z", "2026-09-14T21:47:54+02:00", "1789415274", "1789415274000", "1789415274000000000"} {
		got, err := ParseTime(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if !got.Equal(want) {
			t.Fatalf("%s: got %s want %s", in, got, want)
		}
	}
	if _, err := ParseTime("yesterday"); err == nil {
		t.Fatal("expected error")
	}
}

// fakeVL answers the span query with minTime/maxTime and the window-count query
// with rows; it records every query it sees.
func fakeVL(t *testing.T, minTime, maxTime string, rows []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		switch {
		case r.URL.Path != "/select/logsql/query":
			t.Errorf("unexpected path %s", r.URL.Path)
		case strings.Contains(q, "min(_time)") && strings.Contains(q, "max(_time)"):
			_, _ = w.Write([]byte(`{"min_time":"` + minTime + `","max_time":"` + maxTime + `"}` + "\n"))
		case strings.Contains(q, "count() lines"):
			if !strings.Contains(q, "_time:3600000000000ns offset 3599999999999ns") || !strings.Contains(q, "app, cluster, region") {
				t.Errorf("window query must bucket (end-1h, end] per service: %q", q)
			}
			_, _ = w.Write([]byte(strings.Join(rows, "\n")))
		default:
			t.Errorf("unexpected query %q", q)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// vlRow is a VictoriaLogs window-count row for the window ending at end.
func vlRow(end time.Time, app string, n int) string {
	return `{"_time":"` + end.Add(-time.Hour+time.Nanosecond).UTC().Format(time.RFC3339Nano) + `","app":"` + app + `","cluster":"c","region":"r","lines":"` + strconv.Itoa(n) + `"}`
}

func TestDetectVL(t *testing.T) {
	start := time.Date(2026, 9, 14, 17, 47, 54, 0, time.UTC)
	end := time.Date(2026, 9, 14, 19, 47, 54, 100889666, time.UTC)
	rows := []string{vlRow(start.Truncate(time.Hour).Add(time.Hour), "a", 5), vlRow(start.Truncate(time.Hour).Add(2*time.Hour), "a", 5), vlRow(end.Truncate(time.Hour).Add(time.Hour), "a", 5)}
	srv := fakeVL(t, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), rows)
	s, err := DetectVL(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !s.End.Equal(end) || !s.Start.Equal(start) {
		t.Fatalf("span %+v", s)
	}
}

func TestDetectVLFailsOnGap(t *testing.T) {
	start := time.Date(2026, 9, 14, 10, 30, 0, 0, time.UTC)
	end := time.Date(2026, 9, 14, 14, 30, 0, 0, time.UTC)
	// Windows end 11:00, 12:00, 13:00, 14:00, 15:00; 12:00 and 13:00 are empty.
	rows := []string{vlRow(start.Truncate(time.Hour).Add(time.Hour), "a", 5), vlRow(end.Truncate(time.Hour), "a", 5), vlRow(end.Truncate(time.Hour).Add(time.Hour), "a", 5)}
	srv := fakeVL(t, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano), rows)
	_, err := DetectVL(context.Background(), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "2 empty 1h0m0s windows") || !strings.Contains(err.Error(), "2026-09-14T12:00:00Z") {
		t.Fatalf("expected a gap error, got %v", err)
	}
}

func TestDetectVLEmpty(t *testing.T) {
	srv := fakeVL(t, "", "", nil)
	if _, err := DetectVL(context.Background(), srv.URL); err == nil {
		t.Fatal("expected error for an empty store")
	}
}

func TestWindowEndsCoverTheSpan(t *testing.T) {
	cases := []Span{
		{Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)},
		{Start: time.Date(2026, 1, 1, 0, 0, 0, 1, time.UTC), End: time.Date(2026, 1, 1, 2, 59, 59, 999999999, time.UTC)},
	}
	for _, s := range cases {
		first, last := WindowEnds(s, time.Hour)
		if !first.Add(-time.Hour).Before(s.Start) || last.Before(s.End) || first.UnixNano()%int64(time.Hour) != 0 || last.UnixNano()%int64(time.Hour) != 0 {
			t.Fatalf("windows (%s, %s] do not cover %+v", first.Add(-time.Hour), last, s)
		}
		if first.Sub(s.Start) > time.Hour || last.Sub(s.End) >= time.Hour {
			t.Fatalf("windows (%s, %s] are wider than needed for %+v", first.Add(-time.Hour), last, s)
		}
	}
	if q := LokiWindowCountsQuery(time.Hour); q != `sum by (app, cluster, region) (count_over_time({app=~".+"}[3600s]))` {
		t.Fatalf("loki query %q", q)
	}
}

// lokiMatrix returns a Loki matrix answer with one series per app.
func lokiMatrix(samples map[string][][2]int64) string {
	var parts []string
	for app, vals := range samples {
		var v []string
		for _, s := range vals {
			v = append(v, "["+strconv.FormatInt(s[0], 10)+`,"`+strconv.FormatInt(s[1], 10)+`"]`)
		}
		parts = append(parts, `{"metric":{"app":"`+app+`","cluster":"c","region":"r"},"values":[`+strings.Join(v, ",")+`]}`)
	}
	return `{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(parts, ",") + `]}}`
}

func TestWaitEqualConvergesPerServiceWindow(t *testing.T) {
	span := Span{Start: time.Unix(3600*10+5, 0), End: time.Unix(3600*12-5, 0)}
	w11, w12 := time.Unix(3600*11, 0), time.Unix(3600*12, 0)
	var lokiCalls atomic.Int32
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cache-Control") != "no-cache" {
			t.Errorf("Loki count must bypass the results cache")
		}
		if r.URL.Query().Get("step") != "3600" {
			t.Errorf("step %q", r.URL.Query().Get("step"))
		}
		if lokiCalls.Add(1) < 3 {
			// Same total as VictoriaLogs, but a line sits in the wrong window.
			_, _ = w.Write([]byte(lokiMatrix(map[string][][2]int64{"a": {{w11.Unix(), 11}, {w12.Unix(), 9}}, "b": {{w11.Unix(), 7}}})))
			return
		}
		_, _ = w.Write([]byte(lokiMatrix(map[string][][2]int64{"a": {{w11.Unix(), 10}, {w12.Unix(), 10}}, "b": {{w11.Unix(), 7}}})))
	}))
	defer loki.Close()
	vl := fakeVL(t, "", "", []string{vlRow(w11, "a", 10), vlRow(w12, "a", 10), vlRow(w11, "b", 7)})

	var logs []string
	err := WaitEqual(context.Background(), loki.URL, vl.URL, span, 5*time.Second, 10*time.Millisecond, func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })
	if err != nil {
		t.Fatalf("WaitEqual: %v (logs %v)", err, logs)
	}
	if lokiCalls.Load() != 3 {
		t.Fatalf("loki polled %d times", lokiCalls.Load())
	}
	if !strings.Contains(logs[0], "loki=27 victorialogs=27, 2 service windows differ") {
		t.Fatalf("equal totals must not pass: %v", logs)
	}
}

func TestWaitEqualTimesOut(t *testing.T) {
	span := Span{Start: time.Unix(3600*10+5, 0), End: time.Unix(3600*11-5, 0)}
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer loki.Close()
	vl := fakeVL(t, "", "", []string{vlRow(time.Unix(3600*11, 0), "a", 100)})
	err := WaitEqual(context.Background(), loki.URL, vl.URL, span, 50*time.Millisecond, 20*time.Millisecond, func(string, ...any) {})
	if err == nil || !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "Loki holds 0 lines, VictoriaLogs 100") || !strings.Contains(err.Error(), `app="a"`) {
		t.Fatalf("expected a timeout naming the counts and the service window, got %v", err)
	}
}

func TestLokiWindowCountsRejectsWarnings(t *testing.T) {
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","warnings":["partial"],"data":{"resultType":"matrix","result":[]}}`))
	}))
	defer loki.Close()
	if _, err := LokiWindowCounts(context.Background(), loki.URL, Span{Start: time.Unix(10, 0), End: time.Unix(20, 0)}, time.Hour); err == nil {
		t.Fatal("a Loki answer with warnings must not count")
	}
}
