package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLokiLabelBoundsToVL(t *testing.T) {
	maxNs := strconv.FormatInt(math.MaxInt64, 10)
	for _, tc := range []struct{ in, start, end string }{
		{"", "", ""},
		{"  ", "  ", "  "},
		{"1704067200123456789", "1704067200123000000", "1704067200124000000"}, // nanoseconds: the millisecond, inclusive
		{"1704067200123000000", "1704067200123000000", "1704067200124000000"}, // Grafana's row time (milliseconds in ns)
		{"1704067200", "1704067200000000000", "1704067200001000000"},          // seconds
		{"1704067200123", "1704067200123000000", "1704067200124000000"},       // milliseconds
		{"1704067200.5", "1704067200500000000", "1704067200501000000"},        // float seconds
		{"2024-01-01T00:00:00Z", "1704067200000000000", "1704067200001000000"},
		{"2024-01-01T00:00:00.25Z", "1704067200250000000", "1704067200251000000"},
		{"not-a-time", "not-a-time", "not-a-time"}, // left for the backend to reject as before
		{maxNs, "9223372036854000000", maxNs},      // no overflow
	} {
		if got := lokiLabelStartToVL(tc.in); got != tc.start {
			t.Errorf("lokiLabelStartToVL(%q) = %q, want %q", tc.in, got, tc.start)
		}
		if got := lokiLabelEndToVL(tc.in); got != tc.end {
			t.Errorf("lokiLabelEndToVL(%q) = %q, want %q", tc.in, got, tc.end)
		}
	}
	if got := floorToMillisecond(-1); got != -int64(time.Millisecond) {
		t.Errorf("floorToMillisecond(-1) = %d", got)
	}
}

// inclusiveEndVL is a VictoriaLogs fake that answers every endpoint the label,
// label-value and series handlers use from two rows, honouring VictoriaLogs'
// half-open [start, end) time filter: one row at at, one a millisecond later.
type inclusiveEndVL struct{ at int64 }

type inclusiveEndRow struct {
	ts     int64
	labels map[string]string
}

func (f inclusiveEndVL) rows(r *http.Request) []inclusiveEndRow {
	all := []inclusiveEndRow{
		{ts: f.at, labels: map[string]string{"app": "a1", "service_name": "svc"}},
		{ts: f.at + int64(time.Millisecond), labels: map[string]string{"app": "late", "late_label": "x", "service_name": "late-svc"}},
	}
	start, _ := strconv.ParseInt(r.FormValue("start"), 10, 64)
	end, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		end = math.MaxInt64
	}
	var out []inclusiveEndRow
	for _, row := range all {
		if row.ts >= start && row.ts < end {
			out = append(out, row)
		}
	}
	return out
}

func (f inclusiveEndVL) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		rows := f.rows(r)
		counts := map[string]int64{}
		switch r.URL.Path {
		case "/select/logsql/field_names", "/select/logsql/stream_field_names":
			for _, row := range rows {
				for k := range row.labels {
					counts[k]++
				}
			}
		case "/select/logsql/field_values", "/select/logsql/stream_field_values":
			for _, row := range rows {
				if v, ok := row.labels[r.FormValue("field")]; ok {
					counts[v]++
				}
			}
		case "/select/logsql/streams":
			for _, row := range rows {
				pairs := make([]string, 0, len(row.labels))
				for k, v := range row.labels {
					pairs = append(pairs, fmt.Sprintf("%s=%q", k, v))
				}
				slices.Sort(pairs)
				counts["{"+strings.Join(pairs, ",")+"}"]++
			}
		case "/select/logsql/query":
			w.Header().Set("Content-Type", "application/stream+json")
			for _, row := range rows {
				obj := map[string]string{"_time": time.Unix(0, row.ts).UTC().Format(time.RFC3339Nano), "_msg": "m"}
				for k, v := range row.labels {
					obj[k] = v
				}
				line, _ := json.Marshal(obj)
				_, _ = w.Write(append(line, '\n'))
			}
			return
		case "/select/logsql/hits":
			_, _ = w.Write([]byte(`{"hits":[]}`))
			return
		}
		hits := make([]fieldHit, 0, len(counts))
		for v, n := range counts {
			hits = append(hits, fieldHit{Value: v, Hits: n})
		}
		writeVLFieldValues(w, hits)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Loki lists labels and label values from its index at millisecond precision,
// including data at the request's end (a chunk matches when From <= end and
// Through >= start, in milliseconds), so a zero-width request (start == end:
// Grafana's "Show context" sends the row's time truncated to milliseconds)
// lists the labels of the rows of that millisecond, and a window ending at a
// row lists it. /series stays end-exclusive, as in Loki.
//
// conformance: semantics/labels-end-inclusive-like-loki
func TestLabelsEndIsInclusiveLikeLoki(t *testing.T) {
	at := perfBaseTimeNs - int64(3*time.Hour) + 123456789
	atMs := at / int64(time.Millisecond) * int64(time.Millisecond) // what Grafana sends for this row
	_, mux := newBehaviorProxy(t, inclusiveEndVL{at: at}.server(t).URL, 0)
	hour := int64(time.Hour)

	for _, tc := range []struct {
		name       string
		path       string
		start, end int64
		want       []string
		notWant    []string
	}{
		{name: "labels zero width", path: "/loki/api/v1/labels", start: at, end: at, want: []string{"app", "service_name"}, notWant: []string{"late_label"}},
		{name: "labels zero width at the row's millisecond (Grafana)", path: "/loki/api/v1/labels", start: atMs, end: atMs, want: []string{"app", "service_name"}, notWant: []string{"late_label"}},
		{name: "label values zero width at the row's millisecond (Grafana)", path: "/loki/api/v1/label/app/values", start: atMs, end: atMs, want: []string{"a1"}, notWant: []string{"late"}},
		{name: "labels window ending at the row", path: "/loki/api/v1/labels", start: at - hour, end: at, want: []string{"app", "service_name"}, notWant: []string{"late_label"}},
		{name: "labels window starting at the row", path: "/loki/api/v1/labels", start: at, end: at + hour, want: []string{"app", "late_label", "service_name"}},
		{name: "label values zero width", path: "/loki/api/v1/label/app/values", start: at, end: at, want: []string{"a1"}, notWant: []string{"late"}},
		{name: "label values window ending at the row", path: "/loki/api/v1/label/app/values", start: at - hour, end: at, want: []string{"a1"}, notWant: []string{"late"}},
		{name: "service_name values zero width", path: "/loki/api/v1/label/service_name/values", start: at, end: at, want: []string{"svc"}, notWant: []string{"late-svc"}},
		// A window of its own: metadata cache keys are bucketed, so a window that
		// differs from the ones above by a nanosecond would share their answer.
		{name: "labels before the row's millisecond", path: "/loki/api/v1/labels", start: at - 3*hour, end: atMs - 1, notWant: []string{"app", "late_label"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := fmt.Sprintf("%s?start=%d&end=%d", tc.path, tc.start, tc.end)
			for pass := range 2 { // the second answer comes from the caches
				got := serveLokiStrings(t, mux, path)
				for _, w := range tc.want {
					if !slices.Contains(got, w) {
						t.Fatalf("pass %d: %s = %v, missing %q", pass, path, got, w)
					}
				}
				for _, nw := range tc.notWant {
					if slices.Contains(got, nw) {
						t.Fatalf("pass %d: %s = %v, must not list %q (data after end, or before start)", pass, path, got, nw)
					}
				}
			}
		})
	}

	// /series keeps end exclusive, as Loki's does: a zero-width request and a
	// window ending at the row return no series.
	for _, rng := range [][2]int64{{at, at}, {at - hour, at}} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/loki/api/v1/series?match[]=%s&start=%d&end=%d", `%7Bapp%3D%22a1%22%7D`, rng[0], rng[1]), nil))
		var resp struct {
			Data []map[string]string `json:"data"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
			t.Fatalf("series [%d, %d]: %d %s", rng[0], rng[1], rec.Code, rec.Body.String())
		}
		if len(resp.Data) != 0 {
			t.Fatalf("series [%d, %d] = %v, want none: Loki's series end is exclusive", rng[0], rng[1], resp.Data)
		}
	}
}
