//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Grafana's "Show context" asks /labels for the clicked row's instant and adds the row's structured metadata
// (detected_level) to the context query only when the label is in the answer. Loki lists label names from whole
// index periods and the chunks overlapping the request, not from the instant: a stream written two minutes before
// the row is listed. The proxy listed the names of the instant alone, so the context query differed from Loki's
// (explore-log-context in the visual proof). A zero-width window now lists what Loki lists.
//
// conformance: semantics/labels-point-window-is-the-index-day
func TestCompat_LabelsPointWindowListsTheIndexDayLikeLoki(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "labels-point-" + id
	uniqRow, uniqOther := "lpw_row_"+id, "lpw_other_"+id
	at := time.Now().Add(-10 * time.Minute).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	earlier := at.Add(-3 * time.Minute)
	if at.UTC().Truncate(24*time.Hour) != earlier.UTC().Truncate(24*time.Hour) {
		t.Skip("the fixture would straddle UTC midnight; rerun after it")
	}
	push := func(when time.Time, labels map[string]string, msg string) {
		row := map[string]string{"_time": when.UTC().Format(time.RFC3339Nano), "_msg": msg}
		fields := make([]string, 0, len(labels))
		for k, v := range labels {
			row[k] = v
			fields = append(fields, k)
		}
		slices.Sort(fields)
		encoded, _ := json.Marshal(row)
		if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(fields, ",")),
			string(encoded)+"\n", map[string]string{"Content-Type": "application/stream+json"}); status != http.StatusOK {
			t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
			"stream": labels, "values": [][]string{{strconv.FormatInt(when.UnixNano(), 10), msg}}}}})
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload),
			map[string]string{"Content-Type": "application/json"}); status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	push(at, map[string]string{"app": app, "env": "point-window", uniqRow: "row"}, "point window row "+id)
	push(earlier, map[string]string{"app": app, "env": "point-window", uniqOther: "other"}, "point window earlier "+id)
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs flush: %d %s", status, body)
	}

	ns := strconv.FormatInt(at.UnixNano(), 10)
	ms := strconv.FormatInt(at.Truncate(time.Millisecond).UnixNano(), 10) // what Grafana sends for the row
	day := at.UTC().Truncate(24 * time.Hour)
	dayStart, dayEnd := strconv.FormatInt(day.UnixNano(), 10), strconv.FormatInt(day.Add(24*time.Hour).UnixNano(), 10)
	// A selector of its own: each run has its own cache entry on the proxy, and the listing is the two fixture
	// streams'.
	selector := fmt.Sprintf(`{app=%q}`, app)
	names := func(base, start, end string) []string {
		return labelsFullRangeStrings(t, base+"/loki/api/v1/labels?"+url.Values{"query": {selector}, "start": {start}, "end": {end}}.Encode())
	}
	// Both backends list the earlier stream's label for the point (Loki after its ingester holds the fixture).
	deadline := time.Now().Add(60 * time.Second)
	for !slices.Contains(names(lokiURL, ns, ns), uniqOther) {
		if time.Now().After(deadline) {
			t.Fatalf("Loki does not list %q for the point window", uniqOther)
		}
		time.Sleep(2 * time.Second)
	}
	for _, tc := range []struct{ name, start, end string }{{"zero width", ns, ns}, {"the row's millisecond (Grafana)", ms, ms}} {
		t.Run(tc.name, func(t *testing.T) {
			// Loki's ingester lists the fixture once it holds the push.
			var loki, proxy []string
			for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(3 * time.Second) {
				loki, proxy = names(lokiURL, tc.start, tc.end), names(proxyURL, tc.start, tc.end)
				listed := func(names []string) bool { return slices.Contains(names, uniqRow) && slices.Contains(names, uniqOther) }
				if listed(proxy) && listed(loki) || time.Now().After(deadline) {
					break
				}
			}
			for _, want := range []string{"app", "env", uniqRow, uniqOther} {
				if !slices.Contains(loki, want) || !slices.Contains(proxy, want) {
					t.Fatalf("%q must be listed by both: Loki %v, proxy %v", want, loki, proxy)
				}
			}
			for _, v := range proxy {
				if !slices.Contains(loki, v) {
					t.Fatalf("proxy lists %q, Loki does not: proxy %v, Loki %v", v, proxy, loki)
				}
			}
			if wide := names(proxyURL, dayStart, dayEnd); !slices.Equal(proxy, wide) {
				t.Fatalf("point window %v differs from the day window %v", proxy, wide)
			}
		})
	}
}
