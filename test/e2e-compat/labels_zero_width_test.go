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

// Grafana's "Show context" asks /labels for the selected row's instant
// (start == end, the row's time in milliseconds). Loki lists labels and label
// values from its index at millisecond precision, including data at end, so
// it answers with the row's labels; the proxy used VictoriaLogs' half-open
// [start, end) and answered [] (issue #700), and the context query became {}.
// A window ending at a row lists it the same way. /series stays end-exclusive
// on both.
//
// conformance: semantics/labels-end-inclusive-like-loki
func TestCompat_LabelsZeroWidthLikeLoki(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "labels-zero-width-" + id
	uniq := "lzw_" + id
	// A recent instant with a nanosecond part, as Grafana sends a row's time.
	at := time.Now().Add(-10 * time.Minute).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	labels := map[string]string{"app": app, "env": "zero-width", uniq: "point"}

	msg := "labels zero width fixture " + id
	row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": msg}
	for k, v := range labels {
		row[k] = v
	}
	encoded, _ := json.Marshal(row)
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("app,env,"+uniq),
		string(encoded)+"\n", map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": labels, "values": [][]string{{strconv.FormatInt(at.UnixNano(), 10), msg}},
	}}})
	if status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload),
		map[string]string{"Content-Type": "application/json"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	if status, body = hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs flush: %d %s", status, body)
	}

	// Both backends hold the row before any comparison.
	selector := fmt.Sprintf(`{app=%q}`, app)
	around := url.Values{"query": {selector}, "limit": {"10"},
		"start": {strconv.FormatInt(at.Add(-time.Minute).UnixNano(), 10)}, "end": {strconv.FormatInt(at.Add(time.Minute).UnixNano(), 10)}}
	for _, base := range []string{lokiURL, proxyURL} {
		deadline := time.Now().Add(60 * time.Second)
		for !strings.Contains(string(labelsFullRangeGet(t, base+"/loki/api/v1/query_range?"+around.Encode())), msg) {
			if time.Now().After(deadline) {
				t.Fatalf("%s does not serve the fixture row", base)
			}
			time.Sleep(2 * time.Second)
		}
	}

	ns := strconv.FormatInt(at.UnixNano(), 10)
	// Grafana 13's "Show context" sends the row's time truncated to milliseconds.
	ms := strconv.FormatInt(at.Truncate(time.Millisecond).UnixNano(), 10)
	hourBefore := strconv.FormatInt(at.Add(-time.Hour).UnixNano(), 10)
	for _, tc := range []struct {
		name, path, start, end string
		want                   []string
	}{
		{"labels zero width", "/loki/api/v1/labels", ns, ns, []string{"app", "env", uniq}},
		{"labels zero width at the row's millisecond (Grafana)", "/loki/api/v1/labels", ms, ms, []string{"app", "env", uniq}},
		{"labels window ending at the row", "/loki/api/v1/labels", hourBefore, ns, []string{"app", "env", uniq}},
		{"label values zero width", "/loki/api/v1/label/" + uniq + "/values", ns, ns, []string{"point"}},
		{"label values zero width at the row's millisecond (Grafana)", "/loki/api/v1/label/" + uniq + "/values", ms, ms, []string{"point"}},
		{"label values window ending at the row", "/loki/api/v1/label/" + uniq + "/values", hourBefore, ns, []string{"point"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := url.Values{"query": {selector}, "start": {tc.start}, "end": {tc.end}}
			loki := labelsFullRangeStrings(t, lokiURL+tc.path+"?"+q.Encode())
			proxy := labelsFullRangeStrings(t, proxyURL+tc.path+"?"+q.Encode())
			for _, w := range tc.want {
				if !slices.Contains(loki, w) {
					t.Fatalf("Loki %s = %v, missing %q (fixture or Loki changed)", tc.path, loki, w)
				}
				if !slices.Contains(proxy, w) {
					t.Fatalf("proxy %s = %v, missing %q; Loki %v", tc.path, proxy, w, loki)
				}
			}
			// Loki's ingester index is not time-precise, so it may list more;
			// the proxy never lists what Loki does not.
			for _, v := range proxy {
				if !slices.Contains(loki, v) {
					t.Fatalf("proxy %s lists %q, Loki does not: proxy %v, Loki %v", tc.path, v, proxy, loki)
				}
			}
		})
	}

	// /series keeps end exclusive on both: no series for a zero-width request.
	q := url.Values{"match[]": {selector}, "start": {ns}, "end": {ns}}
	for _, base := range []string{lokiURL, proxyURL} {
		var resp struct {
			Data []map[string]string `json:"data"`
		}
		if err := json.Unmarshal(labelsFullRangeGet(t, base+"/loki/api/v1/series?"+q.Encode()), &resp); err != nil {
			t.Fatalf("%s series: %v", base, err)
		}
		if len(resp.Data) != 0 {
			t.Fatalf("%s zero-width series = %v, want none (Loki's series end is exclusive)", base, resp.Data)
		}
	}
}
