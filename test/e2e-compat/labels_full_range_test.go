//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCompat_LabelsFullRangeFixture proves that the first /labels and
// /label/{name}/values responses cover the whole requested [start, end] range,
// like Loki. The fixture's unique stream label has data only 1-2h and 8h before
// the range end, never in the last minutes of the range, so a proxy that scans
// only a recent slice of the range (or caps values to the last 6h) misses it.
func TestCompat_LabelsFullRangeFixture(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "labels-full-range-" + id
	uniq := "lfr_" + id
	// The proxy buckets metadata cache keys (5 minutes for a 2h window), so an
	// unscoped window reused by a rerun could be answered from the previous
	// run's cache. Shift the range end back by a per-run multiple of 5 minutes:
	// runs 5s to 5min apart always land in different buckets.
	now := time.Now().UTC()
	runShift := time.Duration(now.Unix()%300/5) * 5 * time.Minute
	rangeEnd := now.Truncate(5 * time.Minute).Add(-time.Hour - runShift)

	type fixtureStream struct {
		labels map[string]string
		at     time.Time
		lines  int
	}
	streams := []fixtureStream{
		{labels: map[string]string{"app": app, "env": "early", uniq: "alpha"}, at: rangeEnd.Add(-110 * time.Minute), lines: 3},
		{labels: map[string]string{"app": app, "env": "early", uniq: "beta"}, at: rangeEnd.Add(-70 * time.Minute), lines: 2},
		// More than 6h before the range end: outside any recent-values cap.
		{labels: map[string]string{"app": app, "env": "old", uniq: "gamma"}, at: rangeEnd.Add(-8 * time.Hour), lines: 4},
	}
	totalLines := 0
	for _, s := range streams {
		lines := make([]string, 0, s.lines)
		values := make([][]string, 0, s.lines)
		for i := 0; i < s.lines; i++ {
			ts := s.at.Add(time.Duration(i) * time.Second)
			msg := fmt.Sprintf("labels full range fixture %s %s line %d", id, s.labels[uniq], i)
			row := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": msg}
			for k, v := range s.labels {
				row[k] = v
			}
			encoded, _ := json.Marshal(row)
			lines = append(lines, string(encoded))
			values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), msg})
		}
		totalLines += s.lines
		status, body := hardeningRequest(t, http.MethodPost,
			vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("app,env,"+uniq),
			strings.Join(lines, "\n")+"\n", map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": s.labels, "values": values}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs flush: %d %s", status, body)
	}
	// Loki only queries ingesters for the most recent data; flush so the
	// historical fixture is served from the store.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	fullStart := rangeEnd.Add(-24 * time.Hour)
	selector := fmt.Sprintf(`{app=%q}`, app)
	fixtureRange := url.Values{}
	fixtureRange.Set("start", strconv.FormatInt(fullStart.UnixNano(), 10))
	fixtureRange.Set("end", strconv.FormatInt(rangeEnd.UnixNano(), 10))

	// Both backends must hold the identical fixture before any comparison.
	lokiCount := func() int {
		params := url.Values{"query": {selector}, "limit": {"1000"}}
		for k, v := range fixtureRange {
			params[k] = v
		}
		body := labelsFullRangeGet(t, lokiURL+"/loki/api/v1/query_range?"+params.Encode())
		var resp struct {
			Data struct {
				Result []struct {
					Values [][]string `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode Loki query_range: %v", err)
		}
		n := 0
		for _, r := range resp.Data.Result {
			n += len(r.Values)
		}
		return n
	}
	wantValues := []string{"alpha", "beta", "gamma"}
	deadline := time.Now().Add(90 * time.Second)
	for {
		values := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/label/"+uniq+"/values?"+fixtureRange.Encode())
		if n := lokiCount(); n == totalLines && slices.Equal(values, wantValues) {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("Loki did not serve the flushed fixture: %d/%d lines, %s values %v", n, totalLines, uniq, values)
		}
		time.Sleep(5 * time.Second)
	}
	vlParams := url.Values{"query": {selector + " | stats count() as c"}}
	vlParams.Set("start", fullStart.Format(time.RFC3339Nano))
	vlParams.Set("end", rangeEnd.Format(time.RFC3339Nano))
	vlBody := labelsFullRangeGet(t, vlURL+"/select/logsql/query?"+vlParams.Encode())
	if !strings.Contains(string(vlBody), fmt.Sprintf(`"c":"%d"`, totalLines)) {
		t.Fatalf("VictoriaLogs fixture count mismatch (want %d): %s", totalLines, vlBody)
	}

	wantLabels := []string{"app", "env", uniq, "service_name"}
	// Unscoped, as Grafana's label browser asks: the shared stack holds other
	// data, so compare only the fixture's labels and values. The window ends
	// an hour after the newest fixture line, so none of it is recent.
	//
	// This runs before the scoped requests: with -label-values-indexed-cache
	// (the compose proxy) browse-mode label values (no query) are served from a
	// time-agnostic index once a label is indexed, and the scoped requests
	// index gamma for this label.
	unscoped := url.Values{}
	unscoped.Set("start", strconv.FormatInt(rangeEnd.Add(-2*time.Hour).UnixNano(), 10))
	unscoped.Set("end", strconv.FormatInt(rangeEnd.UnixNano(), 10))
	lokiAll := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/labels?"+unscoped.Encode())
	proxyAll := labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/labels?"+unscoped.Encode())
	for _, name := range wantLabels {
		if !slices.Contains(lokiAll, name) {
			t.Fatalf("Loki unscoped /labels missing fixture label %q: %v", name, lokiAll)
		}
		if !slices.Contains(proxyAll, name) {
			t.Fatalf("first proxy unscoped /labels missing fixture label %q: proxy %v, Loki %v", name, proxyAll, lokiAll)
		}
	}
	lokiUniq := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/label/"+uniq+"/values?"+unscoped.Encode())
	proxyUniq := labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/label/"+uniq+"/values?"+unscoped.Encode())
	// gamma is 8h before this window. Loki answers label values for streams
	// still held by the ingester from its in-memory index, which is not
	// time-precise, so it may add gamma; VictoriaLogs filters by time exactly.
	want := []string{"alpha", "beta"}
	for _, v := range want {
		if !slices.Contains(lokiUniq, v) {
			t.Fatalf("Loki unscoped /label/%s/values missing %q: %v", uniq, v, lokiUniq)
		}
	}
	if !slices.Equal(proxyUniq, want) {
		t.Fatalf("first proxy unscoped /label/%s/values = %v, want %v (Loki %v)", uniq, proxyUniq, want, lokiUniq)
	}

	// Proxy first responses. Scoped by the fixture selector the label set is
	// exact, so the proxy must equal Loki.
	scoped := url.Values{"query": {selector}}
	for k, v := range fixtureRange {
		scoped[k] = v
	}
	lokiLabels := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/labels?"+scoped.Encode())
	if !slices.Equal(lokiLabels, wantLabels) {
		t.Fatalf("Loki fixture labels = %v, want %v", lokiLabels, wantLabels)
	}
	if got := labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/labels?"+scoped.Encode()); !slices.Equal(got, lokiLabels) {
		t.Fatalf("first proxy /labels (query=%s, 24h) = %v, Loki = %v", selector, got, lokiLabels)
	}
	for _, tc := range []struct {
		label string
		want  []string
	}{
		{label: uniq, want: wantValues},
		{label: "env", want: []string{"early", "old"}},
		{label: "app", want: []string{app}},
		{label: "service_name", want: []string{app}},
	} {
		lokiValues := labelsFullRangeStrings(t, lokiURL+"/loki/api/v1/label/"+tc.label+"/values?"+scoped.Encode())
		if !slices.Equal(lokiValues, tc.want) {
			t.Fatalf("Loki %s values = %v, want %v", tc.label, lokiValues, tc.want)
		}
		if got := labelsFullRangeStrings(t, proxyURL+"/loki/api/v1/label/"+tc.label+"/values?"+scoped.Encode()); !slices.Equal(got, lokiValues) {
			t.Fatalf("first proxy /label/%s/values (query=%s, 24h) = %v, Loki = %v", tc.label, selector, got, lokiValues)
		}
	}
}

func labelsFullRangeGet(t *testing.T, target string) []byte {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, target, "", map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK {
		t.Fatalf("GET %s: %d %s", target, status, body)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err == nil {
		if _, ok := envelope["warnings"]; ok {
			t.Fatalf("GET %s returned warnings: %s", target, body)
		}
		if _, ok := envelope["error"]; ok {
			t.Fatalf("GET %s returned an error: %s", target, body)
		}
	}
	return body
}

// labelsFullRangeStrings returns the sorted string data of a Loki metadata
// response. Loki omits "data" for an empty result; that decodes to nil.
func labelsFullRangeStrings(t *testing.T, target string) []string {
	t.Helper()
	body := labelsFullRangeGet(t, target)
	var resp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		t.Fatalf("GET %s: unexpected body %s (%v)", target, body, err)
	}
	sort.Strings(resp.Data)
	return resp.Data
}
