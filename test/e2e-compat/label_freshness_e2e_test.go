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

// TestCompat_LabelsIncludeStreamWrittenAfterCaching proves the freshness rule
// of Loki's metadata results cache (max_metadata_cache_freshness, default 24h):
// /labels and /label/{name}/values over 7d, 24h and 6h windows ending now list a
// stream written after an earlier identical request was answered and cached. The
// proxy used to serve the cached answer for up to an hour.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestCompat_LabelsIncludeStreamWrittenAfterCaching(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "label-fresh-" + id
	uniq := "lfresh_" + id
	windows := []time.Duration{7 * 24 * time.Hour, 24 * time.Hour, 6 * time.Hour}

	get := func(base, path string, window time.Duration) []string {
		now := time.Now()
		q := url.Values{}
		q.Set("start", strconv.FormatInt(now.Add(-window).UnixNano(), 10))
		q.Set("end", strconv.FormatInt(now.UnixNano(), 10))
		// Scoped to the fixture app: the requests warm only cache entries of their
		// own, so later tests that ingest older rows are not served a bucket this
		// test cached empty.
		q.Set("query", fmt.Sprintf(`{app=~%q}`, app))
		return labelsFullRangeStrings(t, base+path+"?"+q.Encode())
	}

	// Warm every window on the proxy (labels, and the values of app) before the
	// stream exists: the answers are empty and cached.
	for _, w := range windows {
		get(proxyURL, "/loki/api/v1/labels", w)
		get(proxyURL, "/loki/api/v1/label/app/values", w)
	}

	// Write one stream carrying a label no earlier stream has, to both backends.
	ts := time.Now()
	msg := "label freshness fixture " + id
	row, _ := json.Marshal(map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": msg, "app": app, uniq: "v1"})
	status, body := hardeningRequest(t, http.MethodPost,
		vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("app,"+uniq), string(row)+"\n",
		map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
		"stream": map[string]string{"app": app, uniq: "v1"},
		"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), msg}},
	}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
		t.Fatalf("VictoriaLogs flush: %d %s", status, body)
	}

	// Both sides healthy and holding the fixture before any comparison: Loki
	// lists the label and VictoriaLogs counts the row.
	deadline := time.Now().Add(90 * time.Second)
	for {
		if slices.Contains(get(lokiURL, "/loki/api/v1/labels", 6*time.Hour), uniq) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Loki never listed %s", uniq)
		}
		time.Sleep(2 * time.Second)
	}
	vlParams := url.Values{"query": {fmt.Sprintf("app:=%q | stats count() as c", app)}}
	vlParams.Set("start", ts.Add(-time.Minute).Format(time.RFC3339Nano))
	vlParams.Set("end", time.Now().Add(time.Minute).Format(time.RFC3339Nano))
	if vlBody := labelsFullRangeGet(t, vlURL+"/select/logsql/query?"+vlParams.Encode()); !strings.Contains(string(vlBody), `"c":"1"`) {
		t.Fatalf("VictoriaLogs does not hold the fixture row: %s", vlBody)
	}

	// Older than -recent-tail-refresh-max-staleness (2s) at the next request.
	time.Sleep(3 * time.Second)
	for _, w := range windows {
		loki := get(lokiURL, "/loki/api/v1/labels", w)
		if !slices.Contains(loki, uniq) {
			t.Fatalf("%s: Loki omits %s: %v", w, uniq, loki)
		}
		proxy := get(proxyURL, "/loki/api/v1/labels", w)
		if !slices.Contains(proxy, uniq) {
			t.Errorf("%s: /labels omits %s that Loki lists: %v", w, uniq, proxy)
		}
		lokiValues := get(lokiURL, "/loki/api/v1/label/app/values", w)
		proxyValues := get(proxyURL, "/loki/api/v1/label/app/values", w)
		if !slices.Contains(lokiValues, app) {
			t.Fatalf("%s: Loki omits app=%s: %v", w, app, lokiValues)
		}
		if !slices.Contains(proxyValues, app) {
			t.Errorf("%s: /label/app/values omits %s that Loki lists", w, app)
		}
	}
}
