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
// /labels and /label/{name}/values over 7d, 24h and 6h windows ending now, scoped
// to an app and unscoped, list a stream written after earlier identical requests
// were answered and cached (non-empty, so with the window-scaled TTL of up to an
// hour). The proxy used to serve the cached answer until it expired.
//
// conformance: semantics/metadata-answers-include-last-24h-like-loki
func TestCompat_LabelsIncludeStreamWrittenAfterCaching(t *testing.T) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36)
	app := "label-fresh-" + id
	uniq := "lfresh_" + id
	windows := []time.Duration{7 * 24 * time.Hour, 24 * time.Hour, 6 * time.Hour}
	selector := fmt.Sprintf(`{app=~%q}`, app)

	get := func(base, path string, window time.Duration, scoped bool) []string {
		now := time.Now()
		q := url.Values{}
		q.Set("start", strconv.FormatInt(now.Add(-window).UnixNano(), 10))
		q.Set("end", strconv.FormatInt(now.UnixNano(), 10))
		if scoped {
			q.Set("query", selector)
		}
		return labelsFullRangeStrings(t, base+path+"?"+q.Encode())
	}
	ingest := func(labels map[string]string, msg string) {
		ts := time.Now()
		row := map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": msg}
		fields := make([]string, 0, len(labels))
		for k, v := range labels {
			row[k] = v
			fields = append(fields, k)
		}
		slices.Sort(fields)
		encoded, _ := json.Marshal(row)
		status, body := hardeningRequest(t, http.MethodPost,
			vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape(strings.Join(fields, ",")), string(encoded)+"\n",
			map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VictoriaLogs ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{
			"stream": labels,
			"values": [][]string{{strconv.FormatInt(ts.UnixNano(), 10), msg}},
		}}})
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", nil); status != http.StatusOK {
			t.Fatalf("VictoriaLogs flush: %d %s", status, body)
		}
	}
	waitLoki := func(what string, ok func() bool) {
		deadline := time.Now().Add(90 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("Loki never showed %s", what)
			}
			time.Sleep(2 * time.Second)
		}
	}
	vlCount := func(want int) {
		params := url.Values{"query": {fmt.Sprintf("app:=%q | stats count() as c", app)}}
		params.Set("start", time.Now().Add(-time.Hour).Format(time.RFC3339Nano))
		params.Set("end", time.Now().Add(time.Minute).Format(time.RFC3339Nano))
		if body := labelsFullRangeGet(t, vlURL+"/select/logsql/query?"+params.Encode()); !strings.Contains(string(body), fmt.Sprintf(`"c":"%d"`, want)) {
			t.Fatalf("VictoriaLogs holds %s rows, want %d: %s", app, want, body)
		}
	}

	// First stream for the app: every later answer is non-empty and cached with
	// the window-scaled TTL.
	ingest(map[string]string{"app": app, "env": "first"}, "label freshness first "+id)
	waitLoki("the first stream", func() bool {
		return slices.Contains(get(lokiURL, "/loki/api/v1/label/app/values", 6*time.Hour, false), app)
	})
	vlCount(1)
	for _, w := range windows {
		for _, scoped := range []bool{true, false} {
			get(proxyURL, "/loki/api/v1/labels", w, scoped)
			get(proxyURL, "/loki/api/v1/label/app/values", w, scoped)
		}
	}

	// A second stream carrying a label no earlier stream has.
	ingest(map[string]string{"app": app, uniq: "v1"}, "label freshness second "+id)
	waitLoki("the second stream", func() bool {
		return slices.Contains(get(lokiURL, "/loki/api/v1/labels", 6*time.Hour, true), uniq)
	})
	vlCount(2)

	// Older than -recent-tail-refresh-max-staleness (2s) at the next request.
	time.Sleep(3 * time.Second)
	fixtureLabels := func(labels []string) []string {
		var out []string
		for _, l := range labels {
			if l == "app" || l == "env" || l == uniq {
				out = append(out, l)
			}
		}
		return out
	}
	for _, w := range windows {
		for _, scoped := range []bool{true, false} {
			name := fmt.Sprintf("%s scoped=%v", w, scoped)
			loki := get(lokiURL, "/loki/api/v1/labels", w, scoped)
			proxy := get(proxyURL, "/loki/api/v1/labels", w, scoped)
			if !slices.Contains(loki, uniq) {
				t.Fatalf("%s: Loki omits %s: %v", name, uniq, loki)
			}
			if !slices.Equal(fixtureLabels(proxy), fixtureLabels(loki)) {
				t.Errorf("%s: /labels fixture labels %v, Loki %v", name, fixtureLabels(proxy), fixtureLabels(loki))
			}
			lokiValues := get(lokiURL, "/loki/api/v1/label/"+uniq+"/values", w, scoped)
			proxyValues := get(proxyURL, "/loki/api/v1/label/"+uniq+"/values", w, scoped)
			if !slices.Equal(lokiValues, []string{"v1"}) || !slices.Equal(proxyValues, lokiValues) {
				t.Errorf("%s: /label/%s/values proxy %v, Loki %v", name, uniq, proxyValues, lokiValues)
			}
			if !slices.Contains(get(proxyURL, "/loki/api/v1/label/app/values", w, scoped), app) {
				t.Errorf("%s: /label/app/values omits %s", name, app)
			}
		}
	}
}
