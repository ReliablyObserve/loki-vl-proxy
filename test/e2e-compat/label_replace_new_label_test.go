//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// labelReplaceFixture is the data this test asks about: three streams of its
// own, told apart from every other test's data by a unique probe label, so
// another test's streams (an anchoring probe with no level, say) never change
// an answer here.
type labelReplaceFixture struct{ probe string }

var (
	labelReplaceOnce sync.Once
	labelReplaceData *labelReplaceFixture
)

// ensureLabelReplaceFixture ingests a JSON line every 10 seconds, for the last
// 14 minutes, into one stream per app (two hyphenated names and one without) in
// Loki and VictoriaLogs, and returns once both count every line and both answer
// exactly the three series of the fixture.
func ensureLabelReplaceFixture(t *testing.T) *labelReplaceFixture {
	t.Helper()
	labelReplaceOnce.Do(func() {
		now := time.Now()
		probe := fmt.Sprintf("lrnew%d", now.UnixNano())
		var vlRows strings.Builder
		var lokiStreams []any
		total := 0
		for _, app := range []string{"api-gateway", "web", "db-main"} {
			labels := map[string]string{"probe": probe, "app": app}
			var values [][]string
			for at := now.Add(-14 * time.Minute); !at.After(now); at = at.Add(10 * time.Second) {
				line := fmt.Sprintf(`{"msg":"tick","at":%d}`, at.UnixNano())
				values = append(values, []string{strconv.FormatInt(at.UnixNano(), 10), line})
				row := map[string]string{"_time": at.UTC().Format(time.RFC3339Nano), "_msg": line, "probe": probe, "app": app}
				encoded, _ := json.Marshal(row)
				vlRows.Write(encoded)
				vlRows.WriteByte('\n')
			}
			total += len(values)
			lokiStreams = append(lokiStreams, map[string]any{"stream": labels, "values": values})
		}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields="+url.QueryEscape("probe,app"), vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
		if status != http.StatusOK {
			t.Fatalf("VL ingest: %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"}); status != http.StatusNoContent {
			t.Fatalf("Loki ingest: %d %s", status, body)
		}
		forceVLFlush(t)
		// Both backends hold every line, and Loki's metric path answers (it
		// answers empty for a few minutes after a fresh stack starts), before
		// any comparison. Each answers one series per app, no more: nothing
		// else carries this probe.
		deadline := time.Now().Add(240 * time.Second)
		for {
			ready := true
			for _, base := range []string{lokiURL, proxyURL} {
				params := url.Values{"query": {fmt.Sprintf(`sum by (app) (count_over_time({probe=%q}[20m]))`, probe)}, "time": {strconv.FormatInt(time.Now().Unix(), 10)}}
				_, _, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
				result := extractArray(extractMap(resp, "data"), "result")
				sum := 0.0
				for _, item := range result {
					sample, _ := item.(map[string]any)
					value, _ := sample["value"].([]any)
					if len(value) == 2 {
						v, _ := strconv.ParseFloat(fmt.Sprint(value[1]), 64)
						sum += v
					}
				}
				if len(result) != 3 || sum != float64(total) {
					ready = false
				}
			}
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("fixture not ready: want 3 series holding %d lines on Loki and on the proxy", total)
			}
			time.Sleep(2 * time.Second)
		}
		labelReplaceData = &labelReplaceFixture{probe: probe}
	})
	if labelReplaceData == nil {
		t.Fatal("the label_replace fixture was not ingested; see the first test that ran")
	}
	return labelReplaceData
}

// label_replace sets, rewrites or removes one label of every series its
// expression yields (pkg/logql/evaluator.go LabelReplaceEvaluator.Next): the
// regexp is anchored, a series it does not match is left alone, an empty
// replacement removes the destination, and series that end with one label set
// are not an error (pkg/logql/engine.go JoinSampleVector files their points
// under one series on a range query, keeps each sample on an instant query).
//
// The proxy's instant answers lacked the destination label (the transform only
// ran on range matrices), an escaped regexp such as `\.` never matched, and a
// label_replace around a binary expression on query_range sent the proxy's
// internal binary marker to VictoriaLogs.
//
// Every shape is asked of Loki and of the proxy on query_range and on query.
// Both must answer the same status; when they answer, the same result type, the
// same label sets (and, on query, the same number of samples per label set). The
// values and the points per step are compared by the other registry items.
// conformance: semantics/label-replace-new-label, semantics/label-replace-instant-vector, semantics/label-replace-range-binary-marker, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_LabelReplaceNewLabel(t *testing.T) {
	fx := ensureLabelReplaceFixture(t)

	// The structured metadata of the fixture streams that Loki adds to a bare
	// range function's series (see the bare field below).
	structuredMetadataLabels := map[string]bool{
		"cloud_provider": true, "cloud_region": true, "deployment_name": true, "deployment_version": true,
		"host_name": true, "k8s_node_name": true, "queue_name": true, "request_id": true, "span_id": true,
		"telemetry_sdk_language": true, "telemetry_sdk_name": true, "telemetry_sdk_version": true,
		"trace_id": true, "user_agent": true, "worker_id": true,
	}

	sel := fmt.Sprintf(`{probe=%q}`, fx.probe)
	apiSel := fmt.Sprintf(`{probe=%q, app="api-gateway"}`, fx.probe)
	count := `count_over_time(` + sel + `[5m])`
	rate := `rate(` + sel + `[5m])`
	agg := `sum by (app) (` + count + `)`
	lr := func(inner, dst, repl, src, re string) string {
		return fmt.Sprintf(`label_replace(%s, %q, %q, %q, %q)`, inner, dst, repl, src, re)
	}
	shapes := []struct {
		name  string
		query string
		// points compares the points per series of a range answer as well:
		// where two series end with one label set, Loki files the points of
		// both under one series.
		points bool
		// bare marks a range function with no aggregation: Loki's series carry
		// the entry's structured metadata and the proxy's keep the stream
		// (registry: metric-identity/structured-metadata-splits-series, an
		// owner-accepted deviation), so those labels are left out of both
		// sides. The destination label, the stream labels and detected_level
		// are still compared.
		bare bool
	}{
		// the request of the registry row, its bare form, and the clusters' forms
		{"row", lr(`sum by (app) (rate(`+apiSel+`[5m]))`, "app_short", "$1", "app", "(.*)-.*"), false, false},
		{"bare rate", lr(`rate(`+apiSel+`[5m])`, "app_short", "$1", "app", "(.*)-.*"), false, true},
		{"bare count_over_time", lr(count, "app_short", "$1", "app", "(.*)-.*"), false, true},
		{"bytes_over_time", lr(`bytes_over_time(`+sel+`[5m])`, "buzz", "$2", "app", "(.*)-(.*)"), false, true},
		{"nested", lr(lr(agg, "service", "$1", "app", "(.*)"), "short", "$1", "service", "^([^-]+)"), false, false},
		{"nested, the outer reads the inner's label", lr(lr(agg, "a1", "$1", "app", "(.*)-.*"), "a2", "x$1", "a1", "(.*)"), false, false},
		{"nested, three deep", lr(lr(lr(agg, "a", "$1", "app", "(.*)-.*"), "b", "$1$1", "a", "(.*)"), "c", "${1}!", "b", "(.*)"), false, false},
		// regexp and replacement rules
		{"anchored: a prefix does not match", lr(agg, "d", "x", "app", "api"), false, false},
		{"anchored: the whole value", lr(agg, "d", "x", "app", "api-gateway"), false, false},
		{"no match keeps the series", lr(agg, "new_label", "default", "app", "^nonexistent$"), false, false},
		{"escaped regexp", lr(agg, "d", "$1", "app", `([a-z]+)\-(\w+)`), false, false},
		{"escaped dot never matches a hyphen", lr(agg, "d", "$1", "app", `(\w+)\.(\w+)`), false, false},
		{"named group", lr(agg, "d", "${head}", "app", `(?P<head>[a-z]+)-.*`), false, false},
		{"bare group name swallows the suffix", lr(agg, "d", "$1x", "app", "(.*)-.*"), false, false},
		{"two groups", lr(agg, "d", "$2/$1", "app", "([a-z]+)-([a-z]+)"), false, false},
		{"constant replacement", lr(agg, "d", "const", "app", ".*"), false, false},
		{"missing source, empty-matching regexp", lr(agg, "d", "dflt", "nope", "(.*)"), false, false},
		{"missing source, non-empty regexp", lr(agg, "d", "dflt", "nope", ".+"), false, false},
		{"empty replacement removes the destination", lr(agg, "app", "", "app", "api-gateway"), false, false},
		{"destination equal to the source", lr(agg, "app", "$1", "app", "(.*)-.*"), false, false},
		{"two series with one label set", lr(agg, "app", "all", "app", ".+"), true, false},
		{"all labels removed", lr(agg, "app", "", "app", ".*"), true, false},
		{"label names are not validated", lr(agg, "1bad", "x", "app", ".*"), false, false},
		// around and with other expressions
		{"over a binary expression", lr(agg+` * 2`, "app_short", "$1", "app", "(.*)-.*"), false, false},
		{"over a binary of two aggregations", lr(agg+` / `+agg, "app_short", "$1", "app", "(.*)-.*"), false, false},
		{"over a rate aggregation", lr(`sum by (app) (`+rate+`)`, "app_short", "$1", "app", "(.*)-.*"), false, false},
		{"over a parser", lr(`sum by (app) (count_over_time(`+sel+` | json | __error__="" [5m]))`, "app_short", "$1", "app", "(.*)-.*"), false, false},
		{"under sort", `sort(` + lr(agg, "app_short", "$1", "app", "(.*)-.*") + `)`, false, false},
		{"under topk", `topk(1, ` + lr(agg, "app_short", "$1", "app", "(.*)-.*") + `)`, false, false},
		// an invalid regexp is Loki's parse error
		{"invalid regexp", lr(agg, "new", "$1", "app", "[invalid"), false, false},
		{"invalid regexp, nested", lr(lr(agg, "d", "$1", "app", "(.*)"), "e", "$1", "d", "(unclosed"), false, false},
		{"invalid regexp under topk", `topk(2, ` + lr(agg, "new", "$1", "app", "*bad") + `)`, false, false},
		// a call the grammar rejects
		{"invalid escape in a string", `label_replace(` + agg + `, "d", "$1", "app", "(.+):\d+")`, false, false},
		{"three string arguments", `label_replace(` + agg + `, "d", "$1", "app")`, false, false},
		{"five string arguments", `label_replace(` + agg + `, "d", "$1", "app", "x", "y")`, false, false},
	}

	// labelCounts reads a decoded answer as {label set: samples}: an instant
	// vector's samples per label set, a range series' points when points is
	// set and 1 otherwise (the points per step are other registry items').
	//
	// An instant answer to a bare range function lacks the detected_level=unknown
	// Loki puts on a stream with no level (semantics/instant-bare-range-detected-level-unknown,
	// open; the gap of "detected_level, unknown included, on every route"): the
	// label is left out of both sides there until it is fixed.
	labelCounts := func(payload map[string]interface{}, instant, points, bare bool) map[string]int {
		out := map[string]int{}
		data, _ := payload["data"].(map[string]interface{})
		result, _ := data["result"].([]interface{})
		for _, item := range result {
			series, _ := item.(map[string]interface{})
			metric, _ := series["metric"].(map[string]interface{})
			if bare {
				kept := map[string]interface{}{}
				for name, value := range metric {
					if !structuredMetadataLabels[name] && !(instant && name == "detected_level") {
						kept[name] = value
					}
				}
				metric = kept
			}
			key := canonicalMetricKey(metric)
			if instant {
				out[key]++
				continue
			}
			values, _ := series["values"].([]interface{})
			if points {
				out[key] += len(values)
			} else {
				out[key] = 1
			}
		}
		return out
	}
	resultType := func(payload map[string]interface{}) string {
		data, _ := payload["data"].(map[string]interface{})
		rt, _ := data["resultType"].(string)
		return rt
	}
	render := func(m map[string]int) string {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, fmt.Sprintf("%s x%d", k, m[k]))
		}
		sort.Strings(keys)
		return strings.Join(keys, "\n  ")
	}

	for _, shape := range shapes {
		for _, endpoint := range []string{"query_range", "query"} {
			t.Run(shape.name+"/"+endpoint, func(t *testing.T) {
				instant := endpoint == "query"
				at := time.Now().UTC().Truncate(time.Minute)
				params := url.Values{"query": {shape.query}}
				if instant {
					params.Set("time", fmt.Sprintf("%d", at.UnixNano()))
				} else {
					params.Set("start", fmt.Sprintf("%d", at.Add(-15*time.Minute).UnixNano()))
					params.Set("end", fmt.Sprintf("%d", at.UnixNano()))
					params.Set("step", "60")
				}
				headers := map[string]string{"X-Scope-OrgID": "0", "X-Loki-Response-Encoding-Flags": "categorize-labels"}
				full := "/loki/api/v1/" + endpoint + "?" + params.Encode()
				lokiStatus, lokiBody, lokiPayload := doJSONGET(t, lokiURL+full, headers)
				proxyStatus, proxyBody, proxyPayload := doJSONGET(t, proxyURL+full, headers)
				if lokiStatus != proxyStatus {
					t.Fatalf("HTTP status loki=%d proxy=%d\nloki:  %s\nproxy: %s", lokiStatus, proxyStatus, lokiBody, proxyBody)
				}
				if lokiStatus != http.StatusOK {
					if lokiStatus == http.StatusBadRequest && !strings.Contains(proxyBody, "parse error") {
						t.Errorf("the proxy's 400 is not Loki's parse error\nloki:  %s\nproxy: %s", lokiBody, proxyBody)
					}
					if strings.Contains(shape.name, "invalid regexp") && !strings.Contains(proxyBody, "invalid regex in label_replace") {
						t.Errorf("the proxy's error is not Loki's invalid regex error\nloki:  %s\nproxy: %s", lokiBody, proxyBody)
					}
					return
				}
				if lt, pt := resultType(lokiPayload), resultType(proxyPayload); lt != pt {
					t.Fatalf("resultType loki=%q proxy=%q\nloki:  %s\nproxy: %s", lt, pt, lokiBody, proxyBody)
				}
				lokiSeries, proxySeries := labelCounts(lokiPayload, instant, shape.points, shape.bare), labelCounts(proxyPayload, instant, shape.points, shape.bare)
				if len(lokiSeries) == 0 {
					t.Skipf("Loki answered no series for %s; nothing to compare", shape.query)
				}
				// The fixture holds one stream per app: the series are known.
				if want, ok := map[string]int{"row": 1, "bare rate": 1, "bare count_over_time": 3, "bytes_over_time": 3, "nested": 3}[shape.name]; ok && (len(lokiSeries) != want || len(proxySeries) != want) {
					t.Errorf("series: loki=%d proxy=%d, want %d (one per stream of the fixture)\nloki:\n  %s\nproxy:\n  %s", len(lokiSeries), len(proxySeries), want, render(lokiSeries), render(proxySeries))
				}
				if render(lokiSeries) != render(proxySeries) {
					t.Errorf("label sets differ\nloki:\n  %s\nproxy:\n  %s", render(lokiSeries), render(proxySeries))
				}
			})
		}
	}
}

// Two series that a label_replace gives one label set are ranked as two
// samples by the vector aggregation above it, and filed under one series only
// at the end (pkg/logql/evaluator.go VectorAggEvaluator.Next, pkg/logql/engine.go
// vectorsToSeries). The proxy merges them before the ranking
// (semantics/label-replace-duplicates-before-ranking, open).
// conformance: semantics/label-replace-duplicates-before-ranking, loki_api_v1_query_range
func TestCompat_LabelReplaceDuplicatesBeforeRanking(t *testing.T) {
	ensureDataIngested(t)
	waitForLokiMetricData(t)

	at := time.Now().UTC().Truncate(time.Minute)
	for _, query := range []string{
		`topk(1, label_replace(sum by (app) (count_over_time({env="production"}[5m])), "app", "all", "app", ".+"))`,
		`bottomk(1, label_replace(sum by (app) (count_over_time({env="production"}[5m])), "app", "all", "app", ".+"))`,
	} {
		params := url.Values{
			"query": {query},
			"start": {fmt.Sprintf("%d", at.Add(-15*time.Minute).UnixNano())},
			"end":   {fmt.Sprintf("%d", at.UnixNano())},
			"step":  {"60"},
		}
		full := "/loki/api/v1/query_range?" + params.Encode()
		lokiStatus, lokiBody, lokiPayload := doJSONGET(t, lokiURL+full, nil)
		proxyStatus, proxyBody, proxyPayload := doJSONGET(t, proxyURL+full, nil)
		if lokiStatus != http.StatusOK || proxyStatus != http.StatusOK {
			t.Fatalf("%s: HTTP loki=%d proxy=%d\nloki:  %s\nproxy: %s", query, lokiStatus, proxyStatus, lokiBody, proxyBody)
		}
		count := func(payload map[string]interface{}) int {
			data, _ := payload["data"].(map[string]interface{})
			result, _ := data["result"].([]interface{})
			points := 0
			for _, item := range result {
				series, _ := item.(map[string]interface{})
				values, _ := series["values"].([]interface{})
				points += len(values)
			}
			return points
		}
		if l, p := count(lokiPayload), count(proxyPayload); l != p {
			t.Errorf("%s: points loki=%d proxy=%d\nloki:  %s\nproxy: %s", query, l, p, lokiBody, proxyBody)
		}
	}
}
