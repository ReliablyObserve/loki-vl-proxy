//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	lineFieldsFixtureOnce sync.Once
	lineFieldsFixtureApp  string
	lineFieldsFixtureEnd  time.Time
)

// ensureLineFieldsFixture pushes the same Loki push payload (JSON lines plus
// OTel structured metadata, as the UI log generator writes them) to Loki and
// to VictoriaLogs' Loki push endpoint with message parsing disabled, as the
// UI generator does. Body keys stay in the line, separate from metadata.
// Loki is flushed, since
// its index/stats counts flushed chunks only.
func ensureLineFieldsFixture(t *testing.T) (string, time.Time) {
	t.Helper()
	lineFieldsFixtureOnce.Do(func() {
		app := fmt.Sprintf("line-fields-%d", time.Now().UnixNano())
		base := time.Now().Add(-10 * time.Minute).Truncate(time.Minute)
		meta := map[string]string{"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736", "k8s.pod.name": "line-fields-pod-1"}
		var values []interface{}
		for i, line := range []string{
			`{"msg":"login ok","user":"u1","status":200}`,
			`{"msg":"login failed","user":"u2","status":401}`,
			`{"msg":"logout","user":"u1","status":200}`,
		} {
			ts := strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10)
			values = append(values, []interface{}{ts, line, meta})
		}
		labels := map[string]string{"app": app, "env": "line-fields"}
		for _, target := range []struct {
			url    string
			values []interface{}
		}{
			{lokiURL + "/loki/api/v1/push", values},
			{vlURL + "/insert/loki/api/v1/push?disable_message_parsing=1", values},
		} {
			body, _ := json.Marshal(map[string]interface{}{"streams": []map[string]interface{}{{"stream": labels, "values": target.values}}})
			resp, err := http.Post(target.url, "application/json", strings.NewReader(string(body)))
			if err != nil {
				t.Fatalf("push %s: %v", target.url, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("push %s: status %d", target.url, resp.StatusCode)
			}
		}
		forceVLFlush(t)
		// Loki's index/stats counts flushed chunks only.
		if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
			t.Fatalf("Loki flush: %d %s", status, body)
		}
		params := lineFieldsWindow(base.Add(2 * time.Minute))
		params.Set("query", fmt.Sprintf(`{app=%q}`, app))
		deadline := time.Now().Add(90 * time.Second)
		for {
			var stats struct{ Entries int }
			status, body := rejectedQueryGet(t, lokiURL, "/loki/api/v1/index/stats", params, "0", nil)
			if status == http.StatusOK && json.Unmarshal(body, &stats) == nil && stats.Entries == 3 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Loki index/stats never counted the flushed fixture: %d %s", status, body)
			}
			time.Sleep(2 * time.Second)
		}
		lineFieldsFixtureApp, lineFieldsFixtureEnd = app, base.Add(2*time.Minute)
	})
	if lineFieldsFixtureApp == "" {
		t.Fatal("line-fields fixture not ingested")
	}
	return lineFieldsFixtureApp, lineFieldsFixtureEnd
}

// lineFieldsWindow is a window holding the whole fixture.
func lineFieldsWindow(end time.Time) url.Values {
	return url.Values{
		"start": {strconv.FormatInt(end.Add(-5*time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
}

// metadataCategories returns, per log line, the structuredMetadata and parsed
// keys of a categorize-labels query_range answer (detected_level left out).
func metadataCategories(t *testing.T, base, query string, end time.Time) map[string][2][]string {
	t.Helper()
	params := lineFieldsWindow(end)
	params.Set("query", query)
	params.Set("limit", "100")
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", params, "0", map[string]string{"X-Loki-Response-Encoding-Flags": "categorize-labels"})
	var resp struct {
		Data struct {
			Result []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
		t.Fatalf("%s %s: %d %.300s", base, query, status, body)
	}
	out := map[string][2][]string{}
	for _, r := range resp.Data.Result {
		for _, v := range r.Values {
			var line string
			_ = json.Unmarshal(v[1], &line)
			var meta struct {
				StructuredMetadata map[string]string `json:"structuredMetadata"`
				Parsed             map[string]string `json:"parsed"`
			}
			if len(v) > 2 {
				_ = json.Unmarshal(v[2], &meta)
			}
			keys := func(m map[string]string) []string {
				out := []string{}
				for k := range m {
					if k != "detected_level" {
						out = append(out, k)
					}
				}
				sort.Strings(out)
				return out
			}
			out[line] = [2][]string{keys(meta.StructuredMetadata), keys(meta.Parsed)}
		}
	}
	if len(out) != 3 {
		t.Fatalf("%s %s: %d lines, want the fixture's 3: %.300s", base, query, len(out), body)
	}
	return out
}

// TestCompat_LokiPushLineFieldsLikeLoki compares the Loki-compatible proxies
// with Loki on data both hold identically (JSON lines with OTel structured
// metadata, pushed through each backend's Loki push API):
//   - a log query without a parser returns no parsed labels, only the
//     structured metadata; with | json the line's keys are parsed labels;
//   - detected_fields lists structured metadata with parsers null and the
//     line's keys with parsers ["json"] and their jsonPath;
//   - /labels for the stream is Loki's sorted list;
//   - index/stats counts the window's entries and streams.
//
// conformance: profiles/parsed-fields-without-parser, profiles/detected-fields-structured-metadata-beside-json-line, semantics/detected-fields-json-body-filter, semantics/label-filter-before-parser-reads-ingest-json-key, loki_api_v1_query_range, loki_api_v1_detected_fields, loki_api_v1_labels, loki_api_v1_index_stats
func TestCompat_LokiPushLineFieldsLikeLoki(t *testing.T) {
	app, end := ensureLineFieldsFixture(t)
	selector := fmt.Sprintf(`{app=%q}`, app)

	lokiPlain := metadataCategories(t, lokiURL, selector, end)
	lokiJSON := metadataCategories(t, lokiURL, selector+" | json", end)
	lokiFields := matrixDetectedFields(t, lokiURL, selector)
	filtered := selector + ` | json | user="u1"`
	lokiFilteredFields := matrixDetectedFields(t, lokiURL, filtered)
	if field := lokiFilteredFields["user"]; !reflect.DeepEqual(field.Parsers, []string{"json"}) {
		t.Fatalf("Loki must find the body-only user in the filtered sample, got %+v", field)
	}
	for _, cats := range lokiPlain {
		if len(cats[0]) == 0 || len(cats[1]) != 0 {
			t.Fatalf("Loki fixture drifted: plain selector metadata %v parsed %v", cats[0], cats[1])
		}
	}

	for _, target := range []struct{ name, url string }{
		{"parity (13100)", proxyURL},
		{"drilldown default (13110)", patternsAutodetectProxyURL},
	} {
		t.Run(target.name, func(t *testing.T) {
			if got := matrixDetectedFields(t, target.url, filtered); !reflect.DeepEqual(got, lokiFilteredFields) {
				t.Errorf("post-parser filter: proxy fields %+v; Loki %+v", got, lokiFilteredFields)
			}
			if got := metadataCategories(t, target.url, selector, end); !reflect.DeepEqual(got, lokiPlain) {
				t.Errorf("plain selector: [structuredMetadata parsed] per line\nproxy %v\nloki  %v", got, lokiPlain)
			}
			if got := metadataCategories(t, target.url, selector+" | json", end); !reflect.DeepEqual(got, lokiJSON) {
				t.Errorf("| json: [structuredMetadata parsed] per line\nproxy %v\nloki  %v", got, lokiJSON)
			}

			// Fields must not query body-only keys as metadata. Exercise the
			// actual per-field metric before/after parsing, on a repeat and a
			// moving window, rather than checking only detected_fields names.
			for _, pipeline := range []string{` | user!=""`, ` | json | __error__="" | user!=""`, ` | trace_id!=""`} {
				field := "user"
				if strings.Contains(pipeline, "trace_id") {
					field = "trace_id"
				}
				query := fmt.Sprintf("sum by (%s) (count_over_time(%s%s [5m]))", field, selector, pipeline)
				for _, shift := range []time.Duration{0, 0, 30 * time.Second} {
					params := lineFieldsWindow(end.Add(shift))
					params.Set("query", query)
					params.Set("step", "60s")
					var answers [2]map[string]any
					for i, base := range []string{lokiURL, target.url} {
						status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", params, "0", map[string]string{"X-Query-Tags": "Source=grafana-lokiexplore-app"})
						if status != http.StatusOK || json.Unmarshal(body, &answers[i]) != nil || answers[i]["status"] != "success" || answers[i]["warnings"] != nil {
							t.Fatalf("%s %s: %d %.500s", base, query, status, body)
						}
					}
					// Canonicalize series order without weakening label/value comparisons.
					canonical := func(answer map[string]any) string {
						data := answer["data"].(map[string]any)
						var rows []string
						for _, row := range data["result"].([]any) {
							b, _ := json.Marshal(row)
							rows = append(rows, string(b))
						}
						sort.Strings(rows)
						return strings.Join(rows, "\n")
					}
					if got, want := canonical(answers[1]), canonical(answers[0]); got != want {
						t.Fatalf("%s shifted %s: proxy %s; Loki %s", query, shift, got, want)
					}
					if pipeline == ` | user!=""` && canonical(answers[0]) != "" {
						t.Fatal("body-only user unexpectedly passed Loki's pre-parser filter")
					}
					if pipeline != ` | user!=""` {
						data := answers[0]["data"].(map[string]any)["result"].([]any)
						want := map[string]string{"u1": "2", "u2": "1"}
						if field == "trace_id" {
							want = map[string]string{"4bf92f3577b34da6a3ce929d0e0e4736": "3"}
						}
						if len(data) != len(want) {
							t.Fatalf("Loki fixture: %s returned %d series, want %d", query, len(data), len(want))
						}
						for _, row := range data {
							series := row.(map[string]any)
							label := series["metric"].(map[string]any)[field].(string)
							found := false
							for _, raw := range series["values"].([]any) {
								found = found || raw.([]any)[1] == want[label]
							}
							if !found || want[label] == "" {
								t.Fatalf("Loki fixture: %s missing full count %q for %s=%q", query, want[label], field, label)
							}
						}
					}
				}
			}

			fields := matrixDetectedFields(t, target.url, selector)
			for label, want := range lokiFields {
				if label == "detected_level" {
					continue
				}
				got, ok := fields[label]
				if !ok || fmt.Sprint(got.Parsers, got.JSONPath) != fmt.Sprint(want.Parsers, want.JSONPath) {
					t.Errorf("detected_fields %s: proxy %+v, loki %+v", label, got, want)
				}
			}

			params := lineFieldsWindow(end)
			params.Set("query", selector)
			var proxyLabels, lokiLabels struct{ Data []string }
			for _, side := range []struct {
				base string
				out  *struct{ Data []string }
			}{{target.url, &proxyLabels}, {lokiURL, &lokiLabels}} {
				status, body := rejectedQueryGet(t, side.base, "/loki/api/v1/labels", params, "0", nil)
				if status != http.StatusOK || json.Unmarshal(body, side.out) != nil {
					t.Fatalf("labels %s: %d %s", side.base, status, body)
				}
			}
			if !reflect.DeepEqual(proxyLabels.Data, lokiLabels.Data) {
				t.Errorf("labels: proxy %v, loki %v", proxyLabels.Data, lokiLabels.Data)
			}

			var proxyStats, lokiStats struct{ Streams, Entries int }
			for _, side := range []struct {
				base string
				out  *struct{ Streams, Entries int }
			}{{target.url, &proxyStats}, {lokiURL, &lokiStats}} {
				status, body := rejectedQueryGet(t, side.base, "/loki/api/v1/index/stats", params, "0", nil)
				if status != http.StatusOK || json.Unmarshal(body, side.out) != nil {
					t.Fatalf("index/stats %s: %d %s", side.base, status, body)
				}
			}
			if proxyStats != lokiStats || lokiStats.Entries != 3 {
				t.Errorf("index/stats streams/entries: proxy %+v, loki %+v (fixture: 1 stream, 3 entries)", proxyStats, lokiStats)
			}
		})
	}
}
