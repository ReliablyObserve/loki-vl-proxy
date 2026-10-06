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
	extractedFixtureOnce sync.Once
	extractedFixtureApp  string
	extractedFixtureEnd  time.Time
)

// ensureExtractedSuffixFixture pushes the same Loki push payload to Loki and
// to VictoriaLogs' Loki push endpoint: a stream whose labels (app, level,
// service_name) are also keys of its JSON and logfmt lines, and a stream with
// OTel structured metadata service.name (Loki's service_name_extracted beside
// the service_name stream label it derives from app). VictoriaLogs keeps each
// line verbatim (disable_message_parsing), as the lines are Loki's own.
func ensureExtractedSuffixFixture(t *testing.T) (string, time.Time) {
	t.Helper()
	extractedFixtureOnce.Do(func() {
		app := fmt.Sprintf("extracted-%d", time.Now().UnixNano())
		base := time.Now().Add(-40 * time.Second)
		ts := func(i int) string { return strconv.FormatInt(base.Add(time.Duration(i)*time.Second).UnixNano(), 10) }
		keys := []interface{}{
			[]interface{}{ts(0), `{"level":"debug","app":"inner","service_name":"inner","msg":"a","user":"u1","status":200}`},
			[]interface{}{ts(1), `{"level":"error","app":"inner","service_name":"inner","msg":"b","user":"u2","status":500}`},
			[]interface{}{ts(2), `level=warn app=inner service_name=inner msg=c user=u3 status=404`},
			[]interface{}{ts(3), `level=info app=inner service_name=inner msg=d user=u4 status=200`},
			[]interface{}{ts(4), `{"msg":"plain","user":"u5"}`},
		}
		meta := map[string]string{"service.name": "meta-svc", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"}
		metadata := []interface{}{
			[]interface{}{ts(10), "metadata line 1", meta},
			[]interface{}{ts(11), "metadata line 2", meta},
		}
		streams := []map[string]interface{}{
			{"stream": map[string]string{"app": app, "env": "keys", "level": "info", "service_name": "extracted-svc"}, "values": keys},
			{"stream": map[string]string{"app": app, "env": "meta"}, "values": metadata},
		}
		body, _ := json.Marshal(map[string]interface{}{"streams": streams})
		for _, target := range []string{lokiURL + "/loki/api/v1/push", vlURL + "/insert/loki/api/v1/push?disable_message_parsing=1"} {
			resp, err := http.Post(target, "application/json", strings.NewReader(string(body)))
			if err != nil {
				t.Fatalf("push %s: %v", target, err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				t.Fatalf("push %s: status %d", target, resp.StatusCode)
			}
		}
		forceVLFlush(t)
		extractedFixtureApp, extractedFixtureEnd = app, base.Add(15*time.Second)
	})
	if extractedFixtureApp == "" {
		t.Fatal("extracted-suffix fixture not ingested")
	}
	return extractedFixtureApp, extractedFixtureEnd
}

// extractedEntry is one log line's labels: the stream's, and with
// categorize-labels the entry's structured metadata and parsed labels. The
// parser error labels are left out, as the parse-error rows are their own gap.
type extractedEntry struct {
	Stream, Metadata, Parsed map[string]string
}

func extractedLogEntries(t *testing.T, base, query string, end time.Time, span time.Duration, categorize bool, want int) map[string]extractedEntry {
	t.Helper()
	params := url.Values{
		"query": {query}, "limit": {"100"},
		"start": {strconv.FormatInt(end.Add(-span).UnixNano(), 10)},
		"end":   {strconv.FormatInt(end.UnixNano(), 10)},
	}
	headers := map[string]string{}
	if categorize {
		headers["X-Loki-Response-Encoding-Flags"] = "categorize-labels"
	}
	var out map[string]extractedEntry
	deadline := time.Now().Add(20 * time.Second)
	for {
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", params, "0", headers)
		var resp struct {
			Data struct {
				Result []struct {
					Stream map[string]string   `json:"stream"`
					Values [][]json.RawMessage `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
			t.Fatalf("%s %s: %d %.300s", base, query, status, body)
		}
		out = map[string]extractedEntry{}
		clean := func(m map[string]string) map[string]string {
			c := map[string]string{}
			for k, v := range m {
				if k != "__error__" && k != "__error_details__" {
					c[k] = v
				}
			}
			return c
		}
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
				out[line] = extractedEntry{clean(r.Stream), clean(meta.StructuredMetadata), clean(meta.Parsed)}
			}
		}
		if len(out) == want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(time.Second)
	}
}

// extractedSeries returns an instant metric query's series and values by the
// series' label set.
func extractedSeries(t *testing.T, base, query string, end time.Time) map[string]string {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}
	status, body := rejectedQueryGet(t, base, "/loki/api/v1/query", params, "0", nil)
	var resp struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &resp) != nil {
		t.Fatalf("%s %s: %d %.300s", base, query, status, body)
	}
	out := map[string]string{}
	for _, r := range resp.Data.Result {
		names := make([]string, 0, len(r.Metric))
		for k, v := range r.Metric {
			names = append(names, k+"="+v)
		}
		sort.Strings(names)
		out["{"+strings.Join(names, ",")+"}"] = fmt.Sprint(r.Value[1])
	}
	return out
}

// TestCompat_ExtractedSuffixOnParsedCollision: a key a parser stage reads
// from the line that is also a stream label's name leaves the stream label
// alone and shows as name_extracted (a collision Loki renames: pkg/logql/log
// parser.go duplicateSuffix); structured metadata named like a stream label
// gets the suffix too. Compared name=value with Loki for JSON, logfmt, regexp
// and extraction-list stages, label filters, drop and metric grouping on the
// renamed label, in the default and categorize-labels encodings.
//
// conformance: semantics/extracted-suffix-collision, profiles/structured-metadata-label-collision, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_ExtractedSuffixOnParsedCollision(t *testing.T) {
	app, end := ensureExtractedSuffixFixture(t)
	keys := fmt.Sprintf(`{app=%q,env="keys"}`, app)
	meta := fmt.Sprintf(`{app=%q,env="meta"}`, app)

	logQueries := []struct {
		name, query string
		lines       int
		span        time.Duration
	}{
		{"json", keys + ` | json`, 5, 5 * time.Minute},
		{"json over a range split into windows", keys + ` | json`, 5, 30 * time.Hour},
		{"logfmt", keys + ` | logfmt`, 5, 5 * time.Minute},
		{"no parser", keys, 5, 5 * time.Minute},
		{"regexp capture", keys + ` | regexp "level=(?P<level>\\w+)"`, 5, 5 * time.Minute},
		{"json extraction list", keys + ` | json level, app`, 5, 5 * time.Minute},
		{"json filter on the renamed label", keys + ` | json | level_extracted="debug"`, 1, 5 * time.Minute},
		{"json filter on the renamed label over windows", keys + ` | json | level_extracted="debug"`, 1, 30 * time.Hour},
		{"filter on the renamed label of an extraction list", keys + ` | json level | level_extracted="debug"`, 1, 5 * time.Minute},
		{"filter on a label an extraction list does not name", keys + ` | json status | level_extracted="debug"`, 0, 5 * time.Minute},
		{"logfmt existence filter on the renamed label", keys + ` | logfmt | level_extracted!=""`, 2, 5 * time.Minute},
		{"json service_name_extracted filter", keys + ` | json | service_name_extracted!=""`, 2, 5 * time.Minute},
		{"negated filter on the renamed label", keys + ` | json | level_extracted!="debug"`, 4, 5 * time.Minute},
		{"or after the renamed label", keys + ` | json | level_extracted="debug" or status="500"`, 2, 5 * time.Minute},
		{"or before the renamed label", keys + ` | json | status="404" or level_extracted="error"`, 1, 5 * time.Minute},
		{"and with the renamed label", keys + ` | json | status="500" and level_extracted="error"`, 1, 5 * time.Minute},
		{"filter after line_format", keys + ` | json | line_format "{{.msg}}" | level_extracted="debug"`, 1, 5 * time.Minute},
		{"a label the query sets wins", keys + ` | json | label_format level_extracted="z"`, 5, 5 * time.Minute},
		{"drop of the renamed label", keys + ` | json | drop level_extracted`, 5, 5 * time.Minute},
		{"structured metadata against the derived service_name", meta, 2, 5 * time.Minute},
	}
	for _, categorize := range []bool{false, true} {
		for _, q := range logQueries {
			name := q.name + map[bool]string{false: "/default", true: "/categorize-labels"}[categorize]
			t.Run(name, func(t *testing.T) {
				loki := extractedLogEntries(t, lokiURL, q.query, end, q.span, categorize, q.lines)
				if len(loki) != q.lines {
					t.Fatalf("Loki fixture: %d entries for %s, want %d", len(loki), q.query, q.lines)
				}
				proxy := extractedLogEntries(t, proxyURL, q.query, end, q.span, categorize, q.lines)
				if !reflect.DeepEqual(proxy, loki) {
					t.Errorf("%s\nproxy %+v\nloki  %+v", q.query, proxy, loki)
				}
			})
		}
	}

	for _, q := range []string{
		fmt.Sprintf(`sum by (level_extracted) (count_over_time(%s | logfmt [5m]))`, keys),
		fmt.Sprintf(`sum by (level_extracted) (count_over_time(%s | json | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum by (app_extracted, level_extracted) (count_over_time(%s | json | __error__="" [5m]))`, keys),
		fmt.Sprintf(`sum(count_over_time(%s | json | level_extracted="debug" [5m]))`, keys),
		fmt.Sprintf(`sum(count_over_time(%s | logfmt | level_extracted="warn" [5m]))`, keys),
		fmt.Sprintf(`count_over_time(%s | json | __error__="" [5m])`, keys),
		fmt.Sprintf(`count_over_time(%s | logfmt | level_extracted!="" [5m])`, keys),
	} {
		t.Run(q, func(t *testing.T) {
			var loki map[string]string
			deadline := time.Now().Add(150 * time.Second)
			for {
				if loki = extractedSeries(t, lokiURL, q, end); len(loki) > 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if len(loki) == 0 {
				t.Fatalf("Loki answered no series for %s", q)
			}
			if proxy := extractedSeries(t, proxyURL, q, end); !reflect.DeepEqual(proxy, loki) {
				t.Errorf("%s\nproxy %v\nloki  %v", q, proxy, loki)
			}
		})
	}
}
