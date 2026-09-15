//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type parsedStreamsLiveResponse struct {
	Status   string   `json:"status"`
	Warnings []string `json:"warnings"`
	Data     struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string   `json:"stream"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// parsedStreamsLiveEntry is one returned log entry reduced to what both
// backends must agree on. Parsed is only populated for categorize-labels.
type parsedStreamsLiveEntry struct {
	Ts     string
	Line   string
	Parsed map[string]string
}

// TestPipeline_ParsedLabelStreamsAcrossQueryWindows proves that | json and
// | logfmt log queries keep Loki's stream identity (stream labels plus
// extracted labels) when the requested range is split into several upstream
// windows. The same fixture lines are pushed to Loki and VictoriaLogs; every
// case compares the proxy against Loki for a single-window range (30m) and
// ranges that cross split boundaries (61m, 3h).
func TestPipeline_ParsedLabelStreamsAcrossQueryWindows(t *testing.T) {
	headers := map[string]string{"X-Scope-OrgID": "0"}
	// Hour-aligned base keeps the 30m range (:25-:55) inside one proxy window and makes
	// the longer ranges cross hour boundaries. 10h back is outside Loki's
	// ingester query window, so the store path is exercised after flush.
	base := time.Now().Add(-10 * time.Hour).UTC().Truncate(time.Hour).Add(time.Minute)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	const lines = 36
	statuses := []int{200, 404, 500, 503}
	levels := []string{"info", "warn", "error"}
	paths := []string{"/a", "/b"}

	type fixture struct {
		app  string
		line func(i int) string
	}
	fixtures := map[string]fixture{
		"json": {app: "parsed-streams-json-" + suffix, line: func(i int) string {
			return fmt.Sprintf(`{"level":%q,"msg":"req %d","path":%q,"status":%d}`, levels[i%3], i%3, paths[i%2], statuses[i%4])
		}},
		"logfmt": {app: "parsed-streams-logfmt-" + suffix, line: func(i int) string {
			return fmt.Sprintf("level=%s msg=req%d path=%s status=%d duration_ms=%d", levels[i%3], i%3, paths[i%2], statuses[i%4], 1000*(i%9))
		}},
		// Without a level in the line, Loki keys categorize-labels streams by
		// the pushed stream labels only, which the proxy must match.
		"json_nolevel": {app: "parsed-streams-json-nolevel-" + suffix, line: func(i int) string {
			return fmt.Sprintf(`{"msg":"req %d","path":%q,"status":%d}`, i%3, paths[i%2], statuses[i%4])
		}},
	}

	for _, fx := range fixtures {
		rows := make([]string, 0, lines)
		values := make([][]string, 0, lines)
		for i := 0; i < lines; i++ {
			ts := base.Add(time.Duration(i) * 5 * time.Minute)
			line := fx.line(i)
			row, _ := json.Marshal(map[string]string{"_time": ts.Format(time.RFC3339Nano), "_msg": line, "app": fx.app})
			rows = append(rows, string(row))
			values = append(values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
		}
		vlHeaders := map[string]string{"X-Scope-OrgID": "0", "Content-Type": "application/stream+json"}
		status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app", strings.Join(rows, "\n")+"\n", vlHeaders)
		if status != http.StatusOK {
			t.Fatalf("VL ingest %s: %d %s", fx.app, status, body)
		}
		payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"app": fx.app}, "values": values}}})
		lokiHeaders := map[string]string{"X-Scope-OrgID": "0", "Content-Type": "application/json"}
		status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), lokiHeaders)
		if status != http.StatusNoContent {
			t.Fatalf("Loki ingest %s: %d %s", fx.app, status, body)
		}
	}
	if status, body := hardeningRequest(t, http.MethodPost, vlURL+"/internal/force_flush", "", headers); status != http.StatusOK {
		t.Fatalf("VL flush: %d %s", status, body)
	}
	// Loki only serves ranges older than query_ingesters_within from flushed chunks.
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", headers); status != http.StatusNoContent && status != http.StatusOK {
		t.Fatalf("Loki flush: %d %s", status, body)
	}

	// Range ends sit between entries so end-inclusivity cannot change counts.
	ranges := []struct {
		name       string
		start, end time.Time
	}{
		{name: "30m", start: base.Add(24*time.Minute + 30*time.Second), end: base.Add(54*time.Minute + 30*time.Second)},
		{name: "61m", start: base.Add(-time.Minute), end: base.Add(60*time.Minute + 30*time.Second)},
		{name: "3h", start: base.Add(-time.Minute), end: base.Add(175*time.Minute + 30*time.Second)},
	}
	full := ranges[len(ranges)-1]

	query := func(t *testing.T, target, logql string, start, end time.Time, direction string, limit int, categorized bool) parsedStreamsLiveResponse {
		t.Helper()
		params := url.Values{
			"query":     {logql},
			"start":     {strconv.FormatInt(start.UnixNano(), 10)},
			"end":       {strconv.FormatInt(end.UnixNano(), 10)},
			"limit":     {strconv.Itoa(limit)},
			"direction": {direction},
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target+"/loki/api/v1/query_range?"+params.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Scope-OrgID", "0")
		if categorized {
			req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
		}
		httpResp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
		_ = httpResp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: status=%d body=%s", target, logql, httpResp.StatusCode, body)
		}
		// A proxy answer assembled after an upstream window failure is not a
		// valid parity sample.
		if httpResp.Header.Get("X-Loki-VL-Partial-Response") != "" || httpResp.Header.Get("Warning") != "" {
			t.Fatalf("%s %s: partial or degraded response headers=%v", target, logql, httpResp.Header)
		}
		var resp parsedStreamsLiveResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("%s %s: decode: %v body=%s", target, logql, err, body)
		}
		if resp.Status != "success" || resp.Data.ResultType != "streams" || len(resp.Warnings) > 0 {
			t.Fatalf("%s %s: unhealthy response: %s", target, logql, body)
		}
		return resp
	}
	entryCount := func(resp parsedStreamsLiveResponse) int {
		n := 0
		for _, stream := range resp.Data.Result {
			n += len(stream.Values)
		}
		return n
	}

	// Both backends must hold exactly the fixture before any comparison.
	for _, fx := range fixtures {
		selector := `{app="` + fx.app + `"}`
		params := url.Values{
			"query": {`app:="` + fx.app + `" | stats count() as c`},
			"start": {full.start.Format(time.RFC3339Nano)},
			"end":   {full.end.Format(time.RFC3339Nano)},
		}
		status, body := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+params.Encode(), "", headers)
		if status != http.StatusOK {
			t.Fatalf("VL count %s: %d %s", fx.app, status, body)
		}
		var vlRow struct {
			C string `json:"c"`
		}
		if err := json.Unmarshal(body, &vlRow); err != nil {
			t.Fatalf("VL count %s: %v body=%s", fx.app, err, body)
		}
		vlCount, _ := strconv.Atoi(vlRow.C)
		if vlCount != lines {
			t.Fatalf("VL holds %d lines for %s, want %d", vlCount, fx.app, lines)
		}
		deadline := time.Now().Add(3 * time.Minute)
		for {
			lokiCount := entryCount(query(t, lokiURL, selector, full.start, full.end, "backward", 1000, false))
			if lokiCount == vlCount {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Loki holds %d lines for %s, VL holds %d", lokiCount, fx.app, vlCount)
			}
			time.Sleep(2 * time.Second)
		}
	}

	collect := func(resp parsedStreamsLiveResponse, categorized bool) map[string][]parsedStreamsLiveEntry {
		out := make(map[string][]parsedStreamsLiveEntry, len(resp.Data.Result))
		for _, stream := range resp.Data.Result {
			keys := make([]string, 0, len(stream.Stream))
			for k, v := range stream.Stream {
				keys = append(keys, k+"="+strconv.Quote(v))
			}
			sort.Strings(keys)
			key := "{" + strings.Join(keys, ",") + "}"
			for _, value := range stream.Values {
				var entry parsedStreamsLiveEntry
				_ = json.Unmarshal(value[0], &entry.Ts)
				_ = json.Unmarshal(value[1], &entry.Line)
				if categorized && len(value) > 2 {
					var metadata struct {
						Parsed map[string]string `json:"parsed"`
					}
					_ = json.Unmarshal(value[2], &metadata)
					entry.Parsed = metadata.Parsed
				}
				out[key] = append(out[key], entry)
			}
		}
		return out
	}

	cases := []struct {
		name        string
		fixture     string
		pipeline    string
		categorized bool
	}{
		{name: "json", fixture: "json", pipeline: " | json"},
		{name: "json_status_filter", fixture: "json", pipeline: " | json | status >= 400"},
		{name: "logfmt", fixture: "logfmt", pipeline: " | logfmt"},
		{name: "logfmt_duration_filter", fixture: "logfmt", pipeline: " | logfmt | duration_ms > 5000"},
		{name: "json_categorized", fixture: "json_nolevel", pipeline: " | json", categorized: true},
		{name: "json_categorized_status_filter", fixture: "json_nolevel", pipeline: " | json | status >= 400", categorized: true},
	}
	for _, tc := range cases {
		logql := `{app="` + fixtures[tc.fixture].app + `"}` + tc.pipeline
		for _, rg := range ranges {
			for _, mode := range []struct {
				direction string
				limit     int
			}{{"backward", 1000}, {"forward", 1000}, {"backward", 10}} {
				name := fmt.Sprintf("%s/%s/%s/limit=%d", tc.name, rg.name, mode.direction, mode.limit)
				t.Run(name, func(t *testing.T) {
					loki := query(t, lokiURL, logql, rg.start, rg.end, mode.direction, mode.limit, tc.categorized)
					proxy := query(t, proxyURL, logql, rg.start, rg.end, mode.direction, mode.limit, tc.categorized)
					if entryCount(loki) == 0 {
						t.Fatalf("Loki returned no entries for %s", logql)
					}
					if len(proxy.Data.Result) != len(loki.Data.Result) {
						t.Errorf("stream count: proxy=%d loki=%d", len(proxy.Data.Result), len(loki.Data.Result))
					}
					want := collect(loki, tc.categorized)
					got := collect(proxy, tc.categorized)
					if !reflect.DeepEqual(got, want) {
						gotJSON, _ := json.Marshal(got)
						wantJSON, _ := json.Marshal(want)
						t.Fatalf("streams differ for %s\nproxy: %s\nloki:  %s", logql, gotJSON, wantJSON)
					}
				})
			}
		}
	}
}
