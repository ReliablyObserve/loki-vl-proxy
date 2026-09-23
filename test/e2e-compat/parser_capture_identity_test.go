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
	"testing"
	"time"
)

// conformance: parsed-label-series-identity, loki_api_v1_query
// Loki names a metric series with the stream labels plus every label the
// pipeline extracted. `| regexp` and `| pattern` name their captures in the
// query, so those labels must be part of the proxy's series identity too.
func TestCompat_ParserCaptureSeriesIdentity(t *testing.T) {
	probe := fmt.Sprintf("pcap%d", time.Now().UnixNano())
	jobs := []string{"job_a", "job_b", "job_c"}
	base := time.Now().Add(-2 * time.Minute).Truncate(time.Second)
	var vlRows strings.Builder
	var values [][]string
	for i, job := range jobs {
		for line := 0; line < 2; line++ {
			stamp := base.Add(time.Duration(i*2+line) * time.Second)
			text := fmt.Sprintf("worker start job_id=%s attempt=%d", job, line)
			values = append(values, []string{strconv.FormatInt(stamp.UnixNano(), 10), text})
			row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": text, "probe": probe})
			vlRows.Write(row)
			vlRows.WriteByte('\n')
		}
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=probe", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"probe": probe}, "values": values}}})
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"}); status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	at := base.Add(time.Minute)
	for _, tc := range []struct{ name, query string }{
		{"regexp", fmt.Sprintf(`count_over_time({probe=%q} | regexp "job_id=(?P<jid>\\w+)" [5m])`, probe)},
		{"pattern", fmt.Sprintf(`count_over_time({probe=%q} | pattern "<_> job_id=<jid> <_>" [5m])`, probe)},
		{"bytes", fmt.Sprintf(`bytes_over_time({probe=%q} | regexp "job_id=(?P<jid>\\w+)" [5m])`, probe)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var loki, proxy map[string]float64
			deadline := time.Now().Add(60 * time.Second)
			for {
				loki = parserCaptureInstant(t, lokiURL, tc.query, at)
				proxy = parserCaptureInstant(t, proxyURL, tc.query, at)
				if len(loki) == len(jobs) || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if len(loki) != len(jobs) {
				t.Fatalf("Loki answered %v, expected one series per captured job", loki)
			}
			if fmt.Sprint(proxy) != fmt.Sprint(loki) {
				t.Fatalf("proxy=%v loki=%v", proxy, loki)
			}
		})
	}
}

// parserCaptureInstant returns an instant vector as the capture value -> value,
// failing when a series lacks the extracted label.
func parserCaptureInstant(t *testing.T, base, query string, at time.Time) map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" || response.Data.ResultType != "vector" {
		t.Fatalf("%s %s: %d %s", base, query, status, body)
	}
	out := map[string]float64{}
	names := make([]string, 0, len(response.Data.Result))
	for _, sample := range response.Data.Result {
		jid, ok := sample.Metric["jid"]
		var raw string
		if !ok || len(sample.Value) != 2 || json.Unmarshal(sample.Value[1], &raw) != nil {
			t.Fatalf("%s %s: series without the extracted label in %s", base, query, body)
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("%s: invalid value in %s", base, body)
		}
		out[jid] = value
		names = append(names, jid)
	}
	sort.Strings(names)
	return out
}
