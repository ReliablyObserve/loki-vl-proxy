//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// A Logs Drilldown breakdown over the series limit is asked again with the
// values ranked by VictoriaLogs in an in() subquery (rankedSingleFieldQuery in
// internal/proxy/metric_binary.go). The bound on that response holds only if
// VictoriaLogs ranks over the whole range, not per stats_query_range bucket:
// here every bucket holds its own two values, so a per-bucket ranking would
// return all twenty.
//
// conformance: limits/drilldown-breakdown-exact
func TestDrilldownRankedBreakdown_VictoriaLogsRanksOverTheWholeRange(t *testing.T) {
	serviceName := fmt.Sprintf("drilldown-ranked-%d", time.Now().UnixNano())
	step := time.Minute
	start := time.Now().UTC().Add(-30 * time.Minute).Truncate(step)

	// Value v<i> has i+1 lines, all in bucket i%10.
	var batch strings.Builder
	for i := 0; i < 20; i++ {
		for n := 0; n <= i; n++ {
			ts := start.Add(time.Duration(i%10)*step + time.Duration(n+1)*time.Second)
			entry, _ := json.Marshal(map[string]string{
				"_time":        ts.Format(time.RFC3339Nano),
				"_msg":         "ranked breakdown line",
				"service_name": serviceName,
				"val":          fmt.Sprintf("v%02d", i),
			})
			batch.Write(entry)
			batch.WriteByte('\n')
		}
	}
	resp, err := http.Post(vlURL+"/insert/jsonline?_stream_fields=service_name", "application/stream+json", strings.NewReader(batch.String()))
	if err != nil {
		t.Fatalf("VL push: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	forceVLFlush(t)

	base := fmt.Sprintf("service_name:=%q", serviceName)
	query := base + ` | filter val:in(` + base + ` | stats by (val) count() as __lvp_rank | sort by (__lvp_rank desc, val) | limit 4 | fields val) | stats by (val) count()`
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start.Format(time.RFC3339))
	params.Set("end", start.Add(10*step).Format(time.RFC3339))
	params.Set("step", "60s")

	var got []string
	deadline := time.Now().Add(30 * time.Second)
	for {
		r, err := http.Get(vlURL + "/select/logsql/stats_query_range?" + params.Encode())
		if err != nil {
			t.Fatalf("stats_query_range: %v", err)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("stats_query_range: %d %s", r.StatusCode, body)
		}
		var parsed struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("decode: %v: %s", err, body)
		}
		got = got[:0]
		for _, series := range parsed.Data.Result {
			got = append(got, series.Metric["val"])
		}
		sort.Strings(got)
		if len(got) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if want := []string{"v16", "v17", "v18", "v19"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ranked breakdown returned %v, want the four busiest values over the whole range %v", got, want)
	}
}
