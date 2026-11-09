//go:build e2e

package e2e_compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The Logs Drilldown footer reads the frame stat "Summary: total bytes
// processed"; Grafana builds the frame's meta.stats from data.stats.summary.
const drilldownFooterStat = "Summary: total bytes processed"

// lokiSummaryKeys is the key order of Loki's data.stats.summary.
var lokiSummaryKeys = []string{
	"bytesProcessedPerSecond", "linesProcessedPerSecond", "totalBytesProcessed", "totalLinesProcessed",
	"execTime", "queueTime", "subqueries", "totalEntriesReturned", "splits", "shards",
	"totalPostFilterLines", "totalStructuredMetadataBytesProcessed",
}

func jsonObjectKeys(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not a JSON object: %s", raw)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

// grafanaFrameStatNames posts one Loki query through a Grafana datasource
// and returns the display names of the first frame's meta.stats.
func grafanaFrameStatNames(t *testing.T, dsUID, expr string, from, to time.Time) []string {
	t.Helper()
	payload := map[string]any{
		"from": strconv.FormatInt(from.UnixMilli(), 10),
		"to":   strconv.FormatInt(to.UnixMilli(), 10),
		"queries": []map[string]any{{
			"refId": "A", "datasource": map[string]string{"type": "loki", "uid": dsUID},
			"expr": expr, "queryType": "range", "maxLines": 1000, "direction": "backward",
		}},
	}
	raw, _ := json.Marshal(payload)
	resp, err := http.Post(grafanaURL+"/api/ds/query", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("grafana ds query: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var decoded struct {
		Results map[string]struct {
			Status int `json:"status"`
			Frames []struct {
				Schema struct {
					Meta struct {
						Stats []struct {
							DisplayName string `json:"displayName"`
						} `json:"stats"`
					} `json:"meta"`
				} `json:"schema"`
			} `json:"frames"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode grafana answer: %v: %s", err, body)
	}
	res := decoded.Results["A"]
	if res.Status != http.StatusOK || len(res.Frames) == 0 {
		t.Fatalf("grafana ds %s: status %d, %d frames: %s", dsUID, res.Status, len(res.Frames), body)
	}
	var names []string
	for _, s := range res.Frames[0].Schema.Meta.Stats {
		names = append(names, s.DisplayName)
	}
	return names
}

// TestDrilldown_QueryStatsEnvelope: a log query_range through the proxy
// answers Loki's data.stats object (summary with Loki's keys, measured bytes
// and lines read from VictoriaLogs for a non-empty answer), and Grafana shows
// the stat the Logs Drilldown footer reads, as for Loki direct.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestDrilldown_QueryStatsEnvelope(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now()
	start := end.Add(-15 * time.Minute)
	// A query no earlier request asked (a line filter on a nonce that no line
	// holds), so no proxy cache answers it and it is read from VictoriaLogs:
	// a cache hit reports no bytes processed, as Loki's results cache does.
	nonce := fmt.Sprintf("qstats-%d", end.UnixNano())
	params := url.Values{
		"query": {`{app="api-gateway"} != "` + nonce + `"`}, "limit": {"1000"}, "direction": {"backward"},
		"start": {fmt.Sprint(start.UnixNano())}, "end": {fmt.Sprint(end.UnixNano())},
	}
	status, raw, _ := queryRangeGET(t, proxyURL, params, map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK {
		t.Fatalf("proxy query_range: %d %s", status, raw)
	}
	var answer struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
			Stats  json.RawMessage   `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &answer); err != nil {
		t.Fatal(err)
	}
	if len(answer.Data.Result) == 0 {
		t.Fatalf("the fixture window must answer streams: %s", raw)
	}
	if got := jsonObjectKeys(t, answer.Data.Stats); len(got) == 0 || got[0] != "summary" {
		t.Fatalf("data.stats sections %v: %s", got, answer.Data.Stats)
	}
	var stats struct {
		Summary json.RawMessage `json:"summary"`
	}
	_ = json.Unmarshal(answer.Data.Stats, &stats)
	if got := jsonObjectKeys(t, stats.Summary); !reflect.DeepEqual(got, lokiSummaryKeys) {
		t.Fatalf("summary keys %v\nwant Loki's %v", got, lokiSummaryKeys)
	}
	var summary struct {
		TotalBytesProcessed  int64   `json:"totalBytesProcessed"`
		TotalLinesProcessed  int64   `json:"totalLinesProcessed"`
		TotalEntriesReturned int64   `json:"totalEntriesReturned"`
		ExecTime             float64 `json:"execTime"`
	}
	_ = json.Unmarshal(stats.Summary, &summary)
	if summary.TotalBytesProcessed <= 0 || summary.TotalLinesProcessed <= 0 || summary.TotalEntriesReturned <= 0 || summary.ExecTime <= 0 {
		t.Fatalf("a non-empty answer read from VictoriaLogs must report what it processed: %s", stats.Summary)
	}

	// Grafana: the footer's stat is in the frame's stats for both datasources,
	// and the proxy's frame stats are the summary entries Loki's frame shows.
	loki := grafanaFrameStatNames(t, grafanaDatasourceUID(t, "Loki (direct)"), `{app="api-gateway"}`, start, end)
	proxy := grafanaFrameStatNames(t, grafanaDatasourceUID(t, "Loki (via VL proxy)"), `{app="api-gateway"}`, start, end)
	hasFooterStat := func(names []string) bool {
		for _, n := range names {
			if n == drilldownFooterStat {
				return true
			}
		}
		return false
	}
	if !hasFooterStat(loki) {
		t.Fatalf("Loki's frame lacks %q: %v", drilldownFooterStat, loki)
	}
	if !hasFooterStat(proxy) {
		t.Fatalf("the proxy's frame lacks %q (Loki: %v): %v", drilldownFooterStat, loki, proxy)
	}
	summaryStats := func(names []string) []string {
		var out []string
		for _, n := range names {
			if strings.HasPrefix(n, "Summary: ") {
				out = append(out, n)
			}
		}
		return out
	}
	if got, want := summaryStats(proxy), summaryStats(loki); !reflect.DeepEqual(got, want) {
		t.Fatalf("the proxy's summary stats %v differ from Loki's %v (all: proxy %v, Loki %v)", got, want, proxy, loki)
	}
}

// TestDrilldown_QueryStatsWindowTotalsFromTheVolumeAnswer: after the panel's
// volume query (Explore's shape, wrapping the logs query's expression), the
// logs query of the same window reports the window's lines: the volume
// answer's steps wholly inside the window. Loki's figure for the same window
// is logged (it equals the window's lines while its ingesters hold the
// window, and stops early beyond).
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestDrilldown_QueryStatsWindowTotalsFromTheVolumeAnswer(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now().Add(-2 * time.Minute).Truncate(time.Minute)
	start := end.Add(-15 * time.Minute)
	nonce := fmt.Sprintf("qstats-w-%d", time.Now().UnixNano())
	expr := `{app="api-gateway"} != "` + nonce + `"`
	headers := map[string]string{"X-Scope-OrgID": "0"}
	volume := url.Values{"query": {`sum by (level, detected_level) (count_over_time(` + expr + ` | drop __error__ [60s]))`},
		"start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(end.Unix())}, "step": {"60"}}
	status, raw, _ := doJSONGET(t, proxyURL+"/loki/api/v1/query_range?"+volume.Encode(), headers)
	if status != http.StatusOK {
		t.Fatalf("volume query: %d %s", status, raw)
	}
	var vol struct {
		Data struct {
			Result []struct {
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &vol); err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, s := range vol.Data.Result {
		for _, v := range s.Values {
			tsf, _ := strconv.ParseFloat(string(v[0]), 64)
			ts := int64(tsf)
			var str string
			_ = json.Unmarshal(v[1], &str)
			n, _ := strconv.ParseInt(str, 10, 64)
			if ts-60 >= start.Unix() && ts <= end.Unix() {
				want += n
			}
		}
	}
	if want == 0 {
		t.Skipf("no lines of %s in [%s, %s]", expr, start, end)
	}
	logs := url.Values{"query": {expr}, "limit": {"100"}, "direction": {"backward"},
		"start": {fmt.Sprint(start.UnixNano())}, "end": {fmt.Sprint(end.UnixNano())}}
	summary := func(base string) map[string]any {
		status, raw, _ := doJSONGET(t, base+"/loki/api/v1/query_range?"+logs.Encode(), headers)
		if status != http.StatusOK {
			t.Fatalf("%s logs query: %d %s", base, status, raw)
		}
		var a struct {
			Data struct {
				Stats struct {
					Summary map[string]any `json:"summary"`
				} `json:"stats"`
			} `json:"data"`
		}
		_ = json.Unmarshal([]byte(raw), &a)
		return a.Data.Stats.Summary
	}
	got := summary(proxyURL)
	if n, _ := got["totalPostFilterLines"].(float64); int64(n) != want {
		t.Fatalf("the logs query reports %v post-filter lines; the volume answer's steps inside the window hold %d (summary %v)", got["totalPostFilterLines"], want, got)
	}
	t.Logf("window lines from the volume answer %d; Loki reports %v lines processed for the same logs query", want, summary(lokiURL)["totalLinesProcessed"])
}
