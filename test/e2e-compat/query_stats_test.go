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
	"testing"
	"time"
)

// The Logs Drilldown footer reads the frame stat "Summary: total bytes
// processed"; Grafana builds the frame's meta.stats from data.stats.summary.
const drilldownFooterStat = "Summary: total bytes processed"

// lokiStatsSections are data.stats' sections in Loki's order.
var lokiStatsSections = []string{"summary", "querier", "ingester", "cache", "index"}

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
	if got := jsonObjectKeys(t, answer.Data.Stats); !reflect.DeepEqual(got, lokiStatsSections) {
		t.Fatalf("data.stats sections %v, want Loki's %v: %s", got, lokiStatsSections, answer.Data.Stats)
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
	// Every stat Grafana lists for Loki (the "Summary:" and "Ingester:"
	// entries) is listed for the proxy, in the same order.
	if !reflect.DeepEqual(proxy, loki) {
		t.Fatalf("the proxy's frame stats %v differ from Loki's %v", proxy, loki)
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

// statsShape is a JSON value's shape: an object's keys in order with their
// shapes, or the JSON type of any other value.
func statsShape(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		t.Fatal("empty JSON value")
	}
	switch raw[0] {
	case '{':
		dec := json.NewDecoder(bytes.NewReader(raw))
		_, _ = dec.Token()
		var out []any
		for dec.More() {
			tok, _ := dec.Token()
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				t.Fatal(err)
			}
			out = append(out, tok.(string), statsShape(t, v))
		}
		return out
	case 't', 'f':
		return "bool"
	case '"':
		return "string"
	}
	return "number"
}

// withoutRecvWaitTime removes the ingester's recvWaitTime from a stats shape.
func withoutRecvWaitTime(shape any) any {
	sections, _ := shape.([]any)
	out := make([]any, 0, len(sections))
	for i := 0; i+1 < len(sections); i += 2 {
		v := sections[i+1]
		if fields, ok := v.([]any); ok && sections[i] == "ingester" {
			var keep []any
			for j := 0; j+1 < len(fields); j += 2 {
				if fields[j] != "recvWaitTime" {
					keep = append(keep, fields[j], fields[j+1])
				}
			}
			v = keep
		}
		out = append(out, sections[i], v)
	}
	return out
}

type e2eStats struct {
	raw     json.RawMessage
	Summary struct {
		Bytes      int64   `json:"totalBytesProcessed"`
		Lines      int64   `json:"totalLinesProcessed"`
		PostFilter int64   `json:"totalPostFilterLines"`
		ExecTime   float64 `json:"execTime"`
		Entries    int64   `json:"totalEntriesReturned"`
	} `json:"summary"`
	Cache map[string]struct {
		EntriesFound      int64 `json:"entriesFound"`
		QueryLengthServed int64 `json:"queryLengthServed"`
	} `json:"cache"`
}

func getE2EStats(t *testing.T, base, path string, params url.Values) e2eStats {
	t.Helper()
	status, raw, _ := doJSONGET(t, base+path+"?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK {
		t.Fatalf("%s%s: %d %s", base, path, status, raw)
	}
	var a struct {
		Status string `json:"status"`
		Data   struct {
			Stats json.RawMessage `json:"stats"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &a); err != nil || a.Status != "success" || len(a.Data.Stats) == 0 {
		t.Fatalf("%s%s: no stats object (%v): %.300s", base, path, err, raw)
	}
	st := e2eStats{raw: a.Data.Stats}
	if err := json.Unmarshal(a.Data.Stats, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestDrilldown_QueryStatsLikeLoki: for log, metric, instant and volume
// answers over 5m and 1h, the proxy's data.stats has exactly the shape Loki
// answers (every section, key, key order and value type), and its values
// follow Loki's accounting (the query-stats-loki-meaning-totals decision):
// a log answer reports the lines and line bytes it processed, also when a
// refresh is a proxy cache hit (Loki computes log queries on every request);
// a tumbling count reports its window's lines; a metric query_range repeat
// is a results-cache hit with no processing and the window served; volume
// answers report no lines or bytes; every refresh reports its own execTime.
//
// conformance: drilldown/logs-footer-query-stats, query-stats-envelope
func TestDrilldown_QueryStatsLikeLoki(t *testing.T) {
	ensureDataIngested(t)
	end := time.Now().Add(-time.Minute).Truncate(time.Minute)
	nonce := fmt.Sprintf("qsl-%d", time.Now().UnixNano())
	for _, rng := range []time.Duration{5 * time.Minute, time.Hour} {
		start := end.Add(-rng)
		logqlRange := map[time.Duration]string{5 * time.Minute: "5m", time.Hour: "1h"}[rng]
		win := func(q string, kv ...string) url.Values {
			v := url.Values{"query": {q}, "start": {fmt.Sprint(start.Unix())}, "end": {fmt.Sprint(end.Unix())}, "step": {"60"}}
			for i := 0; i < len(kv); i += 2 {
				v.Set(kv[i], kv[i+1])
			}
			return v
		}
		sel := `{app="api-gateway"} != "` + nonce + rng.String() + `"`
		for _, tc := range []struct {
			name, path string
			params     url.Values
			check      func(t *testing.T, miss, again e2eStats)
		}{
			{"log", "/loki/api/v1/query_range", win(sel, "limit", "100", "direction", "backward"), func(t *testing.T, miss, again e2eStats) {
				if miss.Summary.Lines <= 0 || miss.Summary.Bytes <= 0 || miss.Summary.Entries <= 0 {
					t.Fatalf("miss %+v: a non-empty log answer reports what it processed", miss.Summary)
				}
				if again.Summary.Lines != miss.Summary.Lines || again.Summary.Bytes != miss.Summary.Bytes {
					t.Fatalf("refresh %+v: Loki computes a log query again; the proxy's hit reports the computation's %+v", again.Summary, miss.Summary)
				}
			}},
			{"metric", "/loki/api/v1/query_range", win(`sum(count_over_time(` + sel + ` [1m]))`), func(t *testing.T, miss, again e2eStats) {
				if miss.Summary.Lines <= 0 || miss.Summary.PostFilter != miss.Summary.Lines {
					t.Fatalf("miss %+v: a tumbling count reports its window's lines", miss.Summary)
				}
				if again.Summary.Lines != 0 || again.Summary.Bytes != 0 || again.Cache["result"].EntriesFound == 0 || again.Cache["result"].QueryLengthServed <= 0 {
					t.Fatalf("repeat %+v cache %+v: a results-cache hit with no processing and the window served", again.Summary, again.Cache["result"])
				}
			}},
			{"instant", "/loki/api/v1/query", url.Values{"query": {`sum(count_over_time(` + sel + ` [` + logqlRange + `]))`}, "time": {fmt.Sprint(end.Unix())}}, func(t *testing.T, miss, again e2eStats) {
				if miss.Summary.Lines <= 0 || again.Summary.Lines != miss.Summary.Lines {
					t.Fatalf("instant miss %+v, repeat %+v: Loki computes instant queries again; the hit reports the computation", miss.Summary, again.Summary)
				}
			}},
			{"volume_range", "/loki/api/v1/index/volume_range", win(`{app="api-gateway"}`), func(t *testing.T, miss, again e2eStats) {
				if miss.Summary.Lines != 0 || miss.Summary.Bytes != 0 || miss.Summary.Entries <= 0 {
					t.Fatalf("volume %+v: Loki reports the series returned and no processed lines or bytes", miss.Summary)
				}
			}},
		} {
			t.Run(rng.String()+"/"+tc.name, func(t *testing.T) {
				loki := getE2EStats(t, lokiURL, tc.path, tc.params)
				miss := getE2EStats(t, proxyURL, tc.path, tc.params)
				// Loki omits ingester.recvWaitTime when it is zero; the proxy has
				// no ingesters, so its ingester section is always that zero form.
				if got, want := statsShape(t, miss.raw), withoutRecvWaitTime(statsShape(t, loki.raw)); !reflect.DeepEqual(got, want) {
					t.Fatalf("stats shape differs from Loki's\nproxy %v\nloki  %v", got, want)
				}
				again := getE2EStats(t, proxyURL, tc.path, tc.params)
				if again.Summary.ExecTime == miss.Summary.ExecTime || again.Summary.ExecTime <= 0 {
					t.Fatalf("refresh execTime %v (first %v): each request reports its own time", again.Summary.ExecTime, miss.Summary.ExecTime)
				}
				tc.check(t, miss, again)
				t.Logf("loki %+v | proxy %+v | refresh %+v", loki.Summary, miss.Summary, again.Summary)
			})
		}
	}
}
