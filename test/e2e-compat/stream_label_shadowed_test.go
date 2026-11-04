//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// VictoriaLogs' Loki push API unpacks the keys of a JSON line into fields
// after the stream labels, and the last value written for a name wins
// (app/vlinsert/loki/loki_json.go, lib/logstorage/block.go): a line whose JSON
// has a key named like a stream label stores the line's value under the
// label's name. Loki keeps the stream label (pkg/logql/log/labels.go). This
// fixture pushes the same Loki push payload to both backends: stream label
// version="v2", JSON lines whose version key is v1 or v2.

// TestCompat_StreamLabelShadowedByIngestJSONKey: grouping by such a stream
// label answers Loki's series and counts, with and without a detected_level
// filter (the Logs Drilldown labels tab breakdown), as an Explore sum and at a
// Drilldown step.
// conformance: semantics/stream-label-shadowed-by-ingest-json-key, loki_api_v1_query_range
func TestCompat_StreamLabelShadowedByIngestJSONKey(t *testing.T) {
	app := fmt.Sprintf("shadowed-%d", time.Now().UnixNano())
	now := time.Now().Truncate(time.Minute)
	s0 := now.Add(-8 * time.Minute)
	byLevel := map[string][][]any{}
	for i := 0; i < 96; i++ {
		ts := s0.Add(time.Duration(i)*5*time.Second + 1700*time.Millisecond)
		level := []string{"info", "warn", "error"}[i%3]
		line, _ := json.Marshal(map[string]any{"msg": "request", "level": level, "version": []string{"v1", "v2"}[(i/3)%2], "n": i})
		byLevel[level] = append(byLevel[level], []any{fmt.Sprint(ts.UnixNano()), string(line)})
	}
	var streams []any
	for level, values := range byLevel {
		streams = append(streams, map[string]any{"stream": map[string]string{"app": app, "version": "v2", "level": level}, "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	for _, target := range []string{lokiURL + "/loki/api/v1/push", vlURL + "/insert/loki/api/v1/push"} {
		status, body := hardeningRequest(t, http.MethodPost, target, string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
		if status != http.StatusNoContent && status != http.StatusOK {
			t.Fatalf("push %s: %d %s", target, status, body)
		}
	}
	forceVLFlush(t)
	selector := fmt.Sprintf(`{app=%q}`, app)
	start, end := s0, s0.Add(8*time.Minute)
	total := fmt.Sprintf(`sum(count_over_time(%s [10m]))`, selector)
	deadline := time.Now().Add(180 * time.Second)
	for {
		loki := slidingRangeSeries(t, lokiURL, total, end, end, time.Minute, nil)
		proxy := slidingRangeSeries(t, proxyURL, total, end, end, time.Minute, nil)
		lines := func(series map[string]map[string]string) string {
			for _, points := range series {
				return points[fmt.Sprint(end.Unix())]
			}
			return ""
		}
		if len(loki) == 1 && len(proxy) == 1 && lines(loki) == "96" && lines(proxy) == "96" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("both backends must hold the 96 lines first: Loki %v, proxy %v", loki, proxy)
		}
		time.Sleep(2 * time.Second)
	}
	for _, c := range []struct {
		name, query string
		step        time.Duration
	}{
		{"labels tab", `sum(count_over_time({app="` + app + `" ,version != ""} [5s])) by (version)`, 5 * time.Second},
		{"labels tab, level pill", `sum(count_over_time({app="` + app + `" ,version != ""}  | detected_level="error"      [5s])) by (version)`, 5 * time.Second},
		{"labels tab, two-level pill", `sum(count_over_time({app="` + app + `" ,version != ""} | detected_level=~"error|warn" [5s])) by (version)`, 5 * time.Second},
		{"explore sum by", `sum by (version) (count_over_time(` + selector + ` [1m]))`, time.Minute},
		{"by two labels", `sum by (app, version) (count_over_time(` + selector + ` [1m]))`, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			loki := slidingRangeSeries(t, lokiURL, c.query, start, end, c.step, nil)
			proxy := slidingRangeSeries(t, proxyURL, c.query, start, end, c.step, nil)
			if len(loki) != 1 {
				t.Fatalf("%s: Loki answered %d series, want the one stream label value: %v", c.query, len(loki), loki)
			}
			compareUnwrapSumSeries(t, c.query, loki, proxy)
		})
	}
}
