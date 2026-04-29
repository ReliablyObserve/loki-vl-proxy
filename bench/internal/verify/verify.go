package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// QueryResult is the raw response from one target for one query.
type QueryResult struct {
	StatusCode int
	Body       []byte
	Err        error
}

// Diff describes a mismatch between two targets for one query.
type Diff struct {
	Field string
	Loki  string
	Proxy string
}

// Result is the verification outcome for one query.
type Result struct {
	QueryName string
	URL       string
	Passed    bool
	Diffs     []Diff
	LokiErr   error
	ProxyErr  error
}

// Run fetches every query in the workload from lokiURL and proxyURL and compares responses.
func Run(ctx context.Context, lokiURL, proxyURL string, queries []workload.Query, timeout time.Duration) []Result {
	results := make([]Result, 0, len(queries))
	for _, q := range queries {
		lURL := q.URL(lokiURL)
		pURL := q.URL(proxyURL)

		lRes := fetchRaw(ctx, lURL, timeout)
		pRes := fetchRaw(ctx, pURL, timeout)

		r := Result{
			QueryName: q.Name,
			URL:       lURL,
			LokiErr:   lRes.Err,
			ProxyErr:  pRes.Err,
		}

		if lRes.Err != nil || pRes.Err != nil {
			r.Passed = false
			results = append(results, r)
			continue
		}

		// Skip non-JSON or VL-native responses.
		if !isJSON(lRes.Body) || !isJSON(pRes.Body) {
			r.Passed = true
			results = append(results, r)
			continue
		}

		diffs := compare(q.Name, lRes.Body, pRes.Body)
		r.Diffs = diffs
		r.Passed = len(diffs) == 0
		results = append(results, r)
	}
	return results
}

func fetchRaw(ctx context.Context, url string, timeout time.Duration) QueryResult {
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return QueryResult{Err: err}
	}
	resp, err := client.Do(req)
	if err != nil {
		return QueryResult{Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return QueryResult{Err: err}
	}
	return QueryResult{StatusCode: resp.StatusCode, Body: body}
}

func isJSON(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		return c == '{'
	}
	return false
}

func compare(queryName string, lokiBody, proxyBody []byte) []Diff {
	var loki, proxy map[string]any
	if err := json.Unmarshal(lokiBody, &loki); err != nil {
		return []Diff{{Field: "parse_loki", Loki: err.Error(), Proxy: ""}}
	}
	if err := json.Unmarshal(proxyBody, &proxy); err != nil {
		return []Diff{{Field: "parse_proxy", Loki: "", Proxy: err.Error()}}
	}

	var diffs []Diff

	// Compare status fields.
	lokiStatus, _ := loki["status"].(string)
	proxyStatus, _ := proxy["status"].(string)
	if lokiStatus != proxyStatus {
		diffs = append(diffs, Diff{Field: "status", Loki: lokiStatus, Proxy: proxyStatus})
	}

	lokiData := loki["data"]
	proxyData := proxy["data"]

	switch d := lokiData.(type) {
	case []any:
		// labels, label_values, or series
		pd, ok := proxyData.([]any)
		if !ok {
			diffs = append(diffs, Diff{Field: "data_type", Loki: "array", Proxy: fmt.Sprintf("%T", proxyData)})
			return diffs
		}
		if len(d) > 0 {
			switch d[0].(type) {
			case string:
				// labels / label_values
				diffs = append(diffs, compareStringArrays(d, pd)...)
			case map[string]any:
				// series
				diffs = append(diffs, compareSeries(d, pd)...)
			}
		} else if len(pd) > 0 {
			diffs = append(diffs, Diff{Field: "data_length", Loki: "0", Proxy: fmt.Sprintf("%d", len(pd))})
		}

	case map[string]any:
		pd, ok := proxyData.(map[string]any)
		if !ok {
			diffs = append(diffs, Diff{Field: "data_type", Loki: "object", Proxy: fmt.Sprintf("%T", proxyData)})
			return diffs
		}

		// detected_fields
		if fields, ok := d["fields"]; ok {
			diffs = append(diffs, compareDetectedFields(fields, pd["fields"])...)
			return diffs
		}

		// index_stats
		if streams, ok := d["streams"]; ok {
			diffs = append(diffs, compareIndexStats(streams, d, pd)...)
			return diffs
		}

		// query_range / instant result (resultType + result)
		if resultType, ok := d["resultType"]; ok {
			pResultType := pd["resultType"]
			if resultType != pResultType {
				diffs = append(diffs, Diff{Field: "resultType", Loki: fmt.Sprintf("%v", resultType), Proxy: fmt.Sprintf("%v", pResultType)})
			}
			diffs = append(diffs, compareQueryResult(d["result"], pd["result"])...)
		}
	}

	return diffs
}

func compareStringArrays(loki, proxy []any) []Diff {
	ls := anyToStrings(loki)
	ps := anyToStrings(proxy)
	sort.Strings(ls)
	sort.Strings(ps)
	if reflect.DeepEqual(ls, ps) {
		return nil
	}
	return []Diff{{
		Field: "data",
		Loki:  fmt.Sprintf("[%s]", strings.Join(ls, ", ")),
		Proxy: fmt.Sprintf("[%s]", strings.Join(ps, ", ")),
	}}
}

func compareSeries(loki, proxy []any) []Diff {
	lokiKeys := seriesLabelSets(loki)
	proxyKeys := seriesLabelSets(proxy)
	sort.Strings(lokiKeys)
	sort.Strings(proxyKeys)
	if reflect.DeepEqual(lokiKeys, proxyKeys) {
		return nil
	}
	lokiShort := truncList(lokiKeys, 5)
	proxyShort := truncList(proxyKeys, 5)
	return []Diff{{
		Field: "series_label_sets",
		Loki:  strings.Join(lokiShort, " | "),
		Proxy: strings.Join(proxyShort, " | "),
	}}
}

func seriesLabelSets(series []any) []string {
	out := make([]string, 0, len(series))
	for _, s := range series {
		m, ok := s.(map[string]any)
		if !ok {
			continue
		}
		// Series entries may have a "labels" key or be flat label maps.
		labels, hasLabels := m["labels"]
		if hasLabels {
			if lm, ok := labels.(map[string]any); ok {
				out = append(out, labelMapKey(lm))
				continue
			}
		}
		out = append(out, labelMapKey(m))
	}
	return out
}

func labelMapKey(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func compareDetectedFields(lokiFields, proxyFields any) []Diff {
	ls := extractFieldNames(lokiFields)
	ps := extractFieldNames(proxyFields)
	sort.Strings(ls)
	sort.Strings(ps)
	if reflect.DeepEqual(ls, ps) {
		return nil
	}
	return []Diff{{
		Field: "detected_fields",
		Loki:  fmt.Sprintf("[%s]", strings.Join(ls, ", ")),
		Proxy: fmt.Sprintf("[%s]", strings.Join(ps, ", ")),
	}}
}

func extractFieldNames(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if name, ok := m["label"].(string); ok {
			out = append(out, name)
		}
	}
	return out
}

func compareIndexStats(lokiStreams any, loki, proxy map[string]any) []Diff {
	toFloat := func(v any) float64 {
		switch n := v.(type) {
		case float64:
			return n
		case json.Number:
			f, _ := n.Float64()
			return f
		}
		return 0
	}
	withinTolerance := func(a, b float64) bool {
		if a == 0 && b == 0 {
			return true
		}
		max := a
		if b > max {
			max = b
		}
		diff := a - b
		if diff < 0 {
			diff = -diff
		}
		return diff/max <= 0.05
	}

	fields := []string{"streams", "chunks", "bytes", "entries"}
	var diffs []Diff
	_ = lokiStreams
	for _, f := range fields {
		lv := toFloat(loki[f])
		pv := toFloat(proxy[f])
		if !withinTolerance(lv, pv) {
			diffs = append(diffs, Diff{
				Field: "index_stats." + f,
				Loki:  fmt.Sprintf("%.0f", lv),
				Proxy: fmt.Sprintf("%.0f", pv),
			})
		}
	}
	return diffs
}

func compareQueryResult(lokiResult, proxyResult any) []Diff {
	lStreams, ok1 := lokiResult.([]any)
	pStreams, ok2 := proxyResult.([]any)
	if !ok1 || !ok2 {
		if fmt.Sprintf("%T", lokiResult) != fmt.Sprintf("%T", proxyResult) {
			return []Diff{{Field: "result_type", Loki: fmt.Sprintf("%T", lokiResult), Proxy: fmt.Sprintf("%T", proxyResult)}}
		}
		return nil
	}

	var diffs []Diff

	// Stream count within 5% tolerance.
	lc, pc := len(lStreams), len(pStreams)
	if !withinPct(float64(lc), float64(pc), 0.05) {
		diffs = append(diffs, Diff{
			Field: "stream_count",
			Loki:  fmt.Sprintf("%d", lc),
			Proxy: fmt.Sprintf("%d", pc),
		})
	}

	// Total entry count.
	lEntries := totalEntries(lStreams)
	pEntries := totalEntries(pStreams)
	if !withinPct(float64(lEntries), float64(pEntries), 0.05) {
		diffs = append(diffs, Diff{
			Field: "entry_count",
			Loki:  fmt.Sprintf("%d", lEntries),
			Proxy: fmt.Sprintf("%d", pEntries),
		})
	}

	// Compare first 20 entries by timestamp-sorted content.
	lVals := collectEntries(lStreams, 20)
	pVals := collectEntries(pStreams, 20)
	if len(lVals) > 0 && len(pVals) > 0 && !reflect.DeepEqual(lVals, pVals) {
		diffs = append(diffs, Diff{
			Field: "entry_content_sample",
			Loki:  strings.Join(truncList(lVals, 3), " | "),
			Proxy: strings.Join(truncList(pVals, 3), " | "),
		})
	}

	return diffs
}

func totalEntries(streams []any) int {
	total := 0
	for _, s := range streams {
		m, ok := s.(map[string]any)
		if !ok {
			continue
		}
		values, _ := m["values"].([]any)
		total += len(values)
	}
	return total
}

// collectEntries gathers up to n log line values from all streams, sorted by timestamp.
func collectEntries(streams []any, n int) []string {
	type entry struct {
		ts  string
		val string
	}
	var all []entry
	for _, s := range streams {
		m, ok := s.(map[string]any)
		if !ok {
			continue
		}
		values, _ := m["values"].([]any)
		for _, v := range values {
			pair, ok := v.([]any)
			if !ok || len(pair) < 2 {
				continue
			}
			ts, _ := pair[0].(string)
			val, _ := pair[1].(string)
			all = append(all, entry{ts, val})
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ts < all[j].ts })
	out := make([]string, 0, n)
	for i, e := range all {
		if i >= n {
			break
		}
		out = append(out, e.ts+":"+e.val)
	}
	return out
}

func withinPct(a, b, pct float64) bool {
	if a == 0 && b == 0 {
		return true
	}
	max := a
	if b > max {
		max = b
	}
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff/max <= pct
}

func anyToStrings(in []any) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func truncList(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return append(s[:n:n], fmt.Sprintf("…+%d", len(s)-n))
}
