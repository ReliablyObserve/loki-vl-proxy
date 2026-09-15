// Package verify checks, before any timing, that Loki and every timed proxy
// target return the same, non-degraded result for every compared query, so the
// benchmark measures equivalent work on all targets.
package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/degraded"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// QueryResult is the raw response from one target for one query.
type QueryResult struct {
	StatusCode int
	Header     http.Header
	Body       []byte
	Err        error
}

// Target is one proxy endpoint verified against Loki.
type Target struct {
	Name string
	URL  string
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
	// Target names the proxy target compared with Loki.
	Target string
	URL    string
	Passed bool
	// Skipped carries the reason when the query's shape is not compared.
	Skipped    string
	Diffs      []Diff
	LokiErr    error
	ProxyErr   error
	LokiShape  Shape
	ProxyShape Shape
}

// Shape is the comparable outline of one Loki API response.
type Shape struct {
	// Status is the API "status" field ("success" / "error").
	Status string
	// Kind is the response family: streams, matrix, vector, scalar, strings
	// (labels / label values), series, detected_fields, index_stats, patterns,
	// object (other JSON objects) or empty (no data array/object).
	Kind string
	// Series counts streams, series, metadata items or detected fields.
	Series int
	// Points counts log lines (streams), samples (matrix / vector / scalar) or
	// pattern samples.
	Points int
	// Stats holds index/stats counters (streams, chunks, bytes, entries).
	Stats map[string]float64
	// Keys is the sorted content identity of the items: label names or values
	// (strings), label sets (series, streams, matrix, vector) or field names
	// (detected_fields).
	Keys []string
	// Sample holds the first SampleSize log entries ("<ts> <line>") of a
	// streams result, ordered by timestamp and line.
	Sample []string
}

// SampleSize is the number of log entries compared by content.
const SampleSize = 20

func (s Shape) String() string {
	if s.Kind == "index_stats" {
		return fmt.Sprintf("index_stats streams=%.0f entries=%.0f bytes=%.0f", s.Stats["streams"], s.Stats["entries"], s.Stats["bytes"])
	}
	return fmt.Sprintf("%s series=%d points=%d", s.Kind, s.Series, s.Points)
}

// Run fetches every query once from Loki and once from each target and
// compares each target's response with Loki's. It returns one Result per query
// and target.
func Run(ctx context.Context, lokiURL string, targets []Target, queries []workload.Query, timeout time.Duration) []Result {
	client := &http.Client{Timeout: timeout}
	results := make([]Result, 0, len(queries)*len(targets))
	for _, q := range queries {
		lURL := q.URL(lokiURL)
		if q.Excluded != "" {
			for _, tgt := range targets {
				results = append(results, Result{QueryName: q.Name, Target: tgt.Name, URL: lURL, Passed: true, Skipped: q.Excluded})
			}
			continue
		}
		lRes := fetchRaw(ctx, client, lURL)
		for _, tgt := range targets {
			r := Result{QueryName: q.Name, Target: tgt.Name, URL: lURL}
			pRes := fetchRaw(ctx, client, q.URL(tgt.URL))
			r.LokiErr, r.ProxyErr = lRes.Err, pRes.Err
			if lRes.Err != nil || pRes.Err != nil {
				results = append(results, r)
				continue
			}
			r.Diffs, r.LokiShape, r.ProxyShape = CompareResponses(lRes, pRes, q.Shape)
			if q.Shape.Skip != "" && len(r.Diffs) == 0 {
				r.Skipped = q.Shape.Skip
			}
			r.Passed = len(r.Diffs) == 0
			results = append(results, r)
		}
	}
	return results
}

func fetchRaw(ctx context.Context, client *http.Client, url string) QueryResult {
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
	return QueryResult{StatusCode: resp.StatusCode, Header: resp.Header, Body: body}
}

// CompareResponses compares the HTTP status, degradation signals and the
// response shapes and content of one query. A query whose tolerance has Skip
// set still has to succeed without degradation on both targets; only its shape
// and content are not compared.
func CompareResponses(loki, proxy QueryResult, tol workload.Tolerance) ([]Diff, Shape, Shape) {
	var diffs []Diff
	lDeg, pDeg := degraded.Reasons(loki.Header, loki.Body), degraded.Reasons(proxy.Header, proxy.Body)
	if len(lDeg) > 0 || len(pDeg) > 0 {
		diffs = append(diffs, Diff{Field: "degraded", Loki: reasonString(lDeg), Proxy: reasonString(pDeg)})
	}
	if loki.StatusCode != proxy.StatusCode {
		diffs = append(diffs, Diff{Field: "http_status", Loki: fmt.Sprint(loki.StatusCode), Proxy: fmt.Sprint(proxy.StatusCode)})
	} else if loki.StatusCode != http.StatusOK {
		diffs = append(diffs, Diff{Field: "http_status", Loki: fmt.Sprintf("%d %s", loki.StatusCode, snippet(loki.Body)), Proxy: fmt.Sprintf("%d %s", proxy.StatusCode, snippet(proxy.Body))})
	}
	ls, lErr := ParseShape(loki.Body)
	ps, pErr := ParseShape(proxy.Body)
	if lErr != nil || pErr != nil {
		diffs = append(diffs, Diff{Field: "parse", Loki: errString(lErr), Proxy: errString(pErr)})
		return diffs, ls, ps
	}
	if len(diffs) > 0 {
		return diffs, ls, ps
	}
	if tol.Skip != "" {
		return nil, ls, ps
	}
	return CompareShapes(ls, ps, tol), ls, ps
}

// CompareShapes applies strict shape comparison with the given tolerance.
func CompareShapes(l, p Shape, tol workload.Tolerance) []Diff {
	var diffs []Diff
	// Some endpoints (index/stats, detected_fields) have no status field in
	// Loki's encoding; only compare it when both sides carry one.
	if l.Status != "" && p.Status != "" && l.Status != p.Status {
		diffs = append(diffs, Diff{Field: "status", Loki: l.Status, Proxy: p.Status})
	}
	if l.Kind != p.Kind {
		// An empty result on one side is reported as a kind mismatch too:
		// e.g. Loki `streams` with lines vs proxy `empty`.
		diffs = append(diffs, Diff{Field: "kind", Loki: l.String(), Proxy: p.String()})
		return diffs
	}
	if l.Kind == "index_stats" {
		// VictoriaLogs has no chunks, so `chunks` is not compared.
		for _, f := range []string{"streams", "entries", "bytes"} {
			pct := tol.PointsPct
			if f == "streams" {
				pct = tol.SeriesPct
			}
			if !withinPct(l.Stats[f], p.Stats[f], pct) {
				diffs = append(diffs, Diff{Field: "index_stats." + f, Loki: fmt.Sprintf("%.0f", l.Stats[f]), Proxy: fmt.Sprintf("%.0f", p.Stats[f])})
			}
		}
		if l.Stats["entries"] == 0 && p.Stats["entries"] == 0 && !tol.AllowEmpty {
			diffs = append(diffs, emptyDiff())
		}
		return diffs
	}
	seriesOK := withinPct(float64(l.Series), float64(p.Series), tol.SeriesPct)
	if !seriesOK {
		diffs = append(diffs, Diff{Field: "series_count", Loki: fmt.Sprint(l.Series), Proxy: fmt.Sprint(p.Series)})
	}
	pointsOK := withinPct(float64(l.Points), float64(p.Points), tol.PointsPct)
	if !pointsOK {
		diffs = append(diffs, Diff{Field: "point_count", Loki: fmt.Sprint(l.Points), Proxy: fmt.Sprint(p.Points)})
	}
	// Equal counts can still hide different content: compare the item identities
	// and a log-line sample when the counts must match exactly.
	if seriesOK && tol.SeriesPct == 0 {
		if lk, pk, differ := firstDifference(l.Keys, p.Keys); differ {
			diffs = append(diffs, Diff{Field: keysField(l.Kind), Loki: lk, Proxy: pk})
		}
	}
	if pointsOK && tol.PointsPct == 0 {
		if ls, ps, differ := firstDifference(l.Sample, p.Sample); differ {
			diffs = append(diffs, Diff{Field: "entry_content_sample", Loki: ls, Proxy: ps})
		}
	}
	if l.Series == 0 && p.Series == 0 && l.Points == 0 && p.Points == 0 && !tol.AllowEmpty {
		diffs = append(diffs, emptyDiff())
	}
	return diffs
}

// keysField names the Diff field for a Keys mismatch of the given kind.
func keysField(kind string) string {
	switch kind {
	case "strings":
		return "values"
	case "detected_fields":
		return "field_names"
	default:
		return "label_sets"
	}
}

// firstDifference reports the first position where two sorted lists differ,
// formatted for a Diff ("<index>: <item>", or "missing").
func firstDifference(a, b []string) (string, string, bool) {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		var av, bv string
		if i < len(a) {
			av = a[i]
		}
		if i < len(b) {
			bv = b[i]
		}
		if i >= len(a) || i >= len(b) || av != bv {
			return itemAt(i, a), itemAt(i, b), true
		}
	}
	return "", "", false
}

func itemAt(i int, list []string) string {
	if i >= len(list) {
		return fmt.Sprintf("#%d missing (%d items)", i, len(list))
	}
	v := list[i]
	if len(v) > 200 {
		v = v[:200] + "…"
	}
	return fmt.Sprintf("#%d %s", i, v)
}

func reasonString(reasons []string) string {
	if len(reasons) == 0 {
		return "ok"
	}
	return strings.Join(reasons, "; ")
}

func emptyDiff() Diff {
	const msg = "no data (window outside the seeded data, or data not ingested)"
	return Diff{Field: "empty_result", Loki: msg, Proxy: msg}
}

// ParseShape extracts the comparable shape of a Loki API JSON response.
func ParseShape(body []byte) (Shape, error) {
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		return Shape{}, fmt.Errorf("not a JSON object: %v", err)
	}
	s := Shape{Kind: "empty"}
	s.Status, _ = resp["status"].(string)

	// index/stats and detected_fields are served as bare objects without data.
	if _, ok := resp["data"]; !ok {
		if _, hasEntries := resp["entries"]; hasEntries {
			return indexStatsShape(resp), nil
		}
		if fields, ok := resp["fields"].([]any); ok && len(fields) > 0 {
			s.Kind, s.Series, s.Keys = "detected_fields", len(fields), fieldNames(fields)
		}
		return s, nil
	}

	switch d := resp["data"].(type) {
	case nil:
		return s, nil
	case []any:
		if len(d) == 0 {
			return s, nil
		}
		switch first := d[0].(type) {
		case string:
			s.Kind, s.Series, s.Keys = "strings", len(d), sortedStrings(d)
		case map[string]any:
			_, hasLabel := first["label"]
			_, hasParsers := first["parsers"]
			if hasLabel && hasParsers {
				// detected_fields encoded under data (the proxy's encoding).
				s.Kind, s.Series, s.Keys = "detected_fields", len(d), fieldNames(d)
			} else if _, ok := first["pattern"]; ok {
				s.Kind, s.Series = "patterns", len(d)
				for _, item := range d {
					if m, ok := item.(map[string]any); ok {
						samples, _ := m["samples"].([]any)
						s.Points += len(samples)
					}
				}
			} else {
				s.Kind, s.Series = "series", len(d)
				for _, item := range d {
					if m, ok := item.(map[string]any); ok {
						s.Keys = append(s.Keys, labelSetKey(m))
					}
				}
				sort.Strings(s.Keys)
			}
		default:
			s.Kind, s.Series = "object", len(d)
		}
		return s, nil
	case map[string]any:
		if fields, ok := d["fields"]; ok {
			arr, _ := fields.([]any)
			if len(arr) > 0 {
				s.Kind, s.Series, s.Keys = "detected_fields", len(arr), fieldNames(arr)
			}
			return s, nil
		}
		if _, ok := d["entries"]; ok {
			return indexStatsShape(d), nil
		}
		rt, _ := d["resultType"].(string)
		if rt == "" {
			s.Kind = "object"
			return s, nil
		}
		return resultShape(s, rt, d["result"]), nil
	default:
		s.Kind = fmt.Sprintf("%T", d)
		return s, nil
	}
}

func resultShape(s Shape, resultType string, result any) Shape {
	if resultType == "scalar" {
		s.Kind, s.Series, s.Points = "scalar", 1, 1
		return s
	}
	arr, _ := result.([]any)
	if len(arr) == 0 {
		// Loki and the proxy may disagree on resultType for an empty result;
		// both are "no data" for shape purposes.
		return s
	}
	s.Kind = resultType
	s.Series = len(arr)
	type logEntry struct{ ts, line string }
	var entries []logEntry
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		switch resultType {
		case "vector":
			if _, ok := m["value"]; ok {
				s.Points++
			}
			s.Keys = append(s.Keys, labelSetKey(asMap(m["metric"])))
		case "matrix":
			values, _ := m["values"].([]any)
			s.Points += len(values)
			s.Keys = append(s.Keys, labelSetKey(asMap(m["metric"])))
		default: // streams
			values, _ := m["values"].([]any)
			s.Points += len(values)
			s.Keys = append(s.Keys, labelSetKey(asMap(m["stream"])))
			for _, v := range values {
				// [ts, line] or [ts, line, metadata]: only ts and line are compared.
				if pair, ok := v.([]any); ok && len(pair) >= 2 {
					entries = append(entries, logEntry{ts: fmt.Sprint(pair[0]), line: fmt.Sprint(pair[1])})
				}
			}
		}
	}
	sort.Strings(s.Keys)
	if len(entries) > 0 {
		sort.Slice(entries, func(i, j int) bool {
			if len(entries[i].ts) != len(entries[j].ts) {
				return len(entries[i].ts) < len(entries[j].ts)
			}
			if entries[i].ts != entries[j].ts {
				return entries[i].ts < entries[j].ts
			}
			return entries[i].line < entries[j].line
		})
		for i := 0; i < len(entries) && i < SampleSize; i++ {
			s.Sample = append(s.Sample, entries[i].ts+" "+entries[i].line)
		}
	}
	return s
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// labelSetKey formats a label map as {k="v",...} with sorted keys.
func labelSetKey(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", k, fmt.Sprint(m[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func sortedStrings(items []any) []string {
	out := make([]string, 0, len(items))
	for _, v := range items {
		out = append(out, fmt.Sprint(v))
	}
	sort.Strings(out)
	return out
}

// fieldNames returns the sorted "label" names of detected_fields entries.
func fieldNames(items []any) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, fmt.Sprint(m["label"]))
		}
	}
	sort.Strings(out)
	return out
}

func indexStatsShape(m map[string]any) Shape {
	s := Shape{Kind: "index_stats", Stats: map[string]float64{}}
	for _, f := range []string{"streams", "chunks", "bytes", "entries"} {
		if v, ok := m[f].(float64); ok {
			s.Stats[f] = v
		}
	}
	return s
}

func withinPct(a, b, pct float64) bool {
	if a == b {
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

func errString(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}

// Summary formats the mismatches of a verification run as a report. It returns
// an empty string when every query passed.
func Summary(workloadName string, results []Result) string {
	var failed []Result
	for _, r := range results {
		if !r.Passed {
			failed = append(failed, r)
		}
	}
	if len(failed) == 0 {
		return ""
	}
	sort.SliceStable(failed, func(i, j int) bool {
		if failed[i].QueryName != failed[j].QueryName {
			return failed[i].QueryName < failed[j].QueryName
		}
		return failed[i].Target < failed[j].Target
	})
	var b strings.Builder
	for _, r := range failed {
		if r.Target != "" {
			fmt.Fprintf(&b, "  %s/%s (loki vs %s)\n", workloadName, r.QueryName, r.Target)
		} else {
			fmt.Fprintf(&b, "  %s/%s\n", workloadName, r.QueryName)
		}
		if r.LokiErr != nil {
			fmt.Fprintf(&b, "      loki error:  %v\n", r.LokiErr)
		}
		if r.ProxyErr != nil {
			fmt.Fprintf(&b, "      proxy error: %v\n", r.ProxyErr)
		}
		for _, d := range r.Diffs {
			fmt.Fprintf(&b, "      %-14s loki=%s  proxy=%s\n", d.Field, d.Loki, d.Proxy)
		}
	}
	return b.String()
}
