package verify

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/degraded"
	"github.com/ReliablyObserve/Loki-VL-proxy/bench/internal/workload"
)

// VLResult is the outcome of checking one VictoriaLogs-native query.
type VLResult struct {
	QueryName string
	URL       string
	Passed    bool
	// Skipped is a short description of what was found (e.g. "rows=200").
	Skipped    string
	StatusCode int
	Rows       int
	Err        error
}

// CheckVL requests every VictoriaLogs-native query once and fails it on a
// transport error, a non-200 status, a degraded answer (warning headers or a
// warnings array) or an empty result (unless the query's tolerance allows
// empty results).
func CheckVL(ctx context.Context, vlURL string, queries []workload.Query, timeout time.Duration) []VLResult {
	client := &http.Client{Timeout: timeout}
	out := make([]VLResult, 0, len(queries))
	for _, q := range queries {
		u := q.URL(vlURL)
		r := VLResult{QueryName: q.Name, URL: u}
		res := fetchRaw(ctx, client, u)
		r.StatusCode, r.Err = res.StatusCode, res.Err
		if res.Err == nil && res.StatusCode == http.StatusOK {
			r.Rows, r.Err = VLRows(q.Path, res.Body)
			if reasons := degraded.Reasons(res.Header, res.Body); r.Err == nil && len(reasons) > 0 {
				r.Err = fmt.Errorf("degraded answer: %s", strings.Join(reasons, "; "))
			}
		}
		r.Passed = r.Err == nil && res.StatusCode == http.StatusOK && (r.Rows > 0 || q.Shape.AllowEmpty)
		if r.Passed {
			r.Skipped = fmt.Sprintf("rows=%d", r.Rows)
		} else if r.Err == nil && res.StatusCode != http.StatusOK {
			r.Err = fmt.Errorf("HTTP %d: %s", res.StatusCode, snippet(res.Body))
		} else if r.Err == nil {
			r.Err = fmt.Errorf("empty result (window outside the seeded data, a filter that matches nothing, or an invalid pipe)")
		}
		out = append(out, r)
	}
	return out
}

// VLRows counts the data a VictoriaLogs select response carries:
//   - /select/logsql/query: NDJSON rows;
//   - /select/logsql/hits: the sum of all bucket values (all-zero buckets are empty);
//   - /select/logsql/stats_query and stats_query_range: result series;
//   - field_names, field_values, streams, stream_ids and similar: returned values.
func VLRows(path string, body []byte) (int, error) {
	switch {
	case strings.HasSuffix(path, "/select/logsql/query"):
		rows := 0
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			if !json.Valid(line) {
				return rows, fmt.Errorf("invalid NDJSON row: %s", snippet(line))
			}
			rows++
		}
		return rows, sc.Err()
	case strings.HasSuffix(path, "/select/logsql/hits"):
		var resp struct {
			Hits []struct {
				Values []float64 `json:"values"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("decode hits: %v", err)
		}
		var total float64
		for _, h := range resp.Hits {
			for _, v := range h.Values {
				total += v
			}
		}
		return int(total), nil
	case strings.Contains(path, "/select/logsql/stats_query"):
		s, err := ParseShape(body)
		if err != nil {
			return 0, err
		}
		if s.Status != "" && s.Status != "success" {
			return 0, fmt.Errorf("status %q", s.Status)
		}
		return s.Series, nil
	default:
		var resp struct {
			Values []json.RawMessage `json:"values"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("decode values: %v", err)
		}
		return len(resp.Values), nil
	}
}

// VLSummary formats the failed VictoriaLogs-native queries, or returns "".
func VLSummary(workloadName string, results []VLResult) string {
	var b strings.Builder
	for _, r := range results {
		if r.Passed {
			continue
		}
		fmt.Fprintf(&b, "  %s/%s\n      %v\n      %s\n", workloadName, r.QueryName, r.Err, r.URL)
	}
	return b.String()
}
