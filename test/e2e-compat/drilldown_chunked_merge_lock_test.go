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

// A Grafana client loads a metric range longer than a day in pieces, one request per piece, and joins the pieces'
// frames in the browser (test/e2e-ui explore-long-range-pieces.spec.ts observes it through the stack's Grafana).
// The joined chart is Loki's when every piece's answer is Loki's answer to the same request, whatever the pieces
// are and however the client joins them. These checks seed one bounded fixture into Loki and VictoriaLogs and
// compare the proxy's answer for each piece with Loki's: the whole ranges, day-long pieces with starts off the step
// grid, and the short first or last piece (shorter than a step, one step, just over). Logs Drilldown's piece
// shorter than one step is the documented exception (TestE2ELock_DrilldownChunkedMerge_LeftoverChunkSuppressed).

const chunkLockStep = 120 * time.Second

// chunkLockPiece is one request: its bounds and the client it comes from.
type chunkLockPiece struct {
	name       string
	start, end time.Time
}

func chunkLockPieces(evaluation time.Time) []chunkLockPiece {
	const day = 24 * time.Hour
	var pieces []chunkLockPiece
	for _, h := range []int{24, 25, 48, 168} {
		pieces = append(pieces, chunkLockPiece{fmt.Sprintf("whole-%dh", h), evaluation.Add(-time.Duration(h) * time.Hour), evaluation})
	}
	for _, off := range []time.Duration{0, 37 * time.Second, 3*time.Hour + 11*time.Second} {
		start := evaluation.Add(-2*day + off)
		for i := 0; i < 2; i++ {
			s := start.Add(time.Duration(i) * day)
			e := s.Add(day - chunkLockStep)
			if e.After(evaluation) {
				e = evaluation
			}
			pieces = append(pieces, chunkLockPiece{fmt.Sprintf("day-piece-%d-off%s", i, off), s, e})
		}
	}
	for _, n := range []time.Duration{30 * time.Second, 119 * time.Second, 120 * time.Second, 121 * time.Second, 239 * time.Second} {
		pieces = append(pieces, chunkLockPiece{fmt.Sprintf("short-end-%s", n), evaluation.Add(-n), evaluation})
		pieces = append(pieces, chunkLockPiece{fmt.Sprintf("short-start-%s", n), evaluation.Add(-day), evaluation.Add(-day + n)})
	}
	return pieces
}

// TestE2ELock_DrilldownChunkedMerge_PiecesMatchLoki: every piece's answer, for a Logs Drilldown, a Grafana and an API
// client, equals Loki's answer to the same request (series, sample times and values), except Logs Drilldown's piece
// shorter than one step, which must be the empty matrix.
//
// conformance: drilldown/long-range-pieces-answer-like-loki, loki_api_v1_query_range
func TestE2ELock_DrilldownChunkedMerge_PiecesMatchLoki(t *testing.T) {
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)
	query, evaluation := seedChunkMergeHistory(t)
	for _, source := range []string{"drilldown", "grafana-ua", "api"} {
		for _, p := range chunkLockPieces(evaluation) {
			t.Run(source+"/"+p.name, func(t *testing.T) {
				loki := chunkLockQuery(t, lokiURL, query, p, "api")
				proxy := chunkLockQuery(t, proxyVmauthURL, query, p, source)
				if source == "drilldown" && p.end.Sub(p.start) < chunkLockStep {
					if len(proxy) != 0 {
						t.Fatalf("Logs Drilldown piece shorter than a step: proxy answered %d samples, want the empty matrix", len(proxy))
					}
					return
				}
				if len(loki) == 0 && p.end.Sub(p.start) >= time.Hour {
					t.Fatalf("Loki answered no samples for %s: the fixture is not visible to Loki's range path yet", p.name)
				}
				if !reflect.DeepEqual(proxy, loki) {
					t.Errorf("%s [%s, %s]: proxy %d samples, Loki %d\nproxy %v\nLoki  %v", p.name, p.start.Format(time.RFC3339), p.end.Format(time.RFC3339), len(proxy), len(loki), proxy, loki)
				}
			})
		}
	}
}

// chunkLockQuery asks base for one piece with the headers of a client and returns the matrix as
// `{labels}@unix-seconds` -> value text.
func chunkLockQuery(t *testing.T, base, query string, p chunkLockPiece, source string) map[string]string {
	t.Helper()
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", strconv.FormatInt(p.start.UnixNano(), 10))
	params.Set("end", strconv.FormatInt(p.end.UnixNano(), 10))
	params.Set("step", strconv.FormatInt(int64(chunkLockStep.Seconds()), 10))
	req, err := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Scope-OrgID", "0")
	switch source {
	case "drilldown":
		req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
	case "grafana-ua":
		req.Header.Set("User-Agent", "Grafana/13.3.0")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query_range %s: %v", base, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query_range %s %s: status %d: %s", base, p.name, resp.StatusCode, body)
	}
	var answer struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&answer); err != nil {
		t.Fatalf("decode %s: %v: %s", base, err, body)
	}
	if answer.Status != "success" || answer.Data.ResultType != "matrix" || len(answer.Warnings) > 0 {
		t.Fatalf("query_range %s %s: status=%s type=%s warnings=%v", base, p.name, answer.Status, answer.Data.ResultType, answer.Warnings)
	}
	out := map[string]string{}
	for _, s := range answer.Data.Result {
		key := canonicalMetricKey(chunkLockAnyMap(s.Metric))
		for _, v := range s.Values {
			out[fmt.Sprintf("%s@%v", key, v[0])] = fmt.Sprint(v[1])
		}
	}
	return out
}

func chunkLockAnyMap(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// seedChunkMergeHistory writes the same 60 hours of lines (two pods, one line every 5 and 7 minutes, inside the
// stack's 72h retention and Loki's 168h reject_old_samples_max_age) to Loki and to VictoriaLogs through the Loki
// push API, in a private app, and waits until Loki's range path counts every line. It returns the query and the
// evaluation time, the end of every whole range (on the step grid).
func seedChunkMergeHistory(t *testing.T) (string, time.Time) {
	t.Helper()
	evaluation := time.Now().UTC().Truncate(chunkLockStep).Add(-5 * chunkLockStep)
	app := fmt.Sprintf("chunk-merge-%d", time.Now().UnixNano())
	var streams []any
	lines := 0
	for _, pod := range []struct {
		name  string
		every time.Duration
	}{{"chunk-merge-a", 5 * time.Minute}, {"chunk-merge-b", 7 * time.Minute}} {
		var values [][2]string
		for at := evaluation.Add(-60 * time.Hour).Add(300 * time.Millisecond); at.Before(evaluation); at = at.Add(pod.every) {
			values = append(values, [2]string{strconv.FormatInt(at.UnixNano(), 10), "bounded chunk merge fixture"})
		}
		lines += len(values)
		streams = append(streams, map[string]any{"stream": map[string]string{"app": app, "namespace": "prod", "pod": pod.name}, "values": values})
	}
	body := dlJSON(map[string]any{"streams": streams})
	dlPost(t, lokiURL+"/loki/api/v1/push", "application/json", body)
	dlPost(t, vlURL+"/insert/loki/api/v1/push?disable_message_parsing=1", "application/json", body)
	forceVLFlush(t)
	query := fmt.Sprintf(`sum by (pod) (count_over_time({app=%q,namespace="prod",pod!=""}[2m]))`, app)

	// Loki's range path can answer empty for a stream it has just received (its sharding reads the ingester's
	// index statistics): wait until it counts every line.
	count := fmt.Sprintf(`sum(count_over_time({app=%q}[61h]))`, app)
	deadline := time.Now().Add(4 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		got := chunkLockQuery(t, lokiURL, count, chunkLockPiece{"fixture-count", evaluation, evaluation}, "api")
		for _, v := range got {
			last = v
		}
		if last == strconv.Itoa(lines) {
			return query, evaluation
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Loki's range path counts %q of %d fixture lines", last, lines)
	return "", time.Time{}
}

// TestE2ELock_DrilldownChunkedMerge_LeftoverChunkSuppressed: the proxy answers Logs Drilldown's piece shorter than
// one step with the empty matrix and `X-Proxy-Drilldown-Path: hits-leftover-suppressed` (documented, see
// docs/KNOWN_ISSUES.md). Joined with the other pieces, that piece's one-bucket answer showed as a spike on one edge
// of Logs Drilldown's charts at 24h and longer.
//
// Only Logs Drilldown's piece is suppressed (isQuerySplitResidualParams reads its X-Query-Tags source). Any other
// Grafana client's piece is answered as Loki answers it: with align_queries_with_step, on the step grid.
func TestE2ELock_DrilldownChunkedMerge_LeftoverChunkSuppressed(t *testing.T) {
	waitForReady(t, proxyVmauthURL+"/ready", 30*time.Second)

	const query = `sum by (pod) (count_over_time({namespace="prod",pod!=""}[2m]))`

	for i, source := range []string{"drilldown", "grafana-ua"} {
		t.Run(source, func(t *testing.T) {
			// Stagger time per subtest so each gets a unique drillCacheKey
			// — the upstream cache doesn't include the source header in its
			// key, so two subtests at identical time windows would have the
			// second serve a cached body from the first (which won't have the
			// suppression response header even if it was originally suppressed).
			now := time.Now().UTC().Add(time.Duration(i) * 7 * time.Second)
			start := now.Add(-30 * time.Second) // range ≤ step → triggers suppression
			end := now

			req, err := http.NewRequest(http.MethodGet, proxyVmauthURL+"/loki/api/v1/query_range", nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			q := url.Values{}
			q.Set("query", query)
			q.Set("start", strconv.FormatInt(start.Unix(), 10))
			q.Set("end", strconv.FormatInt(end.Unix(), 10))
			q.Set("step", strconv.FormatInt(int64(chunkLockStep.Seconds()), 10))
			req.URL.RawQuery = q.Encode()
			req.Header.Set("X-Scope-OrgID", "default")
			switch source {
			case "drilldown":
				req.Header.Set("X-Query-Tags", "Source=grafana-lokiexplore-app")
			case "grafana-ua":
				req.Header.Set("User-Agent", "Grafana/11.5.0")
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			// Either signal proves the suppression fired:
			//   - X-Proxy-Drilldown-Path: hits-leftover-suppressed (fresh hit)
			//   - body is exactly the empty matrix (cached suppressed response,
			//     because the path header isn't stored in the response cache).
			pathHeader := resp.Header.Get("X-Proxy-Drilldown-Path")
			emptyMatrix := `{"status":"success","data":{"resultType":"matrix","result":[]}}`
			gotSuppress := pathHeader == "hits-leftover-suppressed" || string(body) == emptyMatrix
			if source == "grafana-ua" {
				if resp.StatusCode != http.StatusOK || pathHeader == "hits-leftover-suppressed" {
					t.Fatalf("source=%s: status %d header=%q, want 200 and no suppression: only Logs Drilldown's residual is suppressed", source, resp.StatusCode, pathHeader)
				}
				var answer struct {
					Data struct {
						Result []struct {
							Values [][]any `json:"values"`
						} `json:"result"`
					} `json:"data"`
				}
				if err := json.Unmarshal(body, &answer); err != nil {
					t.Fatalf("source=%s: decode %s: %v", source, body, err)
				}
				for _, series := range answer.Data.Result {
					for _, v := range series.Values {
						if ts, _ := v[0].(float64); int64(ts)%int64(chunkLockStep.Seconds()) != 0 {
							t.Fatalf("source=%s: sample at %v is off the %s step grid Loki answers on: %s", source, v[0], chunkLockStep, body)
						}
					}
				}
				return
			}
			if !gotSuppress {
				t.Errorf("source=%s expected suppression (header=hits-leftover-suppressed OR empty matrix body), got header=%q body=%s",
					source, pathHeader, body)
			}
		})
	}
}
