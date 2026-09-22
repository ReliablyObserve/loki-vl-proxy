//go:build e2e

package e2e_compat

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCompat_HeavyQueryProtection covers long-range metric queries that used to
// pull seconds-wide stats grids or raw rows out of VictoriaLogs:
//
//   - topk and grouped rate over windows that do not divide the step match
//     Loki on identical data, and
//   - many of them at once keep VictoriaLogs up while a concurrent /labels
//     request keeps succeeding; every response is 200 or a documented limit
//     error.
func TestCompat_HeavyQueryProtection(t *testing.T) {
	id := time.Now().UnixNano()
	now := time.Now()
	s0 := now.Add(-20 * time.Hour).Truncate(time.Hour)
	fixtures := make([]slidingLiveFixture, 0, 3)
	for copies, name := range []string{"a", "b", "c"} {
		fx := slidingLiveFixture{app: fmt.Sprintf("heavy-%d-%s", id, name)}
		// Every timestamp carries 1, 2 and 3 lines for a, b and c, so window
		// counts never tie between apps and topk has one answer.
		for ts := s0.Add(7 * time.Second); ts.Before(s0.Add(12 * time.Hour)); ts = ts.Add(41 * time.Second) {
			for i := 0; i <= copies; i++ {
				fx.lines = append(fx.lines, slidingLiveLine{ts: ts.Add(time.Duration(i) * time.Millisecond), msg: fmt.Sprintf("heavy %s %d", name, i)})
			}
		}
		fixtures = append(fixtures, fx)
	}
	ingestSlidingFixtures(t, fixtures...)
	selector := fmt.Sprintf(`{app=~"heavy-%d-.*"}`, id)

	t.Run("windows that do not divide the step match Loki", func(t *testing.T) {
		for _, tc := range []struct {
			query string
			step  time.Duration
		}{
			// gcd(87s, 5m) = 3s would need 14400 buckets per app over 12h.
			{`topk(2, sum by (app) (count_over_time(` + selector + `[5m])))`, 87 * time.Second},
			{`bottomk(1, sum by (app) (bytes_over_time(` + selector + `[5m])))`, 87 * time.Second},
			{`sum by (app) (rate(` + selector + `[5m]))`, 87 * time.Second},
			// range < step: each sample counts only (t-5m, t].
			{`sum by (app) (count_over_time(` + selector + `[5m]))`, 10 * time.Minute},
			{`topk(2, sum by (app) (count_over_time(` + selector + `[5m])))`, 601 * time.Second},
		} {
			t.Run(tc.query, func(t *testing.T) {
				step := int64(tc.step / time.Second)
				start := time.Unix((s0.Unix()/step)*step, 0)
				end := start.Add(13 * time.Hour)
				loki := slidingRangeSeries(t, lokiURL, tc.query, start, end, tc.step, nil)
				assertSlidingParity(t, tc.query, loki, slidingRangeSeries(t, proxyURL, tc.query, start, end, tc.step, nil))
			})
		}
	})

	t.Run("topk ranks every series, not only the busiest", func(t *testing.T) {
		app := fmt.Sprintf("heavy-rank-%d", id)
		start := time.Unix((s0.Unix()/87)*87, 0)
		end := start.Add(2 * time.Hour)
		// 510 steady pods with one line per 5 minutes outnumber
		// -max-stats-query-series (500). The leader holds three lines in every
		// 5m window and wins every step except those holding the spike pod's
		// burst; the spike pod has the smallest total of all.
		var vlRows strings.Builder
		streams := map[string][][]any{}
		add := func(pod string, ts time.Time, msg string) {
			row := fmt.Sprintf(`{"_time":%q,"_msg":%q,"app":%q,"pod":%q}`, ts.UTC().Format(time.RFC3339Nano), msg, app, pod)
			vlRows.WriteString(row + "\n")
			streams[pod] = append(streams[pod], []any{strconv.FormatInt(ts.UnixNano(), 10), msg})
		}
		for n := 0; n < 510; n++ {
			pod := fmt.Sprintf("steady-%03d", n)
			for ts := start.Add(time.Duration(n) * time.Second); ts.Before(end); ts = ts.Add(5 * time.Minute) {
				add(pod, ts, "steady "+pod)
			}
		}
		for ts := start.Add(-10 * time.Minute).Add(3 * time.Second); ts.Before(end); ts = ts.Add(100 * time.Second) {
			add("leader", ts, "leader")
		}
		burst := start.Add(time.Hour + 13*time.Second)
		for i := 0; i < 20; i++ {
			add("spike", burst.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("spike %d", i))
		}
		ingestHeavyRankFixture(t, app, vlRows.String(), streams, len(strings.Split(strings.TrimSpace(vlRows.String()), "\n")), start, end)

		query := `topk(1, sum by (pod) (count_over_time({app="` + app + `"}[5m])))`
		loki := slidingRangeSeries(t, lokiURL, query, start, end, 87*time.Second, nil)
		if _, ok := loki["pod=spike"]; !ok {
			t.Fatalf("fixture error: Loki never ranks the spike pod first: %d series", len(loki))
		}
		assertSlidingParity(t, query, loki, slidingRangeSeries(t, proxyURL, query, start, end, 87*time.Second, nil))
	})

	t.Run("concurrent long-range metric queries keep VictoriaLogs up", func(t *testing.T) {
		startedBefore := victoriaLogsStartTimestamp(t)
		end := time.Now()
		shapes := []string{}
		for _, rng := range []time.Duration{24 * time.Hour, 7 * 24 * time.Hour} {
			step := strconv.Itoa(int(rng / time.Second / 1000))
			for _, query := range []string{
				`topk(5, sum by (app) (count_over_time(` + selector + `[5m])))`,
				`sum by (app) (rate(` + selector + `[5m]))`,
				`sum(bytes_rate(` + selector + `[1m]))`,
				`rate(` + selector + `[5m])`,
				`sum by (level, detected_level) (count_over_time({app=~".+"} | drop __error__ [` + step + `s]))`,
			} {
				shapes = append(shapes, "/loki/api/v1/query_range?"+url.Values{
					"query": {query},
					"start": {strconv.FormatInt(end.Add(-rng).UnixNano(), 10)},
					"end":   {strconv.FormatInt(end.UnixNano(), 10)},
					"step":  {step},
				}.Encode())
			}
		}
		client := &http.Client{Timeout: 3 * time.Minute}
		get := func(target string, headers ...map[string]string) (int, string, error) {
			req, err := http.NewRequest(http.MethodGet, proxyURL+target, nil)
			if err != nil {
				return 0, "", err
			}
			req.Header.Set("X-Scope-OrgID", "0")
			for _, set := range headers {
				for key, value := range set {
					req.Header.Set(key, value)
				}
			}
			resp, err := client.Do(req)
			if err != nil {
				return 0, "", err
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			return resp.StatusCode, string(body), nil
		}

		var mu sync.Mutex
		var failures []string
		stop := make(chan struct{})
		labelsDone := make(chan int)
		go func() {
			// Probe immediately and then keep probing: a fast run must still
			// prove that a metadata request is served while the load runs.
			probes := 0
			for {
				probeAt := time.Now()
				status, body, err := get("/loki/api/v1/labels?start=" + strconv.FormatInt(probeAt.Add(-5*time.Minute).UnixNano(), 10) + "&end=" + strconv.FormatInt(probeAt.UnixNano(), 10))
				probes++
				if err != nil || status != http.StatusOK {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("labels probe: %d %v %.200s", status, err, body))
					mu.Unlock()
				}
				select {
				case <-stop:
					labelsDone <- probes
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
		}()
		var wg sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				// Half the workers speak as Grafana Logs Drilldown: its
				// partial-result carve-out must not turn a rejection into an
				// empty 200 chart.
				headers := map[string]string{}
				if worker%2 == 1 {
					headers["X-Query-Tags"] = "Source=grafana-lokiexplore-app"
					headers["User-Agent"] = "Grafana/13.2.1"
				}
				for i := 0; i < len(shapes); i++ {
					target := shapes[(worker+i)%len(shapes)]
					status, body, err := get(target, headers)
					if err == nil && status == http.StatusOK && strings.Contains(body, "too many outstanding requests") {
						mu.Lock()
						failures = append(failures, fmt.Sprintf("%s: queue rejection served as 200 partial result: %.300s", target, body))
						mu.Unlock()
						continue
					}
					if err == nil && (status == http.StatusOK || isDocumentedLimitError(status, body)) {
						continue
					}
					mu.Lock()
					failures = append(failures, fmt.Sprintf("%s: %d %v %.300s", target, status, err, body))
					mu.Unlock()
				}
			}(worker)
		}
		wg.Wait()
		close(stop)
		if probes := <-labelsDone; probes == 0 {
			t.Fatal("labels probe never ran")
		}
		for _, failure := range failures {
			t.Error(failure)
		}
		if status, body := hardeningRequest(t, http.MethodGet, vlURL+"/health", "", nil); status != http.StatusOK {
			t.Fatalf("VictoriaLogs /health = %d %s after the load", status, body)
		}
		if startedAfter := victoriaLogsStartTimestamp(t); startedAfter != startedBefore {
			t.Fatalf("VictoriaLogs restarted during the load: start timestamp %s -> %s", startedBefore, startedAfter)
		}
	})
}

// ingestHeavyRankFixture writes identical lines with app and pod stream labels
// to both backends, flushes them and waits until both count every line.
func ingestHeavyRankFixture(t *testing.T, app, vlRows string, streams map[string][][]any, lines int, start, end time.Time) {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,pod", vlRows, map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	lokiStreams := make([]any, 0, len(streams))
	for pod, values := range streams {
		lokiStreams = append(lokiStreams, map[string]any{"stream": map[string]string{"app": app, "pod": pod}, "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": lokiStreams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json", "X-Scope-OrgID": "0"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	if status, body := hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
	selector := `{app="` + app + `"}`
	start = start.Add(-15 * time.Minute)
	window := int(end.Sub(start).Seconds()) + 60
	deadline := time.Now().Add(180 * time.Second)
	for {
		vlParams := url.Values{"query": {selector + " | stats count() as n"}, "start": {start.Add(-time.Second).UTC().Format(time.RFC3339Nano)}, "end": {end.UTC().Format(time.RFC3339Nano)}}
		_, vlBody := hardeningRequest(t, http.MethodGet, vlURL+"/select/logsql/query?"+vlParams.Encode(), "", nil)
		vlCount := 0
		var vlRow struct{ N string }
		if json.Unmarshal([]byte(strings.TrimSpace(string(vlBody))), &vlRow) == nil {
			vlCount, _ = strconv.Atoi(vlRow.N)
		}
		lokiParams := url.Values{"query": {fmt.Sprintf("sum(count_over_time(%s[%ds]))", selector, window)}, "time": {strconv.FormatInt(end.UnixNano(), 10)}}
		_, _, resp := doJSONGET(t, lokiURL+"/loki/api/v1/query?"+lokiParams.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		lokiCount := 0
		if resp["status"] == "success" && resp["warnings"] == nil {
			for _, item := range extractArray(extractMap(resp, "data"), "result") {
				if value, ok := item.(map[string]interface{})["value"].([]interface{}); ok && len(value) == 2 {
					lokiCount, _ = strconv.Atoi(fmt.Sprint(value[1]))
				}
			}
		}
		if vlCount == lines && lokiCount == lines {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends not healthy: fixture=%d loki=%d victorialogs=%d", lines, lokiCount, vlCount)
		}
		time.Sleep(time.Second)
	}
}

// isDocumentedLimitError accepts the errors the proxy documents for heavy
// queries: the heavy-query queue (429) and execution limits naming their flag.
func isDocumentedLimitError(status int, body string) bool {
	switch {
	case status == http.StatusTooManyRequests:
		return strings.Contains(body, "too many outstanding requests") && strings.Contains(body, "-backend-max-concurrent-heavy-queries")
	case status == http.StatusGatewayTimeout || status == http.StatusServiceUnavailable:
		// -backend-timeout, or VictoriaLogs' own -search.maxQueryDuration,
		// which the proxy now passes as the per-query timeout argument.
		return strings.Contains(body, "timeout") || strings.Contains(body, "couldn't be executed") || strings.Contains(body, "deadline")
	case status >= 400:
		return strings.Contains(body, "-manual-range-metric-row-limit") || strings.Contains(body, "-max-stats-query-series")
	}
	return false
}

// victoriaLogsStartTimestamp returns vm_app_start_timestamp, which changes when
// the VictoriaLogs process restarts.
func victoriaLogsStartTimestamp(t *testing.T) string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, vlURL+"/metrics", "", nil)
	if status != http.StatusOK {
		t.Fatalf("VictoriaLogs /metrics: %d", status)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "vm_app_start_timestamp ") {
			return strings.TrimPrefix(line, "vm_app_start_timestamp ")
		}
	}
	t.Fatal("VictoriaLogs /metrics has no vm_app_start_timestamp")
	return ""
}
