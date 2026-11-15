package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The fixture is the data the recorded Loki holds: the stream the rows select
// ({app="api-gateway"}, with JSON and logfmt lines) and the plain baseline
// stream of the end-to-end setup, a line every ten seconds over the last
// fixtureSpan. The rows' answers do not read these lines; they read the state
// of a Loki that serves data.
const (
	fixtureSpan  = 10 * time.Minute
	fixtureEvery = 10 * time.Second
	// servingProbe is the range metric query that tells a Loki that serves the
	// fixture from one that has not cut its first head block yet (its range
	// metric path answers an empty matrix for a few minutes after it starts).
	servingProbe = `sum(count_over_time({app="api-gateway"}[1m]))`
)

type fixtureStream struct {
	labels map[string]string
	lines  []string
}

var fixtureStreams = []fixtureStream{
	{
		labels: map[string]string{"app": "api-gateway", "namespace": "prod", "env": "production", "cluster": "us-east-1", "container": "api-gateway", "level": "info"},
		lines: []string{
			`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":15,"trace_id":"abc123def456"}`,
			`{"method":"POST","path":"/api/v1/orders","status":201,"duration_ms":142,"trace_id":"def789abc012"}`,
			`{"method":"GET","path":"/health","status":200,"duration_ms":1}`,
		},
	},
	{
		labels: map[string]string{"app": "api-gateway", "namespace": "prod", "env": "production", "cluster": "us-east-1", "container": "api-gateway", "level": "error"},
		lines: []string{
			`level=error msg="upstream timeout" method=GET path=/api/v1/payments status=504 duration=30s`,
			`level=error msg="connection refused" method=POST path=/api/v1/orders status=502 duration=12ms`,
		},
	},
	{
		labels: map[string]string{"app": "e2e-test", "env": "test", "level": "info"},
		lines: []string{
			"GET /api/v1/users 200 15ms",
			"error: connection refused to payment-service",
		},
	},
}

// fixtureBody is Loki's push body of the fixture over the window ending at end.
func fixtureBody(end time.Time) ([]byte, int, error) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][]string        `json:"values"`
	}
	var streams []stream
	lines := 0
	first := end.Add(-fixtureSpan).Truncate(fixtureEvery)
	for _, fs := range fixtureStreams {
		s := stream{Stream: fs.labels}
		for t := first; !t.After(end); t = t.Add(fixtureEvery) {
			for i, line := range fs.lines {
				// A millisecond apart, so no two entries of a stream share a timestamp.
				ts := t.Add(time.Duration(i) * time.Millisecond)
				s.Values = append(s.Values, []string{strconv.FormatInt(ts.UnixNano(), 10), line})
				lines++
			}
		}
		streams = append(streams, s)
	}
	body, err := json.Marshal(map[string]any{"streams": streams})
	return body, lines, err
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

// ingestPoll is how often ingestFixture asks whether the Loki serves the fixture.
var ingestPoll = 5 * time.Second

// ingestFixture pushes the fixture to the Loki at base and waits until its range
// metric path answers with it.
func ingestFixture(base string, wait time.Duration, at time.Time, out io.Writer) error {
	body, lines, err := fixtureBody(at)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(base, "/")+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pushing the fixture: %w", err)
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("pushing the fixture: Loki answered %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	fmt.Fprintf(out, "pushed %d lines in %d streams\n", lines, len(fixtureStreams))
	deadline := time.Now().Add(wait)
	for {
		if serving(base, time.Now()) {
			fmt.Fprintln(out, "Loki serves the fixture on its range metric path")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the Loki did not serve the fixture on its range metric path within %s", wait)
		}
		time.Sleep(ingestPoll)
	}
}

// serving reports whether the Loki at base answers a range metric query over
// the fixture's window ending at end with at least one series.
func serving(base string, end time.Time) bool {
	q := url.Values{}
	q.Set("query", servingProbe)
	q.Set("start", strconv.FormatInt(end.Add(-fixtureSpan-5*time.Minute).Unix(), 10))
	q.Set("end", strconv.FormatInt(end.Unix(), 10))
	q.Set("step", "60")
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(base, "/")+"/loki/api/v1/query_range?"+q.Encode(), nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-Scope-OrgID", "0")
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	var answer struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK || json.Unmarshal(raw, &answer) != nil {
		return false
	}
	return answer.Status == "success" && len(answer.Data.Result) > 0
}
