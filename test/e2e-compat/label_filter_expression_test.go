//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// labelFilterLimit is far above the lines any query of this test selects; a
// side that returns this many has truncated its answer and cannot be compared.
const labelFilterLimit = 5000

// labelFilterLines runs a query_range against one backend and returns the
// status and the sorted log lines of every stream.
func labelFilterLines(t *testing.T, base, query string, start, end time.Time) (int, []string, []byte) {
	t.Helper()
	params := url.Values{
		"query":     {query},
		"start":     {strconv.FormatInt(start.UnixNano(), 10)},
		"end":       {strconv.FormatInt(end.UnixNano(), 10)},
		"limit":     {strconv.Itoa(labelFilterLimit)},
		"direction": {"forward"},
	}
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK {
		return status, nil, body
	}
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil || response.Status != "success" || response.Data.ResultType != "streams" {
		t.Fatalf("%s: %q is not a streams answer: %v %s", base, query, err, body)
	}
	var lines []string
	for _, stream := range response.Data.Result {
		for _, value := range stream.Values {
			lines = append(lines, value[1])
		}
	}
	if len(lines) >= labelFilterLimit {
		t.Fatalf("%s: %q reached the limit of %d lines, the answer is truncated", base, query, labelFilterLimit)
	}
	slices.Sort(lines)
	return status, lines, body
}

// TestCompat_LabelFilterExpressionSyntax pins Loki's grammar for label filter
// expressions: a comma joins filters (and), parentheses group, and/or bind as
// in syntax.y (AND above OR; COMMA and juxtaposition carry no precedence, so
// their right operand runs to the end of the group). The fixture is chosen so
// that a wrong precedence changes the set of lines. Both backends must hold
// the fixture, and Loki's own answer must be the expected one, before the proxy
// is compared with it.
//
// conformance: semantics/label-filter-expression-syntax, loki_api_v1_query_range
func TestCompat_LabelFilterExpressionSyntax(t *testing.T) {
	service := fmt.Sprintf("lfexpr-%d", time.Now().UnixNano())
	fixtureEnd := time.Now().Add(-1 * time.Minute).Truncate(time.Second)
	fixture := []string{
		/* 0 */ `{"status":503,"duration_ms":30,"method":"GET"}`,
		/* 1 */ `{"status":200,"duration_ms":2500,"method":"GET"}`,
		/* 2 */ `{"status":200,"duration_ms":20,"method":"GET"}`,
		/* 3 */ `{"status":500,"duration_ms":10,"method":"POST"}`,
		/* 4 */ `{"status":404,"duration_ms":1200,"method":"PUT"}`,
		/* 5 */ `{"status":201,"duration_ms":5,"method":"PUT"}`,
		/* 6 */ `{"status":502,"duration_ms":5000,"method":"DELETE"}`,
		/* 7 */ `{"status":200,"duration_ms":999,"method":"GET"}`,
	}
	var vlRows strings.Builder
	var lokiValues [][]string
	for i, line := range fixture {
		stamp := fixtureEnd.Add(time.Duration(i-len(fixture)) * 10 * time.Second)
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": line, "service_name": service})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		lokiValues = append(lokiValues, []string{strconv.FormatInt(stamp.UnixNano(), 10), line})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=service_name", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	payload, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"stream": map[string]string{"service_name": service}, "values": lokiValues}}})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)

	selector := fmt.Sprintf(`{service_name=%q}`, service)
	start, end := fixtureEnd.Add(-10*time.Minute), fixtureEnd.Add(time.Minute)
	waitForFixtureOnBothBackends(t, fmt.Sprintf("service_name:=%q", service), selector, start, end, len(fixture))

	pick := func(indexes ...int) []string {
		var lines []string
		for _, i := range indexes {
			lines = append(lines, fixture[i])
		}
		slices.Sort(lines)
		return lines
	}
	cases := []struct {
		name, filter string
		want         []string
	}{
		// The registry request's pipeline: a group, then a comma.
		{"group then comma", `(status>=500 or duration_ms>1000), method="GET"`, pick(0, 1)},
		// COMMA has no precedence: `a or b, c` is `a or (b and c)`, not `(a or b) and c`.
		{"or then comma", `status>=500 or duration_ms>1000, method="GET"`, pick(0, 1, 3, 6)},
		// ... and `a, b or c` is `a and (b or c)`, not `(a and b) or c`.
		{"comma then or", `status>=500, method="GET" or method="PUT"`, pick(0)},
		{"comma joined numbers", `status>=500,duration_ms<100`, pick(0, 3)},
		{"group and, or", `(status < 500 and status > 200) or duration_ms >= 5000`, pick(4, 5, 6)},
		{"matcher then group", `method!="GET", (status>=500 or status==404)`, pick(3, 4, 6)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			query := selector + " | json | " + tc.filter
			lokiStatus, lokiLines, lokiBody := labelFilterLines(t, lokiURL, query, start, end)
			if lokiStatus != http.StatusOK || !slices.Equal(lokiLines, tc.want) {
				t.Fatalf("Loki does not answer the fixture as expected (status %d): got %q want %q\n%s", lokiStatus, lokiLines, tc.want, lokiBody)
			}
			proxyStatus, proxyLines, proxyBody := labelFilterLines(t, proxyURL, query, start, end)
			if proxyStatus != http.StatusOK || !slices.Equal(proxyLines, lokiLines) {
				t.Fatalf("proxy (status %d) answers %q, Loki %q\n%s", proxyStatus, proxyLines, lokiLines, proxyBody)
			}
		})
	}

	t.Run("metric", func(t *testing.T) {
		// A metric over the same filter: both count the lines Loki selects.
		query := fmt.Sprintf(`sum(count_over_time(%s | json | status>=500 or duration_ms>1000, method="GET" [11m]))`, selector)
		at := strconv.FormatInt(end.Unix(), 10)
		var values []string
		for _, base := range []string{lokiURL, proxyURL} {
			status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query?"+url.Values{"query": {query}, "time": {at}}.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
			var response struct {
				Data struct {
					Result []struct {
						Value []json.RawMessage `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response.Data.Result) != 1 || len(response.Data.Result[0].Value) != 2 {
				t.Fatalf("%s: %d %s", base, status, body)
			}
			var value string
			_ = json.Unmarshal(response.Data.Result[0].Value[1], &value)
			values = append(values, value)
		}
		if values[0] != "4" || values[1] != values[0] {
			t.Fatalf("Loki counts %s, proxy %s, want 4 and 4", values[0], values[1])
		}
	})

	t.Run("registry request", func(t *testing.T) {
		// The request of the registry item, verbatim, over the last 15 minutes.
		query := `{service_name="api-gateway"} | json | (status>=500 or duration_ms>1000), method="GET"`
		now := time.Now()
		lokiStatus, lokiLines, lokiBody := labelFilterLines(t, lokiURL, query, now.Add(-15*time.Minute), now)
		proxyStatus, proxyLines, proxyBody := labelFilterLines(t, proxyURL, query, now.Add(-15*time.Minute), now)
		if lokiStatus != http.StatusOK || proxyStatus != lokiStatus {
			t.Fatalf("status: proxy %d (%s), Loki %d (%s)", proxyStatus, proxyBody, lokiStatus, lokiBody)
		}
		if !slices.Equal(proxyLines, lokiLines) {
			t.Fatalf("proxy answers %d lines, Loki %d", len(proxyLines), len(lokiLines))
		}
		if len(lokiLines) == 0 {
			// Empty on both sides is no proof of parity: the fixture subtests are
			// the proof; this one only pins that the request is accepted.
			t.Log("the stack holds no api-gateway lines in the last 15 minutes: status parity only")
		}
	})

	t.Run("rejected like Loki", func(t *testing.T) {
		for _, filter := range []string{`()`, `(status>=500`, `status>=500)`, `status>=500 or`, `status>=500 and and method="GET"`, `, method="GET"`} {
			query := selector + " | json | " + filter
			lokiStatus, _, lokiBody := labelFilterLines(t, lokiURL, query, start, end)
			proxyStatus, _, proxyBody := labelFilterLines(t, proxyURL, query, start, end)
			if lokiStatus != http.StatusBadRequest || proxyStatus != lokiStatus {
				t.Fatalf("%s: proxy %d (%s), Loki %d (%s)", filter, proxyStatus, proxyBody, lokiStatus, lokiBody)
			}
			// Loki's body is the message alone as text/plain, and so is the proxy's.
			if string(proxyBody) != string(lokiBody) {
				t.Fatalf("%s: proxy says %q, Loki %q", filter, proxyBody, lokiBody)
			}
		}
	})
}
