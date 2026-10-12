//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Loki guards a zero right operand of `/` and `%` with NaN, for a literal
// fold (pkg/logql/syntax/ast.go reduceBinOp), a vector against a literal
// (evaluator.go LiteralStepEvaluator) and a vector against a vector
// (evaluator.go vectorBinop), all through syntax.MergeBinOp (ast.go:1978-2005).
// The proxy computed IEEE a/b, so a nonzero numerator over zero was +Inf or -Inf.
// conformance: semantics/binary-division-by-zero-nan, loki_api_v1_query, loki_api_v1_query_range
func TestCompat_BinaryDivisionByZeroIsNaN(t *testing.T) {
	app := fmt.Sprintf("div-zero-%d", time.Now().UnixNano())
	fixtureEnd := time.Now().Add(-3 * time.Hour).Truncate(time.Minute)
	const lines = 6
	var vlRows strings.Builder
	lokiValues := map[string][][]string{}
	for i := 0; i < lines; i++ {
		stamp := fixtureEnd.Add(time.Duration(-240+40*i) * time.Second)
		pod := "p" + strconv.Itoa(i%2)
		message := fmt.Sprintf("division fixture line %d pod=%s", i, pod)
		row, _ := json.Marshal(map[string]string{"_time": stamp.UTC().Format(time.RFC3339Nano), "_msg": message, "app": app, "pod": pod})
		vlRows.Write(row)
		vlRows.WriteByte('\n')
		lokiValues[pod] = append(lokiValues[pod], []string{strconv.FormatInt(stamp.UnixNano(), 10), message})
	}
	status, body := hardeningRequest(t, http.MethodPost, vlURL+"/insert/jsonline?_stream_fields=app,pod", vlRows.String(), map[string]string{"Content-Type": "application/stream+json"})
	if status != http.StatusOK {
		t.Fatalf("VL ingest: %d %s", status, body)
	}
	var streams []any
	for pod, values := range lokiValues {
		streams = append(streams, map[string]any{"stream": map[string]string{"app": app, "pod": pod}, "values": values})
	}
	payload, _ := json.Marshal(map[string]any{"streams": streams})
	status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/loki/api/v1/push", string(payload), map[string]string{"Content-Type": "application/json"})
	if status != http.StatusNoContent {
		t.Fatalf("Loki ingest: %d %s", status, body)
	}
	forceVLFlush(t)
	if status, body = hardeningRequest(t, http.MethodPost, lokiURL+"/flush", "", nil); status >= 300 {
		t.Fatalf("Loki flush: %d %s", status, body)
	}
	selector := fmt.Sprintf(`{app=%q}`, app)
	waitForFixtureOnBothBackends(t, fmt.Sprintf("app:=%q", app), selector, fixtureEnd.Add(-5*time.Minute), fixtureEnd, lines)

	total := fmt.Sprintf(`sum(count_over_time(%s[5m]))`, selector)
	byPod := fmt.Sprintf(`sum by (pod) (count_over_time(%s[5m]))`, selector)
	zero := "(" + total + " * 0)"
	for _, tc := range []struct {
		name, query string
		nan         bool
	}{
		{"vector_div_zero", total + ` / 0`, true},
		{"vector_mod_zero", total + ` % 0`, true},
		{"grouped_vector_div_zero", byPod + ` / 0`, true},
		{"zero_over_vector", `0 / ` + total, false},
		{"literal_over_zero_vector", `5 / ` + zero, true},
		{"vector_over_zero_vector", total + ` / ` + zero, true},
		{"vector_mod_zero_vector", total + ` % ` + zero, true},
		{"nonzero_divisor", total + ` / 2`, false},
		// Literal-only queries (`1 / 0`) are not compared live: the compose Loki did
		// not answer them within the client timeout in CI. Their values are pinned
		// against a port of Loki's MergeBinOp/reduceBinOp in the unit tests
		// (TestBinaryScalarFoldMatchesLokiReduceBinOp).
	} {
		for _, mode := range []string{"query", "query_range"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				params := url.Values{"query": {tc.query}}
				if mode == "query" {
					params.Set("time", strconv.FormatInt(fixtureEnd.Unix(), 10))
				} else {
					params.Set("start", strconv.FormatInt(fixtureEnd.Add(-time.Minute).Unix(), 10))
					params.Set("end", strconv.FormatInt(fixtureEnd.Unix(), 10))
					params.Set("step", "60")
				}
				loki := divisionZeroValues(t, lokiURL, mode, params)
				proxy := divisionZeroValues(t, proxyURL, mode, params)
				if loki != proxy {
					t.Fatalf("%s %s\n loki:  %s\n proxy: %s", mode, tc.query, loki, proxy)
				}
				if got := strings.Contains(loki, `"NaN"`); got != tc.nan {
					t.Fatalf("Loki sanity: NaN=%v, want %v: %s", got, tc.nan, loki)
				}
			})
		}
	}
}

// divisionZeroValues renders the type, the labels and the sample values of a
// response, in a stable order, so two backends can be compared as strings.
func divisionZeroValues(t *testing.T, base, mode string, params url.Values) string {
	t.Helper()
	status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/"+mode+"?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || response.Status != "success" {
		t.Fatalf("%s: %d %s", base, status, body)
	}
	if response.Data.ResultType == "scalar" {
		var point []json.RawMessage
		if json.Unmarshal(response.Data.Result, &point) != nil || len(point) != 2 {
			t.Fatalf("%s: bad scalar %s", base, body)
		}
		return "scalar " + string(point[1])
	}
	var series []struct {
		Metric map[string]string   `json:"metric"`
		Value  []json.RawMessage   `json:"value"`
		Values [][]json.RawMessage `json:"values"`
	}
	if json.Unmarshal(response.Data.Result, &series) != nil {
		t.Fatalf("%s: bad result %s", base, body)
	}
	rendered := make([]string, 0, len(series))
	for _, s := range series {
		labels, _ := json.Marshal(s.Metric)
		points := s.Values
		if len(s.Value) == 2 {
			points = append(points, s.Value)
		}
		values := make([]string, 0, len(points))
		for _, point := range points {
			values = append(values, string(point[1]))
		}
		rendered = append(rendered, string(labels)+"="+strings.Join(values, ","))
	}
	sort.Strings(rendered)
	return response.Data.ResultType + " " + strings.Join(rendered, ";")
}
