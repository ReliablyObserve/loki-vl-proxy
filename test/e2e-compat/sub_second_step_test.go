//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// subSecondRange returns a matrix as grouping labels -> time text -> value. The
// time is the text the backend wrote, not a number: Loki prints seconds with a
// millisecond fraction (model.Time.String), and a sample time written twice in
// one series is a failure of its own.
func subSecondRange(t *testing.T, base, query, start, end, step string) map[string]map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "start": {start}, "end": {end}, "step": {step}}
	return subSecondMatrix(t, base, "/loki/api/v1/query_range?"+params.Encode(), query)
}

func subSecondInstant(t *testing.T, base, query, at string) map[string]map[string]float64 {
	t.Helper()
	params := url.Values{"query": {query}, "time": {at}}
	return subSecondMatrix(t, base, "/loki/api/v1/query?"+params.Encode(), query)
}

func subSecondMatrix(t *testing.T, base, path, query string) map[string]map[string]float64 {
	t.Helper()
	status, body, _ := doJSONGET(t, base+path, map[string]string{"X-Scope-OrgID": "0"})
	if status != http.StatusOK {
		t.Fatalf("%s %s: HTTP %d %s", base, query, status, body)
	}
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]any `json:"metric"`
				Values [][]any        `json:"values"`
				Value  []any          `json:"value"`
			} `json:"result"`
		} `json:"data"`
		Warnings any `json:"warnings"`
	}
	if err := dec.Decode(&resp); err != nil || resp.Status != "success" || resp.Warnings != nil {
		t.Fatalf("%s %s: unhealthy response (%v) %s", base, query, err, body)
	}
	out := map[string]map[string]float64{}
	for _, series := range resp.Data.Result {
		key := nonSumKey(series.Metric)
		if _, dup := out[key]; dup {
			t.Fatalf("%s %s: two series for {%s}: %s", base, query, key, body)
		}
		out[key] = map[string]float64{}
		points := series.Values
		if series.Value != nil {
			points = [][]any{series.Value}
		}
		for _, point := range points {
			v, err := strconv.ParseFloat(fmt.Sprint(point[1]), 64)
			if len(point) != 2 || err != nil {
				t.Fatalf("%s %s: malformed sample %v", base, query, point)
			}
			text := fmt.Sprint(point[0])
			if _, dup := out[key][text]; dup {
				t.Fatalf("%s %s: {%s} has the time %s twice: %s", base, query, key, text, body)
			}
			out[key][text] = v
		}
	}
	return out
}

// TestCompat_SubSecondStep: a metric range query with a step under a second
// answers Loki's series: a sample every step at the millisecond Loki prints,
// the same values. The window is the start of the fixture's streams (lines every
// 5 to 17 s), the bounds sit on the step grid and 123 ms past it (the stack's Loki
// and the proxy, with alignment on, both move them down to the grid), and the instant form is asked at a
// time with milliseconds in the three spellings a client uses. A step that is
// positive but under a millisecond is a 500 on both.
// conformance: semantics/sub-second-step-grid, window-bounds-and-step-alignment, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_SubSecondStep(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	queries := []string{
		fmt.Sprintf(`sum(count_over_time(%s[20s]))`, selector),
		fmt.Sprintf(`sum by (zone) (rate(%s[10s]))`, selector),
		fmt.Sprintf(`count_over_time(%s[1s])`, selector),
		fmt.Sprintf(`sum by (pod) (count_over_time(%s[600ms]))`, selector),
	}
	steps := []string{"0.2", "250ms", "1.5", "0.999", "1", "0.2005"}
	for _, step := range steps {
		stepDur, err := time.ParseDuration(step + "s")
		if strings.HasSuffix(step, "ms") {
			stepDur, err = time.ParseDuration(step)
		}
		if err != nil {
			t.Fatalf("step %q: %v", step, err)
		}
		// Loki keeps whole milliseconds of the step.
		grid := stepDur.Truncate(time.Millisecond)
		// On the grid, and 123 ms past it: the stack's Loki and the proxy (with
		// -align-queries-with-step at its default) both move the bounds down.
		for _, past := range []time.Duration{0, 123 * time.Millisecond} {
			start := fx.s0.Add(10 * time.Minute).Truncate(grid).Add(past)
			end := start.Add(400 * grid)
			for _, query := range queries {
				loki := subSecondRange(t, lokiURL, query, strconv.FormatInt(start.UnixNano(), 10), strconv.FormatInt(end.UnixNano(), 10), step)
				proxy := subSecondRange(t, proxyURL, query, strconv.FormatInt(start.UnixNano(), 10), strconv.FormatInt(end.UnixNano(), 10), step)
				compareNonSum(t, fmt.Sprintf("step=%s start +%v %s", step, past, query), loki, proxy)
			}
		}
	}

	// The instant form, at a time with milliseconds: nanoseconds, float seconds,
	// RFC 3339.
	at := fx.s0.Add(12*time.Minute + 345*time.Millisecond)
	for _, spelling := range []string{
		strconv.FormatInt(at.UnixNano(), 10),
		fmt.Sprintf("%d.%03d", at.Unix(), at.Nanosecond()/1e6),
		at.UTC().Format(time.RFC3339Nano),
	} {
		for _, query := range queries[:2] { // windows that always hold a line
			compareNonSum(t, "instant at "+spelling+" "+query, subSecondInstant(t, lokiURL, query, spelling), subSecondInstant(t, proxyURL, query, spelling))
		}
	}

	// Under a millisecond: Loki's frontend keeps 0 and divides by it.
	for _, base := range []string{lokiURL, proxyURL} {
		params := url.Values{
			"query": {queries[0]},
			"start": {strconv.FormatInt(fx.s0.Add(10*time.Minute).UnixNano(), 10)},
			"end":   {strconv.FormatInt(fx.s0.Add(10*time.Minute+time.Second).UnixNano(), 10)},
			"step":  {"0.0005"},
		}
		status, body, _ := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if status != http.StatusInternalServerError || !strings.Contains(body, "integer divide by zero") {
			t.Fatalf("%s: step=0.0005 answered HTTP %d %s, want 500 integer divide by zero", base, status, body)
		}
	}
}
