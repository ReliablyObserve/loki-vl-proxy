//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// TestCompat_QueryRangeWindowBudget checks a two-request cache transition:
// the first range spends one of five rows before reaching the third aligned
// hour, and the shifted range then needs all five rows from that same hour.
// conformance: semantics/query-range-window-budget, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_QueryRangeWindowBudget(t *testing.T) {
	start := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Hour)
	third := start.Add(2 * time.Hour)
	fx := slidingLiveFixture{app: fmt.Sprintf("window-budget-%d", time.Now().UnixNano())}
	fx.lines = append(fx.lines, slidingLiveLine{ts: start.Add(10 * time.Minute), msg: "earlier-row"})
	for i := 0; i < 5; i++ {
		fx.lines = append(fx.lines, slidingLiveLine{
			ts:  third.Add(10*time.Minute + time.Duration(i)*time.Second),
			msg: fmt.Sprintf("shifted-row-%d", i),
		})
	}
	ingestSlidingFixtures(t, fx)

	query := func(base string, from, to time.Time) []string {
		t.Helper()
		params := url.Values{
			"query":     {fx.selector()},
			"start":     {strconv.FormatInt(from.UnixNano(), 10)},
			"end":       {strconv.FormatInt(to.UnixNano(), 10)},
			"limit":     {"5"},
			"direction": {"forward"},
		}
		status, body := hardeningRequest(t, http.MethodGet, base+"/loki/api/v1/query_range?"+params.Encode(), "", map[string]string{"X-Scope-OrgID": "0"})
		if status != http.StatusOK {
			t.Fatalf("%s query_range status %d: %s", base, status, body)
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
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatalf("%s decode query_range: %v", base, err)
		}
		if response.Status != "success" || response.Data.ResultType != "streams" {
			t.Fatalf("%s unexpected query_range response: %s", base, body)
		}
		var lines []string
		for _, stream := range response.Data.Result {
			for _, value := range stream.Values {
				if len(value) != 2 {
					t.Fatalf("%s malformed log value: %v", base, value)
				}
				lines = append(lines, value[1])
			}
		}
		return lines
	}

	firstStart, firstEnd := start, third.Add(time.Hour-time.Nanosecond)
	shiftedStart, shiftedEnd := third, third.Add(2*time.Hour-time.Nanosecond)
	wantFirst := query(lokiURL, firstStart, firstEnd)
	if len(wantFirst) != 5 || wantFirst[0] != "earlier-row" || wantFirst[4] != "shifted-row-3" {
		t.Fatalf("Loki initial fixture is not discriminating: %v", wantFirst)
	}
	if got := query(proxyURL, firstStart, firstEnd); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("initial range: proxy %v, Loki %v", got, wantFirst)
	}
	t.Logf("initial range: proxy and Loki returned %v", wantFirst)
	wantShifted := query(lokiURL, shiftedStart, shiftedEnd)
	if len(wantShifted) != 5 || wantShifted[0] != "shifted-row-0" || wantShifted[4] != "shifted-row-4" {
		t.Fatalf("Loki shifted fixture is not discriminating: %v", wantShifted)
	}
	if got := query(proxyURL, shiftedStart, shiftedEnd); !reflect.DeepEqual(got, wantShifted) {
		t.Fatalf("shifted range: proxy %v, Loki %v", got, wantShifted)
	}
	t.Logf("shifted range after cache prime: proxy and Loki returned %v", wantShifted)
}
