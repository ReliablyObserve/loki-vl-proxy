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

// TestCompat_QueryRangeLeadWindowLikeLoki asks the windowed log path the
// shapes its first-window read decides, against Loki on the same lines: a
// dense newest window that holds the limit (backward and forward, 6h and
// 24h), a newest window a minute long that does not, a gap of empty hours
// before older lines, and a limit above every line of the range. The lines
// must be Loki's, in Loki's order.
// conformance: semantics/query-range-window-lead-window, window-bounds-and-step-alignment, loki_api_v1_query_range
func TestCompat_QueryRangeLeadWindowLikeLoki(t *testing.T) {
	hour := time.Now().Add(-27 * time.Hour).UTC().Truncate(time.Hour)
	fx := slidingLiveFixture{app: fmt.Sprintf("lead-window-%d", time.Now().UnixNano())}
	add := func(from time.Time, n int, every time.Duration, tag string) {
		for i := 0; i < n; i++ {
			fx.lines = append(fx.lines, slidingLiveLine{ts: from.Add(time.Duration(i) * every), msg: fmt.Sprintf("%s-%d", tag, i)})
		}
	}
	// Hours 0-3: a line every 10 minutes. Hours 4-15: nothing. Hours 16-25:
	// a line every 20 seconds (180 an hour).
	add(hour, 24, 10*time.Minute, "old")
	add(hour.Add(16*time.Hour), 1800, 20*time.Second, "dense")
	ingestSlidingFixtures(t, fx)

	query := func(base string, from, to time.Time, limit int, direction string) []string {
		t.Helper()
		params := url.Values{
			"query": {fx.selector()},
			"start": {strconv.FormatInt(from.UnixNano(), 10)},
			"end":   {strconv.FormatInt(to.UnixNano(), 10)},
			"limit": {strconv.Itoa(limit)},
		}
		if direction != "" {
			params.Set("direction", direction)
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
				lines = append(lines, value[0]+" "+value[1])
			}
		}
		return lines
	}

	end := hour.Add(25*time.Hour + 28*time.Minute)
	for _, c := range []struct {
		name      string
		from, to  time.Time
		limit     int
		direction string
		lines     int // Loki's line count, so the fixture discriminates
	}{
		{"dense newest window holds the limit, 6h", end.Add(-6 * time.Hour), end, 50, "", 50},
		{"dense newest window holds the limit, 24h", end.Add(-24 * time.Hour), end, 50, "", 50},
		{"forward, the oldest window holds the limit", hour.Add(16 * time.Hour), end, 50, "forward", 50},
		{"newest window a minute long", end.Add(-24 * time.Hour), hour.Add(25*time.Hour + time.Minute), 100, "", 100},
		{"empty hours before older lines, forward", hour, hour.Add(26 * time.Hour), 30, "forward", 30},
		{"limit above every line", hour, hour.Add(12 * time.Hour), 1000, "", 24},
	} {
		want := query(lokiURL, c.from, c.to, c.limit, c.direction)
		if len(want) != c.lines {
			t.Fatalf("%s: Loki returned %d lines, the fixture expects %d", c.name, len(want), c.lines)
		}
		if got := query(proxyURL, c.from, c.to, c.limit, c.direction); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: proxy %d lines %v, Loki %d lines %v", c.name, len(got), got, len(want), want)
		}
		t.Logf("%s: proxy and Loki returned the same %d lines", c.name, len(want))
	}
}
