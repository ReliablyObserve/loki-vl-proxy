//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

// TestLogQueryDefaultLimitLikeLoki: a range log query that sends no
// `limit` gets Loki's default of 100 entries (the HTTP API default), and the
// entries are the same as Loki's: the newest 100 of the window, same
// timestamps and lines (and, for a plain selector, the same stream labels). The
// proxy answered 1000, its -max-lines default, before. The fixture holds about
// 800 lines in the window, so the limit decides the answer. An explicit limit
// is still honoured.
// conformance: semantics/log-query-default-limit, loki_api_v1_query_range
func TestLogQueryDefaultLimitLikeLoki(t *testing.T) {
	fx := ensureNonSumFixtures(t)
	selector := fmt.Sprintf(`{probe=%q}`, fx.probe)
	start, end := fx.s0, fx.s0.Add(31*time.Minute)

	type entry struct {
		ts     string
		line   string
		stream string
	}
	read := func(base, query, limit, direction string, withStream bool) []entry {
		t.Helper()
		params := url.Values{
			"query": {query},
			"start": {strconv.FormatInt(start.UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
		}
		if limit != "" {
			params.Set("limit", limit)
		}
		if direction != "" {
			params.Set("direction", direction)
		}
		status, body, resp := doJSONGET(t, base+"/loki/api/v1/query_range?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
		if status != http.StatusOK || resp["status"] != "success" || extractMap(resp, "data")["resultType"] != "streams" {
			t.Fatalf("%s %s limit=%q: unhealthy response %d %.300s", base, query, limit, status, body)
		}
		var entries []entry
		for _, item := range extractArray(extractMap(resp, "data"), "result") {
			stream, _ := item.(map[string]any)
			labels, _ := stream["stream"].(map[string]any)
			key := ""
			if withStream {
				key = fmt.Sprintf("%v|%v|%v", labels["probe"], labels["zone"], labels["pod"])
			}
			values, _ := stream["values"].([]any)
			for _, raw := range values {
				pair, _ := raw.([]any)
				if len(pair) < 2 {
					t.Fatalf("%s %s: malformed entry %v", base, query, raw)
				}
				entries = append(entries, entry{ts: fmt.Sprint(pair[0]), line: fmt.Sprint(pair[1]), stream: key})
			}
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].ts != entries[j].ts {
				return entries[i].ts < entries[j].ts
			}
			return entries[i].stream+entries[i].line < entries[j].stream+entries[j].line
		})
		return entries
	}

	cases := []struct {
		name       string
		query      string
		limit      string
		direction  string
		want       int
		withStream bool
	}{
		{"plain selector", selector, "", "", 100, true},
		{"plain selector, forward", selector, "", "forward", 100, true},
		{"logfmt --strict", selector + ` | logfmt --strict`, "", "", 100, false},
		{"explicit limit 250", selector, "250", "", 250, true},
		{"explicit limit 1", selector, "1", "", 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loki := read(lokiURL, tc.query, tc.limit, tc.direction, tc.withStream)
			proxy := read(proxyURL, tc.query, tc.limit, tc.direction, tc.withStream)
			if len(loki) != tc.want {
				t.Fatalf("Loki answered %d entries, want %d: the stack's Loki is not a valid reference", len(loki), tc.want)
			}
			if len(proxy) != tc.want {
				t.Fatalf("proxy answered %d entries, Loki %d", len(proxy), len(loki))
			}
			for i := range loki {
				if loki[i] != proxy[i] {
					t.Fatalf("entry %d differs: Loki %+v, proxy %+v", i, loki[i], proxy[i])
				}
			}
		})
	}
}
