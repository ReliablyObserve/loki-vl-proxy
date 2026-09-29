//go:build e2e

package e2e_compat

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

// Loki's json parser sanitizes a dotted key (http.method) into the label
// probe_verb; VictoriaLogs' unpack_json keeps the original key. A label filter
// on the sanitized name has to select the same lines through every proxy as it
// does in Loki, in log queries and in the range metric built on them.
//
// conformance: profiles/sanitized-json-key-filter
func TestCompat_SanitizedJSONKeyFilterMatchesLoki(t *testing.T) {
	// Keys no other fixture stores as a VictoriaLogs field, so the proxies
	// cannot resolve the sanitized names from their field inventory.
	app := fmt.Sprintf("compat-sanitized-%d", time.Now().UnixNano())
	pushStream(t, time.Now().Add(-30*time.Second), streamDef{
		Labels: map[string]string{"app": app, "env": "sanitized", "level": "info"},
		Lines: []string{
			`{"msg":"login","probe.verb":"GET","probe.code":200}`,
			`{"msg":"logout","probe.verb":"POST","probe.code":201}`,
		},
	})
	forceVLFlush(t)
	selector := `{app="` + app + `"}`
	waitForLokiMetricDataSelector(t, selector)

	end := time.Now()
	window := func(q string) url.Values {
		return url.Values{
			"query": {q},
			"start": {strconv.FormatInt(end.Add(-10*time.Minute).UnixNano(), 10)},
			"end":   {strconv.FormatInt(end.UnixNano(), 10)},
			"step":  {"60"},
			"limit": {"100"},
		}
	}
	messages := func(base, q string) []string {
		t.Helper()
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", window(q), "0", nil)
		if status != http.StatusOK {
			t.Fatalf("%s %s: %d %s", base, q, status, body)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Values [][]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, s := range resp.Data.Result {
			for _, v := range s.Values {
				out = append(out, v[1].(string))
			}
		}
		sort.Strings(out)
		return out
	}
	total := func(base, q string) float64 {
		t.Helper()
		status, body := rejectedQueryGet(t, base, "/loki/api/v1/query_range", window(q), "0", nil)
		if status != http.StatusOK {
			t.Fatalf("%s %s: %d %s", base, q, status, body)
		}
		var resp struct {
			Data struct {
				Result []struct {
					Values [][]any `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		var sum float64
		for _, s := range resp.Data.Result {
			for _, v := range s.Values {
				f, _ := strconv.ParseFloat(v[1].(string), 64)
				sum += f
			}
		}
		return sum
	}

	logQueries := map[string]int{ // query -> lines Loki returns for the fixture
		selector + ` | json | probe_verb="GET"`:                    1,
		selector + ` | json | probe_verb="POST"`:                   1,
		selector + ` | json | probe_verb!="GET"`:                   1,
		selector + ` | json | probe_verb=~"GET|POST"`:              2,
		selector + ` | json | probe_verb=""`:                       0,
		selector + ` | json | probe_verb!=""`:                      2,
		selector + ` | json | probe_code>=201`:                     1,
		selector + ` | json | probe_verb="GET" | probe_code=200`:   1,
		selector + ` | json | probe_verb="GET" and probe_code=201`: 0,
	}
	metricQueries := []string{
		`sum(count_over_time(` + selector + ` | json | probe_verb="GET" [5m]))`,
		`sum(count_over_time(` + selector + ` | json | probe_verb!="GET" [5m]))`,
		`sum(count_over_time(` + selector + ` | json | probe_code>=201 [5m]))`,
	}

	proxies := map[string]string{
		"parity proxy":        proxyURL,
		"loki-compat proxy":   proxyUnderscoreURL,
		"drilldown proxy":     patternsAutodetectProxyURL,
		"translated-metadata": proxyTranslatedMetadataURL,
	}
	for q, wantLines := range logQueries {
		want := messages(lokiURL, q)
		if len(want) != wantLines {
			t.Fatalf("Loki returned %d lines for %s, fixture expects %d: %v", len(want), q, wantLines, want)
		}
		for name, base := range proxies {
			got := messages(base, q)
			if len(got) != len(want) {
				t.Errorf("%s %s: %d lines %v, Loki %d %v", name, q, len(got), got, len(want), want)
				continue
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("%s %s: lines %v, Loki %v", name, q, got, want)
					break
				}
			}
		}
	}
	nonEmpty := false
	for _, q := range metricQueries {
		want := total(lokiURL, q)
		nonEmpty = nonEmpty || want > 0
		for name, base := range proxies {
			if got := total(base, q); got != want {
				t.Errorf("%s %s: total %v, Loki %v", name, q, got, want)
			}
		}
	}
	if !nonEmpty {
		t.Fatal("Loki returned no samples for any metric query: the comparison proves nothing")
	}
}
