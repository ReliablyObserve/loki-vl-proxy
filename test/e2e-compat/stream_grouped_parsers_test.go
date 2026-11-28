//go:build e2e

package e2e_compat

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// A sum grouped by stream labels over json and logfmt parsers with the errors dropped reads no parsed key in Loki: a
// parsed key named like a stream label is written `<name>_extracted` (semantics/stream-grouped-sum-skips-parsers). The
// proxy counts the query with no parser through VictoriaLogs stats and serves it after its checks. Over streams with
// a level label whose JSON and logfmt lines carry other levels, plain text and broken JSON, each such sum, at an
// instant and over a range, both parser orders, a rate, bytes and two labels, answers Loki's series and values; so
// does a grouping by a parsed key, which keeps the evaluator. (A stream without the label in the window keeps the
// evaluator too; its answers are held by the Loki differential, internal/querycorpus.)
//
// conformance: semantics/stream-grouped-sum-skips-parsers, semantics/two-parsers-keep-one, loki_api_v1_query_range, loki_api_v1_query
func TestCompat_StreamGroupedParserSumsLikeLoki(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	app := fmt.Sprintf("streamgrouped%d", time.Now().UnixNano())
	shapes := []string{`{"level":"error","route":"/a","n":%d}`, `level=debug route=/b n=%d`, `plain text line %d`, `{"level":"x","n":%d`, `{"route":"/c","n":%d}`}
	streams := []map[string]string{
		{"app": app, "level": "info", "pod": "a"},
		{"app": app, "level": "warn", "pod": "b"},
		{"app": app + "-nolevel", "pod": "c"},
	}
	total := 0
	for s, labels := range streams {
		var lines []string
		for i := 0; i < 120; i++ {
			lines = append(lines, fmt.Sprintf(shapes[(i+s)%len(shapes)], i))
		}
		total += len(lines)
		pushStream(t, now.Add(-20*time.Minute), streamDef{Labels: labels, Lines: lines, Interval: 10 * time.Second})
	}
	forceVLFlush(t)
	waitStreamGroupedLines(t, fmt.Sprintf(`{app=~"%s.*"}`, app), total)
	s := fmt.Sprintf(`{app=%q}`, app)
	const drop = ` | drop __error__, __error_details__`
	for i, query := range []string{
		`sum by (level) (count_over_time(` + s + ` | json | logfmt` + drop + ` [5m]))`,
		`sum by (level) (count_over_time(` + s + ` | logfmt | json` + drop + ` [5m]))`,
		`sum by (level) (rate(` + s + ` | json | logfmt | drop __error__ [5m]))`,
		`sum by (level) (bytes_over_time(` + s + ` | json | logfmt` + drop + ` [5m]))`,
		`sum by (level, pod) (count_over_time(` + s + ` |= "n" | json | logfmt` + drop + ` [5m]))`,
		`sum by (level) (count_over_time(` + s + ` | json | logfmt` + drop + ` [5m])) * 2`,
		// A parsed key: the evaluator answers, as before.
		`sum by (route) (count_over_time(` + s + ` | json | logfmt` + drop + ` [5m]))`,
	} {
		for _, endpoint := range []string{"query", "query_range"} {
			t.Run(endpoint+"/"+strconv.Itoa(i), func(t *testing.T) {
				unpackAgainstLoki(t, query, endpoint, now)
			})
		}
	}
}

// waitStreamGroupedLines waits until Loki and the proxy both count the want lines of selector.
func waitStreamGroupedLines(t *testing.T, selector string, want int) {
	t.Helper()
	deadline := time.Now().Add(240 * time.Second)
	for _, base := range []string{lokiURL, proxyURL} {
		for {
			params := url.Values{"query": {`sum(count_over_time(` + selector + `[30m]))`}, "time": {strconv.FormatInt(time.Now().Unix(), 10)}}
			_, _, resp := doJSONGET(t, base+"/loki/api/v1/query?"+params.Encode(), map[string]string{"X-Scope-OrgID": "0"})
			if got := unpackSamples(resp); len(got) == 1 {
				for _, v := range got {
					if v == float64(want) {
						goto ready
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never counted the %d lines of %s", base, want, selector)
			}
			time.Sleep(2 * time.Second)
		}
	ready:
	}
}
