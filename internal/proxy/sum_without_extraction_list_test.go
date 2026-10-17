package proxy

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// TestSumWithoutExtractionList: a parser that names the keys it extracts
// (`| logfmt amount`, a regexp with named groups, a pattern's captures) makes a
// Loki series the stream plus the labels it names, less the unwrapped one
// (pkg/logql/log/metrics_extraction.go): the unique `id` in every line is no
// label, so `sum without (pod)` folds the streams and not the lines. The
// answer is compared with Loki's over rows that carry an `id` and a `route`,
// which VictoriaLogs extracts whatever the list says.
//
// conformance: semantics/unwrap-sum-without-collapses, metric-series-identity
func TestSumWithoutExtractionList(t *testing.T) {
	type tc struct {
		parser string
		keys   []string // the labels besides the unwrapped one the parser extracts
	}
	parsers := []tc{
		{"logfmt amount", nil},
		{"logfmt amount, route", []string{"route"}},
	}
	type fnOp struct{ fn, op string }
	combos := []fnOp{
		{"max_over_time", "sum"}, {"sum_over_time", "sum"}, {"avg_over_time", "sum"}, {"min_over_time", "sum"},
		{"rate", "sum"}, {"max_over_time", "max"}, {"max_over_time", "count"}, {"sum_over_time", "avg"},
	}
	routes := []string{"/x", "/y", "/z"}
	for _, version := range []string{"v1.52.0", "v1.40.0"} {
		for _, p := range parsers {
			for _, c := range combos {
				for _, instant := range []bool{true, false} {
					name := fmt.Sprintf("%s/%s_without(%s)/%s/instant=%v", version, c.op, c.fn, p.parser, instant)
					t.Run(name, func(t *testing.T) {
						rnd := rand.New(rand.NewSource(int64(len(name)) * 6151))
						for i := 0; i < 10; i++ {
							s := generateWithoutScenario(rnd, c.fn, c.op, "logfmt", instant)
							s.parser = p.parser
							for j := range s.rows {
								route := routes[rnd.Intn(len(routes))]
								s.rows[j].labels["route"] = route
								s.rows[j].msg = fmt.Sprintf("amount=%v id=%d route=%s", s.rows[j].value, j, route)
							}
							emu := &vlEmulator{t: t, rows: s.rows, field: "amount", version: version}
							got := runWithoutScenario(t, emu, s)
							identity := func(row emuRow) string {
								return row.stream + "|" + parsedKey(row, p.keys)
							}
							group := func(row emuRow) string {
								return strings.TrimSuffix(withoutPodKey(row.stream)+","+parsedKey(row, p.keys), ",")
							}
							want := lokiWithoutIdentity(s.rows, c.fn, c.op, int64(s.window), s.times(), identity, group)
							mode := compareValues
							switch {
							case instant:
							case c.fn != "rate" && p.keys == nil:
								mode = compareSubset
							case c.fn != "rate":
								continue // a window reads a step-sized bucket, which holds routes Loki's does not
							case version == "v1.40.0" && p.keys != nil:
								continue // off its epoch grid, v1.40 reads raw rows, a series per stream (unwrap-per-series-identity-raw-rows)
							case version == "v1.40.0":
								mode = compareSeries
							}
							compareWithout(t, fmt.Sprintf("%s #%d", name, i), s, got, want, mode)
							if rawRows := emu.endpointCalls("/select/logsql/query"); len(rawRows) > 0 && version != "v1.40.0" {
								t.Fatalf("%s #%d: the metric read raw rows: %s", name, i, rawRows[0])
							}
						}
					})
				}
			}
		}
	}
}

// parsedKey is the labels of a row a parser extracted, as a key.
func parsedKey(row emuRow, keys []string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+row.labels[k])
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
