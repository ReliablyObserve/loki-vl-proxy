package proxy

import (
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	logqlpkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
)

// The JSON parser error mark over the whole plan corpus: every row whose query the check probes runs through the real
// handlers under every route parity profile (TestPlanRouteParity's stub VictoriaLogs, with data), and every stats
// query or raw-row read the handlers send that unpacks JSON with a plain unpack_json is either marked or left for a
// structural reason logsql.JSONMark names: never because the text did not parse, its pipes could not be read, or the
// marked text would not parse (each would be a silent 200 where Loki fails). A query the ordered JSON evaluator answers
// in whole or in an operand is left out: the evaluator answers its own parser errors, its reads are not marked.
//
// conformance: semantics/json-parser-error-under-wrappers
func TestJSONMarkReachesTheCorpus(t *testing.T) {
	if testing.Short() && os.Getenv("CI") == "" {
		t.Skip("runs every probed corpus row on a fresh proxy; run without -short")
	}
	t.Cleanup(querycorpus.PauseRecording())
	reasons := map[string]int{}
	rows, reads := 0, 0
	for _, row := range routeRows(t, filepath.Join("..", ".."), 0) {
		expr, msg := logqlpkg.ValidateLogQLExpr(row.q)
		if msg != "" || expr == nil || len(jsonErrorProbes(expr)) == 0 || orderedJSONOwnsPart(expr) {
			continue
		}
		rows++
		for _, rp := range routeProfiles() {
			rec := runRoute(t, row.q, row.rangeDur, rp, true)
			for _, call := range rec.Calls {
				path, raw, _ := strings.Cut(call, "?")
				form, err := url.ParseQuery(raw)
				if err != nil {
					continue
				}
				q := form.Get("query")
				stats := strings.HasPrefix(path, "/select/logsql/stats_query")
				if !stats && (path != "/select/logsql/query" || !rawRowsRead.MatchString(q)) {
					continue // a probe's `| limit 1` is no metric read
				}
				if !strings.Contains(q, "| unpack_json") || strings.Contains(q, jsonErrorMarkSpec.Sentinel) {
					if strings.Contains(q, jsonErrorMarkSpec.Sentinel) {
						reads++
						reasons["marked (or the evaluator's own)"]++
					}
					continue
				}
				reads++
				var why string
				if stats {
					why = jsonErrorMarkSpec.StatsRefusal(q)
				} else {
					why = jsonErrorMarkSpec.RowsRefusal(strings.TrimSuffix(q[:strings.LastIndex(q, "| limit ")], " "))
				}
				if why == "" {
					why = "markable but sent unmarked"
				}
				reasons[why]++
				switch why {
				case logsql.RefusedParse, logsql.RefusedPipes, logsql.RefusedResult, "markable but sent unmarked":
					t.Errorf("%s %s: %s:\n%s", rp.name, row.q, why, q)
				}
			}
		}
	}
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString("\n  " + k + ": " + strconv.Itoa(reasons[k]))
	}
	t.Logf("JSON parser error mark over %d probed corpus rows, %d reads that unpack JSON:%s", rows, reads, b.String())
	if rows == 0 {
		t.Fatal("no corpus row is probed")
	}
}

// rawRowsRead is a raw-row metric read: its row budget is the limit (a probe reads one row).
var rawRowsRead = regexp.MustCompile(`\| limit \d{4,}$`)

// orderedJSONOwnsPart reports whether the ordered JSON evaluator answers expr or one of its operands.
func orderedJSONOwnsPart(expr logqlpkg.Expr) bool {
	switch n := expr.(type) {
	case *logqlpkg.BinOpExpr:
		return orderedJSONOwnsPart(n.Left) || orderedJSONOwnsPart(n.Right)
	case *logqlpkg.OpaqueMetricExpr:
		return n.Typed && n.Inner != nil && orderedJSONOwnsPart(n.Inner)
	case *logqlpkg.VectorAggregation, *logqlpkg.RangeAggregation:
		_, ok := compileOrderedJSONMetricExpr(n)
		return ok
	}
	return false
}
