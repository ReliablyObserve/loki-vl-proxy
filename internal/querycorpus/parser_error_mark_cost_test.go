package querycorpus_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestParserErrorMarkVictoriaLogsCost times the JSON metrics the parser error check marks, through the proxy's real
// handlers in front of real VictoriaLogs (the serve oracle, v1.53.0 and v1.49.0) holding LVP_VL_COST_ROWS valid JSON
// lines over the hour before metricEnd: the time a cold request takes, VictoriaLogs' work included, median of
// LVP_VL_COST_RUNS runs per query and request kind. Run it on two builds (a base without the mark, the branch) and
// compare the medians: it measures the cost the mark asks of VictoriaLogs on the success path (no line is marked).
// It runs only with LVP_VL_COST=1.
func TestParserErrorMarkVictoriaLogsCost(t *testing.T) {
	if os.Getenv("LVP_VL_COST") != "1" {
		t.Skip("a timing run: LVP_VL_COST=1")
	}
	t.Cleanup(querycorpus.PauseRecording())
	rows, runs := 100000, 9
	if v, err := strconv.Atoi(os.Getenv("LVP_VL_COST_ROWS")); err == nil && v > 0 {
		rows = v
	}
	if v, err := strconv.Atoi(os.Getenv("LVP_VL_COST_RUNS")); err == nil && v > 0 {
		runs = v
	}
	streams := []map[string]string{
		{"job": "cost", "app": "api", "pod": "a", "service_name": "api"},
		{"job": "cost", "app": "web", "pod": "b", "service_name": "web"},
		{"job": "cost", "app": "api", "pod": "c", "service_name": "api"},
		{"job": "cost", "app": "web", "pod": "d", "service_name": "web"},
		// Every line after a space: VictoriaLogs v1.49 does not parse such a line as JSON and Loki does, so on v1.49
		// every read of the stream is marked, looked up and read again.
		{"job": "ws", "app": "api", "pod": "w", "service_name": "api"},
	}
	fix := metricFixture{Streams: streams}
	first := metricEnd.Add(-metricWindow - 5*time.Minute)
	span := metricEnd.Sub(first)
	for i := 0; i < rows; i++ {
		at := first.Add(time.Duration(int64(span) / int64(rows) * int64(i))).Add(500 * time.Microsecond)
		line := `{"level":"info","status":` + strconv.Itoa(200+i%5) + `,"path":"/api/v1/items/` + strconv.Itoa(i%97) + `","duration_ms":` + strconv.Itoa(i%1000) + `,"msg":"request handled"}`
		if streams[i%len(streams)]["job"] == "ws" {
			line = " " + line
		}
		fix.Entries = append(fix.Entries, metricEntry{Stream: i % len(streams), TS: at.UnixNano(), Line: line, Metadata: map[string]string{"detected_level": "info"}})
	}
	queries := []string{
		`sort(rate({job="cost"} | json [1m]))`,
		`topk(3, count_over_time({job="cost"} | json [5m]))`,
		`max by (app) (rate({job="cost"} | json | status="200" [1m]))`,
		`sort(rate({job="ws"} | json [1m]))`,
	}
	root := filepath.Join("..", "..")
	for _, prof := range diffProfiles() {
		srv, err := oraclerun.StartVLServe(filepath.Join(root, "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: 7, Rows: fix.vlRows(prof.toVL())}})
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for _, q := range queries {
			for _, mr := range metricRequests(q, time.Minute) {
				if mr.kind == "step>range" {
					continue
				}
				var took []time.Duration
				for i := 0; i < runs; i++ {
					front := &metricFront{version: prof.version, upstream: srv.URL, tenant: 7, recorded: map[string]metricVLAnswer{}}
					start := time.Now()
					run := runMetricHandler(t, prof, mr, front)
					took = append(took, time.Since(start))
					if run.status != 200 {
						t.Fatalf("%s %s %s: status %d %.200s", prof.name, mr.kind, q, run.status, run.body)
					}
					time.Sleep(50 * time.Millisecond)
				}
				sort.Slice(took, func(a, b int) bool { return took[a] < took[b] })
				lines = append(lines, fmt.Sprintf("%-10s %-11s %-60s median %8.1f ms  min %8.1f ms", prof.version, mr.kind, q, float64(took[len(took)/2].Microseconds())/1000, float64(took[0].Microseconds())/1000))
			}
		}
		srv.Stop()
		t.Logf("cold request time over %d JSON lines:\n%s", rows, strings.Join(lines, "\n"))
	}
}
