package logsql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql/refusalcases"
)

// Every row of the refusal table predicts its rule, with the status VictoriaLogs answers on the row's endpoint.
//
// conformance: vl-query-too-long, vl-pipe-before-release, vl-stats-missing-stats-pipe, vl-stats-range-pipe-before-stats, vl-stats-pipe-after-stats, vl-math-quoted-constant-clone-crash
func TestPredictRefusalTable(t *testing.T) {
	for _, c := range refusalcases.Cases() {
		t.Run(c.Release+"/"+c.Name, func(t *testing.T) {
			r, ok := logsql.PredictRefusal(c.In, logsql.ParseVersion(c.Release))
			if !ok {
				if c.Rule != "" {
					t.Fatalf("no prediction, want %s", c.Rule)
				}
				return
			}
			if r.Rule != c.Rule {
				t.Fatalf("rule %q (%s), want %q", r.Rule, r.Message, c.Rule)
			}
			if r.Crash {
				if r.Status != 0 || r.Message != "" {
					t.Fatalf("a crash has no answer: %+v", r)
				}
				return
			}
			want := http.StatusBadRequest
			if c.In.Path != logsql.PathQuery {
				want = http.StatusUnprocessableEntity
			}
			if r.Status != want || r.Message == "" {
				t.Fatalf("status %d message %q, want %d", r.Status, r.Message, want)
			}
		})
	}
}

// PredictRefusal stays fast and bounded on any text: it terminates on every input, and the screen keeps the
// parser off every text whose pipes no rule reads.
func FuzzPredictRefusal(f *testing.F) {
	for _, c := range refusalcases.Cases() {
		f.Add(c.In.Path, c.In.Query, c.Release, c.In.Limit)
	}
	f.Add(logsql.PathStatsQueryRange, `* | "a|b" | (`, "v1.53.0", false)
	f.Fuzz(func(t *testing.T, path, query, release string, limit bool) {
		if len(query) > 1<<16 {
			return
		}
		start := time.Now()
		r, ok := logsql.PredictRefusal(logsql.RefusalInput{Path: path, Query: query, Limit: limit, MaxQueryLen: 16384}, logsql.ParseVersion(release))
		if time.Since(start) > 2*time.Second {
			t.Fatalf("slow prediction on %q", query)
		}
		if ok && r.Rule == "" {
			t.Fatalf("a prediction without a rule: %+v", r)
		}
	})
}
