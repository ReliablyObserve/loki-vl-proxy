package querycorpus_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	translatepkg "github.com/ReliablyObserve/Loki-VL-proxy/internal/translate"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// VictoriaLogs runs the subquery of a folded read (lower.SeriesFold) over the read's own time range: a series with a
// line inside the read and one outside it lies in one bucket of the read, so its hash is blanked and the series folded.
// The proxy's time chunks read each chunk with its own range (a series may span two chunks and lie in one bucket of
// each), and a subquery over a wider range would only keep more hashes, never a wrong answer: this pins the cost.
//
// The answers of real VictoriaLogs (the serve oracle, test/oracle/vl, v1.53.0 and v1.49.0) to three reads of the
// same data are recorded in seriesFoldScopePath (LVP_RUN_ORACLES=1 records them again):
//   - a two-minute read holding one of the two lines of series x and the one line of series y: both folded (no hash
//     in the answer, a folded row counting two series);
//   - the same read with its bounds 17 s off the minute (the subquery's grid offset from the minute): the same;
//   - a six-minute read holding both lines of x, five minutes apart: x keeps its hash and partials, y is folded.

const seriesFoldScopePath = "testdata/series-fold-scope.json"

// seriesFoldScopeStart is the first bucket edge of the reads.
var seriesFoldScopeStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// seriesFoldScopeReads are the reads: name, from (seconds after seriesFoldScopeStart) and length.
var seriesFoldScopeReads = []struct {
	name     string
	from, to time.Duration
}{
	{"inside", 0, 2 * time.Minute},
	{"inside off the grid", 17 * time.Second, 2*time.Minute + 17*time.Second},
	{"both lines", 0, 6 * time.Minute},
}

// seriesFoldScopeQuery is the folded bucket read of sum(avg_over_time(...)) over a read from start, as the proxy's
// window evaluator writes it (the lowering's text with the hash ahead of the gate, the bucket stats of avg over time).
func seriesFoldScopeQuery(t *testing.T, start time.Time) string {
	t.Helper()
	const q = `sum(avg_over_time({app="scope"} | json | unwrap n [5m]))`
	p, err := planOfText(logql.CanonicalizeLogRanges(q))
	if err != nil {
		t.Fatal(err)
	}
	op, err := lower.PlanQuery(q, p, lower.Profile{IdentityAhead: true})
	if err != nil {
		t.Fatal(err)
	}
	text := physical.Encode(op)
	first := strings.Index(text, " | stats by (")
	ungated, field, ok := translatepkg.SplitUnwrapGate(text[:first])
	if first < 0 || !ok {
		t.Fatalf("no gate before the first stats pipe: %s", text)
	}
	shift := ((start.UnixNano()+1)%int64(time.Minute) + int64(time.Minute)) % int64(time.Minute)
	read, ok := lower.SeriesFold{Outer: logql.VectorSum, By: []string{"__lvp_sid"}, Columns: []string{"__lvp_sum", "__sample_count"},
		Value: "__lvp_sum / __sample_count", ValueColumn: "__lvp_sum", CountColumn: "__sample_count", Singletons: lower.SingletonsKept,
		Field: field, Bucket: "60s", Offset: "-" + strconv.FormatInt(shift, 10) + "ns"}.Pipes(ungated)
	if !ok {
		t.Fatal("no fold")
	}
	out := text[:first]
	for _, pipe := range read.Before {
		out += " " + pipe.String()
	}
	out += " | stats by (" + strings.Join(read.By, ", ") + ") sum(__lvp_v) as __lvp_sum, count() as __sample_count"
	for _, pipe := range read.After {
		out += " " + pipe.String()
	}
	return out
}

// TestSeriesFoldSubqueryScopedToRead checks the recorded answers: the folded rows and the kept hashes of each read.
//
// conformance: semantics/unwrap-per-series-identity-raw-rows
func TestSeriesFoldSubqueryScopedToRead(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") == "1" {
		recordSeriesFoldScope(t)
	}
	raw, err := os.ReadFile(seriesFoldScopePath)
	if err != nil {
		t.Fatal(err)
	}
	var answers map[string]map[string]string
	if err := json.Unmarshal(raw, &answers); err != nil {
		t.Fatal(err)
	}
	for _, prof := range diffProfiles() {
		for _, read := range seriesFoldScopeReads {
			body, ok := answers[prof.version][read.name]
			if !ok {
				t.Fatalf("%s %s: not recorded: record again with LVP_RUN_ORACLES=1", prof.version, read.name)
			}
			var answer struct {
				Data struct {
					Result []struct {
						Metric map[string]string `json:"metric"`
						Values [][2]any          `json:"values"`
					} `json:"result"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &answer); err != nil {
				t.Fatalf("%s %s: %v", prof.version, read.name, err)
			}
			kept, folded := map[string]bool{}, 0.0
			for _, s := range answer.Data.Result {
				if sid := s.Metric["__lvp_sid"]; sid != "" {
					kept[sid] = true
					continue
				}
				if s.Metric["__name__"] == "__sample_count" {
					for _, v := range s.Values {
						n, _ := strconv.ParseFloat(v[1].(string), 64)
						folded += n
					}
				}
			}
			wantKept, wantFolded := 0, 2.0
			if read.name == "both lines" {
				wantKept, wantFolded = 1, 1
			}
			if len(kept) != wantKept || folded != wantFolded {
				t.Errorf("%s %s: %d hashes kept and %g series folded, want %d and %g: %.400s", prof.version, read.name, len(kept), folded, wantKept, wantFolded, body)
			}
		}
	}
}

// recordSeriesFoldScope records VictoriaLogs' answers to the reads, per version.
func recordSeriesFoldScope(t *testing.T) {
	t.Helper()
	var rows []oraclerun.ServeRow
	for _, l := range []struct {
		at   time.Duration
		line string
	}{{30*time.Second + 500*time.Millisecond, `{"n":1,"k":"x"}`}, {40*time.Second + 500*time.Millisecond, `{"n":5,"k":"y"}`}, {330*time.Second + 500*time.Millisecond, `{"n":2,"k":"x"}`}} {
		rows = append(rows, oraclerun.ServeRow{TS: seriesFoldScopeStart.Add(l.at).UnixNano(), Stream: [][2]string{{"app", "scope"}},
			Fields: [][2]string{{"_msg", l.line}}})
	}
	out := map[string]map[string]string{}
	for _, prof := range diffProfiles() {
		srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), prof.modfile, []oraclerun.ServeDataSet{{Tenant: 0, Rows: rows}})
		if err != nil {
			t.Fatal(err)
		}
		out[prof.version] = map[string]string{}
		for _, read := range seriesFoldScopeReads {
			start := seriesFoldScopeStart.Add(read.from)
			form := url.Values{"query": {seriesFoldScopeQuery(t, start)}, "start": {start.Format(time.RFC3339Nano)},
				"end": {seriesFoldScopeStart.Add(read.to).Add(time.Nanosecond).Format(time.RFC3339Nano)}, "step": {"60s"},
				"offset": {"-" + strconv.FormatInt(((start.UnixNano()+1)%int64(time.Minute)+int64(time.Minute))%int64(time.Minute), 10) + "ns"}}
			resp, err := http.PostForm(srv.URL+"/select/logsql/stats_query_range", form)
			if err != nil {
				srv.Stop()
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				srv.Stop()
				t.Fatalf("%s %s: %d %s", prof.version, read.name, resp.StatusCode, body)
			}
			out[prof.version][read.name] = string(body)
		}
		srv.Stop()
	}
	b, _ := json.MarshalIndent(out, "", " ")
	if err := os.WriteFile(seriesFoldScopePath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
