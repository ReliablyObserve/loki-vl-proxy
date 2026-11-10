package querycorpus_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/logsql"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/lower"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/queryplan/physical"
	"github.com/ReliablyObserve/Loki-VL-proxy/internal/translator"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// TestRangeAggVLOracle has VictoriaLogs' own parser read the LogsQL the lowering writes for the range
// aggregation grid, on every VictoriaLogs version of the matrix and under the profiles that change the text
// (label names, stream labels): it must accept each query and read it as it reads the string translator's, and
// read it as our parser does. It runs when LVP_RUN_ORACLES=1.
// conformance: architecture-syntax-package-layering
func TestRangeAggVLOracle(t *testing.T) {
	checkLoweredGridWithVL(t, "range", rangeAggGrid())
}

// TestVectorAggVLOracle is TestRangeAggVLOracle for the vector aggregations of vectorAggGrid: VictoriaLogs' parser
// reads the query the lowering writes (the clean query, without the fold the proxy applies) as it reads the string
// translator's.
// conformance: architecture-syntax-package-layering
func TestVectorAggVLOracle(t *testing.T) {
	checkLoweredGridWithVL(t, "vector", vectorAggGrid())
}

// checkLoweredGridWithVL has VictoriaLogs' parser read, on every version of the matrix, the query the lowering
// writes for each row of grid next to the string translator's. It runs when LVP_RUN_ORACLES=1.
func checkLoweredGridWithVL(t *testing.T, name string, grid []string) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 is not set")
	}
	t.Cleanup(querycorpus.PauseRecording())
	versions := goldenVersions(t)
	work := t.TempDir()
	for _, v := range versions {
		file := oracleModfile(v, versions[0])
		modfile := ""
		if file != "go.mod" {
			modfile = file
		}
		bin, err := oraclerun.Build(oracleDir, modfile, work, "vl-"+v)
		if err != nil {
			t.Fatal(err)
		}
		inputs := filepath.Join(work, name+"-inputs-"+v+".jsonl")
		f, err := os.Create(inputs)
		if err != nil {
			t.Fatal(err)
		}
		w := bufio.NewWriter(f)
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		n := 0
		seen := map[string]bool{}
		texts := map[string]string{}
		for _, prof := range planProfiles() {
			// The oracle reads each version with that version's capabilities: the profiles of the other version
			// are left out.
			if prof.keepLine || !strings.HasPrefix(prof.name, v[:5]) {
				continue
			}
			for _, q := range grid {
				// A regexp that does not compile is rejected by Loki and by VictoriaLogs alike.
				if strings.Contains(q, `~"("`) {
					continue
				}
				expr, err := logql.Parse(logql.CanonicalizeLogRanges(q))
				if err != nil {
					continue
				}
				plan, err := queryplan.Build(expr, queryplan.Inputs{})
				if err != nil {
					continue
				}
				queryplan.Resolve(plan)
				op, err := lower.Plan(plan, prof.lowerProfile())
				if err != nil {
					continue
				}
				core, _ := physical.Split(op)
				lowered, ok := core.(physical.VLQuery)
				legacy, ok2 := prof.translate(q)
				if !ok || !ok2 {
					continue
				}
				// The fold marker of a without clause is the proxy's, not VictoriaLogs'.
				legacy, _ = translator.SplitPost(legacy)
				// The oracle's check reads two texts: the translator's and the lowering's. Profiles that write the
				// same pair once.
				legacy = withLeadingStar(legacy)
				if seen[legacy+"\x00"+lowered.Text] {
					continue
				}
				seen[legacy+"\x00"+lowered.Text] = true
				texts[caseID(legacy+lowered.Text)] = prof.name + ": " + q
				in := oracleInput{ID: caseID(legacy + lowered.Text), Kind: "typed-selector", Query: legacy, Printed: lowered.Text}
				in.Ours = true
				if _, err := logsql.Parse(lowered.Text); err != nil {
					in.Ours = false
				}
				if err := enc.Encode(in); err != nil {
					t.Fatal(err)
				}
				n++
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatalf("%s: no query lowered", v)
		}
		out := filepath.Join(work, name+"-verdicts-"+v+".jsonl")
		if err := oraclerun.Run(bin, "check", "-in", inputs, "-out", out); err != nil {
			t.Fatal(err)
		}
		bad := 0
		sc := bufio.NewScanner(mustOpen(t, out))
		sc.Buffer(make([]byte, 1<<20), 1<<26)
		for sc.Scan() {
			var x oracleVerdict
			if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
				t.Fatal(err)
			}
			if !x.Accept || x.SameMeaning == nil || !*x.SameMeaning {
				if bad++; bad <= 10 {
					t.Errorf("%s: %s (%s): VictoriaLogs accepts %v, same meaning %v", v, x.ID, texts[x.ID], x.Accept, x.SameMeaning != nil && *x.SameMeaning)
				}
			}
		}
		t.Logf("%s: %d lowered queries read by VictoriaLogs' parser, %d not alike", v, n, bad)
		if bad > 0 {
			t.Errorf("%s: %d queries are not read alike", v, bad)
		}
	}
}

func withLeadingStar(s string) string {
	if strings.HasPrefix(s, "|") {
		return "* " + s
	}
	return s
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}
