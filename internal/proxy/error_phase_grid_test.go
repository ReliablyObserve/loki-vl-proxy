package proxy

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// errorPhaseGridRow is one query of the recorded error-phase grid (internal/logql/testdata/error-order-loki.jsonl.gz)
// and Loki's answer to it.
type errorPhaseGridRow struct {
	Carrier string `json:"carrier"`
	Context string `json:"context"`
	Q       string `json:"q"`
	Accept  bool   `json:"accept"`
	Error   string `json:"error"`
}

// knownErrorPhaseGridDifferences is the number of grid queries the proxy's query validation still answers otherwise
// than Loki. It may only fall.
const knownErrorPhaseGridDifferences = 58

func readErrorPhaseGrid(t testing.TB) []errorPhaseGridRow {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "logql", "testdata", "error-order-loki.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var rows []errorPhaseGridRow
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r errorPhaseGridRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if len(rows) < 1000 {
		t.Fatalf("%d grid rows", len(rows))
	}
	return rows
}

// The proxy's whole query validation (the parser, its semantic pass and the query plan's tree checks) answers the
// recorded grid of Loki's error phases as Loki does: a parser action (a regex or parser stage that does not build)
// ahead of a later syntax or lexer error, the tree checks after every parse error and in the order Loki's walk
// meets them, the pipeline build last, and the parameter of a vector operation read as a number token. The queries
// it still answers otherwise are counted; the count may only fall.
//
// conformance: semantics/loki-error-phases
func TestErrorPhaseGridMatchesLoki(t *testing.T) {
	byCarrier := map[string]int{}
	differ := 0
	for _, r := range readErrorPhaseGrid(t) {
		want := ""
		if !r.Accept {
			want = r.Error
		}
		_, got, _ := validateQueryTree(r.Q)
		if got != want {
			differ++
			byCarrier[r.Carrier]++
			if testing.Verbose() {
				t.Logf("%s / %s: %s\n  loki:  %s\n  proxy: %s", r.Carrier, r.Context, r.Q, want, got)
			}
		}
	}
	names := make([]string, 0, len(byCarrier))
	for k := range byCarrier {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		t.Logf("%-30s %d", k, byCarrier[k])
	}
	if differ > knownErrorPhaseGridDifferences {
		t.Fatalf("%d grid queries answer otherwise than Loki, %d known", differ, knownErrorPhaseGridDifferences)
	}
	if differ < knownErrorPhaseGridDifferences {
		t.Errorf("%d grid queries answer otherwise than Loki, fewer than the %d known: lower knownErrorPhaseGridDifferences", differ, knownErrorPhaseGridDifferences)
	}
}
