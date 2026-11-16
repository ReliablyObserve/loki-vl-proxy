package proxy

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// buildPhaseReplayRows are the rows of internal/lokierr/testdata/loki_build_phase.jsonl whose answer does not
// depend on the window or the data: the instant log query (b0028, b0045), the syntax error, the parser action and
// the tree checks over an old window (b0039–b0043), the request errors beside a build error (b0046, b0047), and the
// selector and label filter regex errors on /labels, /series and query_range (b0059–b0064).
var buildPhaseReplayRows = map[string]bool{
	"b0028": true, "b0039": true, "b0040": true, "b0041": true, "b0042": true, "b0043": true, "b0045": true,
	"b0046": true, "b0047": true, "b0059": true, "b0060": true, "b0061": true, "b0062": true, "b0063": true,
	"b0064": true,
}

// The proxy answers the window-independent rows recorded from a Loki that serves data with Loki's status,
// Content-Type and body: an instant log query with a stage that does not build answers the query type error, a
// request error answers ahead of the build error, the parser's actions and the tree checks answer over a window
// older than Loki's retention, and a regex error is Loki's parse error on the selector routes.
//
// conformance: semantics/loki-error-phase-order, semantics/loki-build-phase-window
func TestBuildPhaseRowsReplay(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "lokierr", "testdata", "loki_build_phase.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	addr := newLokiErrorReplayProxy(t)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<22)
	seen := 0
	for first := true; sc.Scan(); first = false {
		if first {
			continue
		}
		var row lokiErrorAnswer
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if !buildPhaseReplayRows[row.ID] {
			continue
		}
		seen++
		if got := row.replay(t, addr); got != row.Loki {
			t.Errorf("%s %s %s:\n  loki  %+v\n  proxy %+v", row.ID, row.Route, row.Target, row.Loki, got)
		}
	}
	if seen != len(buildPhaseReplayRows) {
		t.Fatalf("%d of %d rows found", seen, len(buildPhaseReplayRows))
	}
}
