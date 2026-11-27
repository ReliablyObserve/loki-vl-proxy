//go:build e2e

package e2e_fleet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The latency proof's verdicts: a restore phase within the bound passes, one
// beyond it fails, controls that differ beyond it make the run invalid, and
// only samples taken while a stream was served count for a restore phase.
// It needs no stack.
func TestPeerRestoreLoadAnalysis(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	phases := []loadPhase{
		{name: "control 1", from: base, to: base.Add(10 * time.Second)},
		{name: "restore 1", from: base.Add(10 * time.Second), to: base.Add(20 * time.Second)},
		{name: "control 2", from: base.Add(20 * time.Second), to: base.Add(30 * time.Second)},
	}
	write := func(restoreMs, control2Ms float64, idleRestoreMs float64) string {
		var b strings.Builder
		for i := 0; i < 3000; i++ {
			at := base.Add(time.Duration(i) * 10 * time.Millisecond)
			ms := 2.0
			switch {
			case at.After(phases[1].from) && at.Before(phases[1].to):
				ms = restoreMs
				if at.Before(phases[1].from.Add(3 * time.Second)) {
					ms = idleRestoreMs // before the stream started
				}
			case at.After(phases[2].from):
				ms = control2Ms
			}
			for _, class := range []string{"hit", "peer", "neighbour"} {
				fmt.Fprintf(&b, "%d,%s,%d,200\n", at.UnixNano(), class, int64(ms*1e6))
			}
		}
		path := filepath.Join(t.TempDir(), "samples.csv")
		if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	tl := &activityTimeline{active: map[int64]float64{}}
	for s := phases[1].from.Unix() + 2; s < phases[1].to.Unix(); s++ {
		tl.active[s] = 1
	}
	verdicts := func(lines []string) string { return strings.Join(lines, "\n") }
	if got := verdicts(analyseLoad(t, write(2.5, 2, 50), phases, tl)); strings.Contains(got, "FAIL") || strings.Contains(got, "INVALID") || !strings.Contains(got, "pass") {
		t.Fatalf("within the bound (slow samples outside the stream ignored):\n%s", got)
	}
	if got := verdicts(analyseLoad(t, write(5, 2, 2), phases, tl)); !strings.Contains(got, "FAIL") {
		t.Fatalf("beyond the bound:\n%s", got)
	}
	if got := verdicts(analyseLoad(t, write(2, 6, 2), phases, tl)); !strings.Contains(got, "INVALID") {
		t.Fatalf("controls that differ:\n%s", got)
	}
}
