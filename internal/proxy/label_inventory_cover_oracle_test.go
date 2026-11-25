package proxy

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/querycorpus"
	"github.com/ReliablyObserve/Loki-VL-proxy/test/oraclerun"
)

// The names-only listing that answers the ends of a moved window from their
// hours lists, on VictoriaLogs v1.49.0 and v1.54.0, exactly the label names of
// one full-range stream_field_names call: the windows of the A/B shape
// "refresh labels (label browser)" (24h moved back 3617 s, 6h moved back
// 317 s), a refresh across an hour, and labels that only the first or last
// seconds of a window hold.
//
// conformance: semantics/label-inventory-exact-and-incremental, semantics/label-browser-latency-cold-and-refresh, loki_api_v1_labels
func TestLabelsInventoryCoverOnVictoriaLogs(t *testing.T) {
	if os.Getenv("LVP_RUN_ORACLES") != "1" {
		t.Skip("LVP_RUN_ORACLES=1 runs VictoriaLogs")
	}
	t.Cleanup(querycorpus.PauseRecording())
	now := time.Now()
	end0 := now.Truncate(time.Hour).Add(-2*time.Hour + 26*time.Minute + 50*time.Second)
	var rows oraclerun.ServeDataSet
	add := func(ts time.Time, stream [][2]string) {
		rows.Rows = append(rows.Rows, oraclerun.ServeRow{TS: ts.UnixNano(), Stream: stream, Fields: [][2]string{{"_msg", "line"}}})
	}
	for ts := end0.Add(-34 * time.Hour); ts.Before(end0.Add(time.Hour)); ts = ts.Add(30 * time.Second) {
		pod := "pod-" + strconv.Itoa(int(ts.Unix()/(3*3600)))
		add(ts, [][2]string{{"app", "api"}, {"env", "prod"}, {"pod", pod}})
	}
	// Labels only one end of a moved window holds: the first seconds of the
	// 24h window moved back twice (its hour is not cached), the last seconds of
	// the 24h window moved back once (its hour is).
	left := end0.Add(-2*3617*time.Second - 24*time.Hour + 20*time.Second)
	right := end0.Add(-3617*time.Second - 15*time.Second)
	add(left, [][2]string{{"app", "api"}, {"only_left", "1"}})
	add(right, [][2]string{{"app", "api"}, {"only_right", "1"}})
	type window struct{ start, end time.Time }
	var sequences [][]window
	for _, s := range []struct {
		width, shift time.Duration
	}{{24 * time.Hour, -3617 * time.Second}, {6 * time.Hour, -317 * time.Second}} {
		var seq []window
		for run := 0; run <= 3; run++ {
			end := end0.Add(time.Duration(run) * s.shift)
			seq = append(seq, window{end.Add(-s.width), end})
		}
		sequences = append(sequences, seq)
	}
	refresh := end0.Add(33*time.Minute + 10*time.Second) // 20 s before an hour, then 30 s later
	sequences = append(sequences, []window{{refresh.Add(-24 * time.Hour), refresh}, {refresh.Add(30*time.Second - 24*time.Hour), refresh.Add(30 * time.Second)}})

	for _, v := range []struct{ version, modfile string }{{"v1.49.0", "go.v1.49.0.mod"}, {"v1.54.0", ""}} {
		t.Run(v.version, func(t *testing.T) {
			srv, err := oraclerun.StartVLServe(filepath.Join("..", "..", "test", "oracle", "vl"), v.modfile, []oraclerun.ServeDataSet{rows})
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Stop()
			for i, seq := range sequences {
				bucketed := newInventoryTestProxyAt(t, srv.URL, 4)
				full := newInventoryTestProxyAt(t, srv.URL, -1)
				for j, w := range seq {
					got, want := labelsAnswer(t, bucketed, w.start, w.end), labelsAnswer(t, full, w.start, w.end)
					if got != want {
						t.Fatalf("sequence %d window %d [%s, %s]: bucketed %s, one call %s", i, j, w.start.UTC().Format(time.RFC3339), w.end.UTC().Format(time.RFC3339), got, want)
					}
					t.Logf("sequence %d window %d: %s", i, j, got)
				}
			}
		})
	}
}
