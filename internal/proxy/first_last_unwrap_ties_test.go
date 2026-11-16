package proxy

import (
	"fmt"
	"math"
	"testing"

	"github.com/ReliablyObserve/Loki-VL-proxy/internal/testutil/vlemu"
)

// Samples of one series that share a timestamp, made on purpose: the e2e log generator gives every line its own
// timestamp, so no live run produces them. Loki keeps equal timestamps in the order it received them (its
// LastOverTime keeps the last sample, FirstOverTime the first); VictoriaLogs has no order for them, so row_min and
// row_max keep the first row they meet (semantics/unwrap-first-last-under-aggregation, documented, not worked
// around). The proxy must still answer 200 with one series from stats alone, with a value one of the tied samples
// holds, and it must not change a value that no tie touches.
//
// conformance: semantics/unwrap-first-last-under-aggregation
func TestFirstLastOverTimeTiedSamplesAnswerOneOfTheTiedValues(t *testing.T) {
	end := int64(1_800_000_000) * 1e9
	stream := `{app="uw",pod="a"}`
	for _, tc := range []struct {
		name      string
		fn        string
		rows      []vlemu.Row
		wantExact *float64 // set when no tie touches the sample the function reads
		oneOf     []float64
	}{
		{"last over three samples tied at the newest time", "last_over_time", []vlemu.Row{
			{TS: end - 200e9, Stream: stream, Value: 5},
			{TS: end - 10e9, Stream: stream, Value: 1},
			{TS: end - 10e9, Stream: stream, Value: 2},
			{TS: end - 10e9, Stream: stream, Value: 3},
		}, nil, []float64{1, 2, 3}},
		{"first over three samples tied at the oldest time", "first_over_time", []vlemu.Row{
			{TS: end - 200e9, Stream: stream, Value: 1},
			{TS: end - 200e9, Stream: stream, Value: 2},
			{TS: end - 200e9, Stream: stream, Value: 3},
			{TS: end - 10e9, Stream: stream, Value: 5},
		}, nil, []float64{1, 2, 3}},
		{"first with the tie at the newest time", "first_over_time", []vlemu.Row{
			{TS: end - 200e9, Stream: stream, Value: 5},
			{TS: end - 10e9, Stream: stream, Value: 1},
			{TS: end - 10e9, Stream: stream, Value: 2},
		}, ptr(5), nil},
		{"last with the tie at the oldest time", "last_over_time", []vlemu.Row{
			{TS: end - 200e9, Stream: stream, Value: 1},
			{TS: end - 200e9, Stream: stream, Value: 2},
			{TS: end - 10e9, Stream: stream, Value: 5},
		}, ptr(5), nil},
		{"every sample at one time", "last_over_time", []vlemu.Row{
			{TS: end - 10e9, Stream: stream, Value: 4},
			{TS: end - 10e9, Stream: stream, Value: 4},
		}, ptr(4), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, parser := range []string{"", "logfmt", "json"} {
				for _, agg := range []string{"sum", "max"} {
					rows := append([]vlemu.Row(nil), tc.rows...)
					for i := range rows {
						// The proxy reads a parsed series as the stream and the line
						// (semantics/unwrap-per-series-identity-raw-rows), so the tied samples carry the same line text
						// and stay one series, as Loki's labels (without the unwrapped one) make them.
						rows[i].Msg = "level=info"
					}
					query := fmt.Sprintf(`%s(%s({app="uw"}%s | unwrap amount [5m]))`, agg, tc.fn, parserPipe(parser))
					emu := &vlemu.Emulator{T: t, Rows: rows, Field: "amount"}
					run := runFirstLast(t, emu, query, true, end, end, 0)
					noFirstLastStatsFunction(t, query, emu)
					if len(run.series) != 1 || len(run.points[0]) != 1 {
						t.Fatalf("%s: %d series, want one with a point", query, len(run.series))
					}
					var got float64
					for _, v := range run.points[0] {
						got = v
					}
					switch {
					case tc.wantExact != nil && math.Abs(got-*tc.wantExact) > 1e-9:
						t.Fatalf("%s: got %v, a tie elsewhere must not move it from %v", query, got, *tc.wantExact)
					case tc.oneOf != nil && !containsFloat(tc.oneOf, got):
						t.Fatalf("%s: got %v, want one of the tied values %v", query, got, tc.oneOf)
					}
				}
			}
		})
	}
}

func ptr(v float64) *float64 { return &v }

func containsFloat(set []float64, v float64) bool {
	for _, s := range set {
		if math.Abs(s-v) < 1e-9 {
			return true
		}
	}
	return false
}
