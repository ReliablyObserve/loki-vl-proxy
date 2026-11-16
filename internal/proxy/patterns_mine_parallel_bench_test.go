package proxy

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// patternColdBenchLines is the size of the cold Drilldown patterns read of an environment over an hour on the
// test stack (DP2, {env="production"}: about 148,000 lines of about 274 bytes, many streams of a few lines each).
const patternColdBenchLines = 148_000

var (
	patternColdBenchOnce   sync.Once
	patternColdBenchBodies map[string][]byte
)

// patternColdBenchRows is the answer VictoriaLogs gives the exact tier's lines query for DP2's hour, made once so
// the benchmarks time decoding and mining alone: "many" is the stack's shape (streams of 7 or 8 lines), "skewed"
// the same lines in 4 streams, where workers have the fewest streams to share.
func patternColdBenchRows(shape string) []byte {
	patternColdBenchOnce.Do(func() {
		patternColdBenchBodies = map[string][]byte{}
		var many bytes.Buffer
		rows := &exactBenchRows{}
		chunk := make([]byte, 32<<10)
		lines := 0
		for lines < patternColdBenchLines {
			n, err := rows.Read(chunk)
			for _, c := range chunk[:n] {
				if lines == patternColdBenchLines {
					break
				}
				many.WriteByte(c)
				if c == '\n' {
					lines++
				}
			}
			if err == io.EOF {
				break
			}
		}
		patternColdBenchBodies["many"] = many.Bytes()
		var skewed bytes.Buffer
		at := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)
		for i := 0; i < patternColdBenchLines; i++ {
			stream := i * 4 / patternColdBenchLines // sorted by stream: 4 runs
			fmt.Fprintf(&skewed, `{"_time":%q,"_msg":%q,"_stream_id":"%032x","level":"info"}`+"\n", at.Add(time.Duration(i)*20*time.Millisecond).Format(time.RFC3339Nano), exactBenchLine(i/7, i%7), stream)
		}
		patternColdBenchBodies["skewed"] = skewed.Bytes()
	})
	return patternColdBenchBodies[shape]
}

// BenchmarkPatternColdMine148k decodes and mines DP2's hour of lines as the exact tier does (patternsFromLines),
// from a body already read: the proxy's share of a cold Drilldown patterns request, serially (workers=1) and on
// workers. cpu-ns/op is the process's CPU time per read (all goroutines), next to the wall time of ns/op.
func BenchmarkPatternColdMine148k(b *testing.B) {
	for _, shape := range []string{"many", "skewed"} {
		body := patternColdBenchRows(shape)
		for _, workers := range []int{1, 2, 4} {
			b.Run(fmt.Sprintf("shape=%s/workers=%d", shape, workers), func(b *testing.B) {
				levels := defaultLogRowLevels()
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				cpu := processCPUTime()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					miner := newPatternMiner()
					miner.fillStep = 14
					var pm *patternParallelMiner
					if workers > 1 {
						pm = newPatternParallelMiner(miner, workers)
					}
					err := scanPatternLineRows(bytes.NewReader(body), patternColdBenchLines, levels, workers, patternLineConsumer(miner, patternLineSink(miner, pm)))
					if pm != nil {
						pm.finish()
					}
					if err != nil {
						b.Fatal(err)
					}
					_ = patternResultEntriesFromMaps(buildPatternResponse(miner, maxPatternResponseLimit))
				}
				b.StopTimer()
				if used := processCPUTime() - cpu; used > 0 {
					b.ReportMetric(float64(used)/float64(b.N), "cpu-ns/op")
				}
			})
		}
	}
}
