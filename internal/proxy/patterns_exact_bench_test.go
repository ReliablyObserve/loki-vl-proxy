package proxy

import (
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	fj "github.com/valyala/fastjson"
)

const (
	exactBenchStreams = 50_000
	exactBenchLines   = 355_000
)

// exactBenchRows makes the rows of the exact tier's lines query as it is read, sorted by stream and time: 50,000
// streams of 7 lines (the first 5,000 of 8), each stream one of four formats of lines of about 340 bytes: an
// access log, logfmt, a JSON object with a message, and a Java-style log line.
type exactBenchRows struct {
	stream, line int
	made         int
	pending      []byte
}

func exactBenchLine(stream, line int) string {
	n := stream*8 + line
	at := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC).Add(time.Duration(line)*13*time.Second + time.Duration(stream%977)*time.Millisecond)
	switch stream % 4 {
	case 0:
		return fmt.Sprintf(`10.%d.%d.%d - user%d [06/Nov/2026:18:%02d:%02d +0000] "GET /api/v2/orders/%d/items?include=price,stock&page=%d HTTP/1.1" 200 %d "https://shop.example.com/cart/%d" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36" rt=0.%03d uct="0.001" uht="0.0%02d" urt="0.%03d" req_id=%016x`,
			n%200, n%97, n%13, n%31, line, n%60, n%90000, n%7, 500+n%9000, n%5000, n%1000, n%90, n%1000, n*2654435761)
	case 1:
		return fmt.Sprintf(`ts=%s level=info caller=orders/service.go:%d msg="order processed" order_id=%d customer=cust-%d region=eu-west-%d amount=%d.%02d currency=EUR items=%d duration_ms=%d trace_id=%032x span_id=%016x host=pod-%d.orders.svc.cluster.local build=v2.%d.%d`,
			at.Format(time.RFC3339Nano), 100+n%400, n%900000, n%20000, n%3, n%900, n%100, n%12, n%700, n*40503, n*2654435761, n%50, n%9, n%30)
	case 2:
		return fmt.Sprintf(`{"level":"info","ts":%q,"logger":"payments.gateway","msg":"charge authorised for customer %d on card ending %04d via provider adyen in %d ms","customer":%d,"attempt":%d,"trace_id":"%032x","pod":"payments-%d-%x","region":"eu-west-%d","build":"2.%d.%d","correlation_id":"%016x"}`,
			at.Format(time.RFC3339Nano), n%20000, n%10000, n%900, n%20000, n%3, n*40503, n%50, n%4096, n%3, n%9, n%30, n*2654435761)
	default:
		return fmt.Sprintf(`%s INFO  [http-nio-8080-exec-%d] com.example.inventory.StockService - Reserved %d units of SKU-%d for order %d in warehouse WH-%d (remaining=%d, version=%d) correlationId=%016x user=u%d session=%032x`,
			at.Format("2006-01-02 15:04:05,000"), n%200, n%12, n%9000, n%900000, n%8, n%5000, n%40, n*2654435761, n%20000, n*40503)
	}
}

func (r *exactBenchRows) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.made == exactBenchLines {
			return 0, io.EOF
		}
		lines := 7
		if r.stream < exactBenchLines-7*exactBenchStreams {
			lines = 8
		}
		if r.line == lines {
			r.stream, r.line = r.stream+1, 0
		}
		at := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC).Add(time.Duration(r.line)*13*time.Second + time.Duration(r.stream%977)*time.Millisecond)
		r.pending = []byte(fmt.Sprintf(`{"_time":%q,"_msg":%q,"_stream_id":"%032x","level":"info"}`+"\n", at.Format(time.RFC3339Nano), exactBenchLine(r.stream, r.line), r.stream))
		r.line++
		r.made++
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

// BenchmarkPatternExactRowsOnly is the cost of making the rows of BenchmarkPatternExactMine355k, which that one
// includes: subtract it to get the cost of decoding and mining.
func BenchmarkPatternExactRowsOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := io.Copy(io.Discard, &exactBenchRows{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPatternExactMine355k decodes and mines 355,000 rows of about 340-byte lines in 50,000 streams from a
// reader, as the exact tier does a hot hour's lines.
func BenchmarkPatternExactMine355k(b *testing.B) {
	b.ReportAllocs()
	levels := defaultLogRowLevels()
	for i := 0; i < b.N; i++ {
		miner := newPatternMiner()
		miner.fillStep = 14
		read := newPatternLineReader(miner, levels)
		if err := scanPatternRows(&exactBenchRows{}, exactBenchLines, func(v *fj.Value) bool { return read(v) }); err != nil {
			b.Fatal(err)
		}
		entries := patternResultEntriesFromMaps(buildPatternResponse(miner, maxPatternResponseLimit))
		if i == 0 {
			// What the miner holds once the rows are gone: the clusters and their trees.
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(len(miner.AllClusters())), "clusters")
			b.ReportMetric(float64(len(entries)), "patterns")
			b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/(1<<20), "live-MB")
			runtime.KeepAlive(miner)
		}
	}
}
