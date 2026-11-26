package cache

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Sizing signal costs: request-level lookups and stores with the signals off
// and on.

func sizingBenchCache(b *testing.B, on bool) (*Cache, []string) {
	b.Helper()
	c := NewWithMaxBytes(time.Hour, 8192, 1<<30)
	if on {
		c.EnableSizingSignals()
	}
	keys := retentionBenchKeys(2048, `query_range:{service_name="checkout"} |= "error":`)
	for _, k := range keys {
		c.SetWithTTL(k, []byte(`{"status":"success"}`), time.Hour)
	}
	return c, keys
}

func BenchmarkSizingGetHit(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run("sizing="+strconv.FormatBool(on), func(b *testing.B) {
			c, keys := sizingBenchCache(b, on)
			defer c.Close()
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c.GetWithTier(keys[i%len(keys)])
				i++
			}
		})
	}
}

func BenchmarkSizingGetHitParallel(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run("sizing="+strconv.FormatBool(on), func(b *testing.B) {
			c, keys := sizingBenchCache(b, on)
			defer c.Close()
			var next atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(next.Add(1)) * 7919
				for pb.Next() {
					c.GetWithTier(keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

// A miss and the store that fills it, evicting.
func BenchmarkSizingMissFill(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run("sizing="+strconv.FormatBool(on), func(b *testing.B) {
			c := NewWithMaxBytes(time.Hour, 1024, 1<<30)
			defer c.Close()
			if on {
				c.EnableSizingSignals()
			}
			keys := retentionBenchKeys(4096, "query_range:fill-")
			value := []byte(`{"status":"success"}`)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				k := keys[i%len(keys)]
				if _, _, ok := c.GetWithTier(k); !ok {
					c.SetWithTTL(k, value, time.Hour)
				}
				i++
			}
		})
	}
}
