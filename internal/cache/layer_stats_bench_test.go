package cache

import (
	"testing"
	"time"
)

// Uses only pre-existing cache APIs so the same benchmark can run against a
// baseline checkout. Known namespaces exercise the actual layer classifier.
func BenchmarkSemanticCacheGet(b *testing.B) {
	for _, item := range []struct{ name, key string }{
		{"loki", "query-steps:v1:scope:query"},
		{"vl", "stats-extent:v1:scope:query"},
		{"unknown", "future-auxiliary:scope:query"},
	} {
		for _, hit := range []bool{false, true} {
			state := "miss"
			if hit {
				state = "hit"
			}
			b.Run(item.name+"/"+state, func(b *testing.B) {
				c := New(time.Hour, 100)
				defer c.Close()
				if hit {
					c.Set(item.key, []byte("complete result"))
				}
				b.ReportAllocs()
				for b.Loop() {
					_, _ = c.Get(item.key)
				}
			})
		}
	}
}
